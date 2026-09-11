"""Transactional database services for frozen seeds and append-only provenance."""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
import hmac
from typing import Any
import unicodedata
import uuid

from sqlalchemy import func, select, text
from sqlalchemy.orm import Session, sessionmaker

from musparql.reviewer_provenance import (
    validate_kg_domain_assessments,
    validate_resource_familiarity_assessments,
    validate_reviewer_domain_expertise_assertions,
    validate_reviewer_id,
)
from musparql.source_catalog import validate_kg_seed_snapshots

from .models import (
    ExpertiseDomain,
    KgSeedFamiliarityScope,
    KgSeedReviewDomain,
    KgSeedSnapshot,
    ReviewerDomainExpertise,
    ReviewerKgDomainAssessment,
    ReviewerResourceFamiliarityAssessment,
    Reviewer,
    ReviewGroup,
    ReviewGroupMember,
    WorkshopEntryCode,
    WorkshopEntryRedemption,
    WorkshopRound,
)
from .repositories import AssignmentRepository, ProvenanceRepository, SeedRepository


def normalize_email(value: str) -> str:
    """Apply conservative login normalization without provider-specific rewriting."""
    normalized = unicodedata.normalize("NFC", value).strip()
    if len(normalized) > 254 or normalized.count("@") != 1:
        raise ValueError("Email address is invalid")
    local, domain = normalized.rsplit("@", 1)
    if not local or len(local) > 64 or not domain or any(char.isspace() for char in normalized):
        raise ValueError("Email address is invalid")
    try:
        ascii_domain = domain.encode("idna").decode("ascii").lower()
    except UnicodeError as exc:
        raise ValueError("Email address is invalid") from exc
    if "." not in ascii_domain or ascii_domain.startswith(".") or ascii_domain.endswith("."):
        raise ValueError("Email address is invalid")
    return f"{local.casefold()}@{ascii_domain}"


class WorkshopAdmissionError(ValueError):
    """A shared-code admission could not be completed safely."""


class WorkshopAdmissionService:
    """Serialize shared-code admission against the capacity of the whole round."""

    def __init__(self, sessions: sessionmaker[Session]) -> None:
        self.sessions = sessions

    def redeem(
        self,
        *,
        entry_code_id: str,
        presented_code_digest: str,
        reviewer: Reviewer,
        redeemed_at: str,
    ) -> str:
        if reviewer.registration_method != "workshop_code":
            raise WorkshopAdmissionError(
                "Shared-code registrations must identify their registration method"
            )
        if reviewer.email_verified_at is not None:
            raise WorkshopAdmissionError(
                "A shared-code registration cannot label its email as verified"
            )
        if (
            reviewer.consent_statement_version is not None
            or reviewer.consented_at is not None
        ):
            raise WorkshopAdmissionError(
                "Shared-code admission must route to consent before recording consent"
            )

        session = self.sessions()
        try:
            # SQLite has no row-level SELECT FOR UPDATE. BEGIN IMMEDIATE makes the
            # round-wide count, reviewer insert, redemption, and counter update a
            # single serialized admission decision, including after code reissue.
            session.execute(text("BEGIN IMMEDIATE"))
            redemption_id = self.redeem_in_session(
                session,
                entry_code_id=entry_code_id,
                presented_code_digest=presented_code_digest,
                reviewer=reviewer,
                redeemed_at=redeemed_at,
            )
            session.commit()
            return redemption_id
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    @staticmethod
    def redeem_in_session(
        session: Session,
        *,
        entry_code_id: str,
        presented_code_digest: str,
        reviewer: Reviewer,
        redeemed_at: str,
    ) -> str:
        """Apply the admission invariant inside a caller-owned write transaction."""

        if reviewer.registration_method != "workshop_code":
            raise WorkshopAdmissionError(
                "Shared-code registrations must identify their registration method"
            )
        if reviewer.email_verified_at is not None:
            raise WorkshopAdmissionError(
                "A shared-code registration cannot label its email as verified"
            )
        if reviewer.consent_statement_version is not None or reviewer.consented_at is not None:
            raise WorkshopAdmissionError(
                "Shared-code admission must route to consent before recording consent"
            )
        entry_code = session.get(WorkshopEntryCode, entry_code_id)
        if entry_code is None or not hmac.compare_digest(
            entry_code.code_digest, presented_code_digest
        ):
            raise WorkshopAdmissionError("Workshop entry code is invalid")
        workshop_round = session.get(WorkshopRound, entry_code.workshop_round_id)
        if (
            workshop_round is None
            or workshop_round.status != "open"
            or redeemed_at < workshop_round.opens_at
            or redeemed_at >= workshop_round.closes_at
            or entry_code.revoked_at is not None
            or redeemed_at >= entry_code.expires_at
        ):
            raise WorkshopAdmissionError("Workshop entry code is not active")
        if entry_code.redemption_count >= entry_code.max_redemptions:
            raise WorkshopAdmissionError("Workshop entry code is fully redeemed")
        round_redemptions = int(
            session.scalar(
                select(func.count())
                .select_from(WorkshopEntryRedemption)
                .join(
                    WorkshopEntryCode,
                    WorkshopEntryCode.id == WorkshopEntryRedemption.entry_code_id,
                )
                .where(WorkshopEntryCode.workshop_round_id == workshop_round.id)
            )
            or 0
        )
        if round_redemptions >= workshop_round.max_participants:
            raise WorkshopAdmissionError("Workshop round is at participant capacity")
        if session.get(Reviewer, reviewer.id) is not None:
            raise WorkshopAdmissionError("Reviewer already exists")

        redemption_id = "redemption-" + uuid.uuid4().hex
        session.add(reviewer)
        session.flush()
        session.add(
            WorkshopEntryRedemption(
                id=redemption_id,
                entry_code_id=entry_code.id,
                reviewer_id=reviewer.id,
                redeemed_at=redeemed_at,
            )
        )
        entry_code.redemption_count += 1
        return redemption_id


class SeedSnapshotService:
    def __init__(self, sessions: sessionmaker[Session]) -> None:
        self.sessions = sessions

    def import_archive(self, payload: Mapping[str, Any]) -> int:
        with self.sessions.begin() as session:
            return self.import_archive_in_session(session, payload)

    @staticmethod
    def import_archive_in_session(session: Session, payload: Mapping[str, Any]) -> int:
        """Import an archive within a transaction owned by the caller."""
        snapshots = _ordered_seed_snapshots(validate_kg_seed_snapshots(payload))
        inserted = 0
        repository = SeedRepository(session)
        for record in snapshots:
            kg_id = str(record["kg_id"])
            seed_version = str(record["seed_version"])
            existing = repository.get(kg_id, seed_version)
            if existing is not None:
                if existing.seed_digest != record["seed_digest"]:
                    raise ValueError(f"Seed version was reused: {kg_id}/{seed_version}")
                continue
            seed = record["seed"]
            repository.add_snapshot(
                KgSeedSnapshot(
                    kg_id=kg_id,
                    seed_version=seed_version,
                    seed_digest=record["seed_digest"],
                    previous_seed_digest=record["previous_seed_digest"],
                    seed_json=dict(seed),
                ),
                (
                    KgSeedReviewDomain(
                        kg_id=kg_id,
                        seed_version=seed_version,
                        domain_id=domain["domain_id"],
                        label=domain["label"],
                        description=domain["description"],
                    )
                    for domain in seed["review_domains"]
                ),
                (
                    KgSeedFamiliarityScope(
                        kg_id=kg_id,
                        seed_version=seed_version,
                        scope_id=scope["scope_id"],
                        label=scope["label"],
                        description=scope.get("description"),
                    )
                    for scope in seed["familiarity_scopes"]
                ),
            )
            inserted += 1
        return inserted


def _ordered_seed_snapshots(snapshots: Sequence[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    """Return validated snapshot chains predecessor-first, independent of YAML order."""
    by_previous: dict[tuple[str, str | None], Mapping[str, Any]] = {
        (str(item["kg_id"]), item.get("previous_seed_digest")): item for item in snapshots
    }
    ordered: list[Mapping[str, Any]] = []
    kg_ids = sorted({str(item["kg_id"]) for item in snapshots})
    for kg_id in kg_ids:
        current = by_previous[(kg_id, None)]
        while True:
            ordered.append(current)
            successor = by_previous.get((kg_id, str(current["seed_digest"])))
            if successor is None:
                break
            current = successor
    return ordered


def _assert_head(records: Sequence[Any], predecessor_id: str | None, subject: str) -> None:
    if not records:
        if predecessor_id is not None:
            raise ValueError(f"First {subject} must not identify a predecessor")
        return
    superseded = {row.supersedes_id if hasattr(row, "supersedes_id") else row.previous_assessment_id for row in records}
    head = next(row for row in records if row.id not in superseded)
    if predecessor_id != head.id:
        raise ValueError(f"New {subject} must supersede the current head")


class ProvenanceService:
    def __init__(
        self,
        sessions: sessionmaker[Session],
        current_consent_version: str | None = None,
        current_notice_version: str | None = None,
    ) -> None:
        self.sessions = sessions
        self.current_consent_version = current_consent_version
        self.current_notice_version = current_notice_version

    def append_domain_expertise(self, record: Mapping[str, Any]) -> None:
        reviewer_id = validate_reviewer_id(record.get("reviewer_id"))
        with self.sessions.begin() as session:
            repository = ProvenanceRepository(session)
            domain = repository.expertise_domain(str(record["domain_id"]))
            domain_values = {
                "entered_label": record["entered_label"],
                "normalized_label": record["normalized_label"],
                "vocabulary_name": record["vocabulary_name"],
                "vocabulary_concept_uri": record["vocabulary_concept_uri"],
                "vocabulary_version": record["vocabulary_version"],
            }
            if domain is None:
                domain = ExpertiseDomain(
                    id=record["domain_id"], created_by="reviewer", **domain_values
                )
                repository.add(domain)
            elif any(getattr(domain, key) != value for key, value in domain_values.items()):
                raise ValueError("Stable expertise domain metadata cannot change between assertions")
            history = repository.expertise_assertions(reviewer_id, str(record["domain_id"]))
            _assert_head(history, record.get("supersedes_id"), "domain expertise assertion")
            history_records = [
                {
                    "schema": "musparql.reviewer-domain-expertise-assertion.v1",
                    "id": item.id,
                    "reviewer_id": item.reviewer_id,
                    "domain_id": item.domain_id,
                    **domain_values,
                    "expertise_level": item.expertise_level,
                    "asserted_at": item.asserted_at,
                    "supersedes_id": item.supersedes_id,
                }
                for item in history
            ]
            validate_reviewer_domain_expertise_assertions([*history_records, record])
            repository.add(
                ReviewerDomainExpertise(
                    id=record["id"], reviewer_id=reviewer_id, domain_id=record["domain_id"],
                    expertise_level=record["expertise_level"], asserted_at=record["asserted_at"],
                    supersedes_id=record.get("supersedes_id"),
                )
            )
            session.flush()

    def append_domain_assessment(self, record: Mapping[str, Any]) -> None:
        if record.get("context") == "pre_review":
            raise ValueError("Pre-review assessments must use the atomic batch service")
        with self.sessions.begin() as session:
            self._append_assessment(session, record, domain=True)

    def append_familiarity_assessment(self, record: Mapping[str, Any]) -> None:
        if record.get("context") == "pre_review":
            raise ValueError("Pre-review assessments must use the atomic batch service")
        with self.sessions.begin() as session:
            self._append_assessment(session, record, domain=False)

    def append_pre_review_assessments(
        self,
        domain_records: Sequence[Mapping[str, Any]],
        familiarity_records: Sequence[Mapping[str, Any]],
        *,
        activate_assignment: bool = False,
    ) -> None:
        """Atomically record the complete frozen prompt set for one assignment."""
        records = [*domain_records, *familiarity_records]
        if not records:
            raise ValueError("Pre-review assessment set must not be empty")
        assignment_ids = {record.get("assignment_id") for record in records}
        if len(assignment_ids) != 1 or None in assignment_ids:
            raise ValueError("Pre-review assessments must identify one assignment")
        if any(record.get("context") != "pre_review" for record in records):
            raise ValueError("Pre-review assessment batch requires pre_review context")
        assignment_id = str(next(iter(assignment_ids)))

        with self.sessions() as session:
            # SQLite has no row locks. Acquire the write reservation before any
            # reads so simultaneous group-member assessments cannot fail while
            # upgrading a stale WAL snapshot to a writer.
            session.execute(text("BEGIN IMMEDIATE"))
            assignments = AssignmentRepository(session)
            assignment = assignments.get(assignment_id)
            if assignment is None:
                raise ValueError(f"Unknown review assignment: {assignment_id}")
            reviewer_ids = {validate_reviewer_id(record.get("reviewer_id")) for record in records}
            if assignment.reviewer_id is not None:
                if reviewer_ids != {assignment.reviewer_id}:
                    raise ValueError(
                        "Pre-review assessments must belong to the assigned reviewer"
                    )
            elif assignment.review_group_id is not None:
                now = datetime.now(timezone.utc).isoformat(
                    timespec="microseconds"
                ).replace("+00:00", "Z")
                group = session.get(ReviewGroup, assignment.review_group_id)
                workshop_round = (
                    session.get(WorkshopRound, group.workshop_round_id)
                    if group is not None
                    else None
                )
                reviewers = list(
                    session.scalars(
                        select(Reviewer).where(Reviewer.id.in_(reviewer_ids))
                    )
                )
                open_round = bool(
                    workshop_round is not None
                    and workshop_round.status == "open"
                    and workshop_round.opens_at <= now
                    and workshop_round.closes_at > now
                )
                terminal_assessment = bool(
                    assignment.participant_status
                    in {"completed", "partial", "abandoned"}
                    and reviewer_ids.issubset(
                        set(assignment.closed_contributor_ids or ())
                    )
                )
                if self.current_consent_version and (
                    len(reviewers) != len(reviewer_ids)
                    or any(
                        reviewer.status != "active"
                        or reviewer.consent_statement_version
                        != self.current_consent_version
                        or not reviewer.consented_at
                        or reviewer.privacy_notice_version
                        != self.current_notice_version
                        or not reviewer.privacy_notice_acknowledged_at
                        for reviewer in reviewers
                    )
                    or not (open_round or terminal_assessment)
                ):
                    raise ValueError(
                        "Current consent and workshop assessment access are required"
                    )
                member_ids = set(
                    session.scalars(
                        select(ReviewGroupMember.reviewer_id).where(
                            ReviewGroupMember.group_id == assignment.review_group_id,
                            ReviewGroupMember.reviewer_id.in_(reviewer_ids),
                        )
                    )
                )
                if member_ids != reviewer_ids:
                    raise ValueError(
                        "Pre-review assessments must belong to an assigned group member"
                    )
            else:
                raise ValueError("Review assignment has no owner")

            provided_domains = {
                (
                    str(record.get("kg_id")),
                    str(record.get("seed_version")),
                    str(record.get("review_domain_id")),
                    str(record.get("review_domain_label")),
                )
                for record in domain_records
            }
            provided_familiarities = {
                (
                    str(record.get("kg_id")),
                    str(record.get("seed_version")),
                    str(record.get("familiarity_scope_id")),
                    str(record.get("familiarity_scope_label")),
                )
                for record in familiarity_records
            }
            if len(provided_domains) != len(domain_records):
                raise ValueError("Pre-review domain assessment set contains duplicates")
            if len(provided_familiarities) != len(familiarity_records):
                raise ValueError("Pre-review familiarity assessment set contains duplicates")
            if provided_domains != assignments.domain_prompts(assignment_id):
                raise ValueError("Pre-review domain assessments do not match the assignment")
            if provided_familiarities != assignments.familiarity_prompts(assignment_id):
                raise ValueError("Pre-review familiarity assessments do not match the assignment")

            for record in domain_records:
                self._append_assessment(session, record, domain=True)
            for record in familiarity_records:
                self._append_assessment(session, record, domain=False)
            if activate_assignment:
                if assignment.review_group_id is None:
                    if assignment.status != "ready":
                        raise ValueError("Only a ready assignment can be activated")
                    should_activate = True
                else:
                    if assignment.status not in {"ready", "active"}:
                        raise ValueError("This group assignment is not open for assessment")
                    session.flush()
                    member_ids = list(
                        session.scalars(
                            select(ReviewGroupMember.reviewer_id).where(
                                ReviewGroupMember.group_id
                                == assignment.review_group_id
                            )
                        )
                    )
                    expected_domain_count = len(provided_domains)
                    expected_familiarity_count = len(provided_familiarities)
                    should_activate = assignment.status == "ready" and all(
                        int(
                            session.scalar(
                                select(func.count())
                                .select_from(ReviewerKgDomainAssessment)
                                .where(
                                    ReviewerKgDomainAssessment.assignment_id
                                    == assignment.id,
                                    ReviewerKgDomainAssessment.reviewer_id
                                    == member_id,
                                )
                            )
                            or 0
                        )
                        == expected_domain_count
                        and int(
                            session.scalar(
                                select(func.count())
                                .select_from(ReviewerResourceFamiliarityAssessment)
                                .where(
                                    ReviewerResourceFamiliarityAssessment.assignment_id
                                    == assignment.id,
                                    ReviewerResourceFamiliarityAssessment.reviewer_id
                                    == member_id,
                                )
                            )
                            or 0
                        )
                        == expected_familiarity_count
                        for member_id in member_ids
                    )
                if should_activate:
                    assignment.status = "active"
                    assignment.opened_at = str(records[0]["assessed_at"])
                    assignment.participant_status = "active"
                    if assignment.claimed_at is None:
                        assignment.claimed_at = str(records[0]["assessed_at"])
            session.commit()

    def _append_assessment(
        self, session: Session, record: Mapping[str, Any], *, domain: bool
    ) -> None:
        validator = validate_kg_domain_assessments if domain else validate_resource_familiarity_assessments
        reviewer_id = validate_reviewer_id(record.get("reviewer_id"))
        subject_id = str(record["review_domain_id" if domain else "familiarity_scope_id"])
        repository = ProvenanceRepository(session)
        history = (
            repository.domain_assessments(reviewer_id, str(record["kg_id"]), subject_id)
            if domain
            else repository.familiarity_assessments(reviewer_id, str(record["kg_id"]), subject_id)
        )
        _assert_head(history, record.get("previous_assessment_id"), "assessment")
        history_records: list[dict[str, Any]] = []
        for item in history:
            saved = dict(
                schema=(
                    "musparql.reviewer-kg-domain-assessment.v1"
                    if domain else "musparql.reviewer-resource-familiarity-assessment.v1"
                ),
                id=item.id, reviewer_id=item.reviewer_id, kg_id=item.kg_id,
                assessed_at=item.assessed_at, context=item.context,
                assignment_id=item.assignment_id, seed_version=item.seed_version,
                previous_assessment_id=item.previous_assessment_id,
            )
            if domain:
                saved.update(
                    review_domain_id=item.review_domain_id,
                    review_domain_label=item.review_domain_label,
                    subject_expertise_level=item.subject_expertise_level,
                )
            else:
                saved.update(
                    familiarity_scope_id=item.familiarity_scope_id,
                    familiarity_scope_label=item.familiarity_scope_label,
                    familiarity_level=item.familiarity_level,
                )
            history_records.append(saved)
        validator([*history_records, record])
        common = dict(
            id=record["id"], reviewer_id=reviewer_id, kg_id=record["kg_id"],
            assessed_at=record["assessed_at"], context=record["context"],
            assignment_id=record.get("assignment_id"), seed_version=record["seed_version"],
            previous_assessment_id=record.get("previous_assessment_id"),
        )
        if domain:
            value = ReviewerKgDomainAssessment(
                review_domain_id=record["review_domain_id"],
                review_domain_label=record["review_domain_label"],
                subject_expertise_level=record["subject_expertise_level"], **common,
            )
        else:
            value = ReviewerResourceFamiliarityAssessment(
                familiarity_scope_id=record["familiarity_scope_id"],
                familiarity_scope_label=record["familiarity_scope_label"],
                familiarity_level=record["familiarity_level"], **common,
            )
        repository.add(value)
        session.flush()
