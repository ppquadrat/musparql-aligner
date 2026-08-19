"""Transactional database services for frozen seeds and append-only provenance."""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any
import unicodedata

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


class SeedSnapshotService:
    def __init__(self, sessions: sessionmaker[Session]) -> None:
        self.sessions = sessions

    def import_archive(self, payload: Mapping[str, Any]) -> int:
        snapshots = _ordered_seed_snapshots(validate_kg_seed_snapshots(payload))
        inserted = 0
        with self.sessions.begin() as session:
            repository = SeedRepository(session)
            for record in snapshots:
                kg_id = str(record["kg_id"])
                seed_version = str(record["seed_version"])
                if repository.get(kg_id, seed_version) is not None:
                    existing = repository.get(kg_id, seed_version)
                    if existing is None or existing.seed_digest != record["seed_digest"]:
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
    def __init__(self, sessions: sessionmaker[Session]) -> None:
        self.sessions = sessions

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

        with self.sessions.begin() as session:
            assignments = AssignmentRepository(session)
            assignment = assignments.get(assignment_id)
            if assignment is None:
                raise ValueError(f"Unknown review assignment: {assignment_id}")
            reviewer_ids = {validate_reviewer_id(record.get("reviewer_id")) for record in records}
            if reviewer_ids != {assignment.reviewer_id}:
                raise ValueError("Pre-review assessments must belong to the assigned reviewer")

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
                if assignment.status != "ready":
                    raise ValueError("Only a ready assignment can be activated")
                assignment.status = "active"
                assignment.opened_at = str(records[0]["assessed_at"])

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
