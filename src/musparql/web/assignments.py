"""Phase 5 assignment creation, authorization, and pre-review workflow."""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import random
import re
import secrets
from typing import Any, Sequence

from sqlalchemy import select, text
from sqlalchemy.orm import Session, sessionmaker

from musparql.database.models import (
    AssignmentKgSeed,
    KgSeedFamiliarityScope,
    KgSeedReviewDomain,
    KgSeedSnapshot,
    ReviewAssignment,
    ReviewGroup,
    ReviewGroupMember,
    Reviewer,
    ReviewerKgDomainAssessment,
    ReviewerResourceFamiliarityAssessment,
    WorkshopRound,
    WorkshopAssessmentDeferral,
)
from musparql.database.services import ProvenanceService
from musparql.linguistic_dimensions import BUNDLE_SCHEMA, validate_bundle
from .auth import timestamp, utc_now


_BUNDLE_PREFIX = re.compile(r"^\s*window\.REVIEW_DATA\s*=\s*", re.ASCII)
_SAFE_HOLDOUT_POLICIES = {
    "no_holdout",
    "identity_visible_selectors",
    "identity_private_filtered_upstream",
}
_ASSIGNMENT_RANDOMIZED_WORKSHOP_KGS = {
    "europeana",
    "camera-dei-deputati",
}


def _randomize_workshop_records(
    payload: dict[str, Any], *, assignment_id: str, bundle_digest: str
) -> dict[str, Any]:
    """Apply the package-specific workshop presentation order."""
    workshop_package = payload.get("workshop_package")
    if not isinstance(workshop_package, dict):
        return payload
    pass_order = workshop_package.get("pass_order")
    if pass_order != ["deduplicated", "all_pairs"]:
        return payload
    strata = {name: [] for name in pass_order}
    for record in payload["records"]:
        name = str(record.get("workshop_pass") or "deduplicated")
        if name not in strata:
            raise ValueError(f"Workshop record has an invalid pass: {name}")
        strata[name].append(record)
    kg_id = str(workshop_package.get("kg_id") or "")
    randomized = kg_id in _ASSIGNMENT_RANDOMIZED_WORKSHOP_KGS
    ordered_records = []
    for name in pass_order:
        strata[name].sort(key=lambda record: str(record.get("query_id") or ""))
        if randomized:
            seed = int.from_bytes(
                hashlib.sha256(
                    f"{assignment_id}\0{bundle_digest}\0{name}".encode("utf-8")
                ).digest(),
                "big",
            )
            random.Random(seed).shuffle(strata[name])
        ordered_records.extend(strata[name])
    return {
        **payload,
        "records": ordered_records,
        "workshop_package": {
            **workshop_package,
            "presentation_strategy": (
                "assignment-randomized-within-pass"
                if randomized
                else "query-id-order-within-pass"
            ),
            "presentation_order": [
                str(record.get("query_id") or "") for record in ordered_records
            ],
        },
    }
_RECIPES = {
    "initial": {"validate_initial_review", "stage_initial_benchmark_update"},
    "compare": {
        "validate_comparative_review",
        "stage_comparative_benchmark_update",
    },
    "linguistic": {"validate_linguistic_annotation"},
}


def has_current_consent(
    reviewer: Reviewer | None,
    current_consent_version: str | None,
    current_notice_version: str | None,
) -> bool:
    """Fail closed unless an active reviewer accepted both configured texts."""
    return bool(
        reviewer is not None
        and reviewer.status == "active"
        and current_consent_version
        and current_notice_version
        and reviewer.consent_statement_version == current_consent_version
        and reviewer.consented_at
        and reviewer.privacy_notice_version == current_notice_version
        and reviewer.privacy_notice_acknowledged_at
    )


@dataclass(frozen=True)
class Prompt:
    kg_id: str
    seed_version: str
    subject_id: str
    label: str
    description: str | None
    prior_value: str | None


@dataclass(frozen=True)
class AssignmentView:
    assignment: ReviewAssignment
    domain_prompts: tuple[Prompt, ...]
    familiarity_prompts: tuple[Prompt, ...]
    assessed: bool
    assessment_reused: bool
    workbench_available: bool
    assessment_deferrable: bool


class AssignmentService:
    def __init__(
        self,
        sessions: sessionmaker[Session],
        bundle_root: Path,
        current_consent_version: str | None = None,
        current_notice_version: str | None = None,
    ) -> None:
        self.sessions = sessions
        self.bundle_root = bundle_root.resolve()
        self.current_consent_version = current_consent_version
        self.current_notice_version = current_notice_version
        self.provenance = ProvenanceService(
            sessions,
            current_consent_version=current_consent_version,
            current_notice_version=current_notice_version,
        )

    def owner_choices(self) -> tuple[list[Reviewer], list[KgSeedSnapshot]]:
        with self.sessions() as session:
            reviewers = list(
                session.scalars(
                    select(Reviewer)
                    .where(Reviewer.status == "active")
                    .order_by(Reviewer.id)
                )
            )
            seeds = list(
                session.scalars(
                    select(KgSeedSnapshot).order_by(
                        KgSeedSnapshot.kg_id, KgSeedSnapshot.seed_version
                    )
                )
            )
            return reviewers, seeds

    def list_all(self) -> list[ReviewAssignment]:
        with self.sessions() as session:
            return list(
                session.scalars(
                    select(ReviewAssignment).order_by(ReviewAssignment.created_at.desc())
                )
            )

    def list_for_reviewer(self, reviewer_id: str) -> list[ReviewAssignment]:
        with self.sessions() as session:
            return list(
                session.scalars(
                    select(ReviewAssignment)
                    .where(
                        ReviewAssignment.reviewer_id == reviewer_id,
                        ReviewAssignment.status.in_(("ready", "active")),
                    )
                    .order_by(ReviewAssignment.created_at.desc())
                )
            )

    def list_group_assignments_for_reviewer(
        self, reviewer_id: str
    ) -> list[ReviewAssignment]:
        now = timestamp(utc_now())
        with self.sessions() as session:
            reviewer = session.get(Reviewer, reviewer_id)
            if not has_current_consent(
                reviewer,
                self.current_consent_version,
                self.current_notice_version,
            ):
                return []
            return list(
                session.scalars(
                    select(ReviewAssignment)
                    .join(
                        ReviewGroupMember,
                        ReviewGroupMember.group_id == ReviewAssignment.review_group_id,
                    )
                    .join(ReviewGroup, ReviewGroup.id == ReviewAssignment.review_group_id)
                    .join(
                        WorkshopRound,
                        WorkshopRound.id == ReviewGroup.workshop_round_id,
                    )
                    .where(
                        ReviewGroupMember.reviewer_id == reviewer_id,
                        ReviewAssignment.participant_status.in_(
                            ("not_started", "active")
                        ),
                        WorkshopRound.status == "open",
                        WorkshopRound.opens_at <= now,
                        WorkshopRound.closes_at > now,
                    )
                    .order_by(ReviewAssignment.created_at.desc())
                )
            )

    def create(
        self,
        *,
        reviewer_id: str,
        mode: str,
        bundle_name: str,
        processing_recipe: str,
        seed_keys: Sequence[str],
        previous_benchmark_path: str | None = None,
    ) -> str:
        if mode not in _RECIPES or processing_recipe not in _RECIPES[mode]:
            raise ValueError("Mode and processing recipe do not match")
        payload, relative_path, digest = self.load_neutral_bundle(bundle_name)
        bundle_mode = str(payload.get("mode") or "initial")
        if bundle_mode != mode:
            raise ValueError("Bundle mode does not match the assignment")
        if (mode == "linguistic") != (payload.get("schema") == BUNDLE_SCHEMA):
            raise ValueError("Assignment mode requires its matching bundle contract")
        selected = self._parse_seed_keys(seed_keys)
        if not selected:
            raise ValueError("At least one frozen KG seed is required")
        selected_kg_ids = {kg_id for kg_id, _seed_version in selected}
        if len(selected_kg_ids) != len(selected):
            raise ValueError("Select exactly one frozen seed version per KG")
        bundle_kg_ids = {
            str(record.get("kg_id") or "")
            for record in payload["records"]
            if isinstance(record, dict)
        }
        if "" in bundle_kg_ids or bundle_kg_ids != selected_kg_ids:
            raise ValueError("Frozen KG seed selections must exactly match bundle records")
        assignment_id = "assignment-" + secrets.token_hex(12)
        now = timestamp(utc_now())
        with self.sessions.begin() as session:
            reviewer = session.get(Reviewer, reviewer_id)
            if reviewer is None or reviewer.status != "active":
                raise ValueError("Assignments require an active reviewer")
            seeds: list[AssignmentKgSeed] = []
            for kg_id, seed_version in selected:
                snapshot = session.get(KgSeedSnapshot, (kg_id, seed_version))
                if snapshot is None:
                    raise ValueError("Unknown frozen KG seed")
                seeds.append(
                    AssignmentKgSeed(
                        assignment_id=assignment_id,
                        kg_id=kg_id,
                        seed_version=seed_version,
                        seed_digest=snapshot.seed_digest,
                    )
                )
            assignment = ReviewAssignment(
                id=assignment_id,
                reviewer_id=reviewer_id,
                review_group_id=None,
                work_package_id=None,
                participant_status="not_started",
                claimed_at=None,
                completed_at=None,
                completion_item_count=None,
                completion_total_count=None,
                closed_contributor_ids=None,
                mode=mode,
                status="ready",
                bundle_path=relative_path,
                bundle_digest=digest,
                previous_benchmark_path=previous_benchmark_path or None,
                processing_recipe=processing_recipe,
                holdout_capability=False,
                created_at=now,
                opened_at=None,
                submitted_at=None,
            )
            session.add(assignment)
            session.flush()
            session.add_all(seeds)
        return assignment_id

    def view(self, assignment_id: str, reviewer_id: str) -> AssignmentView:
        with self.sessions() as session:
            assignment = session.get(ReviewAssignment, assignment_id)
            if assignment is None:
                raise LookupError("Assignment is not available")
            ordinary_access = self._reviewer_can_access(
                session, assignment, reviewer_id
            )
            terminal_access = self._reviewer_can_complete_terminal_assessment(
                session, assignment, reviewer_id
            )
            if not ordinary_access and not terminal_access:
                raise LookupError("Assignment is not available")
            domains = self._domain_prompts(session, assignment, reviewer_id)
            familiarities = self._familiarity_prompts(
                session, assignment, reviewer_id
            )
            assessed = self._assessment_is_complete(
                session, assignment, reviewer_id
            )
            recorded_for_assignment = bool(
                session.scalar(
                    select(ReviewerKgDomainAssessment.id).where(
                        ReviewerKgDomainAssessment.assignment_id == assignment_id,
                        ReviewerKgDomainAssessment.reviewer_id == reviewer_id,
                    ).limit(1)
                )
                or session.scalar(
                    select(ReviewerResourceFamiliarityAssessment.id).where(
                        ReviewerResourceFamiliarityAssessment.assignment_id
                        == assignment_id,
                        ReviewerResourceFamiliarityAssessment.reviewer_id
                        == reviewer_id,
                    ).limit(1)
                )
            )
            deferred = session.scalar(
                select(WorkshopAssessmentDeferral.id).where(
                    WorkshopAssessmentDeferral.assignment_id == assignment_id,
                    WorkshopAssessmentDeferral.reviewer_id == reviewer_id,
                )
            )
            terminal_assessment = (
                terminal_access
                and not assessed
            )
            workshop_in_progress = (
                assignment.review_group_id is not None
                and assignment.participant_status == "active"
            )
            if (
                assignment.status not in {"ready", "active"}
                and not workshop_in_progress
                and not terminal_assessment
            ):
                raise LookupError("Assignment is not available")
            if (
                assignment.participant_status
                in {"completed", "partial", "abandoned"}
                and not terminal_assessment
            ):
                raise LookupError("Assignment is not available")
            return AssignmentView(
                assignment,
                domains,
                familiarities,
                assessed,
                assessed and not recorded_for_assignment,
                assignment.participant_status == "active"
                and assignment.status != "ready"
                and (assessed or bool(deferred)),
                assignment.review_group_id is not None
                and assignment.participant_status == "active"
                and assignment.status != "ready"
                and not assessed
                and not deferred,
            )

    def assess(
        self,
        assignment_id: str,
        reviewer_id: str,
        domain_values: Sequence[str],
        familiarity_values: Sequence[str],
        *,
        confirmed: bool,
    ) -> None:
        view = self.view(assignment_id, reviewer_id)
        if view.assessed:
            raise ValueError("This assignment assessment is already complete")
        if not confirmed:
            raise ValueError("Prior values must be confirmed or updated")
        if len(domain_values) != len(view.domain_prompts) or len(
            familiarity_values
        ) != len(view.familiarity_prompts):
            raise ValueError("The complete frozen prompt set is required")
        now = timestamp(utc_now())
        context = "pre_review"
        version = "v1"
        domain_records = [
            {
                "schema": f"musparql.reviewer-kg-domain-assessment.{version}",
                "id": "assessment-" + secrets.token_hex(12),
                "reviewer_id": reviewer_id,
                "kg_id": prompt.kg_id,
                "review_domain_id": prompt.subject_id,
                "review_domain_label": prompt.label,
                "subject_expertise_level": value,
                "assessed_at": now,
                "context": context,
                "assignment_id": assignment_id,
                "seed_version": prompt.seed_version,
                "previous_assessment_id": self._domain_head_id(
                    reviewer_id, prompt.kg_id, prompt.subject_id
                ),
            }
            for prompt, value in zip(view.domain_prompts, domain_values, strict=True)
        ]
        familiarity_records = [
            {
                "schema": f"musparql.reviewer-resource-familiarity-assessment.{version}",
                "id": "assessment-" + secrets.token_hex(12),
                "reviewer_id": reviewer_id,
                "kg_id": prompt.kg_id,
                "familiarity_scope_id": prompt.subject_id,
                "familiarity_scope_label": prompt.label,
                "familiarity_level": value,
                "assessed_at": now,
                "context": context,
                "assignment_id": assignment_id,
                "seed_version": prompt.seed_version,
                "previous_assessment_id": self._familiarity_head_id(
                    reviewer_id, prompt.kg_id, prompt.subject_id
                ),
            }
            for prompt, value in zip(
                view.familiarity_prompts, familiarity_values, strict=True
            )
        ]
        self.provenance.append_pre_review_assessments(
            domain_records,
            familiarity_records,
            activate_assignment=view.assignment.participant_status
            in {"not_started", "active"},
        )

    def defer_assessment(self, assignment_id: str, reviewer_id: str) -> None:
        """Record an explicit no-answer event for a joiner entering active work."""
        view = self.view(assignment_id, reviewer_id)
        if not view.assessment_deferrable:
            raise PermissionError("This assessment cannot be deferred")
        session = self.sessions()
        try:
            session.execute(text("BEGIN IMMEDIATE"))
            assignment = session.get(ReviewAssignment, assignment_id)
            if (
                assignment is None
                or assignment.review_group_id is None
                or assignment.status == "ready"
                or assignment.participant_status != "active"
                or session.get(
                    ReviewGroupMember, (assignment.review_group_id, reviewer_id)
                ) is None
                or self._assessment_is_complete(session, assignment, reviewer_id)
            ):
                raise PermissionError("This assessment cannot be deferred")
            existing = session.scalar(
                select(WorkshopAssessmentDeferral.id).where(
                    WorkshopAssessmentDeferral.assignment_id == assignment_id,
                    WorkshopAssessmentDeferral.reviewer_id == reviewer_id,
                )
            )
            if existing is None:
                session.add(
                    WorkshopAssessmentDeferral(
                        id="deferral-" + secrets.token_hex(12),
                        assignment_id=assignment_id,
                        reviewer_id=reviewer_id,
                        deferred_at=timestamp(utc_now()),
                    )
                )
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def attributed_bundle(self, assignment_id: str, reviewer_id: str) -> dict[str, Any]:
        view = self.view(assignment_id, reviewer_id)
        if not view.workbench_available:
            raise PermissionError("Pre-review assessment is required")
        payload, _path, digest = self.load_neutral_bundle(view.assignment.bundle_path)
        if digest != view.assignment.bundle_digest:
            raise ValueError("Assignment bundle digest has changed")
        attributed = dict(payload)
        if attributed.get("mode") == "linguistic":
            # Provenance remains authoritative in the digest-verified server bundle,
            # but is not delivered to the blinded reviewer interface.
            attributed = json.loads(json.dumps(attributed))
            for record in attributed["records"]:
                record["literal"].pop("validation_provenance", None)
                for candidate in record["candidates"]:
                    candidate.pop("provenance", None)
        attributed = _randomize_workshop_records(
            attributed, assignment_id=assignment_id, bundle_digest=digest
        )
        if attributed.get("mode") != "compare":
            run_ids = attributed.get("run_ids")
            if not isinstance(run_ids, list):
                run_ids = list(
                    dict.fromkeys(
                        str(record.get("generation_run_id") or record.get("run_id"))
                        for record in attributed["records"]
                        if record.get("generation_run_id") or record.get("run_id")
                    )
                )
            attributed["run_ids"] = run_ids
            attributed["single_run_id"] = attributed.get("single_run_id") or (
                run_ids[0] if len(run_ids) == 1 else None
            )
            if not isinstance(attributed.get("runs"), list):
                attributed["runs"] = [{"run_id": run_id} for run_id in run_ids]
        attributed["reviewer_id"] = reviewer_id
        attributed["assignment_id"] = assignment_id
        attributed["bundle_digest"] = digest
        if view.assignment.review_group_id is not None:
            attributed["review_group_id"] = view.assignment.review_group_id
        return attributed

    def submission_bundle(
        self, assignment_id: str, reviewer_id: str
    ) -> tuple[ReviewAssignment, dict[str, Any]]:
        """Return the frozen authoritative bundle for an attributable submission."""
        with self.sessions() as session:
            assignment = session.get(ReviewAssignment, assignment_id)
            if assignment is None or not self._reviewer_can_access(
                session, assignment, reviewer_id
            ):
                raise LookupError("Assignment is not available")
            if assignment.status not in {
                "active", "submitted", "processing", "ready_for_owner_review", "approved", "failed"
            }:
                raise PermissionError("Assignment is not open for submission")
            session.expunge(assignment)
        payload, _path, digest = self.load_neutral_bundle(assignment.bundle_path)
        if digest != assignment.bundle_digest:
            raise ValueError("Assignment bundle digest has changed")
        return assignment, payload

    def submission_contributor_ids(
        self,
        session: Session,
        assignment: ReviewAssignment,
        reviewer_id: str,
    ) -> tuple[str, ...]:
        """Resolve server-owned attribution while the caller holds a write lock."""
        if not self._reviewer_can_access(session, assignment, reviewer_id):
            raise LookupError("Assignment is not available")
        if assignment.reviewer_id is not None:
            return (assignment.reviewer_id,)
        if assignment.review_group_id is None:
            raise LookupError("Assignment is not available")
        contributor_ids = tuple(
            sorted(
                assignment.closed_contributor_ids
                or session.scalars(
                    select(ReviewGroupMember.reviewer_id).where(
                        ReviewGroupMember.group_id == assignment.review_group_id
                    )
                ).all()
            )
        )
        if not contributor_ids or reviewer_id not in contributor_ids:
            raise LookupError("Assignment is not available")
        return contributor_ids

    def load_neutral_bundle(self, bundle_name: str) -> tuple[dict[str, Any], str, str]:
        """Validate and load one reviewer-neutral bundle under the configured root."""
        return load_neutral_bundle_file(self.bundle_root, bundle_name)

    @classmethod
    def _contains_holdout_marker(cls, value: Any) -> bool:
        if isinstance(value, list):
            return any(cls._contains_holdout_marker(item) for item in value)
        if not isinstance(value, dict):
            return False
        for key, item in value.items():
            normalized_key = str(key).casefold()
            if normalized_key in {"holdout", "is_holdout"} and (
                item is True
                or isinstance(item, str)
                and item.casefold() in {"true", "yes", "holdout"}
            ):
                return True
            if (
                normalized_key in {"split", "benchmark_split"}
                and str(item).casefold() in {"holdout", "private_holdout"}
            ):
                return True
            if cls._contains_holdout_marker(item):
                return True
        return False

    @classmethod
    def _contains_key(cls, value: Any, expected: str) -> bool:
        if isinstance(value, list):
            return any(cls._contains_key(item, expected) for item in value)
        if not isinstance(value, dict):
            return False
        return expected in value or any(
            cls._contains_key(item, expected) for item in value.values()
        )

    @staticmethod
    def _parse_seed_keys(values: Sequence[str]) -> list[tuple[str, str]]:
        result: list[tuple[str, str]] = []
        for value in values:
            parts = value.split("|", 1)
            if len(parts) != 2 or not all(parts):
                raise ValueError("Invalid frozen KG seed selection")
            key = (parts[0], parts[1])
            if key in result:
                raise ValueError("Duplicate frozen KG seed selection")
            result.append(key)
        return result

    def _domain_prompts(
        self, session: Session, assignment: ReviewAssignment, reviewer_id: str
    ) -> tuple[Prompt, ...]:
        rows = session.execute(
            select(KgSeedReviewDomain)
            .join(
                AssignmentKgSeed,
                (AssignmentKgSeed.kg_id == KgSeedReviewDomain.kg_id)
                & (AssignmentKgSeed.seed_version == KgSeedReviewDomain.seed_version),
            )
            .where(AssignmentKgSeed.assignment_id == assignment.id)
            .order_by(KgSeedReviewDomain.kg_id, KgSeedReviewDomain.domain_id)
        ).scalars()
        return tuple(
            Prompt(
                row.kg_id,
                row.seed_version,
                row.domain_id,
                row.label,
                row.description,
                self._domain_head_value(
                    session, reviewer_id, row.kg_id, row.domain_id
                ),
            )
            for row in rows
        )

    def _familiarity_prompts(
        self, session: Session, assignment: ReviewAssignment, reviewer_id: str
    ) -> tuple[Prompt, ...]:
        rows = session.execute(
            select(KgSeedFamiliarityScope)
            .join(
                AssignmentKgSeed,
                (AssignmentKgSeed.kg_id == KgSeedFamiliarityScope.kg_id)
                & (
                    AssignmentKgSeed.seed_version
                    == KgSeedFamiliarityScope.seed_version
                ),
            )
            .where(AssignmentKgSeed.assignment_id == assignment.id)
            .order_by(KgSeedFamiliarityScope.kg_id, KgSeedFamiliarityScope.scope_id)
        ).scalars()
        return tuple(
            Prompt(
                row.kg_id,
                row.seed_version,
                row.scope_id,
                row.label,
                row.description,
                self._familiarity_head_value(
                    session, reviewer_id, row.kg_id, row.scope_id
                ),
            )
            for row in rows
        )

    @staticmethod
    def _assessment_is_complete(
        session: Session, assignment: ReviewAssignment, reviewer_id: str
    ) -> bool:
        expected_domains = set(
            session.execute(
                select(
                    KgSeedReviewDomain.kg_id,
                    KgSeedReviewDomain.seed_version,
                    KgSeedReviewDomain.domain_id,
                ).join(
                    AssignmentKgSeed,
                    (AssignmentKgSeed.kg_id == KgSeedReviewDomain.kg_id)
                    & (
                        AssignmentKgSeed.seed_version
                        == KgSeedReviewDomain.seed_version
                    ),
                ).where(AssignmentKgSeed.assignment_id == assignment.id)
            ).all()
        )
        expected_familiarities = set(
            session.execute(
                select(
                    KgSeedFamiliarityScope.kg_id,
                    KgSeedFamiliarityScope.seed_version,
                    KgSeedFamiliarityScope.scope_id,
                ).join(
                    AssignmentKgSeed,
                    (AssignmentKgSeed.kg_id == KgSeedFamiliarityScope.kg_id)
                    & (
                        AssignmentKgSeed.seed_version
                        == KgSeedFamiliarityScope.seed_version
                    ),
                ).where(AssignmentKgSeed.assignment_id == assignment.id)
            ).all()
        )
        if assignment.review_group_id is None or assignment.work_package_id is None:
            domain_scope = (
                ReviewerKgDomainAssessment.assignment_id == assignment.id
            )
            familiarity_scope = (
                ReviewerResourceFamiliarityAssessment.assignment_id == assignment.id
            )
        else:
            group = session.get(ReviewGroup, assignment.review_group_id)
            if group is None:
                return False
            workshop_assignment_ids = select(ReviewAssignment.id).join(
                ReviewGroup, ReviewGroup.id == ReviewAssignment.review_group_id
            ).where(ReviewGroup.workshop_round_id == group.workshop_round_id)
            domain_scope = ReviewerKgDomainAssessment.assignment_id.in_(
                workshop_assignment_ids
            )
            familiarity_scope = (
                ReviewerResourceFamiliarityAssessment.assignment_id.in_(
                    workshop_assignment_ids
                )
            )
        recorded_domains = set(
            session.execute(
                select(
                    ReviewerKgDomainAssessment.kg_id,
                    ReviewerKgDomainAssessment.seed_version,
                    ReviewerKgDomainAssessment.review_domain_id,
                ).where(
                    ReviewerKgDomainAssessment.reviewer_id == reviewer_id,
                    domain_scope,
                )
            ).all()
        )
        recorded_familiarities = set(
            session.execute(
                select(
                    ReviewerResourceFamiliarityAssessment.kg_id,
                    ReviewerResourceFamiliarityAssessment.seed_version,
                    ReviewerResourceFamiliarityAssessment.familiarity_scope_id,
                ).where(
                    ReviewerResourceFamiliarityAssessment.reviewer_id == reviewer_id,
                    familiarity_scope,
                )
            ).all()
        )
        return (
            bool(expected_domains or expected_familiarities)
            and expected_domains.issubset(recorded_domains)
            and expected_familiarities.issubset(recorded_familiarities)
        )

    @classmethod
    def assessment_is_complete(
        cls, session: Session, assignment: ReviewAssignment, reviewer_id: str
    ) -> bool:
        """Expose the frozen-assessment check to the workshop dashboard."""
        return cls._assessment_is_complete(session, assignment, reviewer_id)

    def _reviewer_can_access(
        self, session: Session, assignment: ReviewAssignment, reviewer_id: str
    ) -> bool:
        reviewer = session.get(Reviewer, reviewer_id)
        if assignment.reviewer_id is not None:
            return assignment.reviewer_id == reviewer_id and (
                not self.current_consent_version
                or has_current_consent(
                    reviewer,
                    self.current_consent_version,
                    self.current_notice_version,
                )
            )
        if assignment.review_group_id is None:
            return False
        if not has_current_consent(
            reviewer,
            self.current_consent_version,
            self.current_notice_version,
        ):
            return False
        # The workshop window controls admission, group changes, and new package
        # claims. It must not strand a participant who already has an active
        # assignment when the scheduled session ends.
        return session.get(
            ReviewGroupMember, (assignment.review_group_id, reviewer_id)
        ) is not None

    def _reviewer_can_complete_terminal_assessment(
        self, session: Session, assignment: ReviewAssignment, reviewer_id: str
    ) -> bool:
        """Allow a frozen contributor to finish the form after round closure."""
        if (
            assignment.review_group_id is None
            or assignment.participant_status not in {"completed", "partial", "abandoned"}
            or reviewer_id not in (assignment.closed_contributor_ids or ())
        ):
            return False
        reviewer = session.get(Reviewer, reviewer_id)
        if not has_current_consent(
            reviewer,
            self.current_consent_version,
            self.current_notice_version,
        ):
            return False
        return session.get(
            ReviewGroupMember, (assignment.review_group_id, reviewer_id)
        ) is not None

    def _domain_head_id(
        self, reviewer_id: str, kg_id: str, subject_id: str
    ) -> str | None:
        with self.sessions() as session:
            return self._head(
                session,
                ReviewerKgDomainAssessment,
                reviewer_id,
                kg_id,
                "review_domain_id",
                subject_id,
                "id",
            )

    def _familiarity_head_id(
        self, reviewer_id: str, kg_id: str, subject_id: str
    ) -> str | None:
        with self.sessions() as session:
            return self._head(
                session,
                ReviewerResourceFamiliarityAssessment,
                reviewer_id,
                kg_id,
                "familiarity_scope_id",
                subject_id,
                "id",
            )

    @classmethod
    def _domain_head_value(
        cls, session: Session, reviewer_id: str, kg_id: str, subject_id: str
    ) -> str | None:
        return cls._head(
            session,
            ReviewerKgDomainAssessment,
            reviewer_id,
            kg_id,
            "review_domain_id",
            subject_id,
            "subject_expertise_level",
        )

    @classmethod
    def _familiarity_head_value(
        cls, session: Session, reviewer_id: str, kg_id: str, subject_id: str
    ) -> str | None:
        return cls._head(
            session,
            ReviewerResourceFamiliarityAssessment,
            reviewer_id,
            kg_id,
            "familiarity_scope_id",
            subject_id,
            "familiarity_level",
        )

    @staticmethod
    def _head(
        session: Session,
        model: type[Any],
        reviewer_id: str,
        kg_id: str,
        subject_field: str,
        subject_id: str,
        result_field: str,
    ) -> str | None:
        rows = list(
            session.scalars(
                select(model).where(
                    model.reviewer_id == reviewer_id,
                    model.kg_id == kg_id,
                    getattr(model, subject_field) == subject_id,
                )
            )
        )
        if not rows:
            return None
        predecessors = {row.previous_assessment_id for row in rows}
        head = next(row for row in rows if row.id not in predecessors)
        return str(getattr(head, result_field))


def load_neutral_bundle_file(
    bundle_root: Path, bundle_name: str
) -> tuple[dict[str, Any], str, str]:
    """Validate a reviewer-neutral bundle without requiring a database session."""
    root = bundle_root.resolve()
    if not bundle_name or Path(bundle_name).is_absolute():
        raise ValueError("Bundle path must be relative to the configured root")
    path = (root / bundle_name).resolve()
    try:
        relative = path.relative_to(root).as_posix()
    except ValueError as exc:
        raise ValueError("Bundle path escapes the configured root") from exc
    if not path.is_file():
        raise ValueError("Bundle does not exist")
    raw = path.read_bytes()
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise ValueError("Bundle must be UTF-8 JSON or REVIEW_DATA JavaScript") from exc
    if _BUNDLE_PREFIX.match(text):
        text = _BUNDLE_PREFIX.sub("", text, count=1).strip()
        if text.endswith(";"):
            text = text[:-1]
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError("Bundle is not valid JSON data") from exc
    if not isinstance(payload, dict):
        raise ValueError("Bundle must be a JSON object")
    if payload.get("schema") == BUNDLE_SCHEMA:
        validate_bundle(payload)
    elif payload.get("schema") != "musparql.review-bundle.v2":
        raise ValueError("Bundle uses an unsupported assignment contract")
    if AssignmentService._contains_key(payload, "reviewer_id"):
        raise ValueError("Hosted assignments require a reviewer-neutral bundle")
    if payload.get("holdout_input_policy") not in _SAFE_HOLDOUT_POLICIES:
        raise ValueError("Bundle lacks an approved holdout-exclusion policy")
    if not isinstance(payload.get("records"), list) or not payload.get("dataset_id"):
        raise ValueError("Bundle is missing its dataset or records")
    if payload.get("record_count") != len(payload["records"]):
        raise ValueError("Bundle record count does not match its records")
    if AssignmentService._contains_holdout_marker(payload["records"]):
        raise ValueError("Bundle records contain a holdout marker")
    digest = "sha256:" + hashlib.sha256(raw).hexdigest()
    return payload, relative, digest
