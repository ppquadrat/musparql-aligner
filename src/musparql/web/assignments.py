"""Phase 5 assignment creation, authorization, and pre-review workflow."""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import re
import secrets
from typing import Any, Sequence

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from musparql.database.models import (
    AssignmentKgSeed,
    KgSeedFamiliarityScope,
    KgSeedReviewDomain,
    KgSeedSnapshot,
    ReviewAssignment,
    Reviewer,
    ReviewerKgDomainAssessment,
    ReviewerResourceFamiliarityAssessment,
)
from musparql.database.services import ProvenanceService
from .auth import timestamp, utc_now


_BUNDLE_PREFIX = re.compile(r"^\s*window\.REVIEW_DATA\s*=\s*", re.ASCII)
_SAFE_HOLDOUT_POLICIES = {
    "no_holdout",
    "identity_visible_selectors",
    "identity_private_filtered_upstream",
}
_RECIPES = {
    "initial": {"validate_initial_review", "stage_initial_benchmark_update"},
    "compare": {
        "validate_comparative_review",
        "stage_comparative_benchmark_update",
    },
}


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


class AssignmentService:
    def __init__(self, sessions: sessionmaker[Session], bundle_root: Path) -> None:
        self.sessions = sessions
        self.bundle_root = bundle_root.resolve()
        self.provenance = ProvenanceService(sessions)

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
        payload, relative_path, digest = self._load_neutral_bundle(bundle_name)
        bundle_mode = str(payload.get("mode") or "initial")
        if bundle_mode != mode:
            raise ValueError("Bundle mode does not match the assignment")
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
            if assignment is None or assignment.reviewer_id != reviewer_id:
                raise LookupError("Assignment is not available")
            if assignment.status not in {"ready", "active"}:
                raise LookupError("Assignment is not available")
            domains = self._domain_prompts(session, assignment)
            familiarities = self._familiarity_prompts(session, assignment)
            assessed = self._assessment_is_complete(session, assignment)
            return AssignmentView(assignment, domains, familiarities, assessed)

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
        domain_records = [
            {
                "schema": "musparql.reviewer-kg-domain-assessment.v1",
                "id": "assessment-" + secrets.token_hex(12),
                "reviewer_id": reviewer_id,
                "kg_id": prompt.kg_id,
                "review_domain_id": prompt.subject_id,
                "review_domain_label": prompt.label,
                "subject_expertise_level": value,
                "assessed_at": now,
                "context": "pre_review",
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
                "schema": "musparql.reviewer-resource-familiarity-assessment.v1",
                "id": "assessment-" + secrets.token_hex(12),
                "reviewer_id": reviewer_id,
                "kg_id": prompt.kg_id,
                "familiarity_scope_id": prompt.subject_id,
                "familiarity_scope_label": prompt.label,
                "familiarity_level": value,
                "assessed_at": now,
                "context": "pre_review",
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
            domain_records, familiarity_records, activate_assignment=True
        )

    def attributed_bundle(self, assignment_id: str, reviewer_id: str) -> dict[str, Any]:
        view = self.view(assignment_id, reviewer_id)
        if not view.assessed or view.assignment.status != "active":
            raise PermissionError("Pre-review assessment is required")
        payload, _path, digest = self._load_neutral_bundle(view.assignment.bundle_path)
        if digest != view.assignment.bundle_digest:
            raise ValueError("Assignment bundle digest has changed")
        attributed = dict(payload)
        attributed["reviewer_id"] = reviewer_id
        attributed["assignment_id"] = assignment_id
        attributed["bundle_digest"] = digest
        return attributed

    def _load_neutral_bundle(self, bundle_name: str) -> tuple[dict[str, Any], str, str]:
        if not bundle_name or Path(bundle_name).is_absolute():
            raise ValueError("Bundle path must be relative to the configured root")
        path = (self.bundle_root / bundle_name).resolve()
        try:
            relative = path.relative_to(self.bundle_root).as_posix()
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
        if not isinstance(payload, dict) or payload.get("schema") != "musparql.review-bundle.v2":
            raise ValueError("Bundle must use musparql.review-bundle.v2")
        if self._contains_key(payload, "reviewer_id"):
            raise ValueError("Hosted assignments require a reviewer-neutral bundle")
        if payload.get("holdout_input_policy") not in _SAFE_HOLDOUT_POLICIES:
            raise ValueError("Bundle lacks an approved holdout-exclusion policy")
        if not isinstance(payload.get("records"), list) or not payload.get("dataset_id"):
            raise ValueError("Bundle is missing its dataset or records")
        if payload.get("record_count") != len(payload["records"]):
            raise ValueError("Bundle record count does not match its records")
        if self._contains_holdout_marker(payload["records"]):
            raise ValueError("Bundle records contain a holdout marker")
        digest = "sha256:" + hashlib.sha256(raw).hexdigest()
        return payload, relative, digest

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
        self, session: Session, assignment: ReviewAssignment
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
                    session, assignment.reviewer_id, row.kg_id, row.domain_id
                ),
            )
            for row in rows
        )

    def _familiarity_prompts(
        self, session: Session, assignment: ReviewAssignment
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
                    session, assignment.reviewer_id, row.kg_id, row.scope_id
                ),
            )
            for row in rows
        )

    @staticmethod
    def _assessment_is_complete(
        session: Session, assignment: ReviewAssignment
    ) -> bool:
        domain_count = len(
            session.scalars(
                select(ReviewerKgDomainAssessment).where(
                    ReviewerKgDomainAssessment.assignment_id == assignment.id,
                    ReviewerKgDomainAssessment.reviewer_id == assignment.reviewer_id,
                )
            ).all()
        )
        familiarity_count = len(
            session.scalars(
                select(ReviewerResourceFamiliarityAssessment).where(
                    ReviewerResourceFamiliarityAssessment.assignment_id == assignment.id,
                    ReviewerResourceFamiliarityAssessment.reviewer_id
                    == assignment.reviewer_id,
                )
            ).all()
        )
        expected_domains = len(
            session.scalars(
                select(KgSeedReviewDomain).join(
                    AssignmentKgSeed,
                    (AssignmentKgSeed.kg_id == KgSeedReviewDomain.kg_id)
                    & (
                        AssignmentKgSeed.seed_version
                        == KgSeedReviewDomain.seed_version
                    ),
                ).where(AssignmentKgSeed.assignment_id == assignment.id)
            ).all()
        )
        expected_familiarities = len(
            session.scalars(
                select(KgSeedFamiliarityScope).join(
                    AssignmentKgSeed,
                    (AssignmentKgSeed.kg_id == KgSeedFamiliarityScope.kg_id)
                    & (
                        AssignmentKgSeed.seed_version
                        == KgSeedFamiliarityScope.seed_version
                    ),
                ).where(AssignmentKgSeed.assignment_id == assignment.id)
            ).all()
        )
        return domain_count == expected_domains and familiarity_count == expected_familiarities

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
