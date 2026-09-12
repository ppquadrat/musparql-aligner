"""IPL workshop reviewing-group and reusable-package workflow."""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import hmac
import re
import secrets

from sqlalchemy import func, select, text
from sqlalchemy.orm import Session, sessionmaker

from musparql.database.models import (
    AssignmentKgSeed,
    KgSeedFamiliarityScope,
    KgSeedReviewDomain,
    ReviewAssignment,
    ReviewGroup,
    ReviewGroupMember,
    ReviewSubmission,
    Reviewer,
    WorkshopRound,
    WorkshopWorkPackage,
)

from .assignments import AssignmentService, has_current_consent
from .auth import timestamp, utc_now


_ACTIVE_PARTICIPANT_STATUSES = ("not_started", "active")
_REVIEWER_ID = re.compile(r"reviewer-[0-9]{4,}")


class WorkshopUnavailable(ValueError):
    """There is no unambiguous open workshop round for this operation."""


class WorkshopAccessError(PermissionError):
    """The reviewer is not eligible for the requested workshop operation."""


@dataclass(frozen=True)
class WorkPackageView:
    id: str
    display_name: str
    short_description: str
    kg_id: str
    priority_tier: str
    status: str
    action_label: str | None
    assignment_id: str | None


@dataclass(frozen=True)
class ReviewGroupView:
    id: str
    join_code: str
    member_count: int
    active_assignment_id: str | None
    active_package_name: str | None
    can_claim: bool
    outstanding_assessments: tuple["OutstandingAssessmentView", ...]


@dataclass(frozen=True)
class OutstandingAssessmentView:
    assignment_id: str
    package_name: str


@dataclass(frozen=True)
class WorkshopView:
    round_id: str
    round_name: str
    is_open: bool
    allow_additional_assignments: bool
    current_group_id: str
    groups: tuple[ReviewGroupView, ...]
    packages: tuple[WorkPackageView, ...]


class WorkshopService:
    def __init__(
        self,
        *,
        sessions: sessionmaker[Session],
        assignments: AssignmentService,
        secret: bytes,
        current_consent_version: str | None,
        current_notice_version: str | None,
    ) -> None:
        self.sessions = sessions
        self.assignments = assignments
        self.secret = secret
        self.current_consent_version = current_consent_version
        self.current_notice_version = current_notice_version

    def available(self, reviewer_id: str) -> bool:
        """Return whether a reviewer has an open or previously joined round."""
        now = timestamp(utc_now())
        with self.sessions() as session:
            reviewer = session.get(Reviewer, reviewer_id)
            if not self._eligible(reviewer):
                return False
            if len(self._open_rounds(session, now)) == 1:
                return True
            return self._latest_joined_round(session, reviewer_id) is not None

    def dashboard(
        self, reviewer_id: str, selected_group_id: str | None = None
    ) -> WorkshopView:
        now = timestamp(utc_now())
        self._ensure_personal_group(reviewer_id, now)
        with self.sessions() as session:
            reviewer = session.get(Reviewer, reviewer_id)
            self._require_eligible(reviewer)
            open_rounds = self._open_rounds(session, now)
            if len(open_rounds) > 1:
                raise WorkshopUnavailable("Exactly one workshop round must be open")
            workshop_round = (
                open_rounds[0]
                if open_rounds
                else self._latest_joined_round(session, reviewer_id)
            )
            if workshop_round is None:
                raise WorkshopUnavailable("No workshop round is available")
            round_is_open = bool(open_rounds)
            groups = list(
                session.scalars(
                    select(ReviewGroup)
                    .join(ReviewGroupMember, ReviewGroupMember.group_id == ReviewGroup.id)
                    .where(
                        ReviewGroupMember.reviewer_id == reviewer_id,
                        ReviewGroup.workshop_round_id == workshop_round.id,
                    )
                    .order_by(ReviewGroup.created_at, ReviewGroup.id)
                )
            )
            package_names = {
                package.id: package.display_name
                for package in session.scalars(
                    select(WorkshopWorkPackage).where(
                        WorkshopWorkPackage.workshop_round_id == workshop_round.id
                    )
                )
            }
            group_views: list[ReviewGroupView] = []
            assignments_by_group: dict[str, list[ReviewAssignment]] = {}
            for group in groups:
                assignments = list(
                    session.scalars(
                        select(ReviewAssignment)
                        .where(ReviewAssignment.review_group_id == group.id)
                        .order_by(ReviewAssignment.created_at.desc())
                    )
                )
                assignments_by_group[group.id] = assignments
                active = next(
                    (
                        item
                        for item in assignments
                        if item.participant_status in _ACTIVE_PARTICIPANT_STATUSES
                    ),
                    None,
                )
                member_count = int(
                    session.scalar(
                        select(func.count())
                        .select_from(ReviewGroupMember)
                        .where(ReviewGroupMember.group_id == group.id)
                    )
                    or 0
                )
                outstanding_assessments = tuple(
                    OutstandingAssessmentView(
                        assignment_id=item.id,
                        package_name=package_names.get(
                            item.work_package_id, "Completed package"
                        ),
                    )
                    for item in assignments
                    if item.participant_status in {"completed", "partial", "abandoned"}
                    and reviewer_id in (item.closed_contributor_ids or ())
                    and not self.assignments.assessment_is_complete(
                        session, item, reviewer_id
                    )
                )
                group_views.append(
                    ReviewGroupView(
                        id=group.id,
                        join_code=self._join_code(group.id),
                        member_count=member_count,
                        active_assignment_id=active.id if active else None,
                        active_package_name=(
                            package_names.get(active.work_package_id) if active else None
                        ),
                        # A workshop team may move between the four batches at
                        # will.  Each package has its own durable assignment;
                        # an open assignment for one package must not block the
                        # other three.
                        can_claim=round_is_open,
                        outstanding_assessments=outstanding_assessments,
                    )
                )
            if selected_group_id is not None:
                selected_group = next(
                    (group for group in group_views if group.id == selected_group_id),
                    None,
                )
                if selected_group is None:
                    raise WorkshopAccessError("Team is not available")
            else:
                selected_group = next(
                    (group for group in group_views if group.active_assignment_id),
                    group_views[0],
                )
            selected_assignments = assignments_by_group[selected_group.id]
            package_views: list[WorkPackageView] = []
            for package in session.scalars(
                select(WorkshopWorkPackage)
                .where(
                    WorkshopWorkPackage.workshop_round_id == workshop_round.id,
                    WorkshopWorkPackage.enabled.is_(True),
                )
                .order_by(WorkshopWorkPackage.display_order)
            ):
                assignment = next(
                    (
                        item
                        for item in selected_assignments
                        if item.work_package_id == package.id
                    ),
                    None,
                )
                if assignment is None:
                    status = "available"
                    action_label = "Start" if selected_group.can_claim else None
                elif assignment.participant_status in _ACTIVE_PARTICIPANT_STATUSES:
                    assessed = self.assignments.assessment_is_complete(
                        session, assignment, reviewer_id
                    )
                    status = (
                        "in_progress"
                        if assessed and assignment.status != "ready"
                        else "setup_needed"
                    )
                    action_label = (
                        "Continue"
                        if assessed and assignment.status != "ready"
                        else "Complete setup"
                    )
                else:
                    status = "submitted" if assignment.participant_status in {"completed", "partial"} else "closed"
                    action_label = "View submission" if status == "submitted" else None
                package_views.append(WorkPackageView(
                    id=package.id,
                    display_name=package.display_name,
                    short_description=package.short_description,
                    kg_id=package.kg_id,
                    priority_tier=(
                        "core"
                        if package.kg_id in {"europeana", "nfdi4culture"}
                        else "specialist"
                    ),
                    status=status,
                    action_label=action_label,
                    assignment_id=assignment.id if assignment else None,
                ))
            return WorkshopView(
                round_id=workshop_round.id,
                round_name=workshop_round.name,
                is_open=round_is_open,
                allow_additional_assignments=workshop_round.allow_additional_assignments,
                current_group_id=selected_group.id,
                groups=tuple(group_views),
                packages=tuple(package_views),
            )

    def _ensure_personal_group(self, reviewer_id: str, now: str) -> None:
        """Create the participant's one-person team on first workshop entry."""
        session = self.sessions()
        try:
            session.execute(text("BEGIN IMMEDIATE"))
            reviewer = session.get(Reviewer, reviewer_id)
            self._require_eligible(reviewer)
            open_rounds = self._open_rounds(session, now)
            if not open_rounds:
                if self._latest_joined_round(session, reviewer_id) is not None:
                    session.commit()
                    return
                raise WorkshopUnavailable("No workshop round is available")
            if len(open_rounds) != 1:
                raise WorkshopUnavailable("Exactly one workshop round must be open")
            workshop_round = open_rounds[0]
            existing = session.scalar(select(ReviewGroupMember.group_id).join(
                ReviewGroup, ReviewGroup.id == ReviewGroupMember.group_id
            ).where(
                ReviewGroupMember.reviewer_id == reviewer_id,
                ReviewGroup.workshop_round_id == workshop_round.id,
            ).limit(1))
            if existing is None:
                for _attempt in range(20):
                    group_id = "group-" + secrets.token_hex(12)
                    join_code = self._join_code(group_id)
                    if session.scalar(
                        select(ReviewGroup.id).where(
                            ReviewGroup.join_code_digest == self._join_digest(join_code)
                        )
                    ) is None:
                        break
                else:
                    raise RuntimeError("Could not allocate a unique team code")
                session.add(
                    ReviewGroup(
                        id=group_id,
                        workshop_round_id=workshop_round.id,
                        join_code_digest=self._join_digest(join_code),
                        created_at=now,
                    )
                )
                session.flush()
                session.add(
                    ReviewGroupMember(
                        group_id=group_id, reviewer_id=reviewer_id, joined_at=now
                    )
                )
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def create_group(self, reviewer_id: str) -> str:
        now = timestamp(utc_now())
        session = self.sessions()
        try:
            session.execute(text("BEGIN IMMEDIATE"))
            reviewer = session.get(Reviewer, reviewer_id)
            self._require_eligible(reviewer)
            workshop_round = self._open_round(session, now)
            for _attempt in range(20):
                group_id = "group-" + secrets.token_hex(12)
                join_code = self._join_code(group_id)
                if session.scalar(
                    select(ReviewGroup.id).where(
                        ReviewGroup.join_code_digest == self._join_digest(join_code)
                    )
                ) is None:
                    break
            else:
                raise RuntimeError("Could not allocate a unique reviewing-group code")
            session.add(
                ReviewGroup(
                    id=group_id,
                    workshop_round_id=workshop_round.id,
                    join_code_digest=self._join_digest(join_code),
                    created_at=now,
                )
            )
            session.flush()
            session.add(
                ReviewGroupMember(
                    group_id=group_id, reviewer_id=reviewer_id, joined_at=now
                )
            )
            session.commit()
            return group_id
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def join_group(self, reviewer_id: str, presented_code: str) -> str:
        normalized = self._normalize_join_code(presented_code)
        if len(normalized) != 6:
            raise WorkshopAccessError("Reviewing-group code is invalid")
        now = timestamp(utc_now())
        session = self.sessions()
        try:
            session.execute(text("BEGIN IMMEDIATE"))
            reviewer = session.get(Reviewer, reviewer_id)
            self._require_eligible(reviewer)
            workshop_round = self._open_round(session, now)
            group = session.scalar(
                select(ReviewGroup).where(
                    ReviewGroup.workshop_round_id == workshop_round.id,
                    ReviewGroup.join_code_digest == self._join_digest(normalized),
                )
            )
            if group is None:
                raise WorkshopAccessError("Reviewing-group code is invalid")
            existing = session.get(ReviewGroupMember, (group.id, reviewer_id))
            if existing is not None:
                session.commit()
                return group.id
            assignments = list(
                session.scalars(
                    select(ReviewAssignment).where(
                        ReviewAssignment.review_group_id == group.id
                    )
                )
            )
            if any(
                item.participant_status in {"completed", "partial", "abandoned"}
                for item in assignments
            ):
                raise WorkshopAccessError("This reviewing group is closed to new members")
            session.add(
                ReviewGroupMember(
                    group_id=group.id, reviewer_id=reviewer_id, joined_at=now
                )
            )
            session.commit()
            return group.id
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def active_assignment(self, reviewer_id: str, group_id: str) -> str | None:
        """Return the active review after verifying current team membership."""
        with self.sessions() as session:
            if session.get(ReviewGroupMember, (group_id, reviewer_id)) is None:
                raise WorkshopAccessError("Team is not available")
            return session.scalar(
                select(ReviewAssignment.id)
                .where(
                    ReviewAssignment.review_group_id == group_id,
                    ReviewAssignment.participant_status.in_(_ACTIVE_PARTICIPANT_STATUSES),
                )
                .order_by(ReviewAssignment.created_at.desc())
                .limit(1)
            )

    def workbench_team(self, reviewer_id: str, assignment_id: str) -> dict[str, object]:
        """Return the minimal participant-facing team context for a workbench."""
        with self.sessions() as session:
            assignment = session.get(ReviewAssignment, assignment_id)
            if assignment is None or assignment.review_group_id is None:
                raise WorkshopAccessError("Team review is not available")
            if session.get(
                ReviewGroupMember, (assignment.review_group_id, reviewer_id)
            ) is None:
                raise WorkshopAccessError("Team review is not available")
            members = list(
                session.scalars(
                    select(ReviewGroupMember.reviewer_id).where(
                        ReviewGroupMember.group_id == assignment.review_group_id
                    )
                )
            )
            missing = [
                member_id
                for member_id in members
                if not self.assignments.assessment_is_complete(
                    session, assignment, member_id
                )
            ]
            return {
                "join_code": self._join_code(assignment.review_group_id),
                "member_count": len(members),
                "member_reviewer_ids": sorted(members),
                "batch_name": (
                    package.display_name
                    if assignment.work_package_id
                    and (
                        package := session.get(
                            WorkshopWorkPackage, assignment.work_package_id
                        )
                    )
                    is not None
                    else None
                ),
                "missing_assessment_reviewer_ids": sorted(missing),
            }

    def add_assignment_member(
        self, actor_id: str, assignment_id: str, teammate_id: str
    ) -> bool:
        """Add a consenting workshop participant to an open team assignment."""
        teammate_id = teammate_id.strip()
        if not _REVIEWER_ID.fullmatch(teammate_id):
            raise WorkshopAccessError("Reviewer number is not available")
        now = timestamp(utc_now())
        session = self.sessions()
        try:
            session.execute(text("BEGIN IMMEDIATE"))
            actor = session.get(Reviewer, actor_id)
            teammate = session.get(Reviewer, teammate_id)
            self._require_eligible(actor)
            if teammate_id == actor_id or not self._eligible(teammate):
                raise WorkshopAccessError("Reviewer number is not available")
            assignment = session.get(ReviewAssignment, assignment_id)
            if (
                assignment is None
                or assignment.review_group_id is None
                or assignment.participant_status not in _ACTIVE_PARTICIPANT_STATUSES
                or assignment.closed_contributor_ids is not None
            ):
                raise WorkshopAccessError("Team review is not open to new members")
            group = session.get(ReviewGroup, assignment.review_group_id)
            actor_member = session.get(
                ReviewGroupMember, (assignment.review_group_id, actor_id)
            )
            workshop_round = self._open_round(session, now)
            teammate_enrolled = session.scalar(
                select(ReviewGroupMember.reviewer_id)
                .join(ReviewGroup, ReviewGroup.id == ReviewGroupMember.group_id)
                .where(
                    ReviewGroupMember.reviewer_id == teammate_id,
                    ReviewGroup.workshop_round_id == workshop_round.id,
                )
                .limit(1)
            )
            if (
                group is None
                or actor_member is None
                or group.workshop_round_id != workshop_round.id
                or teammate_enrolled is None
            ):
                raise WorkshopAccessError("Reviewer number is not available")
            existing = session.get(
                ReviewGroupMember, (assignment.review_group_id, teammate_id)
            )
            if existing is not None:
                session.commit()
                return False
            session.add(
                ReviewGroupMember(
                    group_id=assignment.review_group_id,
                    reviewer_id=teammate_id,
                    joined_at=now,
                )
            )
            session.commit()
            return True
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def claim_package(
        self, *, reviewer_id: str, group_id: str, package_id: str
    ) -> str:
        """Atomically claim a reusable package for one reviewing group."""
        with self.sessions() as session:
            package = session.get(WorkshopWorkPackage, package_id)
            if package is None:
                raise WorkshopAccessError("Work package is not available")
            package_values = (
                package.workshop_round_id,
                package.kg_id,
                package.seed_version,
                package.seed_digest,
                package.bundle_path,
                package.bundle_digest,
                package.processing_recipe,
                package.enabled,
            )
        payload, relative_path, bundle_digest = self.assignments.load_neutral_bundle(
            package_values[4]
        )
        bundle_mode = str(payload.get("mode") or "initial")
        bundle_kg_ids = {
            str(record.get("kg_id") or "")
            for record in payload["records"]
            if isinstance(record, dict)
        }
        if (
            bundle_mode != "initial"
            or bundle_digest != package_values[5]
            or relative_path != package_values[4]
            or bundle_kg_ids != {package_values[1]}
        ):
            raise ValueError("Work package does not match its frozen bundle")

        now = timestamp(utc_now())
        db_session = self.sessions()
        try:
            db_session.execute(text("BEGIN IMMEDIATE"))
            reviewer = db_session.get(Reviewer, reviewer_id)
            self._require_eligible(reviewer)
            workshop_round = self._open_round(db_session, now)
            group = db_session.get(ReviewGroup, group_id)
            package = db_session.get(WorkshopWorkPackage, package_id)
            member = db_session.get(ReviewGroupMember, (group_id, reviewer_id))
            if (
                group is None
                or member is None
                or group.workshop_round_id != workshop_round.id
                or package is None
                or package.workshop_round_id != workshop_round.id
                or not package.enabled
                or package_values
                != (
                    package.workshop_round_id,
                    package.kg_id,
                    package.seed_version,
                    package.seed_digest,
                    package.bundle_path,
                    package.bundle_digest,
                    package.processing_recipe,
                    package.enabled,
                )
            ):
                raise WorkshopAccessError("Work package is not available")
            existing = list(
                db_session.scalars(
                    select(ReviewAssignment).where(
                        ReviewAssignment.review_group_id == group_id
                    )
                )
            )
            if any(item.work_package_id == package_id for item in existing):
                raise WorkshopAccessError("The group already has this work package")

            prompt_count = int(
                db_session.scalar(
                    select(func.count())
                    .select_from(KgSeedReviewDomain)
                    .where(
                        KgSeedReviewDomain.kg_id == package.kg_id,
                        KgSeedReviewDomain.seed_version == package.seed_version,
                    )
                )
                or 0
            ) + int(
                db_session.scalar(
                    select(func.count())
                    .select_from(KgSeedFamiliarityScope)
                    .where(
                        KgSeedFamiliarityScope.kg_id == package.kg_id,
                        KgSeedFamiliarityScope.seed_version == package.seed_version,
                    )
                )
                or 0
            )
            if prompt_count == 0:
                raise WorkshopAccessError(
                    "Work package has no frozen assessment prompts"
                )

            assignment_id = "assignment-" + secrets.token_hex(12)
            assignment = ReviewAssignment(
                id=assignment_id,
                reviewer_id=None,
                review_group_id=group_id,
                work_package_id=package_id,
                participant_status="not_started",
                claimed_at=now,
                completed_at=None,
                completion_item_count=None,
                completion_total_count=None,
                closed_contributor_ids=None,
                mode="initial",
                status="ready",
                bundle_path=package.bundle_path,
                bundle_digest=package.bundle_digest,
                previous_benchmark_path=None,
                processing_recipe=package.processing_recipe,
                holdout_capability=False,
                created_at=now,
                opened_at=None,
                submitted_at=None,
            )
            db_session.add(assignment)
            db_session.flush()
            db_session.add(
                AssignmentKgSeed(
                    assignment_id=assignment_id,
                    kg_id=package.kg_id,
                    seed_version=package.seed_version,
                    seed_digest=package.seed_digest,
                )
            )
            db_session.commit()
            return assignment_id
        except Exception:
            db_session.rollback()
            raise
        finally:
            db_session.close()

    def abandon_assignment(self, *, reviewer_id: str, assignment_id: str) -> None:
        """Atomically close a workshop assignment without creating a submission."""
        now = timestamp(utc_now())
        session = self.sessions()
        try:
            session.execute(text("BEGIN IMMEDIATE"))
            reviewer = session.get(Reviewer, reviewer_id)
            self._require_eligible(reviewer)
            workshop_round = self._open_round(session, now)
            assignment = session.get(ReviewAssignment, assignment_id)
            if (
                assignment is None
                or assignment.review_group_id is None
                or assignment.status not in {"ready", "active"}
                or assignment.participant_status not in _ACTIVE_PARTICIPANT_STATUSES
            ):
                raise WorkshopAccessError("Assignment is not open for abandonment")
            if session.scalar(
                select(ReviewSubmission.id)
                .where(ReviewSubmission.assignment_id == assignment_id)
                .limit(1)
            ) is not None:
                raise WorkshopAccessError("A submitted assignment cannot be abandoned")
            group = session.get(ReviewGroup, assignment.review_group_id)
            member = session.get(
                ReviewGroupMember, (assignment.review_group_id, reviewer_id)
            )
            if (
                group is None
                or member is None
                or group.workshop_round_id != workshop_round.id
            ):
                raise WorkshopAccessError("Assignment is not open for abandonment")
            contributor_ids = tuple(
                sorted(
                    session.scalars(
                        select(ReviewGroupMember.reviewer_id).where(
                            ReviewGroupMember.group_id == assignment.review_group_id
                        )
                    ).all()
                )
            )
            if not contributor_ids:
                raise WorkshopAccessError("Assignment has no contributors")
            assignment.participant_status = "abandoned"
            assignment.completed_at = now
            assignment.closed_contributor_ids = list(contributor_ids)
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def _eligible(self, reviewer: Reviewer | None) -> bool:
        return has_current_consent(
            reviewer,
            self.current_consent_version,
            self.current_notice_version,
        )

    def _require_eligible(self, reviewer: Reviewer | None) -> None:
        if not self._eligible(reviewer):
            raise WorkshopAccessError("Current consent is required for workshop access")

    @staticmethod
    def _open_rounds(session: Session, now: str) -> list[WorkshopRound]:
        return list(
            session.scalars(
                select(WorkshopRound)
                .where(
                    WorkshopRound.status == "open",
                    WorkshopRound.opens_at <= now,
                    WorkshopRound.closes_at > now,
                )
                .order_by(WorkshopRound.opens_at, WorkshopRound.id)
            )
        )

    @staticmethod
    def _latest_joined_round(
        session: Session, reviewer_id: str
    ) -> WorkshopRound | None:
        return session.scalar(
            select(WorkshopRound)
            .join(ReviewGroup, ReviewGroup.workshop_round_id == WorkshopRound.id)
            .join(ReviewGroupMember, ReviewGroupMember.group_id == ReviewGroup.id)
            .where(ReviewGroupMember.reviewer_id == reviewer_id)
            .order_by(WorkshopRound.created_at.desc(), WorkshopRound.id.desc())
            .limit(1)
        )

    @classmethod
    def _open_round(cls, session: Session, now: str) -> WorkshopRound:
        rounds = cls._open_rounds(session, now)
        if len(rounds) != 1:
            raise WorkshopUnavailable("Exactly one workshop round must be open")
        return rounds[0]

    def _join_code(self, group_id: str) -> str:
        raw = hmac.digest(
            self.secret, b"review-group-join-code\0" + group_id.encode(), "sha256"
        )
        return f"{int.from_bytes(raw[:8], 'big') % 1_000_000:06d}"

    def _join_digest(self, code: str) -> str:
        return "sha256:" + hmac.new(
            self.secret,
            b"review-group-join-digest\0" + self._normalize_join_code(code).encode(),
            hashlib.sha256,
        ).hexdigest()

    @staticmethod
    def _normalize_join_code(code: str) -> str:
        return "".join(character for character in code if character.isdigit())
