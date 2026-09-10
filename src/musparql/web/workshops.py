"""IPL workshop reviewing-group and reusable-package workflow."""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import hmac
import secrets

from sqlalchemy import func, select, text
from sqlalchemy.orm import Session, sessionmaker

from musparql.database.models import (
    AssignmentKgSeed,
    ReviewAssignment,
    ReviewGroup,
    ReviewGroupMember,
    Reviewer,
    WorkshopRound,
    WorkshopWorkPackage,
)

from .assignments import AssignmentService
from .auth import timestamp, utc_now


_ACTIVE_PARTICIPANT_STATUSES = ("not_started", "active")
_JOIN_CODE_ALPHABET = "23456789ABCDEFGHJKLMNPQRSTUVWXYZ"


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


@dataclass(frozen=True)
class ReviewGroupView:
    id: str
    join_code: str
    member_count: int
    active_assignment_id: str | None
    active_package_name: str | None
    can_claim: bool


@dataclass(frozen=True)
class WorkshopView:
    round_id: str
    round_name: str
    allow_additional_assignments: bool
    groups: tuple[ReviewGroupView, ...]
    packages: tuple[WorkPackageView, ...]


class WorkshopService:
    def __init__(
        self,
        *,
        sessions: sessionmaker[Session],
        assignments: AssignmentService,
        secret: bytes,
    ) -> None:
        self.sessions = sessions
        self.assignments = assignments
        self.secret = secret

    def available(self, reviewer_id: str) -> bool:
        """Return whether a consented reviewer has one currently open round."""
        now = timestamp(utc_now())
        with self.sessions() as session:
            reviewer = session.get(Reviewer, reviewer_id)
            return self._eligible(reviewer) and len(self._open_rounds(session, now)) == 1

    def dashboard(self, reviewer_id: str) -> WorkshopView:
        now = timestamp(utc_now())
        with self.sessions() as session:
            reviewer = session.get(Reviewer, reviewer_id)
            self._require_eligible(reviewer)
            workshop_round = self._open_round(session, now)
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
            for group in groups:
                assignments = list(
                    session.scalars(
                        select(ReviewAssignment)
                        .where(ReviewAssignment.review_group_id == group.id)
                        .order_by(ReviewAssignment.created_at.desc())
                    )
                )
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
                group_views.append(
                    ReviewGroupView(
                        id=group.id,
                        join_code=self._join_code(group.id),
                        member_count=member_count,
                        active_assignment_id=active.id if active else None,
                        active_package_name=(
                            package_names.get(active.work_package_id) if active else None
                        ),
                        can_claim=active is None
                        and (
                            not assignments
                            or workshop_round.allow_additional_assignments
                        ),
                    )
                )
            packages = tuple(
                WorkPackageView(
                    id=package.id,
                    display_name=package.display_name,
                    short_description=package.short_description,
                    kg_id=package.kg_id,
                )
                for package in session.scalars(
                    select(WorkshopWorkPackage)
                    .where(
                        WorkshopWorkPackage.workshop_round_id == workshop_round.id,
                        WorkshopWorkPackage.enabled.is_(True),
                    )
                    .order_by(WorkshopWorkPackage.display_order)
                )
            )
            return WorkshopView(
                round_id=workshop_round.id,
                round_name=workshop_round.name,
                allow_additional_assignments=workshop_round.allow_additional_assignments,
                groups=tuple(group_views),
                packages=packages,
            )

    def create_group(self, reviewer_id: str) -> str:
        now = timestamp(utc_now())
        session = self.sessions()
        try:
            session.execute(text("BEGIN IMMEDIATE"))
            reviewer = session.get(Reviewer, reviewer_id)
            self._require_eligible(reviewer)
            workshop_round = self._open_round(session, now)
            group_id = "group-" + secrets.token_hex(12)
            join_code = self._join_code(group_id)
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
        if len(normalized) != 10:
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
            if assignments and not any(
                item.participant_status in _ACTIVE_PARTICIPANT_STATUSES
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
            if any(
                item.participant_status in _ACTIVE_PARTICIPANT_STATUSES
                for item in existing
            ):
                raise WorkshopAccessError("The group already has an active assignment")
            if existing and not workshop_round.allow_additional_assignments:
                raise WorkshopAccessError("Additional assignments are not enabled")

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

    @staticmethod
    def _eligible(reviewer: Reviewer | None) -> bool:
        return bool(
            reviewer is not None
            and reviewer.status == "active"
            and reviewer.consent_statement_version
            and reviewer.consented_at
        )

    @classmethod
    def _require_eligible(cls, reviewer: Reviewer | None) -> None:
        if not cls._eligible(reviewer):
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
        value = int.from_bytes(raw[:8], "big")
        characters: list[str] = []
        for _ in range(10):
            value, index = divmod(value, len(_JOIN_CODE_ALPHABET))
            characters.append(_JOIN_CODE_ALPHABET[index])
        return "".join(characters)

    def _join_digest(self, code: str) -> str:
        return "sha256:" + hmac.new(
            self.secret,
            b"review-group-join-digest\0" + self._normalize_join_code(code).encode(),
            hashlib.sha256,
        ).hexdigest()

    @staticmethod
    def _normalize_join_code(code: str) -> str:
        return "".join(character for character in code.upper() if character.isalnum())
