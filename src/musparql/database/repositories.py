"""Persistence-only repositories; validation and workflow rules live in services."""
from __future__ import annotations

from collections.abc import Iterable

from sqlalchemy import select
from sqlalchemy.orm import Session

from .models import (
    AssignmentKgSeed,
    ExpertiseDomain,
    KgSeedFamiliarityScope,
    KgSeedReviewDomain,
    KgSeedSnapshot,
    ReviewAssignment,
    Reviewer,
    ReviewerDomainExpertise,
    ReviewerKgDomainAssessment,
    ReviewerResourceFamiliarityAssessment,
)


class ReviewerRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def add(self, reviewer: Reviewer) -> None:
        self.session.add(reviewer)

    def get(self, reviewer_id: str) -> Reviewer | None:
        return self.session.get(Reviewer, reviewer_id)


class SeedRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def add_snapshot(
        self,
        snapshot: KgSeedSnapshot,
        domains: Iterable[KgSeedReviewDomain],
        scopes: Iterable[KgSeedFamiliarityScope],
    ) -> None:
        self.session.add(snapshot)
        self.session.flush()
        self.session.add_all(list(domains))
        self.session.add_all(list(scopes))

    def get(self, kg_id: str, seed_version: str) -> KgSeedSnapshot | None:
        return self.session.get(KgSeedSnapshot, (kg_id, seed_version))


class AssignmentRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def add(self, assignment: ReviewAssignment, seeds: Iterable[AssignmentKgSeed]) -> None:
        self.session.add(assignment)
        self.session.flush()
        self.session.add_all(list(seeds))


class ProvenanceRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def expertise_domain(self, domain_id: str) -> ExpertiseDomain | None:
        return self.session.get(ExpertiseDomain, domain_id)

    def expertise_assertions(
        self, reviewer_id: str, domain_id: str
    ) -> list[ReviewerDomainExpertise]:
        return list(
            self.session.scalars(
                select(ReviewerDomainExpertise)
                .where(
                    ReviewerDomainExpertise.reviewer_id == reviewer_id,
                    ReviewerDomainExpertise.domain_id == domain_id,
                )
                .order_by(ReviewerDomainExpertise.asserted_at)
            )
        )

    def domain_assessments(
        self, reviewer_id: str, kg_id: str, domain_id: str
    ) -> list[ReviewerKgDomainAssessment]:
        return list(
            self.session.scalars(
                select(ReviewerKgDomainAssessment)
                .where(
                    ReviewerKgDomainAssessment.reviewer_id == reviewer_id,
                    ReviewerKgDomainAssessment.kg_id == kg_id,
                    ReviewerKgDomainAssessment.review_domain_id == domain_id,
                )
                .order_by(ReviewerKgDomainAssessment.assessed_at)
            )
        )

    def familiarity_assessments(
        self, reviewer_id: str, kg_id: str, scope_id: str
    ) -> list[ReviewerResourceFamiliarityAssessment]:
        return list(
            self.session.scalars(
                select(ReviewerResourceFamiliarityAssessment)
                .where(
                    ReviewerResourceFamiliarityAssessment.reviewer_id == reviewer_id,
                    ReviewerResourceFamiliarityAssessment.kg_id == kg_id,
                    ReviewerResourceFamiliarityAssessment.familiarity_scope_id == scope_id,
                )
                .order_by(ReviewerResourceFamiliarityAssessment.assessed_at)
            )
        )

    def add(self, value: object) -> None:
        self.session.add(value)
