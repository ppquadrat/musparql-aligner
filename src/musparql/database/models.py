"""SQLAlchemy models for confidential profiles and portal operations."""
from __future__ import annotations

from typing import Any

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    JSON,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


TECHNICAL_LEVELS = "'none','occasional','regular','expert'"
SUBJECT_LEVELS = "'none','basic','working','advanced','expert'"
FAMILIARITY_LEVELS = "'none','inspected','worked','regular_user','creator'"
PROCESSING_RECIPES = (
    "'validate_initial_review','stage_initial_benchmark_update',"
    "'validate_comparative_review','stage_comparative_benchmark_update'"
)


class Base(DeclarativeBase):
    pass


class Reviewer(Base):
    __tablename__ = "reviewers"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    name: Mapped[str] = mapped_column(Text)
    affiliation: Mapped[str] = mapped_column(Text, default="")
    email_display: Mapped[str] = mapped_column(Text)
    email_normalized: Mapped[str] = mapped_column(Text, unique=True)
    status: Mapped[str] = mapped_column(String)
    created_at: Mapped[str] = mapped_column(String)
    updated_at: Mapped[str] = mapped_column(String)
    privacy_notice_version: Mapped[str | None] = mapped_column(String, nullable=True)
    privacy_notice_acknowledged_at: Mapped[str | None] = mapped_column(String, nullable=True)
    __table_args__ = (
        CheckConstraint(
            "length(id) >= 13 AND substr(id, 1, 9) = 'reviewer-' "
            "AND substr(id, 10) NOT GLOB '*[^0-9]*'",
            name="ck_reviewers_id",
        ),
        CheckConstraint("status IN ('invited','active','disabled','withdrawn')", name="ck_reviewers_status"),
    )


class ReviewerExperience(Base):
    __tablename__ = "reviewer_experience"
    reviewer_id: Mapped[str] = mapped_column(ForeignKey("reviewers.id"), primary_key=True)
    kg_ontology_experience: Mapped[str] = mapped_column(String)
    sparql_experience: Mapped[str] = mapped_column(String)
    nlp_llm_experience: Mapped[str] = mapped_column(String)
    assessed_at: Mapped[str] = mapped_column(String)
    __table_args__ = tuple(
        CheckConstraint(f"{field} IN ({TECHNICAL_LEVELS})", name=f"ck_reviewer_experience_{field}")
        for field in ("kg_ontology_experience", "sparql_experience", "nlp_llm_experience")
    )


class ReviewerLanguage(Base):
    __tablename__ = "reviewer_languages"
    reviewer_id: Mapped[str] = mapped_column(ForeignKey("reviewers.id"), primary_key=True)
    language_tag: Mapped[str] = mapped_column(String, primary_key=True)
    level: Mapped[str] = mapped_column(String)
    first_asserted_at: Mapped[str] = mapped_column(String)
    updated_at: Mapped[str] = mapped_column(String)
    __table_args__ = (
        CheckConstraint("level IN ('basic','advanced','fluent','native')", name="ck_reviewer_languages_level"),
    )


class ExpertiseDomain(Base):
    __tablename__ = "expertise_domains"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    entered_label: Mapped[str] = mapped_column(Text)
    normalized_label: Mapped[str] = mapped_column(Text)
    vocabulary_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    vocabulary_concept_uri: Mapped[str | None] = mapped_column(Text, nullable=True)
    vocabulary_version: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_by: Mapped[str] = mapped_column(String)
    __table_args__ = (
        CheckConstraint(
            "(vocabulary_name IS NULL AND vocabulary_concept_uri IS NULL AND vocabulary_version IS NULL) OR "
            "(vocabulary_name IS NOT NULL AND vocabulary_concept_uri IS NOT NULL AND vocabulary_version IS NOT NULL)",
            name="ck_expertise_domains_vocabulary",
        ),
        CheckConstraint("created_by IN ('reviewer','owner')", name="ck_expertise_domains_created_by"),
    )


class ReviewerDomainExpertise(Base):
    __tablename__ = "reviewer_domain_expertise"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    reviewer_id: Mapped[str] = mapped_column(ForeignKey("reviewers.id"))
    domain_id: Mapped[str] = mapped_column(ForeignKey("expertise_domains.id"))
    expertise_level: Mapped[str] = mapped_column(String)
    asserted_at: Mapped[str] = mapped_column(String)
    supersedes_id: Mapped[str | None] = mapped_column(String, nullable=True, unique=True)
    __table_args__ = (
        UniqueConstraint("id", "reviewer_id", "domain_id", name="uq_domain_assertion_subject"),
        ForeignKeyConstraint(
            ["supersedes_id", "reviewer_id", "domain_id"],
            ["reviewer_domain_expertise.id", "reviewer_domain_expertise.reviewer_id", "reviewer_domain_expertise.domain_id"],
            name="fk_domain_assertion_predecessor_subject",
        ),
        CheckConstraint(f"expertise_level IN ({SUBJECT_LEVELS})", name="ck_domain_assertion_level"),
        CheckConstraint("supersedes_id IS NULL OR supersedes_id <> id", name="ck_domain_assertion_not_self"),
        Index(
            "uq_domain_assertion_root",
            "reviewer_id",
            "domain_id",
            unique=True,
            sqlite_where=text("supersedes_id IS NULL"),
        ),
    )


class KgSeedSnapshot(Base):
    __tablename__ = "kg_seed_snapshots"
    kg_id: Mapped[str] = mapped_column(String, primary_key=True)
    seed_version: Mapped[str] = mapped_column(String, primary_key=True)
    seed_digest: Mapped[str] = mapped_column(String, unique=True)
    previous_seed_digest: Mapped[str | None] = mapped_column(String, nullable=True, unique=True)
    seed_json: Mapped[dict[str, Any]] = mapped_column(JSON)
    __table_args__ = (
        UniqueConstraint("kg_id", "seed_version", "seed_digest", name="uq_seed_version_digest"),
        UniqueConstraint("seed_digest", "kg_id", name="uq_seed_digest_kg"),
        ForeignKeyConstraint(
            ["previous_seed_digest", "kg_id"],
            ["kg_seed_snapshots.seed_digest", "kg_seed_snapshots.kg_id"],
            name="fk_seed_predecessor_kg",
        ),
        CheckConstraint("seed_digest LIKE 'sha256:%'", name="ck_seed_digest_format"),
        CheckConstraint(
            "previous_seed_digest IS NULL OR previous_seed_digest <> seed_digest",
            name="ck_seed_not_self",
        ),
        Index(
            "uq_seed_snapshot_root",
            "kg_id",
            unique=True,
            sqlite_where=text("previous_seed_digest IS NULL"),
        ),
    )


class KgSeedReviewDomain(Base):
    __tablename__ = "kg_seed_review_domains"
    kg_id: Mapped[str] = mapped_column(String, primary_key=True)
    seed_version: Mapped[str] = mapped_column(String, primary_key=True)
    domain_id: Mapped[str] = mapped_column(String, primary_key=True)
    label: Mapped[str] = mapped_column(Text)
    description: Mapped[str] = mapped_column(Text)
    __table_args__ = (
        ForeignKeyConstraint(["kg_id", "seed_version"], ["kg_seed_snapshots.kg_id", "kg_seed_snapshots.seed_version"]),
        UniqueConstraint("kg_id", "seed_version", "domain_id", "label", name="uq_seed_domain_label"),
    )


class KgSeedFamiliarityScope(Base):
    __tablename__ = "kg_seed_familiarity_scopes"
    kg_id: Mapped[str] = mapped_column(String, primary_key=True)
    seed_version: Mapped[str] = mapped_column(String, primary_key=True)
    scope_id: Mapped[str] = mapped_column(String, primary_key=True)
    label: Mapped[str] = mapped_column(Text)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    __table_args__ = (
        ForeignKeyConstraint(["kg_id", "seed_version"], ["kg_seed_snapshots.kg_id", "kg_seed_snapshots.seed_version"]),
        UniqueConstraint("kg_id", "seed_version", "scope_id", "label", name="uq_seed_scope_label"),
    )


class LoginCode(Base):
    __tablename__ = "login_codes"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    email_normalized: Mapped[str] = mapped_column(Text)
    code_hash: Mapped[str] = mapped_column(Text)
    requested_at: Mapped[str] = mapped_column(String)
    expires_at: Mapped[str] = mapped_column(String)
    consumed_at: Mapped[str | None] = mapped_column(String, nullable=True)
    failed_attempt_count: Mapped[int] = mapped_column(Integer, default=0)
    request_context_digest: Mapped[str] = mapped_column(Text)
    __table_args__ = (CheckConstraint("failed_attempt_count >= 0", name="ck_login_codes_attempts"),)


class AuthSession(Base):
    __tablename__ = "auth_sessions"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    reviewer_id: Mapped[str] = mapped_column(ForeignKey("reviewers.id"), index=True)
    token_hash: Mapped[str] = mapped_column(Text, unique=True)
    created_at: Mapped[str] = mapped_column(String)
    last_used_at: Mapped[str] = mapped_column(String)
    expires_at: Mapped[str] = mapped_column(String)
    revoked_at: Mapped[str | None] = mapped_column(String, nullable=True)
    remembered: Mapped[bool] = mapped_column(Boolean)


class ReviewAssignment(Base):
    __tablename__ = "review_assignments"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    reviewer_id: Mapped[str] = mapped_column(ForeignKey("reviewers.id"))
    mode: Mapped[str] = mapped_column(String)
    status: Mapped[str] = mapped_column(String)
    bundle_path: Mapped[str] = mapped_column(Text)
    bundle_digest: Mapped[str] = mapped_column(String)
    previous_benchmark_path: Mapped[str | None] = mapped_column(Text, nullable=True)
    processing_recipe: Mapped[str] = mapped_column(String)
    holdout_capability: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[str] = mapped_column(String)
    opened_at: Mapped[str | None] = mapped_column(String, nullable=True)
    submitted_at: Mapped[str | None] = mapped_column(String, nullable=True)
    __table_args__ = (
        UniqueConstraint("id", "reviewer_id", name="uq_assignment_reviewer"),
        UniqueConstraint("id", "processing_recipe", name="uq_assignment_recipe"),
        CheckConstraint("mode IN ('initial','compare','sparql_correction')", name="ck_assignment_mode"),
        CheckConstraint(
            "status IN ('draft','ready','active','submitted','processing','ready_for_owner_review','approved','failed')",
            name="ck_assignment_status",
        ),
        CheckConstraint("holdout_capability = 0", name="ck_assignment_no_holdout"),
        CheckConstraint("bundle_digest LIKE 'sha256:%'", name="ck_assignment_bundle_digest"),
        CheckConstraint(
            f"processing_recipe IN ({PROCESSING_RECIPES})", name="ck_assignment_recipe"
        ),
    )


class AssignmentKgSeed(Base):
    __tablename__ = "assignment_kg_seeds"
    assignment_id: Mapped[str] = mapped_column(ForeignKey("review_assignments.id"), primary_key=True)
    kg_id: Mapped[str] = mapped_column(String, primary_key=True)
    seed_version: Mapped[str] = mapped_column(String)
    seed_digest: Mapped[str] = mapped_column(String)
    __table_args__ = (
        UniqueConstraint("assignment_id", "kg_id", "seed_version", name="uq_assignment_seed_version"),
        ForeignKeyConstraint(
            ["kg_id", "seed_version", "seed_digest"],
            ["kg_seed_snapshots.kg_id", "kg_seed_snapshots.seed_version", "kg_seed_snapshots.seed_digest"],
            name="fk_assignment_seed_snapshot",
        ),
    )


class ReviewerKgDomainAssessment(Base):
    __tablename__ = "reviewer_kg_domain_assessments"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    reviewer_id: Mapped[str] = mapped_column(ForeignKey("reviewers.id"))
    kg_id: Mapped[str] = mapped_column(String)
    review_domain_id: Mapped[str] = mapped_column(String)
    review_domain_label: Mapped[str] = mapped_column(Text)
    subject_expertise_level: Mapped[str] = mapped_column(String)
    assessed_at: Mapped[str] = mapped_column(String)
    context: Mapped[str] = mapped_column(String)
    assignment_id: Mapped[str | None] = mapped_column(String, nullable=True)
    seed_version: Mapped[str] = mapped_column(String)
    previous_assessment_id: Mapped[str | None] = mapped_column(String, nullable=True, unique=True)
    __table_args__ = (
        UniqueConstraint("id", "reviewer_id", "kg_id", "review_domain_id", name="uq_domain_assessment_subject"),
        ForeignKeyConstraint(
            ["previous_assessment_id", "reviewer_id", "kg_id", "review_domain_id"],
            ["reviewer_kg_domain_assessments.id", "reviewer_kg_domain_assessments.reviewer_id", "reviewer_kg_domain_assessments.kg_id", "reviewer_kg_domain_assessments.review_domain_id"],
            name="fk_domain_assessment_predecessor_subject",
        ),
        ForeignKeyConstraint(["assignment_id", "reviewer_id"], ["review_assignments.id", "review_assignments.reviewer_id"]),
        ForeignKeyConstraint(
            ["assignment_id", "kg_id", "seed_version"],
            ["assignment_kg_seeds.assignment_id", "assignment_kg_seeds.kg_id", "assignment_kg_seeds.seed_version"],
            name="fk_domain_assessment_assignment_seed",
        ),
        ForeignKeyConstraint(
            ["kg_id", "seed_version", "review_domain_id", "review_domain_label"],
            ["kg_seed_review_domains.kg_id", "kg_seed_review_domains.seed_version", "kg_seed_review_domains.domain_id", "kg_seed_review_domains.label"],
            name="fk_domain_assessment_seed_prompt",
        ),
        CheckConstraint(f"subject_expertise_level IN ({SUBJECT_LEVELS})", name="ck_domain_assessment_level"),
        CheckConstraint("context IN ('pre_review','profile')", name="ck_domain_assessment_context"),
        CheckConstraint(
            "(context = 'pre_review' AND assignment_id IS NOT NULL) OR (context = 'profile' AND assignment_id IS NULL)",
            name="ck_domain_assessment_assignment",
        ),
        CheckConstraint("previous_assessment_id IS NULL OR previous_assessment_id <> id", name="ck_domain_assessment_not_self"),
        Index(
            "uq_domain_assessment_root",
            "reviewer_id", "kg_id", "review_domain_id",
            unique=True,
            sqlite_where=text("previous_assessment_id IS NULL"),
        ),
    )


class ReviewerResourceFamiliarityAssessment(Base):
    __tablename__ = "reviewer_resource_familiarity_assessments"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    reviewer_id: Mapped[str] = mapped_column(ForeignKey("reviewers.id"))
    kg_id: Mapped[str] = mapped_column(String)
    familiarity_scope_id: Mapped[str] = mapped_column(String)
    familiarity_scope_label: Mapped[str] = mapped_column(Text)
    familiarity_level: Mapped[str] = mapped_column(String)
    assessed_at: Mapped[str] = mapped_column(String)
    context: Mapped[str] = mapped_column(String)
    assignment_id: Mapped[str | None] = mapped_column(String, nullable=True)
    seed_version: Mapped[str] = mapped_column(String)
    previous_assessment_id: Mapped[str | None] = mapped_column(String, nullable=True, unique=True)
    __table_args__ = (
        UniqueConstraint("id", "reviewer_id", "kg_id", "familiarity_scope_id", name="uq_familiarity_assessment_subject"),
        ForeignKeyConstraint(
            ["previous_assessment_id", "reviewer_id", "kg_id", "familiarity_scope_id"],
            ["reviewer_resource_familiarity_assessments.id", "reviewer_resource_familiarity_assessments.reviewer_id", "reviewer_resource_familiarity_assessments.kg_id", "reviewer_resource_familiarity_assessments.familiarity_scope_id"],
            name="fk_familiarity_assessment_predecessor_subject",
        ),
        ForeignKeyConstraint(["assignment_id", "reviewer_id"], ["review_assignments.id", "review_assignments.reviewer_id"]),
        ForeignKeyConstraint(
            ["assignment_id", "kg_id", "seed_version"],
            ["assignment_kg_seeds.assignment_id", "assignment_kg_seeds.kg_id", "assignment_kg_seeds.seed_version"],
            name="fk_familiarity_assessment_assignment_seed",
        ),
        ForeignKeyConstraint(
            ["kg_id", "seed_version", "familiarity_scope_id", "familiarity_scope_label"],
            ["kg_seed_familiarity_scopes.kg_id", "kg_seed_familiarity_scopes.seed_version", "kg_seed_familiarity_scopes.scope_id", "kg_seed_familiarity_scopes.label"],
            name="fk_familiarity_assessment_seed_prompt",
        ),
        CheckConstraint(f"familiarity_level IN ({FAMILIARITY_LEVELS})", name="ck_familiarity_assessment_level"),
        CheckConstraint("context IN ('pre_review','profile')", name="ck_familiarity_assessment_context"),
        CheckConstraint(
            "(context = 'pre_review' AND assignment_id IS NOT NULL) OR (context = 'profile' AND assignment_id IS NULL)",
            name="ck_familiarity_assessment_assignment",
        ),
        CheckConstraint("previous_assessment_id IS NULL OR previous_assessment_id <> id", name="ck_familiarity_assessment_not_self"),
        Index(
            "uq_familiarity_assessment_root",
            "reviewer_id", "kg_id", "familiarity_scope_id",
            unique=True,
            sqlite_where=text("previous_assessment_id IS NULL"),
        ),
    )


class ReviewSubmission(Base):
    __tablename__ = "review_submissions"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    assignment_id: Mapped[str] = mapped_column(String)
    reviewer_id: Mapped[str] = mapped_column(String)
    export_path: Mapped[str] = mapped_column(Text)
    export_digest: Mapped[str] = mapped_column(String)
    submitted_at: Mapped[str] = mapped_column(String)
    revision: Mapped[int] = mapped_column(Integer)
    validation_status: Mapped[str] = mapped_column(String)
    __table_args__ = (
        ForeignKeyConstraint(["assignment_id", "reviewer_id"], ["review_assignments.id", "review_assignments.reviewer_id"]),
        UniqueConstraint("id", "assignment_id", name="uq_submission_assignment"),
        UniqueConstraint("assignment_id", "revision", name="uq_submission_revision"),
        UniqueConstraint("assignment_id", "export_digest", name="uq_submission_retry"),
        CheckConstraint("revision >= 1", name="ck_submission_revision"),
        CheckConstraint("export_digest LIKE 'sha256:%'", name="ck_submission_digest"),
    )


class ProcessingJob(Base):
    __tablename__ = "processing_jobs"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    assignment_id: Mapped[str] = mapped_column(ForeignKey("review_assignments.id"))
    submission_id: Mapped[str] = mapped_column(String)
    recipe: Mapped[str] = mapped_column(String)
    status: Mapped[str] = mapped_column(String)
    created_at: Mapped[str] = mapped_column(String)
    started_at: Mapped[str | None] = mapped_column(String, nullable=True)
    finished_at: Mapped[str | None] = mapped_column(String, nullable=True)
    safe_summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    candidate_output_path: Mapped[str | None] = mapped_column(Text, nullable=True)
    __table_args__ = (
        ForeignKeyConstraint(
            ["assignment_id", "recipe"],
            ["review_assignments.id", "review_assignments.processing_recipe"],
            name="fk_processing_job_assignment_recipe",
        ),
        ForeignKeyConstraint(
            ["submission_id", "assignment_id"],
            ["review_submissions.id", "review_submissions.assignment_id"],
            name="fk_processing_job_submission_assignment",
        ),
        CheckConstraint("status IN ('queued','running','succeeded','failed')", name="ck_processing_job_status"),
    )


APPEND_ONLY_TABLES = (
    "kg_seed_snapshots",
    "kg_seed_review_domains",
    "kg_seed_familiarity_scopes",
    "assignment_kg_seeds",
    "reviewer_domain_expertise",
    "reviewer_kg_domain_assessments",
    "reviewer_resource_familiarity_assessments",
)
