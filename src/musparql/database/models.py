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
INITIAL_PROCESSING_RECIPES = "'validate_initial_review','stage_initial_benchmark_update'"
COMPARATIVE_PROCESSING_RECIPES = (
    "'validate_comparative_review','stage_comparative_benchmark_update'"
)
LINGUISTIC_PROCESSING_RECIPES = "'validate_linguistic_annotation'"
PROCESSING_RECIPES = (
    f"{INITIAL_PROCESSING_RECIPES},{COMPARATIVE_PROCESSING_RECIPES},"
    f"{LINGUISTIC_PROCESSING_RECIPES}"
)


class Base(DeclarativeBase):
    pass


class Reviewer(Base):
    __tablename__ = "reviewers"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    name: Mapped[str] = mapped_column(Text)
    title: Mapped[str] = mapped_column(Text, default="")
    first_name: Mapped[str] = mapped_column(Text, default="")
    last_name: Mapped[str] = mapped_column(Text, default="")
    affiliation: Mapped[str] = mapped_column(Text, default="")
    email_display: Mapped[str] = mapped_column(Text)
    email_normalized: Mapped[str] = mapped_column(Text, unique=True)
    status: Mapped[str] = mapped_column(String)
    disabled_from_status: Mapped[str | None] = mapped_column(String, nullable=True)
    created_at: Mapped[str] = mapped_column(String)
    updated_at: Mapped[str] = mapped_column(String)
    privacy_notice_version: Mapped[str | None] = mapped_column(String, nullable=True)
    privacy_notice_acknowledged_at: Mapped[str | None] = mapped_column(String, nullable=True)
    registration_method: Mapped[str] = mapped_column(String)
    email_verified_at: Mapped[str | None] = mapped_column(String, nullable=True)
    consent_statement_version: Mapped[str | None] = mapped_column(String, nullable=True)
    consented_at: Mapped[str | None] = mapped_column(String, nullable=True)
    __table_args__ = (
        CheckConstraint(
            "length(id) >= 13 AND substr(id, 1, 9) = 'reviewer-' "
            "AND substr(id, 10) NOT GLOB '*[^0-9]*'",
            name="ck_reviewers_id",
        ),
        CheckConstraint("status IN ('invited','active','disabled','withdrawn')", name="ck_reviewers_status"),
        CheckConstraint(
            "registration_method IN ('email_invitation','workshop_code')",
            name="ck_reviewers_registration_method",
        ),
        CheckConstraint(
            "(consent_statement_version IS NULL AND consented_at IS NULL) OR "
            "(consent_statement_version IS NOT NULL AND consented_at IS NOT NULL)",
            name="ck_reviewers_consent_pair",
        ),
        CheckConstraint(
            "registration_method <> 'workshop_code' OR email_verified_at IS NULL",
            name="ck_reviewers_workshop_email_unverified",
        ),
    )


class WorkshopRound(Base):
    __tablename__ = "workshop_rounds"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    name: Mapped[str] = mapped_column(Text)
    status: Mapped[str] = mapped_column(String)
    opens_at: Mapped[str] = mapped_column(String)
    closes_at: Mapped[str] = mapped_column(String)
    max_participants: Mapped[int] = mapped_column(Integer)
    allow_additional_assignments: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[str] = mapped_column(String)
    __table_args__ = (
        CheckConstraint("status IN ('draft','open','closed')", name="ck_workshop_rounds_status"),
        CheckConstraint("max_participants > 0", name="ck_workshop_rounds_capacity"),
        CheckConstraint("opens_at < closes_at", name="ck_workshop_rounds_window"),
    )


class WorkshopEntryCode(Base):
    __tablename__ = "workshop_entry_codes"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    workshop_round_id: Mapped[str] = mapped_column(ForeignKey("workshop_rounds.id"))
    code_digest: Mapped[str] = mapped_column(Text)
    expires_at: Mapped[str] = mapped_column(String)
    max_redemptions: Mapped[int] = mapped_column(Integer)
    redemption_count: Mapped[int] = mapped_column(Integer, default=0)
    revoked_at: Mapped[str | None] = mapped_column(String, nullable=True)
    created_at: Mapped[str] = mapped_column(String)
    __table_args__ = (
        CheckConstraint("max_redemptions > 0", name="ck_workshop_entry_codes_capacity"),
        CheckConstraint(
            "redemption_count >= 0 AND redemption_count <= max_redemptions",
            name="ck_workshop_entry_codes_redemptions",
        ),
        Index(
            "uq_workshop_entry_codes_unrevoked_round",
            "workshop_round_id",
            unique=True,
            sqlite_where=text("revoked_at IS NULL"),
        ),
    )


class WorkshopEntryRedemption(Base):
    __tablename__ = "workshop_entry_redemptions"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    entry_code_id: Mapped[str] = mapped_column(ForeignKey("workshop_entry_codes.id"))
    reviewer_id: Mapped[str] = mapped_column(ForeignKey("reviewers.id"), unique=True)
    redeemed_at: Mapped[str] = mapped_column(String)


class WorkshopAdmissionNonce(Base):
    """One-time browser-bound evidence preventing duplicate shared-code admission."""

    __tablename__ = "workshop_admission_nonces"
    nonce_digest: Mapped[str] = mapped_column(Text, primary_key=True)
    reviewer_id: Mapped[str] = mapped_column(ForeignKey("reviewers.id"), unique=True)
    created_at: Mapped[str] = mapped_column(String)


class WorkshopAdmissionAttempt(Base):
    """Durable, digest-only throttling evidence for admission and recovery."""

    __tablename__ = "workshop_admission_attempts"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    candidate_digest: Mapped[str] = mapped_column(Text, index=True)
    context_digest: Mapped[str] = mapped_column(Text, index=True)
    requested_at: Mapped[str] = mapped_column(String, index=True)


class ReviewGroup(Base):
    __tablename__ = "review_groups"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    workshop_round_id: Mapped[str] = mapped_column(ForeignKey("workshop_rounds.id"))
    join_code_digest: Mapped[str] = mapped_column(Text, unique=True)
    created_at: Mapped[str] = mapped_column(String)


class ReviewGroupMember(Base):
    __tablename__ = "review_group_members"
    group_id: Mapped[str] = mapped_column(ForeignKey("review_groups.id"), primary_key=True)
    reviewer_id: Mapped[str] = mapped_column(ForeignKey("reviewers.id"), primary_key=True)
    joined_at: Mapped[str] = mapped_column(String)


class WorkshopAssessmentDeferral(Base):
    """An explicit no-answer event when a teammate joins an active review."""

    __tablename__ = "workshop_assessment_deferrals"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    assignment_id: Mapped[str] = mapped_column(
        ForeignKey("review_assignments.id"), index=True
    )
    reviewer_id: Mapped[str] = mapped_column(ForeignKey("reviewers.id"), index=True)
    deferred_at: Mapped[str] = mapped_column(String)
    __table_args__ = (
        UniqueConstraint(
            "assignment_id", "reviewer_id", name="uq_workshop_assessment_deferral"
        ),
    )


class WorkshopWorkPackage(Base):
    __tablename__ = "workshop_work_packages"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    workshop_round_id: Mapped[str] = mapped_column(ForeignKey("workshop_rounds.id"))
    kg_id: Mapped[str] = mapped_column(String)
    seed_version: Mapped[str] = mapped_column(String)
    seed_digest: Mapped[str] = mapped_column(String)
    display_name: Mapped[str] = mapped_column(Text)
    short_description: Mapped[str] = mapped_column(Text, default="")
    display_order: Mapped[int] = mapped_column(Integer)
    bundle_path: Mapped[str] = mapped_column(Text)
    bundle_digest: Mapped[str] = mapped_column(String)
    processing_recipe: Mapped[str] = mapped_column(String)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[str] = mapped_column(String)
    __table_args__ = (
        ForeignKeyConstraint(
            ["kg_id", "seed_version", "seed_digest"],
            ["kg_seed_snapshots.kg_id", "kg_seed_snapshots.seed_version", "kg_seed_snapshots.seed_digest"],
            name="fk_workshop_package_seed",
        ),
        UniqueConstraint("workshop_round_id", "kg_id", name="uq_workshop_package_round_kg"),
        UniqueConstraint("workshop_round_id", "display_order", name="uq_workshop_package_display_order"),
        CheckConstraint("display_order > 0", name="ck_workshop_package_display_order"),
        CheckConstraint("bundle_digest LIKE 'sha256:%'", name="ck_workshop_package_bundle_digest"),
        CheckConstraint(
            f"processing_recipe IN ({INITIAL_PROCESSING_RECIPES})",
            name="ck_workshop_package_processing_recipe",
        ),
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


class OwnerAuditEvent(Base):
    """Append-only record of security-sensitive owner account actions."""

    __tablename__ = "owner_audit_events"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    actor_reviewer_id: Mapped[str] = mapped_column(ForeignKey("reviewers.id"), index=True)
    target_reviewer_id: Mapped[str] = mapped_column(String, index=True)
    action: Mapped[str] = mapped_column(String)
    created_at: Mapped[str] = mapped_column(String)
    __table_args__ = (
        CheckConstraint(
            "action IN ('invite','disable','restore','delete')",
            name="ck_owner_audit_events_action",
        ),
    )


class WorkshopSessionReset(Base):
    """Append-only evidence of facilitator-assisted fallback recovery."""

    __tablename__ = "workshop_session_resets"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    actor_reviewer_id: Mapped[str] = mapped_column(ForeignKey("reviewers.id"))
    target_reviewer_id: Mapped[str] = mapped_column(ForeignKey("reviewers.id"))
    created_at: Mapped[str] = mapped_column(String)


class ReviewAssignment(Base):
    __tablename__ = "review_assignments"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    reviewer_id: Mapped[str | None] = mapped_column(ForeignKey("reviewers.id"), nullable=True)
    review_group_id: Mapped[str | None] = mapped_column(
        ForeignKey("review_groups.id"), nullable=True
    )
    work_package_id: Mapped[str | None] = mapped_column(
        ForeignKey("workshop_work_packages.id"), nullable=True
    )
    participant_status: Mapped[str] = mapped_column(String, default="not_started")
    claimed_at: Mapped[str | None] = mapped_column(String, nullable=True)
    completed_at: Mapped[str | None] = mapped_column(String, nullable=True)
    completion_item_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    completion_total_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    closed_contributor_ids: Mapped[list[str] | None] = mapped_column(
        JSON(none_as_null=True), nullable=True
    )
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
        UniqueConstraint("id", "review_group_id", name="uq_assignment_review_group"),
        UniqueConstraint("id", "processing_recipe", name="uq_assignment_recipe"),
        CheckConstraint("mode IN ('initial','compare','linguistic')", name="ck_assignment_mode"),
        CheckConstraint(
            "status IN ('draft','ready','active','submitted','processing','ready_for_owner_review','approved','failed')",
            name="ck_assignment_status",
        ),
        CheckConstraint("holdout_capability = 0", name="ck_assignment_no_holdout"),
        CheckConstraint(
            "(reviewer_id IS NOT NULL AND review_group_id IS NULL) OR "
            "(reviewer_id IS NULL AND review_group_id IS NOT NULL)",
            name="ck_assignment_owner",
        ),
        CheckConstraint(
            "participant_status IN ('not_started','active','completed','partial','abandoned')",
            name="ck_assignment_participant_status",
        ),
        CheckConstraint(
            "(completion_item_count IS NULL AND completion_total_count IS NULL) OR "
            "(completion_item_count IS NOT NULL AND completion_total_count IS NOT NULL "
            "AND completion_item_count >= 0 AND completion_total_count >= 0 "
            "AND completion_item_count <= completion_total_count)",
            name="ck_assignment_completion_counts",
        ),
        CheckConstraint(
            "closed_contributor_ids IS NULL OR json_valid(closed_contributor_ids)",
            name="ck_assignment_closed_contributors_json",
        ),
        CheckConstraint(
            "participant_status NOT IN ('completed','partial','abandoned') OR "
            "closed_contributor_ids IS NOT NULL",
            name="ck_assignment_terminal_contributors",
        ),
        CheckConstraint("bundle_digest LIKE 'sha256:%'", name="ck_assignment_bundle_digest"),
        CheckConstraint(
            f"processing_recipe IN ({PROCESSING_RECIPES})", name="ck_assignment_recipe"
        ),
        CheckConstraint(
            f"(mode = 'initial' AND processing_recipe IN ({INITIAL_PROCESSING_RECIPES})) OR "
            f"(mode = 'compare' AND processing_recipe IN ({COMPARATIVE_PROCESSING_RECIPES})) OR "
            f"(mode = 'linguistic' AND processing_recipe IN ({LINGUISTIC_PROCESSING_RECIPES}))",
            name="ck_assignment_mode_recipe",
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
        ForeignKeyConstraint(
            ["assignment_id"], ["review_assignments.id"],
            name="fk_domain_assessment_assignment",
        ),
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
        CheckConstraint(
            "context IN ('pre_review','post_review_followup','profile')",
            name="ck_domain_assessment_context",
        ),
        CheckConstraint(
            "(context IN ('pre_review','post_review_followup') AND assignment_id IS NOT NULL) "
            "OR (context = 'profile' AND assignment_id IS NULL)",
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
        ForeignKeyConstraint(
            ["assignment_id"], ["review_assignments.id"],
            name="fk_familiarity_assessment_assignment",
        ),
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
        CheckConstraint(
            "context IN ('pre_review','post_review_followup','profile')",
            name="ck_familiarity_assessment_context",
        ),
        CheckConstraint(
            "(context IN ('pre_review','post_review_followup') AND assignment_id IS NOT NULL) "
            "OR (context = 'profile' AND assignment_id IS NULL)",
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
    reviewer_id: Mapped[str | None] = mapped_column(String, nullable=True)
    review_group_id: Mapped[str | None] = mapped_column(String, nullable=True)
    submitted_by_reviewer_id: Mapped[str | None] = mapped_column(String, nullable=True)
    contributor_reviewer_ids: Mapped[list[str] | None] = mapped_column(
        JSON(none_as_null=True), nullable=True
    )
    export_path: Mapped[str] = mapped_column(Text)
    export_digest: Mapped[str] = mapped_column(String)
    submitted_at: Mapped[str] = mapped_column(String)
    revision: Mapped[int] = mapped_column(Integer)
    validation_status: Mapped[str] = mapped_column(String)
    inclusion_status: Mapped[str] = mapped_column(String, default="pending")
    inclusion_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    decided_by_reviewer_id: Mapped[str | None] = mapped_column(String, nullable=True)
    decided_at: Mapped[str | None] = mapped_column(String, nullable=True)
    __table_args__ = (
        ForeignKeyConstraint(["assignment_id", "reviewer_id"], ["review_assignments.id", "review_assignments.reviewer_id"]),
        ForeignKeyConstraint(
            ["assignment_id", "review_group_id"],
            ["review_assignments.id", "review_assignments.review_group_id"],
            name="fk_submission_assignment_group",
        ),
        ForeignKeyConstraint(
            ["review_group_id", "submitted_by_reviewer_id"],
            ["review_group_members.group_id", "review_group_members.reviewer_id"],
            name="fk_submission_group_submitter",
        ),
        ForeignKeyConstraint(
            ["submitted_by_reviewer_id"], ["reviewers.id"],
            name="fk_submission_submitter",
        ),
        UniqueConstraint("id", "assignment_id", name="uq_submission_assignment"),
        UniqueConstraint("assignment_id", "revision", name="uq_submission_revision"),
        UniqueConstraint("assignment_id", "export_digest", name="uq_submission_retry"),
        CheckConstraint("revision >= 1", name="ck_submission_revision"),
        CheckConstraint("export_digest LIKE 'sha256:%'", name="ck_submission_digest"),
        CheckConstraint(
            "(reviewer_id IS NOT NULL AND review_group_id IS NULL) OR "
            "(reviewer_id IS NULL AND review_group_id IS NOT NULL "
            "AND submitted_by_reviewer_id IS NOT NULL "
            "AND contributor_reviewer_ids IS NOT NULL)",
            name="ck_submission_owner",
        ),
        CheckConstraint(
            "contributor_reviewer_ids IS NULL OR json_valid(contributor_reviewer_ids)",
            name="ck_submission_contributors_json",
        ),
        CheckConstraint(
            "inclusion_status IN ('pending','included','revision_requested','rejected')",
            name="ck_submission_inclusion_status",
        ),
        ForeignKeyConstraint(
            ["decided_by_reviewer_id"], ["reviewers.id"],
            name="fk_submission_deciding_owner",
        ),
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
    job_kind: Mapped[str] = mapped_column(String, default="submission")
    selected_submission_ids: Mapped[list[str] | None] = mapped_column(JSON, nullable=True)
    approval_status: Mapped[str] = mapped_column(String, default="pending")
    approval_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    approved_by_reviewer_id: Mapped[str | None] = mapped_column(String, nullable=True)
    approved_at: Mapped[str | None] = mapped_column(String, nullable=True)
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
        CheckConstraint("job_kind IN ('submission','combined_candidate')", name="ck_processing_job_kind"),
        CheckConstraint(
            "job_kind = 'submission' OR selected_submission_ids IS NOT NULL",
            name="ck_processing_job_selection",
        ),
        CheckConstraint(
            "approval_status IN ('pending','approved','rejected')",
            name="ck_processing_job_approval_status",
        ),
        ForeignKeyConstraint(
            ["approved_by_reviewer_id"], ["reviewers.id"],
            name="fk_processing_job_approving_owner",
        ),
    )


class OwnerProcessingDecision(Base):
    """Append-only audit of owner scientific-inclusion and promotion decisions."""

    __tablename__ = "owner_processing_decisions"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    owner_reviewer_id: Mapped[str] = mapped_column(ForeignKey("reviewers.id"))
    target_type: Mapped[str] = mapped_column(String)
    submission_id: Mapped[str | None] = mapped_column(ForeignKey("review_submissions.id"), nullable=True)
    job_id: Mapped[str | None] = mapped_column(ForeignKey("processing_jobs.id"), nullable=True)
    item_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    decision: Mapped[str] = mapped_column(String)
    reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[str] = mapped_column(String)
    __table_args__ = (
        CheckConstraint("target_type IN ('submission','item','candidate')", name="ck_owner_processing_target"),
        CheckConstraint(
            "decision IN ('included','omitted','revision_requested','rejected','approved')",
            name="ck_owner_processing_decision",
        ),
        CheckConstraint(
            "(target_type = 'submission' AND submission_id IS NOT NULL AND job_id IS NULL AND item_id IS NULL) OR "
            "(target_type = 'item' AND submission_id IS NOT NULL AND job_id IS NULL AND item_id IS NOT NULL) OR "
            "(target_type = 'candidate' AND submission_id IS NULL AND job_id IS NOT NULL AND item_id IS NULL)",
            name="ck_owner_processing_target_identity",
        ),
    )


APPEND_ONLY_TABLES = (
    "kg_seed_snapshots",
    "kg_seed_review_domains",
    "kg_seed_familiarity_scopes",
    "assignment_kg_seeds",
    "reviewer_domain_expertise",
    "reviewer_kg_domain_assessments",
    "reviewer_resource_familiarity_assessments",
    "owner_processing_decisions",
)
