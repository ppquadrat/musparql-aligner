"""Create the Musparql v2 confidential and operational schema."""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260818_01"
down_revision = None
branch_labels = None
depends_on = None

APPEND_ONLY_TABLES = (
    "kg_seed_snapshots", "kg_seed_review_domains", "kg_seed_familiarity_scopes",
    "assignment_kg_seeds", "reviewer_domain_expertise",
    "reviewer_kg_domain_assessments", "reviewer_resource_familiarity_assessments",
)


def upgrade() -> None:
    op.create_table(
        "expertise_domains",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("entered_label", sa.Text(), nullable=False),
        sa.Column("normalized_label", sa.Text(), nullable=False),
        sa.Column("vocabulary_name", sa.Text()),
        sa.Column("vocabulary_concept_uri", sa.Text()),
        sa.Column("vocabulary_version", sa.Text()),
        sa.Column("created_by", sa.String(), nullable=False),
        sa.CheckConstraint("created_by IN ('reviewer','owner')", name="ck_expertise_domains_created_by"),
        sa.CheckConstraint(
            "(vocabulary_name IS NULL AND vocabulary_concept_uri IS NULL AND vocabulary_version IS NULL) OR "
            "(vocabulary_name IS NOT NULL AND vocabulary_concept_uri IS NOT NULL AND vocabulary_version IS NOT NULL)",
            name="ck_expertise_domains_vocabulary",
        ),
    )
    op.create_table(
        "kg_seed_snapshots",
        sa.Column("kg_id", sa.String(), primary_key=True),
        sa.Column("seed_version", sa.String(), primary_key=True),
        sa.Column("seed_digest", sa.String(), nullable=False, unique=True),
        sa.Column("previous_seed_digest", sa.String(), unique=True),
        sa.Column("seed_json", sa.JSON(), nullable=False),
        sa.UniqueConstraint("kg_id", "seed_version", "seed_digest", name="uq_seed_version_digest"),
        sa.UniqueConstraint("seed_digest", "kg_id", name="uq_seed_digest_kg"),
        sa.ForeignKeyConstraint(
            ["previous_seed_digest", "kg_id"], ["kg_seed_snapshots.seed_digest", "kg_seed_snapshots.kg_id"],
            name="fk_seed_predecessor_kg",
        ),
        sa.CheckConstraint("seed_digest LIKE 'sha256:%'", name="ck_seed_digest_format"),
        sa.CheckConstraint("previous_seed_digest IS NULL OR previous_seed_digest <> seed_digest", name="ck_seed_not_self"),
    )
    op.create_index(
        "uq_seed_snapshot_root", "kg_seed_snapshots", ["kg_id"], unique=True,
        sqlite_where=sa.text("previous_seed_digest IS NULL"),
    )
    op.create_table(
        "login_codes",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("email_normalized", sa.Text(), nullable=False),
        sa.Column("code_hash", sa.Text(), nullable=False),
        sa.Column("requested_at", sa.String(), nullable=False),
        sa.Column("expires_at", sa.String(), nullable=False),
        sa.Column("consumed_at", sa.String()),
        sa.Column("failed_attempt_count", sa.Integer(), nullable=False),
        sa.Column("request_context_digest", sa.Text(), nullable=False),
        sa.CheckConstraint("failed_attempt_count >= 0", name="ck_login_codes_attempts"),
    )
    op.create_table(
        "reviewers",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("affiliation", sa.Text(), nullable=False),
        sa.Column("email_display", sa.Text(), nullable=False),
        sa.Column("email_normalized", sa.Text(), nullable=False, unique=True),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("created_at", sa.String(), nullable=False),
        sa.Column("updated_at", sa.String(), nullable=False),
        sa.Column("privacy_notice_version", sa.String()),
        sa.Column("privacy_notice_acknowledged_at", sa.String()),
        sa.CheckConstraint(
            "length(id) >= 13 AND substr(id, 1, 9) = 'reviewer-' AND substr(id, 10) NOT GLOB '*[^0-9]*'",
            name="ck_reviewers_id",
        ),
        sa.CheckConstraint("status IN ('invited','active','disabled','withdrawn')", name="ck_reviewers_status"),
    )
    op.create_table(
        "auth_sessions",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("reviewer_id", sa.String(), sa.ForeignKey("reviewers.id"), nullable=False),
        sa.Column("token_hash", sa.Text(), nullable=False, unique=True),
        sa.Column("created_at", sa.String(), nullable=False),
        sa.Column("last_used_at", sa.String(), nullable=False),
        sa.Column("expires_at", sa.String(), nullable=False),
        sa.Column("revoked_at", sa.String()),
        sa.Column("remembered", sa.Boolean(), nullable=False),
    )
    op.create_index("ix_auth_sessions_reviewer_id", "auth_sessions", ["reviewer_id"])
    op.create_table(
        "kg_seed_familiarity_scopes",
        sa.Column("kg_id", sa.String(), primary_key=True),
        sa.Column("seed_version", sa.String(), primary_key=True),
        sa.Column("scope_id", sa.String(), primary_key=True),
        sa.Column("label", sa.Text(), nullable=False),
        sa.Column("description", sa.Text()),
        sa.ForeignKeyConstraint(["kg_id", "seed_version"], ["kg_seed_snapshots.kg_id", "kg_seed_snapshots.seed_version"]),
        sa.UniqueConstraint("kg_id", "seed_version", "scope_id", "label", name="uq_seed_scope_label"),
    )
    op.create_table(
        "kg_seed_review_domains",
        sa.Column("kg_id", sa.String(), primary_key=True),
        sa.Column("seed_version", sa.String(), primary_key=True),
        sa.Column("domain_id", sa.String(), primary_key=True),
        sa.Column("label", sa.Text(), nullable=False),
        sa.Column("description", sa.Text(), nullable=False),
        sa.ForeignKeyConstraint(["kg_id", "seed_version"], ["kg_seed_snapshots.kg_id", "kg_seed_snapshots.seed_version"]),
        sa.UniqueConstraint("kg_id", "seed_version", "domain_id", "label", name="uq_seed_domain_label"),
    )
    op.create_table(
        "review_assignments",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("reviewer_id", sa.String(), sa.ForeignKey("reviewers.id"), nullable=False),
        sa.Column("mode", sa.String(), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("bundle_path", sa.Text(), nullable=False),
        sa.Column("bundle_digest", sa.String(), nullable=False),
        sa.Column("previous_benchmark_path", sa.Text()),
        sa.Column("processing_recipe", sa.String(), nullable=False),
        sa.Column("holdout_capability", sa.Boolean(), nullable=False),
        sa.Column("created_at", sa.String(), nullable=False),
        sa.Column("opened_at", sa.String()),
        sa.Column("submitted_at", sa.String()),
        sa.UniqueConstraint("id", "reviewer_id", name="uq_assignment_reviewer"),
        sa.UniqueConstraint("id", "processing_recipe", name="uq_assignment_recipe"),
        sa.CheckConstraint("mode IN ('initial','compare')", name="ck_assignment_mode"),
        sa.CheckConstraint(
            "status IN ('draft','ready','active','submitted','processing','ready_for_owner_review','approved','failed')",
            name="ck_assignment_status",
        ),
        sa.CheckConstraint("holdout_capability = 0", name="ck_assignment_no_holdout"),
        sa.CheckConstraint("bundle_digest LIKE 'sha256:%'", name="ck_assignment_bundle_digest"),
        sa.CheckConstraint(
            "processing_recipe IN ('validate_initial_review','stage_initial_benchmark_update',"
            "'validate_comparative_review','stage_comparative_benchmark_update')",
            name="ck_assignment_recipe",
        ),
        sa.CheckConstraint(
            "(mode = 'initial' AND processing_recipe IN "
            "('validate_initial_review','stage_initial_benchmark_update')) OR "
            "(mode = 'compare' AND processing_recipe IN "
            "('validate_comparative_review','stage_comparative_benchmark_update'))",
            name="ck_assignment_mode_recipe",
        ),
    )
    op.create_table(
        "reviewer_domain_expertise",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("reviewer_id", sa.String(), sa.ForeignKey("reviewers.id"), nullable=False),
        sa.Column("domain_id", sa.String(), sa.ForeignKey("expertise_domains.id"), nullable=False),
        sa.Column("expertise_level", sa.String(), nullable=False),
        sa.Column("asserted_at", sa.String(), nullable=False),
        sa.Column("supersedes_id", sa.String(), unique=True),
        sa.UniqueConstraint("id", "reviewer_id", "domain_id", name="uq_domain_assertion_subject"),
        sa.ForeignKeyConstraint(
            ["supersedes_id", "reviewer_id", "domain_id"],
            ["reviewer_domain_expertise.id", "reviewer_domain_expertise.reviewer_id", "reviewer_domain_expertise.domain_id"],
            name="fk_domain_assertion_predecessor_subject",
        ),
        sa.CheckConstraint("expertise_level IN ('none','basic','working','advanced','expert')", name="ck_domain_assertion_level"),
        sa.CheckConstraint("supersedes_id IS NULL OR supersedes_id <> id", name="ck_domain_assertion_not_self"),
    )
    op.create_index(
        "uq_domain_assertion_root", "reviewer_domain_expertise", ["reviewer_id", "domain_id"],
        unique=True, sqlite_where=sa.text("supersedes_id IS NULL"),
    )
    op.create_table(
        "reviewer_experience",
        sa.Column("reviewer_id", sa.String(), sa.ForeignKey("reviewers.id"), primary_key=True),
        sa.Column("kg_ontology_experience", sa.String(), nullable=False),
        sa.Column("sparql_experience", sa.String(), nullable=False),
        sa.Column("nlp_llm_experience", sa.String(), nullable=False),
        sa.Column("assessed_at", sa.String(), nullable=False),
        sa.CheckConstraint("kg_ontology_experience IN ('none','occasional','regular','expert')", name="ck_reviewer_experience_kg_ontology_experience"),
        sa.CheckConstraint("sparql_experience IN ('none','occasional','regular','expert')", name="ck_reviewer_experience_sparql_experience"),
        sa.CheckConstraint("nlp_llm_experience IN ('none','occasional','regular','expert')", name="ck_reviewer_experience_nlp_llm_experience"),
    )
    op.create_table(
        "reviewer_languages",
        sa.Column("reviewer_id", sa.String(), sa.ForeignKey("reviewers.id"), primary_key=True),
        sa.Column("language_tag", sa.String(), primary_key=True),
        sa.Column("level", sa.String(), nullable=False),
        sa.Column("first_asserted_at", sa.String(), nullable=False),
        sa.Column("updated_at", sa.String(), nullable=False),
        sa.CheckConstraint("level IN ('basic','advanced','fluent','native')", name="ck_reviewer_languages_level"),
    )
    op.create_table(
        "assignment_kg_seeds",
        sa.Column("assignment_id", sa.String(), sa.ForeignKey("review_assignments.id"), primary_key=True),
        sa.Column("kg_id", sa.String(), primary_key=True),
        sa.Column("seed_version", sa.String(), nullable=False),
        sa.Column("seed_digest", sa.String(), nullable=False),
        sa.UniqueConstraint("assignment_id", "kg_id", "seed_version", name="uq_assignment_seed_version"),
        sa.ForeignKeyConstraint(
            ["kg_id", "seed_version", "seed_digest"],
            ["kg_seed_snapshots.kg_id", "kg_seed_snapshots.seed_version", "kg_seed_snapshots.seed_digest"],
            name="fk_assignment_seed_snapshot",
        ),
    )
    op.create_table(
        "review_submissions",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("assignment_id", sa.String(), nullable=False),
        sa.Column("reviewer_id", sa.String(), nullable=False),
        sa.Column("export_path", sa.Text(), nullable=False),
        sa.Column("export_digest", sa.String(), nullable=False),
        sa.Column("submitted_at", sa.String(), nullable=False),
        sa.Column("revision", sa.Integer(), nullable=False),
        sa.Column("validation_status", sa.String(), nullable=False),
        sa.ForeignKeyConstraint(["assignment_id", "reviewer_id"], ["review_assignments.id", "review_assignments.reviewer_id"]),
        sa.UniqueConstraint("id", "assignment_id", name="uq_submission_assignment"),
        sa.UniqueConstraint("assignment_id", "revision", name="uq_submission_revision"),
        sa.UniqueConstraint("assignment_id", "export_digest", name="uq_submission_retry"),
        sa.CheckConstraint("revision >= 1", name="ck_submission_revision"),
        sa.CheckConstraint("export_digest LIKE 'sha256:%'", name="ck_submission_digest"),
    )
    op.create_table(
        "processing_jobs",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("assignment_id", sa.String(), sa.ForeignKey("review_assignments.id"), nullable=False),
        sa.Column("submission_id", sa.String(), nullable=False),
        sa.Column("recipe", sa.String(), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("created_at", sa.String(), nullable=False),
        sa.Column("started_at", sa.String()),
        sa.Column("finished_at", sa.String()),
        sa.Column("safe_summary", sa.Text()),
        sa.Column("candidate_output_path", sa.Text()),
        sa.ForeignKeyConstraint(
            ["assignment_id", "recipe"], ["review_assignments.id", "review_assignments.processing_recipe"],
            name="fk_processing_job_assignment_recipe",
        ),
        sa.ForeignKeyConstraint(
            ["submission_id", "assignment_id"], ["review_submissions.id", "review_submissions.assignment_id"],
            name="fk_processing_job_submission_assignment",
        ),
        sa.CheckConstraint("status IN ('queued','running','succeeded','failed')", name="ck_processing_job_status"),
    )
    _create_assessment_tables()
    for table in APPEND_ONLY_TABLES:
        op.execute(
            f"CREATE TRIGGER {table}_immutable_update "
            f"BEFORE UPDATE ON {table} BEGIN "
            "SELECT RAISE(ABORT, 'append-only table'); END"
        )
        op.execute(
            f"CREATE TRIGGER {table}_immutable_delete "
            f"BEFORE DELETE ON {table} BEGIN "
            "SELECT RAISE(ABORT, 'append-only table'); END"
        )
    op.execute(
        "CREATE TRIGGER expertise_domains_immutable_update "
        "BEFORE UPDATE ON expertise_domains "
        "WHEN EXISTS (SELECT 1 FROM reviewer_domain_expertise WHERE domain_id = OLD.id) "
        "BEGIN SELECT RAISE(ABORT, 'referenced expertise domain is immutable'); END"
    )
    op.execute(
        "CREATE TRIGGER expertise_domains_immutable_delete "
        "BEFORE DELETE ON expertise_domains "
        "WHEN EXISTS (SELECT 1 FROM reviewer_domain_expertise WHERE domain_id = OLD.id) "
        "BEGIN SELECT RAISE(ABORT, 'referenced expertise domain is immutable'); END"
    )


def downgrade() -> None:
    op.execute("DROP TRIGGER IF EXISTS expertise_domains_immutable_update")
    op.execute("DROP TRIGGER IF EXISTS expertise_domains_immutable_delete")
    for table in APPEND_ONLY_TABLES:
        op.execute(f"DROP TRIGGER IF EXISTS {table}_immutable_update")
        op.execute(f"DROP TRIGGER IF EXISTS {table}_immutable_delete")
    op.drop_index("uq_familiarity_assessment_root", table_name="reviewer_resource_familiarity_assessments")
    op.drop_table("reviewer_resource_familiarity_assessments")
    op.drop_index("uq_domain_assessment_root", table_name="reviewer_kg_domain_assessments")
    op.drop_table("reviewer_kg_domain_assessments")
    for table in (
        "processing_jobs", "review_submissions", "assignment_kg_seeds", "reviewer_languages",
        "reviewer_experience", "reviewer_domain_expertise", "review_assignments",
        "kg_seed_review_domains", "kg_seed_familiarity_scopes", "auth_sessions", "reviewers",
        "login_codes", "kg_seed_snapshots", "expertise_domains",
    ):
        if table == "reviewer_domain_expertise":
            op.drop_index("uq_domain_assertion_root", table_name=table)
        if table == "auth_sessions":
            op.drop_index("ix_auth_sessions_reviewer_id", table_name=table)
        if table == "kg_seed_snapshots":
            op.drop_index("uq_seed_snapshot_root", table_name=table)
        op.drop_table(table)


def _create_assessment_tables() -> None:
    op.create_table(
        "reviewer_kg_domain_assessments",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("reviewer_id", sa.String(), sa.ForeignKey("reviewers.id"), nullable=False),
        sa.Column("kg_id", sa.String(), nullable=False),
        sa.Column("review_domain_id", sa.String(), nullable=False),
        sa.Column("review_domain_label", sa.Text(), nullable=False),
        sa.Column("subject_expertise_level", sa.String(), nullable=False),
        sa.Column("assessed_at", sa.String(), nullable=False),
        sa.Column("context", sa.String(), nullable=False),
        sa.Column("assignment_id", sa.String()),
        sa.Column("seed_version", sa.String(), nullable=False),
        sa.Column("previous_assessment_id", sa.String(), unique=True),
        sa.UniqueConstraint("id", "reviewer_id", "kg_id", "review_domain_id", name="uq_domain_assessment_subject"),
        sa.ForeignKeyConstraint(
            ["previous_assessment_id", "reviewer_id", "kg_id", "review_domain_id"],
            ["reviewer_kg_domain_assessments.id", "reviewer_kg_domain_assessments.reviewer_id", "reviewer_kg_domain_assessments.kg_id", "reviewer_kg_domain_assessments.review_domain_id"],
            name="fk_domain_assessment_predecessor_subject",
        ),
        sa.ForeignKeyConstraint(["assignment_id", "reviewer_id"], ["review_assignments.id", "review_assignments.reviewer_id"]),
        sa.ForeignKeyConstraint(
            ["assignment_id", "kg_id", "seed_version"],
            ["assignment_kg_seeds.assignment_id", "assignment_kg_seeds.kg_id", "assignment_kg_seeds.seed_version"],
            name="fk_domain_assessment_assignment_seed",
        ),
        sa.ForeignKeyConstraint(
            ["kg_id", "seed_version", "review_domain_id", "review_domain_label"],
            ["kg_seed_review_domains.kg_id", "kg_seed_review_domains.seed_version", "kg_seed_review_domains.domain_id", "kg_seed_review_domains.label"],
            name="fk_domain_assessment_seed_prompt",
        ),
        sa.CheckConstraint("subject_expertise_level IN ('none','basic','working','advanced','expert')", name="ck_domain_assessment_level"),
        sa.CheckConstraint("context IN ('pre_review','profile')", name="ck_domain_assessment_context"),
        sa.CheckConstraint("(context = 'pre_review' AND assignment_id IS NOT NULL) OR (context = 'profile' AND assignment_id IS NULL)", name="ck_domain_assessment_assignment"),
        sa.CheckConstraint("previous_assessment_id IS NULL OR previous_assessment_id <> id", name="ck_domain_assessment_not_self"),
    )
    op.create_index(
        "uq_domain_assessment_root", "reviewer_kg_domain_assessments",
        ["reviewer_id", "kg_id", "review_domain_id"], unique=True,
        sqlite_where=sa.text("previous_assessment_id IS NULL"),
    )
    op.create_table(
        "reviewer_resource_familiarity_assessments",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("reviewer_id", sa.String(), sa.ForeignKey("reviewers.id"), nullable=False),
        sa.Column("kg_id", sa.String(), nullable=False),
        sa.Column("familiarity_scope_id", sa.String(), nullable=False),
        sa.Column("familiarity_scope_label", sa.Text(), nullable=False),
        sa.Column("familiarity_level", sa.String(), nullable=False),
        sa.Column("assessed_at", sa.String(), nullable=False),
        sa.Column("context", sa.String(), nullable=False),
        sa.Column("assignment_id", sa.String()),
        sa.Column("seed_version", sa.String(), nullable=False),
        sa.Column("previous_assessment_id", sa.String(), unique=True),
        sa.UniqueConstraint("id", "reviewer_id", "kg_id", "familiarity_scope_id", name="uq_familiarity_assessment_subject"),
        sa.ForeignKeyConstraint(
            ["previous_assessment_id", "reviewer_id", "kg_id", "familiarity_scope_id"],
            ["reviewer_resource_familiarity_assessments.id", "reviewer_resource_familiarity_assessments.reviewer_id", "reviewer_resource_familiarity_assessments.kg_id", "reviewer_resource_familiarity_assessments.familiarity_scope_id"],
            name="fk_familiarity_assessment_predecessor_subject",
        ),
        sa.ForeignKeyConstraint(["assignment_id", "reviewer_id"], ["review_assignments.id", "review_assignments.reviewer_id"]),
        sa.ForeignKeyConstraint(
            ["assignment_id", "kg_id", "seed_version"],
            ["assignment_kg_seeds.assignment_id", "assignment_kg_seeds.kg_id", "assignment_kg_seeds.seed_version"],
            name="fk_familiarity_assessment_assignment_seed",
        ),
        sa.ForeignKeyConstraint(
            ["kg_id", "seed_version", "familiarity_scope_id", "familiarity_scope_label"],
            ["kg_seed_familiarity_scopes.kg_id", "kg_seed_familiarity_scopes.seed_version", "kg_seed_familiarity_scopes.scope_id", "kg_seed_familiarity_scopes.label"],
            name="fk_familiarity_assessment_seed_prompt",
        ),
        sa.CheckConstraint("familiarity_level IN ('none','inspected','worked','regular_user','creator')", name="ck_familiarity_assessment_level"),
        sa.CheckConstraint("context IN ('pre_review','profile')", name="ck_familiarity_assessment_context"),
        sa.CheckConstraint("(context = 'pre_review' AND assignment_id IS NOT NULL) OR (context = 'profile' AND assignment_id IS NULL)", name="ck_familiarity_assessment_assignment"),
        sa.CheckConstraint("previous_assessment_id IS NULL OR previous_assessment_id <> id", name="ck_familiarity_assessment_not_self"),
    )
    op.create_index(
        "uq_familiarity_assessment_root", "reviewer_resource_familiarity_assessments",
        ["reviewer_id", "kg_id", "familiarity_scope_id"], unique=True,
        sqlite_where=sa.text("previous_assessment_id IS NULL"),
    )
