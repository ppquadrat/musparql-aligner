"""Add the IPL workshop admission, reviewing-group, and package foundation."""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260910_07"
down_revision = "20260820_06"
branch_labels = None
depends_on = None

NAMING_CONVENTION = {
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
}

ASSESSMENT_TABLES = (
    (
        "reviewer_kg_domain_assessments",
        "fk_domain_assessment_assignment",
    ),
    (
        "reviewer_resource_familiarity_assessments",
        "fk_familiarity_assessment_assignment",
    ),
)


def upgrade() -> None:
    op.add_column(
        "reviewers",
        sa.Column(
            "registration_method",
            sa.String(),
            nullable=True,
        )
    )
    op.add_column("reviewers", sa.Column("email_verified_at", sa.String()))
    op.add_column("reviewers", sa.Column("consent_statement_version", sa.String()))
    op.add_column("reviewers", sa.Column("consented_at", sa.String()))
    op.execute(
        "UPDATE reviewers SET registration_method = 'email_invitation' "
        "WHERE registration_method IS NULL"
    )
    # Treat pre-migration active email-invitation accounts as verified for
    # compatibility, using the last account-change timestamp. All future
    # verifications record their exact verification time.
    op.execute(
        "UPDATE reviewers SET email_verified_at = updated_at "
        "WHERE status = 'active' AND email_verified_at IS NULL"
    )
    # Rebuilding reviewers rewrites the table name embedded in this trigger, so
    # remove and restore it explicitly around the batch operation.
    op.execute("DROP TRIGGER reviewer_domain_expertise_immutable_delete")
    with op.batch_alter_table("reviewers", naming_convention=NAMING_CONVENTION) as batch:
        batch.alter_column("registration_method", existing_type=sa.String(), nullable=False)
        batch.create_check_constraint(
            "ck_reviewers_registration_method",
            "registration_method IN ('email_invitation','workshop_code')",
        )
        batch.create_check_constraint(
            "ck_reviewers_consent_pair",
            "(consent_statement_version IS NULL AND consented_at IS NULL) OR "
            "(consent_statement_version IS NOT NULL AND consented_at IS NOT NULL)",
        )
    op.execute(
        "CREATE TRIGGER reviewer_domain_expertise_immutable_delete "
        "BEFORE DELETE ON reviewer_domain_expertise "
        "WHEN COALESCE((SELECT status FROM reviewers WHERE id = OLD.reviewer_id), '') "
        "<> 'withdrawn' BEGIN "
        "SELECT RAISE(ABORT, 'append-only table'); END"
    )

    op.create_table(
        "workshop_rounds",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("opens_at", sa.String(), nullable=False),
        sa.Column("closes_at", sa.String(), nullable=False),
        sa.Column("max_participants", sa.Integer(), nullable=False),
        sa.Column(
            "allow_additional_assignments",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
        sa.Column("created_at", sa.String(), nullable=False),
        sa.CheckConstraint(
            "status IN ('draft','open','closed')",
            name="ck_workshop_rounds_status",
        ),
        sa.CheckConstraint(
            "max_participants > 0", name="ck_workshop_rounds_capacity"
        ),
        sa.CheckConstraint(
            "opens_at < closes_at", name="ck_workshop_rounds_window"
        ),
    )
    op.create_table(
        "workshop_entry_codes",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column(
            "workshop_round_id",
            sa.String(),
            sa.ForeignKey("workshop_rounds.id"),
            nullable=False,
        ),
        sa.Column("code_digest", sa.Text(), nullable=False),
        sa.Column("expires_at", sa.String(), nullable=False),
        sa.Column("max_redemptions", sa.Integer(), nullable=False),
        sa.Column(
            "redemption_count", sa.Integer(), nullable=False, server_default="0"
        ),
        sa.Column("revoked_at", sa.String()),
        sa.Column("created_at", sa.String(), nullable=False),
        sa.CheckConstraint(
            "max_redemptions > 0", name="ck_workshop_entry_codes_capacity"
        ),
        sa.CheckConstraint(
            "redemption_count >= 0 AND redemption_count <= max_redemptions",
            name="ck_workshop_entry_codes_redemptions",
        ),
    )
    op.create_index(
        "uq_workshop_entry_codes_unrevoked_round",
        "workshop_entry_codes",
        ["workshop_round_id"],
        unique=True,
        sqlite_where=sa.text("revoked_at IS NULL"),
    )
    for action in ("insert", "update"):
        op.execute(
            f"CREATE TRIGGER workshop_entry_codes_round_capacity_{action} "
            f"BEFORE {action.upper()} ON workshop_entry_codes "
            "WHEN NEW.max_redemptions > ("
            "SELECT max_participants FROM workshop_rounds "
            "WHERE id = NEW.workshop_round_id"
            ") BEGIN SELECT RAISE(ABORT, 'entry code exceeds round capacity'); END"
        )
    op.create_table(
        "workshop_entry_redemptions",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column(
            "entry_code_id",
            sa.String(),
            sa.ForeignKey("workshop_entry_codes.id"),
            nullable=False,
        ),
        sa.Column(
            "reviewer_id",
            sa.String(),
            sa.ForeignKey("reviewers.id"),
            nullable=False,
            unique=True,
        ),
        sa.Column("redeemed_at", sa.String(), nullable=False),
    )
    op.execute(
        "CREATE TRIGGER workshop_rounds_capacity_update "
        "BEFORE UPDATE OF max_participants ON workshop_rounds "
        "WHEN NEW.max_participants < ("
        "SELECT COUNT(*) FROM workshop_entry_redemptions AS redemption "
        "JOIN workshop_entry_codes AS code ON code.id = redemption.entry_code_id "
        "WHERE code.workshop_round_id = NEW.id"
        ") OR EXISTS ("
        "SELECT 1 FROM workshop_entry_codes AS code "
        "WHERE code.workshop_round_id = NEW.id "
        "AND code.revoked_at IS NULL "
        "AND code.max_redemptions > NEW.max_participants"
        ") BEGIN SELECT RAISE(ABORT, 'round capacity is already allocated'); END"
    )
    op.create_table(
        "review_groups",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column(
            "workshop_round_id",
            sa.String(),
            sa.ForeignKey("workshop_rounds.id"),
            nullable=False,
        ),
        sa.Column("join_code_digest", sa.Text(), nullable=False, unique=True),
        sa.Column("created_at", sa.String(), nullable=False),
    )
    op.create_table(
        "review_group_members",
        sa.Column(
            "group_id",
            sa.String(),
            sa.ForeignKey("review_groups.id"),
            primary_key=True,
        ),
        sa.Column(
            "reviewer_id",
            sa.String(),
            sa.ForeignKey("reviewers.id"),
            primary_key=True,
        ),
        sa.Column("joined_at", sa.String(), nullable=False),
    )
    op.create_table(
        "workshop_work_packages",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column(
            "workshop_round_id",
            sa.String(),
            sa.ForeignKey("workshop_rounds.id"),
            nullable=False,
        ),
        sa.Column("kg_id", sa.String(), nullable=False),
        sa.Column("seed_version", sa.String(), nullable=False),
        sa.Column("seed_digest", sa.String(), nullable=False),
        sa.Column("display_name", sa.Text(), nullable=False),
        sa.Column("short_description", sa.Text(), nullable=False, server_default=""),
        sa.Column("display_order", sa.Integer(), nullable=False),
        sa.Column("bundle_path", sa.Text(), nullable=False),
        sa.Column("bundle_digest", sa.String(), nullable=False),
        sa.Column("processing_recipe", sa.String(), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("created_at", sa.String(), nullable=False),
        sa.ForeignKeyConstraint(
            ["kg_id", "seed_version", "seed_digest"],
            [
                "kg_seed_snapshots.kg_id",
                "kg_seed_snapshots.seed_version",
                "kg_seed_snapshots.seed_digest",
            ],
            name="fk_workshop_package_seed",
        ),
        sa.UniqueConstraint(
            "workshop_round_id", "kg_id", name="uq_workshop_package_round_kg"
        ),
        sa.UniqueConstraint(
            "workshop_round_id",
            "display_order",
            name="uq_workshop_package_display_order",
        ),
        sa.CheckConstraint(
            "display_order > 0", name="ck_workshop_package_display_order"
        ),
        sa.CheckConstraint(
            "bundle_digest LIKE 'sha256:%'",
            name="ck_workshop_package_bundle_digest",
        ),
        sa.CheckConstraint(
            "processing_recipe IN ('validate_initial_review','stage_initial_benchmark_update')",
            name="ck_workshop_package_processing_recipe",
        ),
    )

    # Assignment ownership remains compatible with all legacy individual rows,
    # while group-owned assignments carry their package and completion snapshot.
    op.add_column("review_assignments", sa.Column("review_group_id", sa.String()))
    op.add_column("review_assignments", sa.Column("work_package_id", sa.String()))
    op.add_column(
        "review_assignments",
        sa.Column(
            "participant_status",
            sa.String(),
            nullable=False,
            server_default="not_started",
        ),
    )
    op.add_column("review_assignments", sa.Column("claimed_at", sa.String()))
    op.add_column("review_assignments", sa.Column("completed_at", sa.String()))
    op.add_column("review_assignments", sa.Column("completion_item_count", sa.Integer()))
    op.add_column("review_assignments", sa.Column("completion_total_count", sa.Integer()))
    op.add_column("review_assignments", sa.Column("closed_contributor_ids", sa.JSON()))
    op.execute(
        "UPDATE review_assignments SET participant_status = CASE "
        "WHEN status = 'active' THEN 'active' "
        "WHEN status IN ('submitted','processing','ready_for_owner_review','approved','failed') "
        "THEN 'completed' ELSE 'not_started' END"
    )
    op.execute(
        "UPDATE review_assignments SET completed_at = submitted_at, "
        "closed_contributor_ids = json_array(reviewer_id) "
        "WHERE participant_status = 'completed'"
    )
    with op.batch_alter_table(
        "review_assignments", naming_convention=NAMING_CONVENTION
    ) as batch:
        batch.alter_column("reviewer_id", existing_type=sa.String(), nullable=True)
        batch.create_foreign_key(
            "fk_assignment_review_group",
            "review_groups",
            ["review_group_id"],
            ["id"],
        )
        batch.create_foreign_key(
            "fk_assignment_work_package",
            "workshop_work_packages",
            ["work_package_id"],
            ["id"],
        )
        batch.create_unique_constraint(
            "uq_assignment_review_group", ["id", "review_group_id"]
        )
        batch.create_check_constraint(
            "ck_assignment_owner",
            "(reviewer_id IS NOT NULL AND review_group_id IS NULL) OR "
            "(reviewer_id IS NULL AND review_group_id IS NOT NULL)",
        )
        batch.create_check_constraint(
            "ck_assignment_participant_status",
            "participant_status IN ('not_started','active','completed','partial','abandoned')",
        )
        batch.create_check_constraint(
            "ck_assignment_completion_counts",
            "(completion_item_count IS NULL AND completion_total_count IS NULL) OR "
            "(completion_item_count IS NOT NULL AND completion_total_count IS NOT NULL "
            "AND completion_item_count >= 0 AND completion_total_count >= 0 "
            "AND completion_item_count <= completion_total_count)",
        )
        batch.create_check_constraint(
            "ck_assignment_closed_contributors_json",
            "closed_contributor_ids IS NULL OR json_valid(closed_contributor_ids)",
        )
        batch.create_check_constraint(
            "ck_assignment_terminal_contributors",
            "participant_status NOT IN ('completed','partial','abandoned') OR "
            "closed_contributor_ids IS NOT NULL",
        )

    # The old composite assignment/reviewer FK rejected a member of a group-owned
    # assignment. A direct assignment FK plus a membership trigger preserves the
    # legacy rule and admits any member who has joined the assigned group.
    for table, constraint_name in ASSESSMENT_TABLES:
        op.execute(f"DROP TRIGGER {table}_immutable_update")
        op.execute(f"DROP TRIGGER {table}_immutable_delete")
        reflected_fk_name = f"fk_{table}_assignment_id_review_assignments"
        with op.batch_alter_table(table, naming_convention=NAMING_CONVENTION) as batch:
            batch.drop_constraint(reflected_fk_name, type_="foreignkey")
            batch.create_foreign_key(
                constraint_name,
                "review_assignments",
                ["assignment_id"],
                ["id"],
            )
        op.execute(
            f"CREATE TRIGGER {table}_assignment_member_insert "
            f"BEFORE INSERT ON {table} "
            "WHEN NEW.context = 'pre_review' AND NOT EXISTS ("
            "SELECT 1 FROM review_assignments AS assignment "
            "LEFT JOIN review_group_members AS member "
            "ON member.group_id = assignment.review_group_id "
            "AND member.reviewer_id = NEW.reviewer_id "
            "WHERE assignment.id = NEW.assignment_id "
            "AND (assignment.reviewer_id = NEW.reviewer_id "
            "OR member.reviewer_id IS NOT NULL)"
            ") BEGIN SELECT RAISE(ABORT, 'assessment reviewer is not an assignment participant'); END"
        )
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

    op.add_column("review_submissions", sa.Column("review_group_id", sa.String()))
    op.add_column(
        "review_submissions", sa.Column("submitted_by_reviewer_id", sa.String())
    )
    op.add_column(
        "review_submissions", sa.Column("contributor_reviewer_ids", sa.JSON())
    )
    op.execute(
        "UPDATE review_submissions SET submitted_by_reviewer_id = reviewer_id, "
        "contributor_reviewer_ids = json_array(reviewer_id)"
    )
    with op.batch_alter_table(
        "review_submissions", naming_convention=NAMING_CONVENTION
    ) as batch:
        batch.alter_column("reviewer_id", existing_type=sa.String(), nullable=True)
        batch.create_foreign_key(
            "fk_submission_assignment_group",
            "review_assignments",
            ["assignment_id", "review_group_id"],
            ["id", "review_group_id"],
        )
        batch.create_foreign_key(
            "fk_submission_group_submitter",
            "review_group_members",
            ["review_group_id", "submitted_by_reviewer_id"],
            ["group_id", "reviewer_id"],
        )
        batch.create_foreign_key(
            "fk_submission_submitter",
            "reviewers",
            ["submitted_by_reviewer_id"],
            ["id"],
        )
        batch.create_check_constraint(
            "ck_submission_owner",
            "(reviewer_id IS NOT NULL AND review_group_id IS NULL) OR "
            "(reviewer_id IS NULL AND review_group_id IS NOT NULL "
            "AND submitted_by_reviewer_id IS NOT NULL "
            "AND contributor_reviewer_ids IS NOT NULL)",
        )
        batch.create_check_constraint(
            "ck_submission_contributors_json",
            "contributor_reviewer_ids IS NULL OR json_valid(contributor_reviewer_ids)",
        )
    op.execute(
        "CREATE TRIGGER review_submissions_group_contributors_insert "
        "BEFORE INSERT ON review_submissions WHEN NEW.review_group_id IS NOT NULL AND ("
        "json_type(NEW.contributor_reviewer_ids) <> 'array' "
        "OR json_array_length(NEW.contributor_reviewer_ids) = 0 "
        "OR NOT EXISTS (SELECT 1 FROM json_each(NEW.contributor_reviewer_ids) "
        "WHERE value = NEW.submitted_by_reviewer_id) "
        "OR EXISTS (SELECT 1 FROM json_each(NEW.contributor_reviewer_ids) AS contributor "
        "LEFT JOIN review_group_members AS member "
        "ON member.group_id = NEW.review_group_id "
        "AND member.reviewer_id = contributor.value "
        "WHERE contributor.type <> 'text' OR member.reviewer_id IS NULL)"
        ") BEGIN SELECT RAISE(ABORT, 'submission contributors must be group members'); END"
    )
    op.execute(
        "CREATE TRIGGER review_submissions_contributors_immutable "
        "BEFORE UPDATE OF review_group_id, submitted_by_reviewer_id, contributor_reviewer_ids "
        "ON review_submissions WHEN "
        "OLD.review_group_id IS NOT NEW.review_group_id "
        "OR OLD.submitted_by_reviewer_id IS NOT NEW.submitted_by_reviewer_id "
        "OR OLD.contributor_reviewer_ids IS NOT NEW.contributor_reviewer_ids "
        "BEGIN SELECT RAISE(ABORT, 'submission attribution is immutable'); END"
    )
    op.execute(
        "CREATE TRIGGER review_assignments_closed_contributors_immutable "
        "BEFORE UPDATE OF closed_contributor_ids ON review_assignments "
        "WHEN OLD.closed_contributor_ids IS NOT NULL "
        "AND OLD.closed_contributor_ids IS NOT NEW.closed_contributor_ids "
        "BEGIN SELECT RAISE(ABORT, 'closed contributors are immutable'); END"
    )
    for action in ("insert", "update"):
        op.execute(
            f"CREATE TRIGGER review_assignments_group_contributors_{action} "
            f"BEFORE {action.upper()} ON review_assignments "
            "WHEN NEW.review_group_id IS NOT NULL "
            "AND NEW.participant_status IN ('completed','partial','abandoned') AND ("
            "json_type(NEW.closed_contributor_ids) <> 'array' "
            "OR json_array_length(NEW.closed_contributor_ids) = 0 "
            "OR EXISTS (SELECT 1 FROM json_each(NEW.closed_contributor_ids) AS contributor "
            "LEFT JOIN review_group_members AS member "
            "ON member.group_id = NEW.review_group_id "
            "AND member.reviewer_id = contributor.value "
            "WHERE contributor.type <> 'text' OR member.reviewer_id IS NULL)"
            ") BEGIN SELECT RAISE(ABORT, 'closed contributors must be group members'); END"
        )


def downgrade() -> None:
    op.execute("DROP TRIGGER IF EXISTS review_assignments_group_contributors_update")
    op.execute("DROP TRIGGER IF EXISTS review_assignments_group_contributors_insert")
    op.execute("DROP TRIGGER IF EXISTS review_assignments_closed_contributors_immutable")
    op.execute("DROP TRIGGER IF EXISTS review_submissions_contributors_immutable")
    op.execute("DROP TRIGGER IF EXISTS review_submissions_group_contributors_insert")
    with op.batch_alter_table(
        "review_submissions", naming_convention=NAMING_CONVENTION
    ) as batch:
        batch.drop_constraint("ck_submission_contributors_json", type_="check")
        batch.drop_constraint("ck_submission_owner", type_="check")
        batch.drop_constraint("fk_submission_submitter", type_="foreignkey")
        batch.drop_constraint("fk_submission_group_submitter", type_="foreignkey")
        batch.drop_constraint("fk_submission_assignment_group", type_="foreignkey")
        batch.alter_column("reviewer_id", existing_type=sa.String(), nullable=False)
        batch.drop_column("contributor_reviewer_ids")
        batch.drop_column("submitted_by_reviewer_id")
        batch.drop_column("review_group_id")

    for table, constraint_name in ASSESSMENT_TABLES:
        op.execute(f"DROP TRIGGER IF EXISTS {table}_assignment_member_insert")
        op.execute(f"DROP TRIGGER {table}_immutable_update")
        op.execute(f"DROP TRIGGER {table}_immutable_delete")
        with op.batch_alter_table(table, naming_convention=NAMING_CONVENTION) as batch:
            batch.drop_constraint(constraint_name, type_="foreignkey")
            batch.create_foreign_key(
                f"fk_{table}_assignment_reviewer",
                "review_assignments",
                ["assignment_id", "reviewer_id"],
                ["id", "reviewer_id"],
            )
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

    with op.batch_alter_table(
        "review_assignments", naming_convention=NAMING_CONVENTION
    ) as batch:
        batch.drop_constraint("ck_assignment_terminal_contributors", type_="check")
        batch.drop_constraint("ck_assignment_closed_contributors_json", type_="check")
        batch.drop_constraint("ck_assignment_completion_counts", type_="check")
        batch.drop_constraint("ck_assignment_participant_status", type_="check")
        batch.drop_constraint("ck_assignment_owner", type_="check")
        batch.drop_constraint("uq_assignment_review_group", type_="unique")
        batch.drop_constraint("fk_assignment_work_package", type_="foreignkey")
        batch.drop_constraint("fk_assignment_review_group", type_="foreignkey")
        batch.alter_column("reviewer_id", existing_type=sa.String(), nullable=False)
        batch.drop_column("closed_contributor_ids")
        batch.drop_column("completion_total_count")
        batch.drop_column("completion_item_count")
        batch.drop_column("completed_at")
        batch.drop_column("claimed_at")
        batch.drop_column("participant_status")
        batch.drop_column("work_package_id")
        batch.drop_column("review_group_id")

    op.drop_table("workshop_work_packages")
    op.drop_table("review_group_members")
    op.drop_table("review_groups")
    op.execute("DROP TRIGGER IF EXISTS workshop_rounds_capacity_update")
    op.drop_table("workshop_entry_redemptions")
    op.execute("DROP TRIGGER IF EXISTS workshop_entry_codes_round_capacity_update")
    op.execute("DROP TRIGGER IF EXISTS workshop_entry_codes_round_capacity_insert")
    op.drop_index(
        "uq_workshop_entry_codes_unrevoked_round",
        table_name="workshop_entry_codes",
    )
    op.drop_table("workshop_entry_codes")
    op.drop_table("workshop_rounds")
    op.execute("DROP TRIGGER reviewer_domain_expertise_immutable_delete")
    with op.batch_alter_table("reviewers", naming_convention=NAMING_CONVENTION) as batch:
        batch.drop_constraint("ck_reviewers_consent_pair", type_="check")
        batch.drop_constraint("ck_reviewers_registration_method", type_="check")
        batch.drop_column("consented_at")
        batch.drop_column("consent_statement_version")
        batch.drop_column("email_verified_at")
        batch.drop_column("registration_method")
    op.execute(
        "CREATE TRIGGER reviewer_domain_expertise_immutable_delete "
        "BEFORE DELETE ON reviewer_domain_expertise "
        "WHEN COALESCE((SELECT status FROM reviewers WHERE id = OLD.reviewer_id), '') "
        "<> 'withdrawn' BEGIN "
        "SELECT RAISE(ABORT, 'append-only table'); END"
    )
