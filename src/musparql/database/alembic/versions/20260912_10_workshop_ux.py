"""Record workshop assessment deferrals and late follow-up timing."""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260912_10"
down_revision = "20260911_09"
branch_labels = None
depends_on = None

NAMING_CONVENTION = {
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
}

ASSESSMENT_TABLES = (
    (
        "reviewer_kg_domain_assessments",
        "ck_domain_assessment_context",
        "ck_domain_assessment_assignment",
    ),
    (
        "reviewer_resource_familiarity_assessments",
        "ck_familiarity_assessment_context",
        "ck_familiarity_assessment_assignment",
    ),
)


def _assessment_membership_trigger(table: str) -> None:
    op.execute(
        f"CREATE TRIGGER {table}_assignment_member_insert "
        f"BEFORE INSERT ON {table} "
        "WHEN NEW.context IN ('pre_review','post_review_followup') AND NOT EXISTS ("
        "SELECT 1 FROM review_assignments AS assignment "
        "LEFT JOIN review_group_members AS member "
        "ON member.group_id = assignment.review_group_id "
        "AND member.reviewer_id = NEW.reviewer_id "
        "WHERE assignment.id = NEW.assignment_id "
        "AND (assignment.reviewer_id = NEW.reviewer_id "
        "OR member.reviewer_id IS NOT NULL)"
        ") BEGIN SELECT RAISE(ABORT, 'assessment reviewer is not an assignment participant'); END"
    )


def _assessment_triggers(table: str) -> None:
    _assessment_membership_trigger(table)
    op.execute(
        f"CREATE TRIGGER {table}_immutable_update BEFORE UPDATE ON {table} BEGIN "
        "SELECT RAISE(ABORT, 'append-only table'); END"
    )
    op.execute(
        f"CREATE TRIGGER {table}_immutable_delete BEFORE DELETE ON {table} BEGIN "
        "SELECT RAISE(ABORT, 'append-only table'); END"
    )


def upgrade() -> None:
    op.create_table(
        "workshop_assessment_deferrals",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column(
            "assignment_id",
            sa.String(),
            sa.ForeignKey("review_assignments.id"),
            nullable=False,
        ),
        sa.Column(
            "reviewer_id",
            sa.String(),
            sa.ForeignKey("reviewers.id"),
            nullable=False,
        ),
        sa.Column("deferred_at", sa.String(), nullable=False),
        sa.UniqueConstraint(
            "assignment_id", "reviewer_id", name="uq_workshop_assessment_deferral"
        ),
    )
    op.create_index(
        "ix_workshop_assessment_deferrals_assignment_id",
        "workshop_assessment_deferrals",
        ["assignment_id"],
    )
    op.create_index(
        "ix_workshop_assessment_deferrals_reviewer_id",
        "workshop_assessment_deferrals",
        ["reviewer_id"],
    )
    op.execute(
        "CREATE TRIGGER workshop_assessment_deferrals_immutable_update "
        "BEFORE UPDATE ON workshop_assessment_deferrals BEGIN "
        "SELECT RAISE(ABORT, 'append-only table'); END"
    )
    op.execute(
        "CREATE TRIGGER workshop_assessment_deferrals_immutable_delete "
        "BEFORE DELETE ON workshop_assessment_deferrals BEGIN "
        "SELECT RAISE(ABORT, 'append-only table'); END"
    )
    op.execute(
        "CREATE TRIGGER workshop_assessment_deferrals_assignment_member_insert "
        "BEFORE INSERT ON workshop_assessment_deferrals WHEN NOT EXISTS ("
        "SELECT 1 FROM review_assignments AS assignment "
        "JOIN review_group_members AS member "
        "ON member.group_id = assignment.review_group_id "
        "AND member.reviewer_id = NEW.reviewer_id "
        "WHERE assignment.id = NEW.assignment_id"
        ") BEGIN SELECT RAISE(ABORT, 'deferral reviewer is not an assignment participant'); END"
    )

    for table, context_constraint, assignment_constraint in ASSESSMENT_TABLES:
        op.execute(f"DROP TRIGGER {table}_immutable_update")
        op.execute(f"DROP TRIGGER {table}_immutable_delete")
        with op.batch_alter_table(
            table, naming_convention=NAMING_CONVENTION
        ) as batch:
            batch.drop_constraint(context_constraint, type_="check")
            batch.drop_constraint(assignment_constraint, type_="check")
            batch.create_check_constraint(
                context_constraint,
                "context IN ('pre_review','post_review_followup','profile')",
            )
            batch.create_check_constraint(
                assignment_constraint,
                "(context IN ('pre_review','post_review_followup') "
                "AND assignment_id IS NOT NULL) OR "
                "(context = 'profile' AND assignment_id IS NULL)",
            )
        _assessment_triggers(table)


def downgrade() -> None:
    connection = op.get_bind()
    deferral_count = connection.execute(
        sa.text("SELECT COUNT(*) FROM workshop_assessment_deferrals")
    ).scalar_one()
    followup_count = sum(
        connection.execute(
            sa.text(f"SELECT COUNT(*) FROM {table} WHERE context = 'post_review_followup'")
        ).scalar_one()
        for table, _context_constraint, _assignment_constraint in ASSESSMENT_TABLES
    )
    if deferral_count or followup_count:
        raise RuntimeError(
            "Revision 20260912_10 cannot be downgraded after assessment deferrals "
            "or post-review follow-up assessments have been recorded"
        )
    for table, context_constraint, assignment_constraint in ASSESSMENT_TABLES:
        op.execute(f"DROP TRIGGER {table}_immutable_update")
        op.execute(f"DROP TRIGGER {table}_immutable_delete")
        with op.batch_alter_table(
            table, naming_convention=NAMING_CONVENTION
        ) as batch:
            batch.drop_constraint(context_constraint, type_="check")
            batch.drop_constraint(assignment_constraint, type_="check")
            batch.create_check_constraint(
                context_constraint, "context IN ('pre_review','profile')"
            )
            batch.create_check_constraint(
                assignment_constraint,
                "(context = 'pre_review' AND assignment_id IS NOT NULL) OR "
                "(context = 'profile' AND assignment_id IS NULL)",
            )
        _assessment_triggers(table)

    op.execute("DROP TRIGGER workshop_assessment_deferrals_assignment_member_insert")
    op.execute("DROP TRIGGER workshop_assessment_deferrals_immutable_delete")
    op.execute("DROP TRIGGER workshop_assessment_deferrals_immutable_update")
    op.drop_index(
        "ix_workshop_assessment_deferrals_reviewer_id",
        table_name="workshop_assessment_deferrals",
    )
    op.drop_index(
        "ix_workshop_assessment_deferrals_assignment_id",
        table_name="workshop_assessment_deferrals",
    )
    op.drop_table("workshop_assessment_deferrals")
