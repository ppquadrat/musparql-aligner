"""Remember each reviewer's last-opened team assignment per workshop batch."""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260913_12"
down_revision = "20260912_11"
branch_labels = None
depends_on = None


def _context_trigger(action: str) -> None:
    op.execute(
        f"CREATE TRIGGER reviewer_workshop_batch_contexts_valid_{action} "
        f"BEFORE {action.upper()} ON reviewer_workshop_batch_contexts "
        "WHEN NOT EXISTS ("
        "SELECT 1 FROM review_assignments AS assignment "
        "JOIN review_group_members AS member "
        "ON member.group_id = assignment.review_group_id "
        "WHERE assignment.id = NEW.assignment_id "
        "AND assignment.work_package_id = NEW.work_package_id "
        "AND member.reviewer_id = NEW.reviewer_id"
        ") BEGIN SELECT RAISE(ABORT, "
        "'Workshop batch context must reference the reviewer membership and package'"
        "); END"
    )


def upgrade() -> None:
    op.create_table(
        "reviewer_workshop_batch_contexts",
        sa.Column("reviewer_id", sa.String(), nullable=False),
        sa.Column("work_package_id", sa.String(), nullable=False),
        sa.Column("assignment_id", sa.String(), nullable=False),
        sa.Column("selected_at", sa.String(), nullable=False),
        sa.ForeignKeyConstraint(["reviewer_id"], ["reviewers.id"]),
        sa.ForeignKeyConstraint(
            ["work_package_id"], ["workshop_work_packages.id"]
        ),
        sa.ForeignKeyConstraint(["assignment_id"], ["review_assignments.id"]),
        sa.PrimaryKeyConstraint("reviewer_id", "work_package_id"),
    )
    _context_trigger("insert")
    _context_trigger("update")
    op.execute(
        "INSERT INTO reviewer_workshop_batch_contexts "
        "(reviewer_id, work_package_id, assignment_id, selected_at) "
        "SELECT reviewer_id, work_package_id, assignment_id, selected_at FROM ("
        "SELECT member.reviewer_id AS reviewer_id, "
        "assignment.work_package_id AS work_package_id, "
        "assignment.id AS assignment_id, "
        "COALESCE(assignment.opened_at, assignment.claimed_at, "
        "assignment.created_at) AS selected_at, "
        "ROW_NUMBER() OVER ("
        "PARTITION BY member.reviewer_id, assignment.work_package_id "
        "ORDER BY COALESCE(assignment.opened_at, assignment.claimed_at, "
        "assignment.created_at) DESC, assignment.created_at DESC, "
        "assignment.id DESC"
        ") AS choice "
        "FROM review_group_members AS member "
        "JOIN review_assignments AS assignment "
        "ON assignment.review_group_id = member.group_id "
        "WHERE assignment.work_package_id IS NOT NULL"
        ") WHERE choice = 1"
    )


def downgrade() -> None:
    op.execute(
        "DROP TRIGGER IF EXISTS reviewer_workshop_batch_contexts_valid_update"
    )
    op.execute(
        "DROP TRIGGER IF EXISTS reviewer_workshop_batch_contexts_valid_insert"
    )
    op.drop_table("reviewer_workshop_batch_contexts")
