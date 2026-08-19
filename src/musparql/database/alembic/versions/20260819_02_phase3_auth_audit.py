"""Add the append-only owner action audit trail for Phase 3."""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260819_02"
down_revision = "20260818_01"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "owner_audit_events",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column(
            "actor_reviewer_id",
            sa.String(),
            sa.ForeignKey("reviewers.id"),
            nullable=False,
        ),
        sa.Column("target_reviewer_id", sa.String(), nullable=False),
        sa.Column("action", sa.String(), nullable=False),
        sa.Column("created_at", sa.String(), nullable=False),
        sa.CheckConstraint(
            "action IN ('invite','disable','restore','delete')",
            name="ck_owner_audit_events_action",
        ),
    )
    op.create_index(
        "ix_owner_audit_events_actor_reviewer_id",
        "owner_audit_events",
        ["actor_reviewer_id"],
    )
    op.create_index(
        "ix_owner_audit_events_target_reviewer_id",
        "owner_audit_events",
        ["target_reviewer_id"],
    )
    # Ordinary application code can only insert events. Updates are forbidden at
    # the database boundary, while deletion remains available to the future
    # owner-operated retention job required by the one-year governance rule.
    op.execute(
        "CREATE TRIGGER owner_audit_events_immutable_update "
        "BEFORE UPDATE ON owner_audit_events BEGIN "
        "SELECT RAISE(ABORT, 'append-only table'); END"
    )


def downgrade() -> None:
    op.execute("DROP TRIGGER IF EXISTS owner_audit_events_immutable_update")
    op.drop_index(
        "ix_owner_audit_events_target_reviewer_id",
        table_name="owner_audit_events",
    )
    op.drop_index(
        "ix_owner_audit_events_actor_reviewer_id",
        table_name="owner_audit_events",
    )
    op.drop_table("owner_audit_events")
