"""Allow an audited owner reset for workshop-code participants."""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260911_08"
down_revision = "20260910_07"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "workshop_session_resets",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column(
            "actor_reviewer_id",
            sa.String(),
            sa.ForeignKey("reviewers.id"),
            nullable=False,
        ),
        sa.Column(
            "target_reviewer_id",
            sa.String(),
            sa.ForeignKey("reviewers.id"),
            nullable=False,
        ),
        sa.Column("created_at", sa.String(), nullable=False),
    )
    op.execute(
        "CREATE TRIGGER workshop_session_resets_immutable_update "
        "BEFORE UPDATE ON workshop_session_resets BEGIN "
        "SELECT RAISE(ABORT, 'append-only table'); END"
    )


def downgrade() -> None:
    op.execute("DROP TRIGGER IF EXISTS workshop_session_resets_immutable_update")
    op.drop_table("workshop_session_resets")
