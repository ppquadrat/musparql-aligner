"""Preserve a reviewer's state while the account is disabled."""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260819_03"
down_revision = "20260819_02"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "reviewers",
        sa.Column("disabled_from_status", sa.String(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("reviewers", "disabled_from_status")
