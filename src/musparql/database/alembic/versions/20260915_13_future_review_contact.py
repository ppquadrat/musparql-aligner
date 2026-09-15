"""Store the optional future-review contact preference."""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260915_13"
down_revision = "20260913_12"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "reviewers",
        sa.Column(
            "future_review_contact_allowed",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
    )


def downgrade() -> None:
    op.drop_column("reviewers", "future_review_contact_allowed")
