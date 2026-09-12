"""Store reviewer title, first name, and last name separately."""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260912_11"
down_revision = "20260912_10"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Do not guess how to split existing personal names. Leaving these fields
    # blank causes the profile gate to request an explicit structured name.
    op.add_column(
        "reviewers",
        sa.Column("title", sa.Text(), nullable=False, server_default=""),
    )
    op.add_column(
        "reviewers",
        sa.Column("first_name", sa.Text(), nullable=False, server_default=""),
    )
    op.add_column(
        "reviewers",
        sa.Column("last_name", sa.Text(), nullable=False, server_default=""),
    )


def downgrade() -> None:
    op.drop_column("reviewers", "last_name")
    op.drop_column("reviewers", "first_name")
    op.drop_column("reviewers", "title")
