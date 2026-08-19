"""Permit domain-profile erasure only for withdrawn reviewers."""
from __future__ import annotations

from alembic import op


revision = "20260819_04"
down_revision = "20260819_03"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("DROP TRIGGER reviewer_domain_expertise_immutable_delete")
    op.execute(
        "CREATE TRIGGER reviewer_domain_expertise_immutable_delete "
        "BEFORE DELETE ON reviewer_domain_expertise "
        "WHEN COALESCE((SELECT status FROM reviewers WHERE id = OLD.reviewer_id), '') "
        "<> 'withdrawn' BEGIN "
        "SELECT RAISE(ABORT, 'append-only table'); END"
    )


def downgrade() -> None:
    op.execute("DROP TRIGGER reviewer_domain_expertise_immutable_delete")
    op.execute(
        "CREATE TRIGGER reviewer_domain_expertise_immutable_delete "
        "BEFORE DELETE ON reviewer_domain_expertise BEGIN "
        "SELECT RAISE(ABORT, 'append-only table'); END"
    )
