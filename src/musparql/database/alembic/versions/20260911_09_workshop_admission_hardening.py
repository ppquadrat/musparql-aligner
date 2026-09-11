"""Harden workshop admission, throttling, and recovery invariants."""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260911_09"
down_revision = "20260911_08"
branch_labels = None
depends_on = None

NAMING_CONVENTION = {
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
}


def _create_expertise_delete_trigger() -> None:
    op.execute(
        "CREATE TRIGGER reviewer_domain_expertise_immutable_delete "
        "BEFORE DELETE ON reviewer_domain_expertise "
        "WHEN COALESCE((SELECT status FROM reviewers WHERE id = OLD.reviewer_id), '') "
        "<> 'withdrawn' BEGIN "
        "SELECT RAISE(ABORT, 'append-only table'); END"
    )


def upgrade() -> None:
    op.create_table(
        "workshop_admission_attempts",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("candidate_digest", sa.Text(), nullable=False),
        sa.Column("context_digest", sa.Text(), nullable=False),
        sa.Column("requested_at", sa.String(), nullable=False),
    )
    op.create_index(
        "ix_workshop_admission_attempts_candidate_digest",
        "workshop_admission_attempts",
        ["candidate_digest"],
    )
    op.create_index(
        "ix_workshop_admission_attempts_context_digest",
        "workshop_admission_attempts",
        ["context_digest"],
    )
    op.create_index(
        "ix_workshop_admission_attempts_requested_at",
        "workshop_admission_attempts",
        ["requested_at"],
    )
    op.create_table(
        "workshop_admission_nonces",
        sa.Column("nonce_digest", sa.Text(), primary_key=True),
        sa.Column(
            "reviewer_id",
            sa.String(),
            sa.ForeignKey("reviewers.id"),
            nullable=False,
            unique=True,
        ),
        sa.Column("created_at", sa.String(), nullable=False),
    )
    op.execute(
        "CREATE TRIGGER workshop_session_resets_immutable_delete "
        "BEFORE DELETE ON workshop_session_resets BEGIN "
        "SELECT RAISE(ABORT, 'append-only table'); END"
    )
    op.execute("DROP TRIGGER reviewer_domain_expertise_immutable_delete")
    with op.batch_alter_table(
        "reviewers", naming_convention=NAMING_CONVENTION
    ) as batch:
        batch.create_check_constraint(
            "ck_reviewers_workshop_email_unverified",
            "registration_method <> 'workshop_code' OR email_verified_at IS NULL",
        )
    _create_expertise_delete_trigger()


def downgrade() -> None:
    op.execute("DROP TRIGGER reviewer_domain_expertise_immutable_delete")
    with op.batch_alter_table(
        "reviewers", naming_convention=NAMING_CONVENTION
    ) as batch:
        batch.drop_constraint(
            "ck_reviewers_workshop_email_unverified", type_="check"
        )
    _create_expertise_delete_trigger()
    op.execute("DROP TRIGGER IF EXISTS workshop_session_resets_immutable_delete")
    op.drop_table("workshop_admission_nonces")
    op.drop_index(
        "ix_workshop_admission_attempts_requested_at",
        table_name="workshop_admission_attempts",
    )
    op.drop_index(
        "ix_workshop_admission_attempts_context_digest",
        table_name="workshop_admission_attempts",
    )
    op.drop_index(
        "ix_workshop_admission_attempts_candidate_digest",
        table_name="workshop_admission_attempts",
    )
    op.drop_table("workshop_admission_attempts")
