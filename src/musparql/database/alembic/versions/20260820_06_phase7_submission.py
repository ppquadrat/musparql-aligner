"""Add Phase 7 owner inclusion and candidate-promotion state."""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260820_06"
down_revision = "20260819_05"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("review_submissions") as batch:
        batch.add_column(sa.Column("inclusion_status", sa.String(), nullable=False, server_default="pending"))
        batch.add_column(sa.Column("inclusion_reason", sa.Text()))
        batch.add_column(sa.Column("decided_by_reviewer_id", sa.String()))
        batch.add_column(sa.Column("decided_at", sa.String()))
        batch.create_check_constraint(
            "ck_submission_inclusion_status",
            "inclusion_status IN ('pending','included','revision_requested','rejected')",
        )
        batch.create_foreign_key(
            "fk_submission_deciding_owner", "reviewers",
            ["decided_by_reviewer_id"], ["id"],
        )
    with op.batch_alter_table("processing_jobs") as batch:
        batch.add_column(sa.Column("job_kind", sa.String(), nullable=False, server_default="submission"))
        batch.add_column(sa.Column("selected_submission_ids", sa.JSON()))
        batch.add_column(sa.Column("approval_status", sa.String(), nullable=False, server_default="pending"))
        batch.add_column(sa.Column("approval_reason", sa.Text()))
        batch.add_column(sa.Column("approved_by_reviewer_id", sa.String()))
        batch.add_column(sa.Column("approved_at", sa.String()))
        batch.create_check_constraint(
            "ck_processing_job_approval_status",
            "approval_status IN ('pending','approved','rejected')",
        )
        batch.create_check_constraint(
            "ck_processing_job_kind", "job_kind IN ('submission','combined_candidate')"
        )
        batch.create_check_constraint(
            "ck_processing_job_selection",
            "job_kind = 'submission' OR selected_submission_ids IS NOT NULL",
        )
        batch.create_foreign_key(
            "fk_processing_job_approving_owner", "reviewers",
            ["approved_by_reviewer_id"], ["id"],
        )
    op.create_table(
        "owner_processing_decisions",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("owner_reviewer_id", sa.String(), sa.ForeignKey("reviewers.id"), nullable=False),
        sa.Column("target_type", sa.String(), nullable=False),
        sa.Column("submission_id", sa.String(), sa.ForeignKey("review_submissions.id")),
        sa.Column("job_id", sa.String(), sa.ForeignKey("processing_jobs.id")),
        sa.Column("item_id", sa.Text()),
        sa.Column("decision", sa.String(), nullable=False),
        sa.Column("reason", sa.Text()),
        sa.Column("created_at", sa.String(), nullable=False),
        sa.CheckConstraint("target_type IN ('submission','item','candidate')", name="ck_owner_processing_target"),
        sa.CheckConstraint("decision IN ('included','omitted','revision_requested','rejected','approved')", name="ck_owner_processing_decision"),
        sa.CheckConstraint(
            "(target_type = 'submission' AND submission_id IS NOT NULL AND job_id IS NULL AND item_id IS NULL) OR "
            "(target_type = 'item' AND submission_id IS NOT NULL AND job_id IS NULL AND item_id IS NOT NULL) OR "
            "(target_type = 'candidate' AND submission_id IS NULL AND job_id IS NOT NULL AND item_id IS NULL)",
            name="ck_owner_processing_target_identity",
        ),
    )
    for action in ("update", "delete"):
        op.execute(
            f"CREATE TRIGGER owner_processing_decisions_immutable_{action} "
            f"BEFORE {action.upper()} ON owner_processing_decisions BEGIN "
            "SELECT RAISE(ABORT, 'append-only table'); END"
        )


def downgrade() -> None:
    op.execute("DROP TRIGGER IF EXISTS owner_processing_decisions_immutable_update")
    op.execute("DROP TRIGGER IF EXISTS owner_processing_decisions_immutable_delete")
    op.drop_table("owner_processing_decisions")
    with op.batch_alter_table("processing_jobs") as batch:
        batch.drop_constraint("fk_processing_job_approving_owner", type_="foreignkey")
        batch.drop_constraint("ck_processing_job_approval_status", type_="check")
        batch.drop_constraint("ck_processing_job_selection", type_="check")
        batch.drop_constraint("ck_processing_job_kind", type_="check")
        batch.drop_column("approved_at")
        batch.drop_column("approved_by_reviewer_id")
        batch.drop_column("approval_reason")
        batch.drop_column("approval_status")
        batch.drop_column("selected_submission_ids")
        batch.drop_column("job_kind")
    with op.batch_alter_table("review_submissions") as batch:
        batch.drop_constraint("fk_submission_deciding_owner", type_="foreignkey")
        batch.drop_constraint("ck_submission_inclusion_status", type_="check")
        batch.drop_column("decided_at")
        batch.drop_column("decided_by_reviewer_id")
        batch.drop_column("inclusion_reason")
        batch.drop_column("inclusion_status")
