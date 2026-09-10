"""Add the IPL workshop admission, reviewing-group, and package foundation."""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260910_07"
down_revision = "20260820_06"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # SQLite batch mode rebuilds reviewers, which invalidates existing triggers
    # that protect reviewer-linked provenance tables during the rename. These
    # nullable/default-compatible additions are supported safely in place.
    op.add_column(
        "reviewers",
        sa.Column(
            "registration_method",
            sa.String(),
            nullable=False,
            server_default="email_invitation",
        )
    )
    op.add_column("reviewers", sa.Column("email_verified_at", sa.String()))
    op.add_column("reviewers", sa.Column("consent_statement_version", sa.String()))
    op.add_column("reviewers", sa.Column("consented_at", sa.String()))

    op.create_table(
        "workshop_rounds",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("opens_at", sa.String(), nullable=False),
        sa.Column("closes_at", sa.String(), nullable=False),
        sa.Column("max_participants", sa.Integer(), nullable=False),
        sa.Column(
            "allow_additional_assignments",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
        sa.Column("created_at", sa.String(), nullable=False),
        sa.CheckConstraint(
            "status IN ('draft','open','closed')",
            name="ck_workshop_rounds_status",
        ),
        sa.CheckConstraint(
            "max_participants > 0", name="ck_workshop_rounds_capacity"
        ),
        sa.CheckConstraint(
            "opens_at < closes_at", name="ck_workshop_rounds_window"
        ),
    )
    op.create_table(
        "workshop_entry_codes",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column(
            "workshop_round_id",
            sa.String(),
            sa.ForeignKey("workshop_rounds.id"),
            nullable=False,
        ),
        sa.Column("code_digest", sa.Text(), nullable=False),
        sa.Column("expires_at", sa.String(), nullable=False),
        sa.Column("max_redemptions", sa.Integer(), nullable=False),
        sa.Column(
            "redemption_count", sa.Integer(), nullable=False, server_default="0"
        ),
        sa.Column("revoked_at", sa.String()),
        sa.Column("created_at", sa.String(), nullable=False),
        sa.CheckConstraint(
            "max_redemptions > 0", name="ck_workshop_entry_codes_capacity"
        ),
        sa.CheckConstraint(
            "redemption_count >= 0 AND redemption_count <= max_redemptions",
            name="ck_workshop_entry_codes_redemptions",
        ),
    )
    op.create_index(
        "uq_workshop_entry_codes_unrevoked_round",
        "workshop_entry_codes",
        ["workshop_round_id"],
        unique=True,
        sqlite_where=sa.text("revoked_at IS NULL"),
    )
    op.create_table(
        "workshop_entry_redemptions",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column(
            "entry_code_id",
            sa.String(),
            sa.ForeignKey("workshop_entry_codes.id"),
            nullable=False,
        ),
        sa.Column(
            "reviewer_id",
            sa.String(),
            sa.ForeignKey("reviewers.id"),
            nullable=False,
            unique=True,
        ),
        sa.Column("redeemed_at", sa.String(), nullable=False),
    )
    op.create_table(
        "review_groups",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column(
            "workshop_round_id",
            sa.String(),
            sa.ForeignKey("workshop_rounds.id"),
            nullable=False,
        ),
        sa.Column("join_code_digest", sa.Text(), nullable=False, unique=True),
        sa.Column("created_at", sa.String(), nullable=False),
    )
    op.create_table(
        "review_group_members",
        sa.Column(
            "group_id",
            sa.String(),
            sa.ForeignKey("review_groups.id"),
            primary_key=True,
        ),
        sa.Column(
            "reviewer_id",
            sa.String(),
            sa.ForeignKey("reviewers.id"),
            primary_key=True,
        ),
        sa.Column("joined_at", sa.String(), nullable=False),
    )
    op.create_table(
        "workshop_work_packages",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column(
            "workshop_round_id",
            sa.String(),
            sa.ForeignKey("workshop_rounds.id"),
            nullable=False,
        ),
        sa.Column("kg_id", sa.String(), nullable=False),
        sa.Column("seed_version", sa.String(), nullable=False),
        sa.Column("seed_digest", sa.String(), nullable=False),
        sa.Column("display_name", sa.Text(), nullable=False),
        sa.Column("short_description", sa.Text(), nullable=False, server_default=""),
        sa.Column("display_order", sa.Integer(), nullable=False),
        sa.Column("bundle_path", sa.Text(), nullable=False),
        sa.Column("bundle_digest", sa.String(), nullable=False),
        sa.Column("processing_recipe", sa.String(), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("created_at", sa.String(), nullable=False),
        sa.ForeignKeyConstraint(
            ["kg_id", "seed_version", "seed_digest"],
            [
                "kg_seed_snapshots.kg_id",
                "kg_seed_snapshots.seed_version",
                "kg_seed_snapshots.seed_digest",
            ],
            name="fk_workshop_package_seed",
        ),
        sa.UniqueConstraint(
            "workshop_round_id", "kg_id", name="uq_workshop_package_round_kg"
        ),
        sa.UniqueConstraint(
            "workshop_round_id",
            "display_order",
            name="uq_workshop_package_display_order",
        ),
        sa.CheckConstraint(
            "display_order > 0", name="ck_workshop_package_display_order"
        ),
        sa.CheckConstraint(
            "bundle_digest LIKE 'sha256:%'",
            name="ck_workshop_package_bundle_digest",
        ),
        sa.CheckConstraint(
            "processing_recipe IN ('validate_initial_review','stage_initial_benchmark_update')",
            name="ck_workshop_package_processing_recipe",
        ),
    )


def downgrade() -> None:
    op.drop_table("workshop_work_packages")
    op.drop_table("review_group_members")
    op.drop_table("review_groups")
    op.drop_table("workshop_entry_redemptions")
    op.drop_index(
        "uq_workshop_entry_codes_unrevoked_round",
        table_name="workshop_entry_codes",
    )
    op.drop_table("workshop_entry_codes")
    op.drop_table("workshop_rounds")
    op.drop_column("reviewers", "consented_at")
    op.drop_column("reviewers", "consent_statement_version")
    op.drop_column("reviewers", "email_verified_at")
    op.drop_column("reviewers", "registration_method")
