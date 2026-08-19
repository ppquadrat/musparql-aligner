"""Allow the separately contracted Phase 6b linguistic assignment mode."""
from __future__ import annotations

from alembic import op


revision = "20260819_05"
down_revision = "20260819_04"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("review_assignments") as batch:
        batch.drop_constraint("ck_assignment_mode", type_="check")
        batch.drop_constraint("ck_assignment_recipe", type_="check")
        batch.drop_constraint("ck_assignment_mode_recipe", type_="check")
        batch.create_check_constraint(
            "ck_assignment_mode", "mode IN ('initial','compare','linguistic')"
        )
        batch.create_check_constraint(
            "ck_assignment_recipe",
            "processing_recipe IN ('validate_initial_review','stage_initial_benchmark_update',"
            "'validate_comparative_review','stage_comparative_benchmark_update',"
            "'validate_linguistic_annotation')",
        )
        batch.create_check_constraint(
            "ck_assignment_mode_recipe",
            "(mode = 'initial' AND processing_recipe IN ('validate_initial_review','stage_initial_benchmark_update')) OR "
            "(mode = 'compare' AND processing_recipe IN ('validate_comparative_review','stage_comparative_benchmark_update')) OR "
            "(mode = 'linguistic' AND processing_recipe = 'validate_linguistic_annotation')",
        )


def downgrade() -> None:
    with op.batch_alter_table("review_assignments") as batch:
        batch.drop_constraint("ck_assignment_mode", type_="check")
        batch.drop_constraint("ck_assignment_recipe", type_="check")
        batch.drop_constraint("ck_assignment_mode_recipe", type_="check")
        batch.create_check_constraint("ck_assignment_mode", "mode IN ('initial','compare')")
        batch.create_check_constraint(
            "ck_assignment_recipe",
            "processing_recipe IN ('validate_initial_review','stage_initial_benchmark_update',"
            "'validate_comparative_review','stage_comparative_benchmark_update')",
        )
        batch.create_check_constraint(
            "ck_assignment_mode_recipe",
            "(mode = 'initial' AND processing_recipe IN ('validate_initial_review','stage_initial_benchmark_update')) OR "
            "(mode = 'compare' AND processing_recipe IN ('validate_comparative_review','stage_comparative_benchmark_update'))",
        )
