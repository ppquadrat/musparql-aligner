from __future__ import annotations

from pathlib import Path

import pytest

from scripts.runs.build_run_snapshot import infer_run_id


def test_infer_run_id_from_generation_start_and_model() -> None:
    records = [
        {"model": "MiniMax-M2.5", "generated_at": "2026-08-05T19:12:03+00:00"},
        {"model": "MiniMax-M2.5", "generated_at": "2026-08-05T19:11:55+00:00"},
    ]
    assert infer_run_id(records, Path("outputs.jsonl")) == "2026-08-05-191155-minimax-m2-5"


def test_infer_run_id_rejects_empty_or_mixed_outputs() -> None:
    with pytest.raises(ValueError, match="empty outputs"):
        infer_run_id([], Path("outputs.jsonl"))
    with pytest.raises(ValueError, match="mixed or unidentified models"):
        infer_run_id(
            [
                {"model": "model-a", "generated_at": "2026-08-05T19:11:55+00:00"},
                {"model": "model-b", "generated_at": "2026-08-05T19:11:56+00:00"},
            ],
            Path("outputs.jsonl"),
        )
