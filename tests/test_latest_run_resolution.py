from __future__ import annotations

import json
from pathlib import Path

from scripts.build_review_bundle import resolve_latest_frozen_run


def write_run(root: Path, run_id: str, created_at: str, output_count: int = 1) -> None:
    run_dir = root / run_id
    run_dir.mkdir()
    (run_dir / "llm_inputs.jsonl").write_text('{"query_id":"q"}\n', encoding="utf-8")
    (run_dir / "llm_outputs.jsonl").write_text(
        "".join('{"query_id":"q"}\n' for _ in range(output_count)),
        encoding="utf-8",
    )
    (run_dir / "manifest.json").write_text(
        json.dumps(
            {
                "run_id": run_id,
                "created_at": created_at,
                "files": {
                    "llm_inputs": {"filename": "llm_inputs.jsonl"},
                    "llm_outputs": {"filename": "llm_outputs.jsonl"},
                },
            }
        ),
        encoding="utf-8",
    )


def test_latest_run_resolves_files_from_newest_manifest(tmp_path: Path) -> None:
    write_run(tmp_path, "older", "2026-08-04T10:00:00+00:00")
    write_run(tmp_path, "newer", "2026-08-05T10:00:00+00:00")

    inputs, outputs, manifest = resolve_latest_frozen_run(tmp_path)

    assert inputs == (tmp_path / "newer" / "llm_inputs.jsonl").resolve()
    assert outputs == (tmp_path / "newer" / "llm_outputs.jsonl").resolve()
    assert manifest == (tmp_path / "newer" / "manifest.json").resolve()
