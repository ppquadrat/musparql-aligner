from __future__ import annotations

import json
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker
import pytest

from musparql.web.workshop_verify import _median, main, run_verification


def test_phase8_workshop_scenario_verifies_load_restart_and_failure_isolation(
    tmp_path: Path,
) -> None:
    report = run_verification(tmp_path / "phase8", reviewer_count=10)

    schema_path = Path(__file__).resolve().parents[1] / "schemas/workshop_verification.schema.json"
    validator = Draft202012Validator(
        json.loads(schema_path.read_text(encoding="utf-8")),
        format_checker=FormatChecker(),
    )
    validator.validate(report)

    assert report["schema"] == "musparql.workshop-verification.v1"
    assert report["result"] == "passed"
    assert report["synthetic_only"] is True
    assert report["target"] == {"concurrent_reviewers": 10}
    assert report["submission_latency_ms"]["samples"] == 10
    assert report["submission_latency_ms"]["worker_busy"] is True
    assert report["submission_latency_ms"]["p95"] > 0
    assert report["accepted_revisions"] == 12
    assert report["durable_files_verified"] == 12
    assert report["idempotent_retries"] == 1
    assert report["explicit_revisions"] == 1
    assert report["recovered_running_jobs"] == 1
    assert report["isolated_failed_jobs"] == 1
    assert report["processed_after_failure"] > 0
    assert report["queue_order_verified"] is True


def test_phase8_rejects_unsafe_or_undersized_workspaces(tmp_path: Path) -> None:
    occupied = tmp_path / "occupied"
    occupied.mkdir()
    (occupied / "keep.txt").write_text("keep\n", encoding="utf-8")

    with pytest.raises(ValueError, match="empty"):
        run_verification(occupied, reviewer_count=10)
    with pytest.raises(ValueError, match="between 10 and 50"):
        run_verification(tmp_path / "too-small", reviewer_count=9)


def test_phase8_cli_writes_a_machine_readable_report(tmp_path: Path, capsys) -> None:
    report_path = tmp_path / "reports" / "phase8.json"

    assert main(["--reviewers", "10", "--output", str(report_path)]) == 0

    stdout_report = json.loads(capsys.readouterr().out)
    saved_report = json.loads(report_path.read_text(encoding="utf-8"))
    assert stdout_report == saved_report
    assert saved_report["result"] == "passed"


def test_phase8_median_averages_the_middle_pair() -> None:
    assert _median([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]) == 5.5
