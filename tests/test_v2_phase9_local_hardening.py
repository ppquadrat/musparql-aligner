from __future__ import annotations

import json
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker
import pytest

from musparql.web.local_hardening import HardeningError, main, run_verification


OBSERVATION = {
    "observer": "synthetic-operator",
    "onboarding_seconds": 180,
    "repeat_assessment_seconds": 30,
    "mobile_login_success": True,
    "feedback": "Synthetic controls were clear and no assistance was required.",
}


def test_phase9_hardening_gate_exercises_local_pilot_and_recovery(tmp_path: Path) -> None:
    report = run_verification(tmp_path / "phase9", OBSERVATION)

    schema_path = Path(__file__).resolve().parents[1] / "schemas/local_hardening.schema.json"
    validator = Draft202012Validator(
        json.loads(schema_path.read_text(encoding="utf-8")),
        format_checker=FormatChecker(),
    )
    validator.validate(report)

    assert report["result"] == "passed"
    assert report["synthetic_only"] is True
    assert all(report["flows"].values())
    assert all(report["browser_checks"].values())
    assert report["processing_restart"]["recovered_jobs"] == 1
    assert report["database_restore"]["integrity_check"] == "ok"
    assert report["privacy_log_inspection"]["forbidden_values_absent"] is True
    assert report["usability_observation"]["targets_met"] is True


def test_phase9_rejects_occupied_workspace_and_failed_friction_target(
    tmp_path: Path,
) -> None:
    occupied = tmp_path / "occupied"
    occupied.mkdir()
    (occupied / "keep.txt").write_text("keep\n", encoding="utf-8")
    with pytest.raises(ValueError, match="empty"):
        run_verification(occupied, OBSERVATION)

    slow = dict(OBSERVATION, onboarding_seconds=301)
    with pytest.raises(HardeningError, match="five-minute"):
        run_verification(tmp_path / "slow", slow)


def test_phase9_requires_complete_human_observation(tmp_path: Path) -> None:
    missing_feedback = dict(OBSERVATION, feedback="")
    with pytest.raises(ValueError, match="required"):
        run_verification(tmp_path / "missing-feedback", missing_feedback)

    failed_mobile = dict(OBSERVATION, mobile_login_success=False)
    with pytest.raises(ValueError, match="mobile login"):
        run_verification(tmp_path / "failed-mobile", failed_mobile)


def test_phase9_cli_writes_report(tmp_path: Path, capsys) -> None:
    observation_path = tmp_path / "observation.json"
    observation_path.write_text(json.dumps(OBSERVATION), encoding="utf-8")
    report_path = tmp_path / "reports" / "phase9.json"

    assert main(
        [
            "--usability-observation",
            str(observation_path),
            "--output",
            str(report_path),
        ]
    ) == 0

    stdout_report = json.loads(capsys.readouterr().out)
    saved_report = json.loads(report_path.read_text(encoding="utf-8"))
    assert stdout_report == saved_report
    assert saved_report["schema"] == "musparql.local-hardening.v1"
