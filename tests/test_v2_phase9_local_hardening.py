from __future__ import annotations

import json
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker
import pytest

from musparql.web.local_hardening import (
    HardeningError,
    InteractiveSyntheticEmailSender,
    REVIEWER_EMAIL,
    _bundle_bytes,
    _linguistic_bundle_bytes,
    main,
    run_interactive_pilot,
    run_verification,
)


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


@pytest.mark.parametrize(
    "changes, message",
    [
        ({"observer": None}, "strings"),
        ({"feedback": None}, "strings"),
        ({"onboarding_seconds": float("nan")}, "finite"),
        ({"repeat_assessment_seconds": float("inf")}, "finite"),
    ],
)
def test_phase9_rejects_non_text_and_non_finite_observations(
    tmp_path: Path, changes: dict[str, object], message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        run_verification(tmp_path / "invalid-observation", dict(OBSERVATION, **changes))


def test_phase9_bundle_can_supply_the_browser_export_run_contract() -> None:
    bundle = json.loads(_bundle_bytes())

    assert bundle["single_run_id"] == "synthetic-phase9-run"
    assert bundle["single_run_id"] in bundle["run_ids"]
    assert isinstance(bundle["runs"], list)


def test_phase9_interactive_pilot_includes_synthetic_linguistic_work() -> None:
    bundle = json.loads(_linguistic_bundle_bytes())

    assert bundle["mode"] == "linguistic"
    assert bundle["record_count"] == 1
    assert bundle["records"][0]["trial_id"] == "synthetic-phase9-linguistic-trial"


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


def test_interactive_pilot_is_loopback_only_and_exposes_synthetic_code(
    tmp_path: Path,
) -> None:
    emitted: list[str] = []
    captured: dict[str, object] = {}

    class FakeServer:
        server_port = 43123

        def serve_forever(self) -> None:
            raise KeyboardInterrupt

        def server_close(self) -> None:
            captured["closed"] = True

    def server_factory(host, port, app, *, threaded):
        captured.update(host=host, port=port, threaded=threaded)
        client = app.test_client()
        client.get("/")
        csrf = client.get_cookie("musparql_csrf", path="/")
        assert csrf is not None
        response = client.post(
            "/auth/login",
            data={"csrf_token": csrf.value, "email": REVIEWER_EMAIL},
        )
        assert response.status_code == 302
        app.extensions["musparql_email_dispatcher"].wait_for_idle()
        return FakeServer()

    assert run_interactive_pilot(
        tmp_path / "interactive",
        port=0,
        emit=emitted.append,
        server_factory=server_factory,
    ) == 0

    assert captured == {
        "host": "127.0.0.1",
        "port": 0,
        "threaded": True,
        "closed": True,
    }
    output = "\n".join(emitted)
    assert "http://127.0.0.1:43123/" in output
    assert REVIEWER_EMAIL in output
    assert "Synthetic login code" in output
    assert "not emailed or written to the application log" in output


def test_interactive_sender_retains_test_outbox_and_emits_code() -> None:
    emitted: list[str] = []
    sender = InteractiveSyntheticEmailSender(emitted.append)

    sender.send_login_code(REVIEWER_EMAIL, "123456")

    assert sender.outbox[0].value == "123456"
    assert "123456" in emitted[0]
