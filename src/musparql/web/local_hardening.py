"""Synthetic local hardening and pilot gate for Musparql v2 Phase 9."""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import math
import os
from pathlib import Path
import platform
import sqlite3
import tempfile
import time
from typing import Any, Callable, Mapping

from sqlalchemy import func, select
from werkzeug.datastructures import MultiDict
from werkzeug.serving import BaseWSGIServer, make_server

from musparql.database import create_database_engine, session_factory
from musparql.database.migrations import upgrade_database
from musparql.database.models import (
    AssignmentKgSeed,
    KgSeedFamiliarityScope,
    KgSeedReviewDomain,
    KgSeedSnapshot,
    ProcessingJob,
    ReviewAssignment,
    Reviewer,
    ReviewerKgDomainAssessment,
    ReviewerResourceFamiliarityAssessment,
    ReviewSubmission,
)
from musparql.linguistic_dimensions import build_bundle, text_digest
from . import create_app
from .auth import timestamp, utc_now
from .email import SyntheticEmailSender


APP_SECRET = "synthetic-phase-9-secret-with-at-least-32-bytes"
OWNER_ID = "reviewer-9000"
REVIEWER_ID = "reviewer-9001"
OTHER_REVIEWER_ID = "reviewer-9002"
REVIEWER_EMAIL = "phase9-reviewer@example.invalid"
OTHER_REVIEWER_EMAIL = "phase9-other@example.invalid"
REVIEWER_NAME = "Synthetic Phase 9 Reviewer"
DATASET_ID = "synthetic-phase9-dataset"
KG_ID = "synthetic-phase9-kg"
SEED_VERSION = "synthetic-phase9-seed-v1"
ONBOARDING_TARGET_SECONDS = 300
REPEAT_ASSESSMENT_TARGET_SECONDS = 60
LINGUISTIC_ASSIGNMENT_ID = "assignment-000000000000000000000903"


class HardeningError(RuntimeError):
    """A Phase 9 hardening invariant did not hold."""


class _LogCapture(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.messages: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.messages.append(self.format(record))


class InteractiveSyntheticEmailSender(SyntheticEmailSender):
    """Expose synthetic-only deliveries to the local pilot operator."""

    def __init__(self, emit: Callable[[str], None] = print) -> None:
        super().__init__()
        self._emit = emit

    def send_login_code(self, recipient: str, code: str) -> None:
        super().send_login_code(recipient, code)
        self._emit(
            f"\nSynthetic login code for {recipient}: {code}\n"
            "This fictional code was not emailed or written to the application log."
        )


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise HardeningError(message)


def _digest(raw: bytes) -> str:
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def _reviewer(reviewer_id: str, email: str, name: str) -> Reviewer:
    now = timestamp(utc_now())
    return Reviewer(
        id=reviewer_id,
        name=name,
        affiliation="",
        email_display=email,
        email_normalized=email,
        status="active",
        created_at=now,
        updated_at=now,
        privacy_notice_version=None,
        privacy_notice_acknowledged_at=None,
    )


def _bundle_bytes() -> bytes:
    run_id = "synthetic-phase9-run"
    payload = {
        "schema": "musparql.review-bundle.v2",
        "mode": "initial",
        "dataset_id": DATASET_ID,
        "built_at": "2026-08-21T12:00:00Z",
        "holdout_input_policy": "no_holdout",
        "run_ids": [run_id],
        "single_run_id": run_id,
        "runs": [{"run_id": run_id, "label": "Synthetic pilot"}],
        "record_count": 1,
        "records": [
            {
                "review_id": "synthetic-phase9-record",
                "kg_id": KG_ID,
                "query_id": "synthetic-phase9-query",
                "query_label": "Synthetic works by creator",
                "run_label": "Synthetic pilot",
                "input": {
                    "sparql_clean": (
                        "SELECT ?work WHERE { "
                        "?work <urn:synthetic:creator> <urn:synthetic:person> . }"
                    ),
                    "evidence": [
                        {
                            "evidence_id": "synthetic-evidence-1",
                            "type": "synthetic_documentation",
                            "snippet": (
                                "The fictional creator property connects a work to "
                                "its fictional creator."
                            ),
                        }
                    ],
                },
                "output": {
                    "nl_question": "Which fictional works were created by this person?",
                    "nl_question_origin": {
                        "mode": "synthetic",
                        "evidence_ids": ["synthetic-evidence-1"],
                    },
                    "confidence": 0.9,
                    "confidence_rationale": (
                        "Synthetic wording mirrors the single fictional graph pattern."
                    ),
                    "ranked_evidence_phrases": [
                        {
                            "rank": 1,
                            "source_type": "synthetic_documentation",
                            "evidence_id": "synthetic-evidence-1",
                            "verbatim": False,
                            "text": "creator connects a work to its creator",
                        }
                    ],
                },
                "output_meta": {"model": "synthetic-pilot", "elapsed_ms": 1},
            }
        ],
    }
    return (json.dumps(payload, sort_keys=True) + "\n").encode("utf-8")


def _linguistic_bundle_bytes() -> bytes:
    sparql = "SELECT ?work WHERE { ?work a <urn:synthetic:Work> . }"
    literal = "Which resources are instances of the class synthetic Work?"
    first = "Which synthetic works are there?"
    second = "Show me the works in this fictional collection."
    record = {
        "trial_id": "synthetic-phase9-linguistic-trial",
        "kg_id": KG_ID,
        "query_id": "synthetic-phase9-linguistic-query",
        "query_label": "Synthetic works",
        "sparql": sparql,
        "sparql_version": "v1",
        "sparql_digest": text_digest(sparql),
        "literal": {
            "formulation_id": "synthetic-literal",
            "version": "v1",
            "text": literal,
            "digest": text_digest(literal),
            "validated": True,
            "validation_provenance": {"source": "synthetic-phase9"},
        },
        "candidates": [
            {
                "formulation_id": "synthetic-candidate-a",
                "version": "v1",
                "text": first,
                "digest": text_digest(first),
                "provenance": {"origin": "synthetic-source"},
            },
            {
                "formulation_id": "synthetic-candidate-b",
                "version": "v1",
                "text": second,
                "digest": text_digest(second),
                "provenance": {"origin": "synthetic-model"},
            },
        ],
        "eligible": True,
        "non_holdout": True,
        "presentation_arity": 3,
        "sampling_stratum": "synthetic",
        "contrast_id": "synthetic-phase9-contrast",
    }
    payload = build_bundle(
        [record],
        dataset_id="synthetic-phase9-linguistic-dataset",
        seed="synthetic-phase9-linguistic-seed",
    )
    return (json.dumps(payload, sort_keys=True) + "\n").encode("utf-8")


def _seed_workspace(root: Path) -> tuple[Path, tuple[str, str]]:
    database_path = root / "phase9.sqlite3"
    bundle_root = root / "bundles"
    bundle_root.mkdir(mode=0o700)
    bundle_raw = _bundle_bytes()
    bundle_name = "synthetic-phase9-bundle.json"
    (bundle_root / bundle_name).write_bytes(bundle_raw)
    linguistic_raw = _linguistic_bundle_bytes()
    linguistic_bundle_name = "synthetic-phase9-linguistic-bundle.json"
    (bundle_root / linguistic_bundle_name).write_bytes(linguistic_raw)
    upgrade_database(database_path)
    engine = create_database_engine(database_path)
    sessions = session_factory(engine)
    now = timestamp(utc_now())
    assignment_ids = (
        "assignment-000000000000000000000901",
        "assignment-000000000000000000000902",
    )
    with sessions.begin() as session:
        session.add_all(
            [
                _reviewer(OWNER_ID, "phase9-owner@example.invalid", "Synthetic Phase 9 Owner"),
                _reviewer(REVIEWER_ID, REVIEWER_EMAIL, REVIEWER_NAME),
                _reviewer(
                    OTHER_REVIEWER_ID,
                    OTHER_REVIEWER_EMAIL,
                    "Synthetic Phase 9 Other Reviewer",
                ),
            ]
        )
        session.add(
            KgSeedSnapshot(
                kg_id=KG_ID,
                seed_version=SEED_VERSION,
                seed_digest="sha256:" + "9" * 64,
                previous_seed_digest=None,
                seed_json={"name": "Synthetic Phase 9 graph"},
            )
        )
        session.flush()
        session.add(
            KgSeedReviewDomain(
                kg_id=KG_ID,
                seed_version=SEED_VERSION,
                domain_id="synthetic-phase9-domain",
                label="Synthetic music knowledge",
                description="A fictional subject used only by the hardening gate.",
            )
        )
        session.add(
            KgSeedFamiliarityScope(
                kg_id=KG_ID,
                seed_version=SEED_VERSION,
                scope_id="synthetic-phase9-resource",
                label="Synthetic Phase 9 resource",
                description="A fictional resource used only by the hardening gate.",
            )
        )
        session.flush()
        for assignment_id in assignment_ids:
            session.add(
                ReviewAssignment(
                    id=assignment_id,
                    reviewer_id=REVIEWER_ID,
                    mode="initial",
                    status="ready",
                    bundle_path=bundle_name,
                    bundle_digest=_digest(bundle_raw),
                    previous_benchmark_path=None,
                    processing_recipe="validate_initial_review",
                    holdout_capability=False,
                    created_at=now,
                    opened_at=None,
                    submitted_at=None,
                )
            )
            session.flush()
            session.add(
                AssignmentKgSeed(
                    assignment_id=assignment_id,
                    kg_id=KG_ID,
                    seed_version=SEED_VERSION,
                    seed_digest="sha256:" + "9" * 64,
                )
            )
        session.add(
            ReviewAssignment(
                id=LINGUISTIC_ASSIGNMENT_ID,
                reviewer_id=REVIEWER_ID,
                mode="linguistic",
                status="ready",
                bundle_path=linguistic_bundle_name,
                bundle_digest=_digest(linguistic_raw),
                previous_benchmark_path=None,
                processing_recipe="validate_linguistic_annotation",
                holdout_capability=False,
                created_at=now,
                opened_at=None,
                submitted_at=None,
            )
        )
        session.flush()
        session.add(
            AssignmentKgSeed(
                assignment_id=LINGUISTIC_ASSIGNMENT_ID,
                kg_id=KG_ID,
                seed_version=SEED_VERSION,
                seed_digest="sha256:" + "9" * 64,
            )
        )
    engine.dispose()
    return database_path, assignment_ids


def _app_config(root: Path, database_path: Path, sender: SyntheticEmailSender) -> dict[str, Any]:
    project_root = Path(__file__).resolve().parents[3]
    return {
        "TESTING": True,
        "DATABASE_PATH": database_path,
        "APP_SECRET": APP_SECRET,
        "OWNER_REVIEWER_ID": OWNER_ID,
        "COOKIE_SECURE": False,
        "EMAIL_SENDER": sender,
        "ASSIGNMENT_BUNDLE_ROOT": root / "bundles",
        "SUBMISSION_ROOT": root / "submissions",
        "CANDIDATE_ROOT": root / "candidates",
        "REVIEW_WORKBENCH_ROOT": project_root / "review",
        "LINGUISTIC_WORKBENCH_ROOT": project_root / "review/linguistic",
        "REVIEW_EXPORT_SCHEMA_PATH": project_root / "schemas/review_export.schema.json",
        "LINGUISTIC_EXPORT_SCHEMA_PATH": (
            project_root / "schemas/linguistic_annotation_export.schema.json"
        ),
        "EXPERTISE_SUGGESTIONS_PATH": project_root / "catalog/expertise_domain_suggestions.yaml",
        "LANGUAGE_OPTIONS_PATH": project_root / "catalog/language_options.json",
        "PRIVACY_NOTICE_VERSION": "synthetic-phase9-v1",
        "PRIVACY_NOTICE_BODY": "Synthetic Phase 9 notice. Do not enter real data.",
    }


def _close_app(app: Any) -> None:
    app.extensions["musparql_email_dispatcher"].shutdown()
    app.extensions["musparql_engine"].dispose()


def run_interactive_pilot(
    root: Path,
    *,
    port: int = 8765,
    emit: Callable[[str], None] = print,
    server_factory: Callable[..., BaseWSGIServer] = make_server,
) -> int:
    """Serve a fictional reviewer pilot on loopback until the operator stops it."""
    if not 0 <= port <= 65535:
        raise ValueError("Interactive pilot port must be between 0 and 65535")
    root = root.resolve()
    if root.exists() and (not root.is_dir() or any(root.iterdir())):
        raise ValueError("The interactive pilot workspace must be an empty directory")
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    database_path, _assignment_ids = _seed_workspace(root)
    sender = InteractiveSyntheticEmailSender(emit)
    app = create_app(_app_config(root, database_path, sender))
    server = server_factory("127.0.0.1", port, app, threaded=True)
    actual_port = int(server.server_port)
    emit(
        "Musparql Phase 9 interactive synthetic pilot\n"
        f"Open: http://127.0.0.1:{actual_port}/\n"
        f"Fictional invited email: {REVIEWER_EMAIL}\n"
        "The login code will appear in this terminal after you request it.\n"
        "Use browser responsive-design mode for the narrow/mobile login check.\n"
        "Press Ctrl-C here when the pilot is complete."
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        emit("\nInteractive synthetic pilot stopped. The fictional workspace is disposable.")
    finally:
        server.server_close()
        _close_app(app)
    return 0


def _csrf(client: Any) -> str:
    cookie = client.get_cookie("musparql_csrf", path="/")
    if cookie is None:
        client.get("/")
        cookie = client.get_cookie("musparql_csrf", path="/")
    _require(cookie is not None, "The browser did not receive a CSRF cookie")
    return cookie.value


def _login(
    client: Any,
    app: Any,
    sender: SyntheticEmailSender,
    *,
    remembered: bool,
    user_agent: str,
    email: str = REVIEWER_EMAIL,
    verify_mobile_contract: bool = False,
) -> tuple[str, str]:
    position = sender.position()
    response = client.post(
        "/auth/login",
        data={"csrf_token": _csrf(client), "email": email},
        headers={"User-Agent": user_agent},
    )
    _require(response.status_code == 302, "Login-code request was not accepted")
    message = sender.wait_for("login_code", email, after_index=position)
    app.extensions["musparql_email_dispatcher"].wait_for_idle()
    if verify_mobile_contract:
        verify_page = client.get("/auth/verify", headers={"User-Agent": user_agent})
        _require(
            verify_page.status_code == 200
            and b'name="viewport"' in verify_page.data
            and b'inputmode="numeric"' in verify_page.data
            and b'autocomplete="one-time-code"' in verify_page.data,
            "Mobile verification page lacks its one-time-code input contract",
        )
    form = {"csrf_token": _csrf(client), "code": message.value}
    if remembered:
        form["remembered"] = "yes"
    response = client.post(
        "/auth/verify", data=form, headers={"User-Agent": user_agent}
    )
    _require(response.status_code == 302, "Login-code verification failed")
    cookie = client.get_cookie(app.config["AUTH_COOKIE_NAME"], path="/")
    _require(cookie is not None, "Successful login did not establish a session")
    return message.value, cookie.value


def _profile_form(client: Any) -> MultiDict:
    return MultiDict(
        [
            ("csrf_token", _csrf(client)),
            ("notice_acknowledged", "yes"),
            ("name", REVIEWER_NAME),
            ("affiliation", "Synthetic Phase 9 Institute"),
            ("kg_ontology_experience", "regular"),
            ("sparql_experience", "regular"),
            ("nlp_llm_experience", "occasional"),
            ("language_tag", "en"),
            ("language_level", "native"),
            ("new_domain_label", "Synthetic computational musicology"),
            ("new_domain_level", "advanced"),
        ]
    )


def _assessment_form(client: Any) -> MultiDict:
    return MultiDict(
        [
            ("csrf_token", _csrf(client)),
            ("domain_level", "advanced"),
            ("familiarity_level", "worked"),
            ("confirmed", "yes"),
        ]
    )


def _export(assignment_id: str, attributed_bundle: Mapping[str, Any]) -> dict[str, Any]:
    review_id = "synthetic-phase9-record"
    event_id = f"{review_id}::{REVIEWER_ID}"
    run_id = attributed_bundle.get("single_run_id")
    run_ids = attributed_bundle.get("run_ids")
    runs = attributed_bundle.get("runs")
    _require(
        isinstance(run_id, str)
        and bool(run_id)
        and isinstance(run_ids, list)
        and run_id in run_ids
        and isinstance(runs, list),
        "Synthetic hosted bundle cannot produce the workbench export contract",
    )
    return {
        "schema": "musparql.review-export.v2",
        "kind": "non_holdout_review_export",
        "assignment_id": assignment_id,
        "bundle_digest": attributed_bundle["bundle_digest"],
        "reviewer_id": REVIEWER_ID,
        "dataset_id": DATASET_ID,
        "run_id": run_id,
        "run_ids": run_ids,
        "runs": runs,
        "exported_at": "2026-08-21T12:10:00Z",
        "reviews": {
            review_id: {
                "review_id": event_id,
                "reviewer_id": REVIEWER_ID,
                "reviewed_at": "2026-08-21T12:09:00Z",
                "prior_review_ids": [],
                "authored_formulation_ids": [],
                "approved_formulation_ids": [f"{event_id}::formulation::candidate"],
                "benchmark_disposition": "included",
                "pipeline_assessment": "accepted",
                "preferred_question": "",
                "literal_wording": "",
                "public_comment": "Synthetic Phase 9 pilot decision.",
                "internal_comment": "",
                "split": "",
                "interpretive": {
                    "naturalness": None,
                    "pragmatism": None,
                    "room_for_interpretation": None,
                    "requires_graph_context_knowledge": False,
                },
            }
        },
    }


def _validate_observation(value: Mapping[str, Any]) -> dict[str, Any]:
    required = {
        "observer",
        "onboarding_seconds",
        "repeat_assessment_seconds",
        "mobile_login_success",
        "feedback",
    }
    if set(value) != required:
        raise ValueError("Usability observation fields do not match the Phase 9 contract")
    if not isinstance(value["observer"], str) or not isinstance(value["feedback"], str):
        raise ValueError("Usability observation text must be strings")
    observer = value["observer"].strip()
    feedback = value["feedback"].strip()
    onboarding = value["onboarding_seconds"]
    repeat = value["repeat_assessment_seconds"]
    if not observer or not feedback or isinstance(onboarding, bool) or isinstance(repeat, bool):
        raise ValueError("Usability observation text and timings are required")
    if not isinstance(onboarding, (int, float)) or not isinstance(repeat, (int, float)):
        raise ValueError("Usability timings must be numeric seconds")
    onboarding_seconds = float(onboarding)
    repeat_seconds = float(repeat)
    if not math.isfinite(onboarding_seconds) or not math.isfinite(repeat_seconds):
        raise ValueError("Usability timings must be finite numeric seconds")
    if onboarding_seconds <= 0 or repeat_seconds <= 0:
        raise ValueError("Usability timings must be positive")
    if value["mobile_login_success"] is not True:
        raise ValueError("The observed mobile login must succeed before Phase 9 can pass")
    return {
        "observer": observer,
        "onboarding_seconds": onboarding_seconds,
        "repeat_assessment_seconds": repeat_seconds,
        "mobile_login_success": True,
        "feedback": feedback,
    }


def _backup_database(source: Path, destination: Path) -> None:
    with sqlite3.connect(source) as source_connection, sqlite3.connect(destination) as target:
        source_connection.backup(target)


def run_verification(
    root: Path, usability_observation: Mapping[str, Any]
) -> dict[str, Any]:
    """Run Phase 9 in a new empty directory using only synthetic identities and data."""
    observation = _validate_observation(usability_observation)
    if observation["onboarding_seconds"] > ONBOARDING_TARGET_SECONDS:
        raise HardeningError("Observed onboarding exceeded the five-minute friction target")
    if observation["repeat_assessment_seconds"] > REPEAT_ASSESSMENT_TARGET_SECONDS:
        raise HardeningError("Observed repeat assessment exceeded the one-minute friction target")
    root = root.resolve()
    if root.exists() and (not root.is_dir() or any(root.iterdir())):
        raise ValueError("The hardening workspace must be an empty directory")
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    database_path, assignment_ids = _seed_workspace(root)
    sender = SyntheticEmailSender()
    app = create_app(_app_config(root, database_path, sender))
    capture = _LogCapture()
    app.logger.addHandler(capture)
    started = time.perf_counter()

    private = app.test_client()
    login_code, private_token = _login(
        private,
        app,
        sender,
        remembered=True,
        user_agent="Synthetic Phase 9 private desktop browser",
    )
    remembered_cookie = private.get_cookie(app.config["AUTH_COOKIE_NAME"], path="/")
    _require(
        remembered_cookie is not None and remembered_cookie.expires is not None,
        "Private-browser remembered login did not receive a persistent cookie",
    )
    _require(private.get("/").status_code == 302, "Incomplete profile was not gated")
    profile_page = private.get("/profile")
    _require(
        profile_page.status_code == 200 and b"Complete your profile" in profile_page.data,
        "First-time onboarding was not available",
    )
    saved = private.post("/profile", data=_profile_form(private))
    _require(saved.status_code == 302, "Synthetic onboarding was not saved")

    first_assignment, repeat_assignment = assignment_ids
    first_page = private.get(f"/assignments/{first_assignment}")
    _require(first_page.status_code == 200, "First assessment page was unavailable")
    assessed = private.post(
        f"/assignments/{first_assignment}", data=_assessment_form(private)
    )
    _require(assessed.status_code == 302, "First assessment was not accepted")
    bundle_response = private.get(f"/assignments/{first_assignment}/bundle")
    _require(bundle_response.status_code == 200, "Attributed bundle was unavailable")
    attributed = bundle_response.get_json()
    _require(
        attributed["reviewer_id"] == REVIEWER_ID
        and attributed["assignment_id"] == first_assignment,
        "Hosted bundle attribution was incorrect",
    )
    _require(
        private.get(f"/assignments/{first_assignment}/workbench/").status_code == 200
        and private.get(
            f"/assignments/{first_assignment}/workbench/host_context.js"
        ).status_code
        == 200,
        "Hosted workbench assets were unavailable",
    )
    submission_response = private.post(
        f"/assignments/{first_assignment}/submissions",
        json=_export(first_assignment, attributed),
        headers={"X-CSRF-Token": _csrf(private)},
    )
    _require(submission_response.status_code == 202, "Synthetic review was not accepted")
    receipt = submission_response.get_json()

    repeat_page = private.get(f"/assignments/{repeat_assignment}")
    _require(
        repeat_page.status_code == 200
        and b'<option value="advanced" selected>' in repeat_page.data
        and b'<option value="worked" selected>' in repeat_page.data,
        "Repeat assessment did not preselect the most recent answers",
    )
    repeated = private.post(
        f"/assignments/{repeat_assignment}", data=_assessment_form(private)
    )
    _require(repeated.status_code == 302, "Repeat assessment was not accepted")

    shared = app.test_client()
    shared_code, shared_token = _login(
        shared,
        app,
        sender,
        remembered=False,
        user_agent="Synthetic Phase 9 shared browser",
    )
    shared_cookie = shared.get_cookie(app.config["AUTH_COOKIE_NAME"], path="/")
    _require(
        shared_cookie is not None and shared_cookie.expires is None,
        "Shared-browser default created a persistent login cookie",
    )
    logout = shared.post("/auth/logout", data={"csrf_token": _csrf(shared)})
    _require(
        logout.status_code == 302
        and shared.get_cookie(app.config["AUTH_COOKIE_NAME"], path="/") is None,
        "Shared-browser logout did not clear its session",
    )
    _require(private.get("/").status_code == 200, "Shared-browser logout revoked another browser")

    outsider = app.test_client()
    _require(
        outsider.get(f"/assignments/{first_assignment}/bundle").status_code == 302,
        "An unauthenticated browser reached an assignment bundle",
    )
    other_reviewer = app.test_client()
    other_code, other_token = _login(
        other_reviewer,
        app,
        sender,
        remembered=False,
        user_agent="Synthetic Phase 9 other reviewer browser",
        email=OTHER_REVIEWER_EMAIL,
    )
    _require(
        other_reviewer.post("/profile", data=_profile_form(other_reviewer)).status_code
        == 302,
        "Synthetic isolation reviewer could not complete its profile",
    )
    _require(
        other_reviewer.get(f"/assignments/{first_assignment}/bundle").status_code
        == 404,
        "One authenticated reviewer reached another reviewer's assignment",
    )

    mobile = app.test_client()
    mobile_agent = (
        "Mozilla/5.0 (iPhone; CPU iPhone OS 18_0 like Mac OS X) "
        "AppleWebKit/605.1.15 Mobile/15E148"
    )
    mobile_login_page = mobile.get("/auth/login", headers={"User-Agent": mobile_agent})
    _require(
        mobile_login_page.status_code == 200
        and b'name="viewport"' in mobile_login_page.data,
        "Mobile login page lacks its responsive viewport contract",
    )
    mobile_code, mobile_token = _login(
        mobile,
        app,
        sender,
        remembered=False,
        user_agent=mobile_agent,
        verify_mobile_contract=True,
    )
    _require(
        mobile.get("/profile", headers={"User-Agent": mobile_agent}).status_code == 200,
        "Mobile-shaped login did not establish an authenticated session",
    )

    sessions = app.extensions["musparql_sessions"]
    with sessions.begin() as session:
        job = session.get(ProcessingJob, receipt["job_id"])
        _require(job is not None and job.status == "queued", "Accepted review has no queued job")
        job.status = "running"
        job.started_at = timestamp(utc_now())
    app.logger.removeHandler(capture)
    _close_app(app)

    restarted_sender = SyntheticEmailSender()
    restarted = create_app(_app_config(root, database_path, restarted_sender))
    recovered = restarted.extensions["musparql_processing"].recover_interrupted()
    _require(recovered == 1, "Application restart did not recover the interrupted job")
    processed_job_id = restarted.extensions["musparql_processing"].process_next()
    _require(processed_job_id == receipt["job_id"], "Recovered job was not processed")
    dashboard = restarted.extensions["musparql_processing"].dashboard()
    _require(len(dashboard) == 1, "Owner diagnostics did not contain the submitted review")
    _submission, processed_job, _assignment = dashboard[0]
    _require(
        processed_job.status == "succeeded"
        and processed_job.safe_summary
        == "Validated 1 reviews; isolated candidate audit is ready.",
        "Owner diagnostics did not expose the expected safe success summary",
    )

    restarted.logger.addHandler(capture)
    bundle_path = root / "bundles" / "synthetic-phase9-bundle.json"
    original_bundle = bundle_path.read_bytes()
    bundle_path.write_text("{}\n", encoding="utf-8")
    private = restarted.test_client()
    private.set_cookie(restarted.config["AUTH_COOKIE_NAME"], private_token)
    response = private.get(f"/assignments/{repeat_assignment}/bundle")
    _require(response.status_code == 409, "Bundle-integrity failure was not safely reported")
    bundle_path.write_bytes(original_bundle)
    restarted.logger.removeHandler(capture)

    forbidden_log_values = [
        REVIEWER_EMAIL,
        OTHER_REVIEWER_EMAIL,
        REVIEWER_NAME,
        login_code,
        shared_code,
        mobile_code,
        other_code,
        private_token,
        shared_token,
        other_token,
        mobile_token,
        receipt["receipt_id"],
    ]
    rendered_logs = "\n".join(capture.messages)
    _require(capture.messages, "Privacy inspection did not capture a diagnostic event")
    _require(
        not any(value in rendered_logs for value in forbidden_log_values),
        "Operational logs exposed a synthetic identity, credential, or receipt",
    )
    _require(
        rendered_logs.strip() == "Assignment bundle failed integrity validation",
        "Integrity diagnostics were not limited to the approved safe summary",
    )

    with restarted.extensions["musparql_sessions"]() as session:
        source_counts = {
            "reviewers": int(session.scalar(select(func.count()).select_from(Reviewer)) or 0),
            "submissions": int(
                session.scalar(select(func.count()).select_from(ReviewSubmission)) or 0
            ),
            "jobs": int(session.scalar(select(func.count()).select_from(ProcessingJob)) or 0),
            "domain_assessments": int(
                session.scalar(
                    select(func.count()).select_from(ReviewerKgDomainAssessment)
                )
                or 0
            ),
            "familiarity_assessments": int(
                session.scalar(
                    select(func.count()).select_from(
                        ReviewerResourceFamiliarityAssessment
                    )
                )
                or 0
            ),
        }
    restored_path = root / "restore" / "phase9-restored.sqlite3"
    restored_path.parent.mkdir(mode=0o700)
    _backup_database(database_path, restored_path)
    _close_app(restarted)
    with sqlite3.connect(restored_path) as connection:
        integrity = connection.execute("PRAGMA integrity_check").fetchone()[0]
    _require(integrity == "ok", "Restored SQLite database failed its integrity check")
    restored_sender = SyntheticEmailSender()
    restored = create_app(_app_config(root, restored_path, restored_sender))
    with restored.extensions["musparql_sessions"]() as session:
        restored_counts = {
            "reviewers": int(session.scalar(select(func.count()).select_from(Reviewer)) or 0),
            "submissions": int(
                session.scalar(select(func.count()).select_from(ReviewSubmission)) or 0
            ),
            "jobs": int(session.scalar(select(func.count()).select_from(ProcessingJob)) or 0),
            "domain_assessments": int(
                session.scalar(
                    select(func.count()).select_from(ReviewerKgDomainAssessment)
                )
                or 0
            ),
            "familiarity_assessments": int(
                session.scalar(
                    select(func.count()).select_from(
                        ReviewerResourceFamiliarityAssessment
                    )
                )
                or 0
            ),
        }
        restored_job = session.get(ProcessingJob, receipt["job_id"])
        restored_submission = session.get(ReviewSubmission, receipt["receipt_id"])
    _require(restored_counts == source_counts, "Isolated restore changed durable row counts")
    _require(
        restored_job is not None
        and restored_job.status == "succeeded"
        and restored_submission is not None
        and (root / "submissions" / restored_submission.export_path).is_file(),
        "Restored database did not reconnect to its durable review state",
    )
    _close_app(restored)

    return {
        "schema": "musparql.local-hardening.v1",
        "result": "passed",
        "synthetic_only": True,
        "measured_at": timestamp(utc_now()),
        "automated_elapsed_ms": round((time.perf_counter() - started) * 1000, 3),
        "flows": {
            "first_visit_onboarding": True,
            "repeat_assessment_with_prior_values": True,
            "hosted_workbench_and_submission": True,
            "assignment_isolation": True,
        },
        "browser_checks": {
            "private_remembered_cookie": True,
            "shared_browser_session_cookie": True,
            "shared_logout_isolated": True,
            "mobile_login": True,
            "responsive_viewport": True,
        },
        "processing_restart": {
            "recovered_jobs": recovered,
            "completed_jobs": 1,
            "safe_summary_verified": True,
        },
        "database_restore": {
            "scope": "sqlite_database_with_file_reference_verification",
            "integrity_check": integrity,
            "row_counts_match": True,
            "durable_submission_reconnected": True,
        },
        "privacy_log_inspection": {
            "records_inspected": len(capture.messages),
            "forbidden_values_absent": True,
            "approved_summary_only": True,
        },
        "usability_observation": {
            **observation,
            "onboarding_target_seconds": ONBOARDING_TARGET_SECONDS,
            "repeat_assessment_target_seconds": REPEAT_ASSESSMENT_TARGET_SECONDS,
            "targets_met": True,
        },
        "environment": {
            "python": platform.python_version(),
            "platform": platform.platform(),
            "sqlite": sqlite3.sqlite_version,
        },
    }


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        description="Run the synthetic Musparql v2 Phase 9 local hardening gate"
    )
    result.add_argument(
        "--usability-observation",
        type=Path,
        help="JSON observation recorded during a human synthetic pilot",
    )
    result.add_argument(
        "--interactive",
        action="store_true",
        help="Serve a loopback-only fictional pilot and print synthetic codes locally",
    )
    result.add_argument(
        "--port",
        type=int,
        default=8765,
        help="Loopback port for --interactive (default: 8765)",
    )
    result.add_argument(
        "--workspace",
        type=Path,
        help="Empty directory to retain after the run; omitted uses a temporary directory",
    )
    result.add_argument("--output", type=Path, help="Write the JSON report to this path")
    return result


def main(argv: list[str] | None = None) -> int:
    argument_parser = parser()
    args = argument_parser.parse_args(argv)
    if args.interactive:
        if args.usability_observation is not None or args.output is not None:
            argument_parser.error(
                "--interactive cannot be combined with --usability-observation or --output"
            )
        if args.workspace is None:
            with tempfile.TemporaryDirectory(prefix="musparql-phase9-interactive-") as temporary:
                return run_interactive_pilot(Path(temporary), port=args.port)
        return run_interactive_pilot(args.workspace, port=args.port)
    if args.usability_observation is None:
        argument_parser.error(
            "--usability-observation is required unless --interactive is used"
        )
    observation = json.loads(args.usability_observation.read_text(encoding="utf-8"))
    if not isinstance(observation, dict):
        raise ValueError("The usability observation must be a JSON object")
    if args.workspace is None:
        with tempfile.TemporaryDirectory(prefix="musparql-phase9-") as temporary:
            report = run_verification(Path(temporary), observation)
    else:
        report = run_verification(args.workspace, observation)
    rendered = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        temporary_output = args.output.with_name(f".{args.output.name}.{os.getpid()}.tmp")
        temporary_output.write_text(rendered, encoding="utf-8")
        os.replace(temporary_output, args.output)
    print(rendered, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
