"""Synthetic end-to-end workshop concurrency verification for Musparql v2."""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import timedelta
import hashlib
import hmac
import json
import math
import os
from pathlib import Path
import platform
import secrets
import sqlite3
from statistics import median
import tempfile
import threading
import time
from typing import Any
import uuid

from sqlalchemy import select

from musparql.database import create_database_engine, session_factory
from musparql.database.migrations import upgrade_database
from musparql.database.models import (
    AuthSession,
    ProcessingJob,
    ReviewAssignment,
    Reviewer,
    ReviewSubmission,
)
from . import create_app
from .auth import timestamp, utc_now
from .submissions import ProcessingService


MINIMUM_REVIEWERS = 10
MAXIMUM_REVIEWERS = 50
APP_SECRET = "synthetic-phase-8-secret-with-at-least-32-bytes"
OWNER_ID = "reviewer-9000"
SEED_REVIEWER_ID = "reviewer-9001"


class VerificationError(RuntimeError):
    """A Phase 8 workshop invariant did not hold."""


class _BusyProcessingService(ProcessingService):
    """Pause one claimed job to model a long-running worker operation."""

    def __init__(self, *args: Any, busy: threading.Event, release: threading.Event) -> None:
        super().__init__(*args)
        self.busy = busy
        self.release = release

    def _run(self, job_id: str) -> None:
        self.busy.set()
        if not self.release.wait(timeout=30):
            raise TimeoutError("Synthetic busy worker was not released")
        super()._run(job_id)


def _digest(raw: bytes) -> str:
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def _reviewer(reviewer_id: str, index: int) -> Reviewer:
    now = timestamp(utc_now())
    return Reviewer(
        id=reviewer_id,
        name=f"Synthetic Phase 8 reviewer {index}",
        affiliation="Synthetic Phase 8 Institute",
        email_display=f"phase8-{index}@example.invalid",
        email_normalized=f"phase8-{index}@example.invalid",
        status="active",
        created_at=now,
        updated_at=now,
        privacy_notice_version="synthetic-phase-8-v1",
        privacy_notice_acknowledged_at=now,
    )


def _bundle(path: Path, index: int) -> tuple[str, str]:
    record_id = f"synthetic-phase8-record-{index}"
    payload = {
        "schema": "musparql.review-bundle.v2",
        "mode": "initial",
        "dataset_id": f"synthetic-phase8-dataset-{index}",
        "built_at": "2026-08-21T10:00:00Z",
        "holdout_input_policy": "no_holdout",
        "record_count": 1,
        "records": [{"review_id": record_id, "kg_id": "synthetic-phase8-kg"}],
    }
    raw = (json.dumps(payload, sort_keys=True) + "\n").encode("utf-8")
    path.write_bytes(raw)
    return record_id, _digest(raw)


def _payload(
    index: int, reviewer_id: str, assignment_id: str, bundle_digest: str, record_id: str
) -> dict[str, Any]:
    review_id = f"{record_id}::{reviewer_id}"
    return {
        "schema": "musparql.review-export.v2",
        "kind": "non_holdout_review_export",
        "assignment_id": assignment_id,
        "bundle_digest": bundle_digest,
        "reviewer_id": reviewer_id,
        "dataset_id": f"synthetic-phase8-dataset-{index}",
        "run_id": "synthetic-phase8-run",
        "run_ids": ["synthetic-phase8-run"],
        "runs": [],
        "exported_at": "2026-08-21T10:05:00Z",
        "reviews": {
            record_id: {
                "review_id": review_id,
                "reviewer_id": reviewer_id,
                "reviewed_at": "2026-08-21T10:04:00Z",
                "prior_review_ids": [],
                "authored_formulation_ids": [],
                "approved_formulation_ids": [f"{review_id}::formulation::candidate"],
                "benchmark_disposition": "included",
                "pipeline_assessment": "accepted",
                "preferred_question": "",
                "literal_wording": "",
                "public_comment": "",
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


def _token_hash(raw_token: str) -> str:
    return hmac.new(
        APP_SECRET.encode("utf-8"),
        f"session\0{raw_token}".encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def _app_config(root: Path) -> dict[str, Any]:
    project_root = Path(__file__).resolve().parents[3]
    return {
        "TESTING": True,
        "DATABASE_PATH": root / "phase8.sqlite3",
        "APP_SECRET": APP_SECRET,
        "OWNER_REVIEWER_ID": OWNER_ID,
        "COOKIE_SECURE": False,
        "ALLOW_SYNTHETIC_EMAIL": True,
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
    }


def _close_app(app: Any) -> None:
    app.extensions["musparql_email_dispatcher"].shutdown()
    app.extensions["musparql_engine"].dispose()


def _post(
    app: Any, token: str, assignment_id: str, payload: dict[str, Any]
) -> tuple[float, int, dict[str, Any]]:
    csrf = secrets.token_urlsafe(24)
    client = app.test_client()
    client.set_cookie(app.config["AUTH_COOKIE_NAME"], token)
    client.set_cookie(app.config["CSRF_COOKIE_NAME"], csrf)
    started = time.perf_counter()
    response = client.post(
        f"/assignments/{assignment_id}/submissions",
        json=payload,
        headers={"X-CSRF-Token": csrf},
    )
    elapsed_ms = (time.perf_counter() - started) * 1000
    return elapsed_ms, response.status_code, response.get_json()


def _percentile(values: list[float], percentile: float) -> float:
    ordered = sorted(values)
    return ordered[max(0, math.ceil(percentile * len(ordered)) - 1)]


def _median(values: list[float]) -> float:
    return float(median(values))


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise VerificationError(message)


def run_verification(root: Path, reviewer_count: int = MINIMUM_REVIEWERS) -> dict[str, Any]:
    """Run the synthetic Phase 8 scenario in an empty, caller-owned directory."""
    if not MINIMUM_REVIEWERS <= reviewer_count <= MAXIMUM_REVIEWERS:
        raise ValueError(
            f"reviewer_count must be between {MINIMUM_REVIEWERS} and {MAXIMUM_REVIEWERS}"
        )
    root = root.resolve()
    if root.exists() and (not root.is_dir() or any(root.iterdir())):
        raise ValueError("The verification workspace must be an empty directory")
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    bundle_root = root / "bundles"
    bundle_root.mkdir(mode=0o700)
    database_path = root / "phase8.sqlite3"
    upgrade_database(database_path)
    engine = create_database_engine(database_path)
    sessions = session_factory(engine)
    contexts: list[dict[str, Any]] = []
    raw_tokens: dict[str, str] = {}
    now = utc_now()
    reviewer_ids = [f"reviewer-{1000 + index:04d}" for index in range(reviewer_count)]
    all_reviewers = [(OWNER_ID, 9000), (SEED_REVIEWER_ID, 9001)] + [
        (reviewer_id, index) for index, reviewer_id in enumerate(reviewer_ids)
    ]
    with sessions.begin() as session:
        session.add_all(_reviewer(reviewer_id, index) for reviewer_id, index in all_reviewers)
        session.flush()
        for index, reviewer_id in enumerate([SEED_REVIEWER_ID, *reviewer_ids], start=1):
            assignment_id = f"assignment-{index:024x}"
            bundle_name = f"bundle-{index}.json"
            record_id, bundle_digest = _bundle(bundle_root / bundle_name, index)
            session.add(
                ReviewAssignment(
                    id=assignment_id,
                    reviewer_id=reviewer_id,
                    mode="initial",
                    status="active",
                    bundle_path=bundle_name,
                    bundle_digest=bundle_digest,
                    previous_benchmark_path=None,
                    processing_recipe="validate_initial_review",
                    holdout_capability=False,
                    created_at=timestamp(now + timedelta(microseconds=index)),
                    opened_at=timestamp(now + timedelta(microseconds=index)),
                    submitted_at=None,
                )
            )
            context = {
                "index": index,
                "reviewer_id": reviewer_id,
                "assignment_id": assignment_id,
                "bundle_digest": bundle_digest,
                "record_id": record_id,
            }
            if reviewer_id == SEED_REVIEWER_ID:
                seed_context = context
            else:
                contexts.append(context)
                raw_token = secrets.token_urlsafe(32)
                raw_tokens[reviewer_id] = raw_token
                session.add(
                    AuthSession(
                        id=str(uuid.uuid4()),
                        reviewer_id=reviewer_id,
                        token_hash=_token_hash(raw_token),
                        created_at=timestamp(now),
                        last_used_at=timestamp(now),
                        expires_at=timestamp(now + timedelta(hours=4)),
                        revoked_at=None,
                        remembered=False,
                    )
                )
    engine.dispose()

    app = create_app(_app_config(root))
    seed_payload = _payload(**seed_context)
    seed_receipt = app.extensions["musparql_submissions"].submit(
        seed_context["assignment_id"], SEED_REVIEWER_ID, seed_payload
    )
    busy = threading.Event()
    release = threading.Event()
    busy_worker = _BusyProcessingService(
        app.extensions["musparql_sessions"],
        root / "submissions",
        root / "candidates",
        busy=busy,
        release=release,
    )
    worker_thread = threading.Thread(target=busy_worker.process_next, daemon=True)
    worker_thread.start()
    _require(busy.wait(timeout=10), "The synthetic worker did not enter its busy state")

    def submit(context: dict[str, Any]) -> tuple[dict[str, Any], float, int, dict[str, Any]]:
        payload = _payload(**context)
        elapsed, status, body = _post(
            app, raw_tokens[context["reviewer_id"]], context["assignment_id"], payload
        )
        return context, elapsed, status, body

    with ThreadPoolExecutor(max_workers=reviewer_count) as executor:
        burst = list(executor.map(submit, contexts))
    _require(
        all(status == 202 for _context, _elapsed, status, _body in burst),
        "A concurrent submission was not accepted",
    )
    latencies = [elapsed for _context, elapsed, _status, _body in burst]
    receipts = [body for _context, _elapsed, _status, body in burst]
    _require(
        len({item["receipt_id"] for item in receipts}) == reviewer_count,
        "Concurrent receipts were not unique",
    )
    for context, _elapsed, _status, body in burst:
        expected_payload = _payload(**context)
        expected_digest = _digest(
            (
                json.dumps(
                    expected_payload,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                )
                + "\n"
            ).encode("utf-8")
        )
        _require(
            body.get("assignment_id") == context["assignment_id"]
            and body.get("revision") == 1
            and body.get("digest") == expected_digest
            and body.get("duplicate") is False,
            "A concurrent receipt was not bound to its submitting assignment",
        )

    first = contexts[0]
    first_payload = _payload(**first)
    _retry_elapsed, retry_status, retry = _post(
        app, raw_tokens[first["reviewer_id"]], first["assignment_id"], deepcopy(first_payload)
    )
    revised_payload = deepcopy(first_payload)
    revised_payload["exported_at"] = "2026-08-21T10:06:00Z"
    revised_payload["reviews"][first["record_id"]]["public_comment"] = "Synthetic Phase 8 revision"
    _revision_elapsed, revision_status, revision = _post(
        app, raw_tokens[first["reviewer_id"]], first["assignment_id"], revised_payload
    )
    _require(retry_status == 200 and retry["duplicate"], "Identical retry was not idempotent")
    _require(
        retry["receipt_id"] == receipts[0]["receipt_id"],
        "Identical retry changed its receipt",
    )
    _require(
        revision_status == 202 and revision["revision"] == 2,
        "Changed retry was not an explicit revision",
    )

    expected_accepted = reviewer_count + 2  # busy seed, concurrent burst, and one revision
    with app.extensions["musparql_sessions"]() as session:
        accepted_before_failure = list(session.scalars(select(ReviewSubmission)))
        jobs_before_failure = list(session.scalars(select(ProcessingJob)))
    _require(
        len(accepted_before_failure) == expected_accepted,
        "Database submission count does not match accepted revisions",
    )
    _require(
        sorted(job.submission_id for job in jobs_before_failure)
        == sorted(submission.id for submission in accepted_before_failure),
        "Each accepted revision must have exactly one job",
    )
    submissions_by_id = {submission.id: submission for submission in accepted_before_failure}
    for context, _elapsed, _status, body in burst:
        submission = submissions_by_id.get(body["receipt_id"])
        _require(
            submission is not None
            and submission.assignment_id == context["assignment_id"]
            and submission.reviewer_id == context["reviewer_id"]
            and submission.export_digest == body["digest"],
            "A concurrent receipt was persisted for the wrong reviewer or assignment",
        )
    durable_paths = {
        path.resolve() for path in (root / "submissions").glob("*/*.json")
    }
    _require(
        len(durable_paths) == expected_accepted,
        "Durable file count does not match accepted revisions",
    )
    _require(
        not list((root / "submissions").glob("**/*.tmp")),
        "An atomic-write temporary file remained",
    )
    for submission in accepted_before_failure:
        path = (root / "submissions" / submission.export_path).resolve()
        _require(path in durable_paths, "A registered submission file is missing")
        _require(
            _digest(path.read_bytes()) == submission.export_digest,
            "A durable submission was truncated or changed",
        )

    release.set()
    worker_thread.join(timeout=10)
    _require(not worker_thread.is_alive(), "The synthetic busy worker did not finish")
    _close_app(app)

    restart_engine = create_database_engine(database_path)
    restart_sessions = session_factory(restart_engine)
    with restart_sessions.begin() as session:
        interrupted = session.scalar(
            select(ProcessingJob)
            .where(ProcessingJob.status == "queued")
            .order_by(ProcessingJob.created_at, ProcessingJob.id)
            .limit(1)
        )
        _require(interrupted is not None, "No queued job was available for restart verification")
        interrupted.status = "running"
        interrupted.started_at = timestamp(utc_now())
    restart_engine.dispose()

    restarted_app = create_app(_app_config(root))
    processing = restarted_app.extensions["musparql_processing"]
    _require(
        processing.recover_interrupted() == 1,
        "Restart did not recover exactly one running job",
    )
    sessions = restarted_app.extensions["musparql_sessions"]
    with sessions() as session:
        queued_jobs = list(
            session.scalars(
                select(ProcessingJob)
                .where(ProcessingJob.status == "queued")
                .order_by(ProcessingJob.created_at, ProcessingJob.id)
            )
        )
        expected_order = [job.id for job in queued_jobs]
        failed_submission = session.get(ReviewSubmission, queued_jobs[0].submission_id)
        _require(failed_submission is not None, "The selected failure job has no submission")
        failed_receipt_id = failed_submission.id
        failed_path = root / "submissions" / failed_submission.export_path
    failed_path.write_text("{}\n", encoding="utf-8")
    observed_order: list[str] = []
    while True:
        job_id = processing.process_next()
        if job_id is None:
            break
        observed_order.append(job_id)
    _require(
        observed_order == expected_order,
        "The processing queue did not preserve its declared ordering",
    )

    with sessions() as session:
        submissions = list(session.scalars(select(ReviewSubmission)))
        jobs = list(session.scalars(select(ProcessingJob)))
        assignments = {item.id: item for item in session.scalars(select(ReviewAssignment))}
    _require(
        len(submissions) == expected_accepted,
        "Database submission count does not match accepted revisions",
    )
    _require(
        sorted(job.submission_id for job in jobs)
        == sorted(submission.id for submission in submissions),
        "Each accepted revision must have exactly one job",
    )
    _require(
        sum(job.status == "failed" for job in jobs) == 1,
        "Failure isolation scenario did not produce exactly one failed job",
    )
    _require(
        all(job.status in {"succeeded", "failed"} for job in jobs),
        "A later job remained blocked",
    )
    _require(
        any(
            job.id == seed_receipt.job_id and job.status == "succeeded"
            for job in jobs
        ),
        "Busy seed job was not completed",
    )

    successful_outputs = [
        (root / "candidates" / job.candidate_output_path).resolve()
        for job in jobs
        if job.status == "succeeded" and job.candidate_output_path is not None
    ]
    _require(
        len(successful_outputs) == expected_accepted - 1,
        "A successful job has no isolated candidate output",
    )
    _require(
        len(set(successful_outputs)) == len(successful_outputs)
        and all(path.is_file() for path in successful_outputs),
        "Candidate outputs were missing or shared between jobs",
    )

    submission_paths = {path.resolve() for path in (root / "submissions").glob("*/*.json")}
    _require(
        len(submission_paths) == expected_accepted,
        "Durable file count does not match accepted revisions",
    )
    _require(
        not list((root / "submissions").glob("**/*.tmp")),
        "An atomic-write temporary file remained",
    )
    for submission in submissions:
        path = (root / "submissions" / submission.export_path).resolve()
        _require(path in submission_paths, "A registered submission file is missing")
        _require(
            submission.reviewer_id
            == assignments[submission.assignment_id].reviewer_id,
            "A submission was misattributed",
        )
        if submission.id != failed_receipt_id:
            _require(
                _digest(path.read_bytes()) == submission.export_digest,
                "A durable submission was truncated or changed",
            )

    failed_job = next(job for job in jobs if job.submission_id == failed_receipt_id)
    _require(
        failed_job.status == "failed",
        "The corrupt synthetic receipt did not fail in isolation",
    )
    _require(failed_path.is_file(), "Processing failure removed the immutable receipt")
    processed_after_failure = len(observed_order[observed_order.index(failed_job.id) + 1 :])
    _require(processed_after_failure > 0, "The failed job did not have a later job to unblock")
    _close_app(restarted_app)

    return {
        "schema": "musparql.workshop-verification.v1",
        "result": "passed",
        "synthetic_only": True,
        "measured_at": timestamp(utc_now()),
        "target": {"concurrent_reviewers": reviewer_count},
        "submission_latency_ms": {
            "samples": reviewer_count,
            "minimum": round(min(latencies), 3),
            "median": round(_median(latencies), 3),
            "p95": round(_percentile(latencies, 0.95), 3),
            "maximum": round(max(latencies), 3),
            "worker_busy": True,
        },
        "accepted_revisions": expected_accepted,
        "idempotent_retries": 1,
        "explicit_revisions": 1,
        "recovered_running_jobs": 1,
        "isolated_failed_jobs": 1,
        "processed_after_failure": processed_after_failure,
        "queue_order_verified": True,
        "durable_files_verified": expected_accepted,
        "environment": {
            "python": platform.python_version(),
            "platform": platform.platform(),
            "sqlite": sqlite3.sqlite_version,
        },
    }


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        description="Run the synthetic Musparql v2 workshop concurrency verification"
    )
    result.add_argument("--reviewers", type=int, default=MINIMUM_REVIEWERS)
    result.add_argument(
        "--workspace",
        type=Path,
        help="Empty directory to retain after the run; omitted uses a temporary directory",
    )
    result.add_argument("--output", type=Path, help="Write the JSON report to this path")
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    if args.workspace is None:
        with tempfile.TemporaryDirectory(prefix="musparql-phase8-") as temporary:
            report = run_verification(Path(temporary), args.reviewers)
    else:
        report = run_verification(args.workspace, args.reviewers)
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
