from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
import hashlib
import json
from pathlib import Path

import pytest
from sqlalchemy import select

from musparql.database import create_database_engine, session_factory
from musparql.database.migrations import upgrade_database
from musparql.database.models import (
    OwnerProcessingDecision,
    ProcessingJob,
    ReviewAssignment,
    Reviewer,
    ReviewSubmission,
)
from musparql.web.assignments import AssignmentService
from musparql.web.auth import timestamp, utc_now
from musparql.web.submissions import ProcessingService, SubmissionService


ROOT = Path(__file__).resolve().parents[1]


def _reviewer(index: int) -> Reviewer:
    reviewer_id = f"reviewer-{index:04d}"
    now = timestamp(utc_now())
    return Reviewer(
        id=reviewer_id,
        name=f"Synthetic reviewer {index}",
        affiliation="Synthetic Institute",
        email_display=f"reviewer-{index}@example.invalid",
        email_normalized=f"reviewer-{index}@example.invalid",
        status="active",
        created_at=now,
        updated_at=now,
        privacy_notice_version="synthetic-v1",
        privacy_notice_acknowledged_at=now,
    )


def _bundle(path: Path, index: int) -> tuple[str, str]:
    record_id = f"synthetic-record-{index}"
    payload = {
        "schema": "musparql.review-bundle.v2",
        "mode": "initial",
        "dataset_id": f"synthetic-dataset-{index}",
        "built_at": "2026-08-20T10:00:00Z",
        "holdout_input_policy": "no_holdout",
        "record_count": 1,
        "records": [{"review_id": record_id, "kg_id": "synthetic-kg"}],
    }
    raw = (json.dumps(payload, sort_keys=True) + "\n").encode()
    path.write_bytes(raw)
    return record_id, "sha256:" + hashlib.sha256(raw).hexdigest()


def _payload(index: int, assignment_id: str, digest: str, record_id: str) -> dict:
    reviewer_id = f"reviewer-{index:04d}"
    review_id = f"{record_id}::{reviewer_id}"
    return {
        "schema": "musparql.review-export.v2",
        "kind": "non_holdout_review_export",
        "assignment_id": assignment_id,
        "bundle_digest": digest,
        "reviewer_id": reviewer_id,
        "dataset_id": f"synthetic-dataset-{index}",
        "run_id": "synthetic-run",
        "run_ids": ["synthetic-run"],
        "runs": [],
        "exported_at": "2026-08-20T10:05:00Z",
        "reviews": {
            record_id: {
                "review_id": review_id,
                "reviewer_id": reviewer_id,
                "reviewed_at": "2026-08-20T10:04:00Z",
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


@pytest.fixture
def phase7(tmp_path: Path):
    database_path = tmp_path / "phase7.sqlite3"
    bundle_root = tmp_path / "bundles"
    bundle_root.mkdir()
    submission_root = tmp_path / "submissions"
    candidate_root = tmp_path / "candidates"
    upgrade_database(database_path)
    engine = create_database_engine(database_path)
    sessions = session_factory(engine)
    contexts = []
    with sessions.begin() as session:
        session.add_all(_reviewer(index) for index in range(100, 110))
        session.flush()
        for index in range(100, 110):
            reviewer_id = f"reviewer-{index:04d}"
            assignment_id = f"assignment-{index:024x}"
            bundle_name = f"bundle-{index}.json"
            record_id, digest = _bundle(bundle_root / bundle_name, index)
            session.add(
                ReviewAssignment(
                    id=assignment_id,
                    reviewer_id=reviewer_id,
                    mode="initial",
                    status="active",
                    bundle_path=bundle_name,
                    bundle_digest=digest,
                    previous_benchmark_path=None,
                    processing_recipe="validate_initial_review",
                    holdout_capability=False,
                    created_at="2026-08-20T10:00:00Z",
                    opened_at="2026-08-20T10:01:00Z",
                    submitted_at=None,
                )
            )
            contexts.append((index, assignment_id, digest, record_id))
    assignments = AssignmentService(sessions, bundle_root)
    submissions = SubmissionService(
        sessions,
        assignments,
        submission_root,
        ROOT / "schemas/review_export.schema.json",
        ROOT / "schemas/linguistic_annotation_export.schema.json",
    )
    processing = ProcessingService(sessions, submission_root, candidate_root)
    yield sessions, submissions, processing, contexts, submission_root, candidate_root
    engine.dispose()


def test_ten_concurrent_submissions_are_durable_unique_and_queued(phase7) -> None:
    sessions, submissions, _processing, contexts, submission_root, _candidate_root = phase7

    def submit(context):
        index, assignment_id, digest, record_id = context
        return submissions.submit(
            assignment_id, f"reviewer-{index:04d}",
            _payload(index, assignment_id, digest, record_id),
        )

    with ThreadPoolExecutor(max_workers=10) as executor:
        receipts = list(executor.map(submit, contexts))

    assert len({receipt.receipt_id for receipt in receipts}) == 10
    assert all(receipt.revision == 1 and not receipt.duplicate for receipt in receipts)
    with sessions() as session:
        assert len(list(session.scalars(select(ReviewSubmission)))) == 10
        assert len(list(session.scalars(select(ProcessingJob)))) == 10
        assert set(session.scalars(select(ProcessingJob.status))) == {"queued"}
    for receipt in receipts:
        stored = submission_root / receipt.assignment_id / f"{receipt.receipt_id}.json"
        assert stored.is_file()
        assert "sha256:" + hashlib.sha256(stored.read_bytes()).hexdigest() == receipt.digest


def test_retry_is_idempotent_changed_payload_is_revision_and_schema_is_strict(phase7) -> None:
    _sessions, submissions, _processing, contexts, _submission_root, _candidate_root = phase7
    index, assignment_id, digest, record_id = contexts[0]
    payload = _payload(index, assignment_id, digest, record_id)
    first = submissions.submit(assignment_id, f"reviewer-{index:04d}", payload)
    retry = submissions.submit(assignment_id, f"reviewer-{index:04d}", deepcopy(payload))
    assert retry.receipt_id == first.receipt_id
    assert retry.revision == 1 and retry.duplicate

    revised = deepcopy(payload)
    revised["exported_at"] = "2026-08-20T10:06:00Z"
    revised["reviews"][record_id]["public_comment"] = "Synthetic revision"
    second = submissions.submit(assignment_id, f"reviewer-{index:04d}", revised)
    assert second.receipt_id != first.receipt_id
    assert second.revision == 2 and not second.duplicate

    unknown = deepcopy(payload)
    unknown["debug"] = True
    with pytest.raises(ValueError, match="Additional properties"):
        submissions.submit(assignment_id, f"reviewer-{index:04d}", unknown)
    bad_enum = deepcopy(payload)
    bad_enum["reviews"][record_id]["pipeline_assessment"] = "looks_good"
    with pytest.raises(ValueError, match="not one of"):
        submissions.submit(assignment_id, f"reviewer-{index:04d}", bad_enum)
    wrong_reviewer = deepcopy(payload)
    wrong_reviewer["reviewer_id"] = "reviewer-9999"
    with pytest.raises(ValueError, match="does not match"):
        submissions.submit(assignment_id, f"reviewer-{index:04d}", wrong_reviewer)
    wrong_event = deepcopy(payload)
    wrong_event["reviews"][record_id]["review_id"] = "synthetic-event"
    with pytest.raises(ValueError, match="must end with"):
        submissions.submit(assignment_id, f"reviewer-{index:04d}", wrong_event)


def test_worker_isolates_candidates_recovers_jobs_and_owner_gates_promotion(phase7) -> None:
    sessions, submissions, processing, contexts, _submission_root, candidate_root = phase7
    first_context, second_context = contexts[:2]
    receipts = []
    for index, assignment_id, digest, record_id in (first_context, second_context):
        receipts.append(submissions.submit(
            assignment_id, f"reviewer-{index:04d}",
            _payload(index, assignment_id, digest, record_id),
        ))
    with sessions.begin() as session:
        interrupted = session.scalar(select(ProcessingJob).where(ProcessingJob.id == receipts[0].job_id))
        assert interrupted is not None
        interrupted.status = "running"
        interrupted.started_at = "2026-08-20T10:10:00Z"
    assert processing.recover_interrupted() == 1
    assert processing.process_next() == receipts[0].job_id
    assert processing.process_next() == receipts[1].job_id
    assert processing.process_next() is None
    for receipt in receipts:
        audit_path = candidate_root / receipt.job_id / "audit.json"
        audit = json.loads(audit_path.read_text())
        assert audit["receipt_id"] == receipt.receipt_id
        assert audit["source_digest"] == receipt.digest

    owner_id = first_context[0]
    owner_reviewer_id = f"reviewer-{owner_id:04d}"
    with pytest.raises(ValueError, match="combined"):
        processing.decide_candidate(receipts[0].job_id, owner_reviewer_id, "approved", "")
    processing.decide_inclusion(receipts[0].receipt_id, owner_reviewer_id, "included", "")
    processing.decide_item(
        receipts[0].receipt_id, "synthetic-record-100", owner_reviewer_id,
        "omitted", "Synthetic unusable item",
    )
    with pytest.raises(ValueError, match="included revisions"):
        processing.create_combined_candidate([item.receipt_id for item in receipts])
    processing.decide_inclusion(receipts[1].receipt_id, owner_reviewer_id, "included", "")
    combined_id = processing.create_combined_candidate([item.receipt_id for item in receipts])
    assert processing.process_next() == combined_id
    processing.decide_candidate(combined_id, owner_reviewer_id, "approved", "")
    with sessions() as session:
        job = session.get(ProcessingJob, combined_id)
        assignment = session.get(ReviewAssignment, receipts[0].assignment_id)
        assert job is not None and job.approval_status == "approved"
        assert assignment is not None and assignment.status == "approved"
        decisions = list(session.scalars(select(OwnerProcessingDecision)))
        assert [item.target_type for item in decisions] == ["submission", "item", "submission", "candidate"]
        manifest = json.loads((candidate_root / job.candidate_output_path).read_text())
        assert [item["receipt_id"] for item in manifest["selected_submissions"]] == [item.receipt_id for item in receipts]


def test_processing_failure_preserves_receipt_and_prior_benchmark(phase7, tmp_path: Path) -> None:
    sessions, submissions, processing, contexts, submission_root, _candidate_root = phase7
    index, assignment_id, digest, record_id = contexts[0]
    receipt = submissions.submit(
        assignment_id, f"reviewer-{index:04d}",
        _payload(index, assignment_id, digest, record_id),
    )
    baseline = tmp_path / "baseline.json"
    baseline.write_text('{"immutable":"synthetic"}\n')
    stored = submission_root / assignment_id / f"{receipt.receipt_id}.json"
    stored.write_text("{}\n")
    assert processing.process_next() == receipt.job_id
    assert baseline.read_text() == '{"immutable":"synthetic"}\n'
    assert stored.is_file()
    with sessions() as session:
        job = session.get(ProcessingJob, receipt.job_id)
        assert job is not None and job.status == "failed"
        assert "immutable receipt remains available" in (job.safe_summary or "")
