"""Authenticated Phase 7 submission, receipt, queue, and owner-control services."""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import secrets
from typing import Any, Mapping

from jsonschema import Draft202012Validator, FormatChecker
from sqlalchemy import func, select, text
from sqlalchemy.orm import Session, sessionmaker

from musparql.database.models import (
    OwnerProcessingDecision,
    ProcessingJob,
    ReviewAssignment,
    ReviewSubmission,
)
from musparql.reviewer_provenance import validate_review_provenance
from .assignments import AssignmentService
from .auth import timestamp, utc_now


@dataclass(frozen=True)
class Receipt:
    receipt_id: str
    assignment_id: str
    revision: int
    digest: str
    submitted_at: str
    duplicate: bool
    job_id: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "receipt_id": self.receipt_id,
            "assignment_id": self.assignment_id,
            "revision": self.revision,
            "digest": self.digest,
            "submitted_at": self.submitted_at,
            "duplicate": self.duplicate,
            "job_id": self.job_id,
            "status": "accepted",
        }


def _canonical_bytes(payload: Mapping[str, Any]) -> bytes:
    return (json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")


def _digest(raw: bytes) -> str:
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def _atomic_write(root: Path, name: str, raw: bytes) -> Path:
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    path = root / name
    temporary = root / f".{name}.{secrets.token_hex(8)}.tmp"
    try:
        with temporary.open("xb") as handle:
            os.chmod(temporary, 0o600)
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        directory_fd = os.open(root, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        temporary.unlink(missing_ok=True)
    return path


class SubmissionService:
    def __init__(
        self,
        sessions: sessionmaker[Session],
        assignments: AssignmentService,
        submission_root: Path,
        review_schema_path: Path,
        linguistic_schema_path: Path,
    ) -> None:
        self.sessions = sessions
        self.assignments = assignments
        self.submission_root = submission_root.resolve()
        self.validators = {
            "initial": self._validator(review_schema_path),
            "compare": self._validator(review_schema_path),
            "linguistic": self._validator(linguistic_schema_path),
        }

    @staticmethod
    def _validator(path: Path) -> Draft202012Validator:
        schema = json.loads(path.read_text(encoding="utf-8"))
        Draft202012Validator.check_schema(schema)
        return Draft202012Validator(schema, format_checker=FormatChecker())

    def submit(
        self, assignment_id: str, reviewer_id: str, payload: Mapping[str, Any]
    ) -> Receipt:
        assignment, bundle = self.assignments.submission_bundle(assignment_id, reviewer_id)
        self._validate(assignment, bundle, payload)
        raw = _canonical_bytes(payload)
        digest = _digest(raw)
        session = self.sessions()
        stored_path: Path | None = None
        try:
            session.execute(text("BEGIN IMMEDIATE"))
            existing = session.scalar(
                select(ReviewSubmission).where(
                    ReviewSubmission.assignment_id == assignment_id,
                    ReviewSubmission.export_digest == digest,
                )
            )
            if existing is not None:
                job = session.scalar(
                    select(ProcessingJob).where(ProcessingJob.submission_id == existing.id)
                )
                session.commit()
                if job is None:
                    raise RuntimeError("Accepted submission has no processing job")
                return self._receipt(existing, job, duplicate=True)
            if assignment.status == "approved":
                raise PermissionError("Approved assignments accept retries but not new revisions")
            revision = int(
                session.scalar(
                    select(func.coalesce(func.max(ReviewSubmission.revision), 0)).where(
                        ReviewSubmission.assignment_id == assignment_id
                    )
                )
                or 0
            ) + 1
            receipt_id = "receipt-" + secrets.token_hex(16)
            job_id = "job-" + secrets.token_hex(16)
            now = timestamp(utc_now())
            relative = f"{assignment_id}/{receipt_id}.json"
            stored_path = _atomic_write(self.submission_root / assignment_id, f"{receipt_id}.json", raw)
            submission = ReviewSubmission(
                id=receipt_id,
                assignment_id=assignment_id,
                reviewer_id=reviewer_id,
                export_path=relative,
                export_digest=digest,
                submitted_at=now,
                revision=revision,
                validation_status="schema_valid",
                inclusion_status="pending",
            )
            persisted = session.get(ReviewAssignment, assignment_id)
            if persisted is None:
                raise RuntimeError("Assignment disappeared during submission")
            session.add(submission)
            session.flush()
            job = ProcessingJob(
                id=job_id,
                assignment_id=assignment_id,
                submission_id=receipt_id,
                recipe=assignment.processing_recipe,
                status="queued",
                created_at=now,
                job_kind="submission",
                selected_submission_ids=None,
                approval_status="pending",
            )
            session.add(job)
            persisted.status = "submitted"
            persisted.submitted_at = now
            session.commit()
            return self._receipt(submission, job, duplicate=False)
        except Exception:
            session.rollback()
            if stored_path is not None:
                stored_path.unlink(missing_ok=True)
            raise
        finally:
            session.close()

    @staticmethod
    def _receipt(submission: ReviewSubmission, job: ProcessingJob, *, duplicate: bool) -> Receipt:
        return Receipt(
            submission.id, submission.assignment_id, submission.revision,
            submission.export_digest, submission.submitted_at, duplicate, job.id,
        )

    def _validate(
        self,
        assignment: ReviewAssignment,
        bundle: Mapping[str, Any],
        payload: Mapping[str, Any],
    ) -> None:
        errors = sorted(self.validators[assignment.mode].iter_errors(payload), key=lambda item: list(item.path))
        if errors:
            location = ".".join(str(part) for part in errors[0].absolute_path) or "export"
            raise ValueError(f"Invalid {location}: {errors[0].message}")
        expected_schema = (
            "musparql.linguistic-annotation-export.v1"
            if assignment.mode == "linguistic" else "musparql.review-export.v2"
        )
        if payload.get("schema") != expected_schema:
            raise ValueError("Export schema does not match assignment mode")
        for field, expected in (
            ("assignment_id", assignment.id),
            ("reviewer_id", assignment.reviewer_id),
            ("dataset_id", bundle.get("dataset_id")),
        ):
            if payload.get(field) != expected:
                raise ValueError(f"Export {field} does not match the assignment")
        if assignment.mode != "linguistic" and payload.get("bundle_digest") != assignment.bundle_digest:
            raise ValueError("Export bundle_digest does not match the assignment")
        if assignment.mode == "compare" and payload.get("mode") != "compare":
            raise ValueError("Comparative assignment requires compare mode")
        if assignment.mode == "initial" and "mode" in payload:
            raise ValueError("Initial assignment must not declare compare mode")
        records = bundle.get("records", [])
        if assignment.mode == "linguistic":
            allowed = {str(item["trial_id"]) for item in records}
            annotations = payload.get("annotations", [])
            identities = [str(item.get("trial_id")) for item in annotations]
            if len(identities) != len(set(identities)) or not set(identities).issubset(allowed):
                raise ValueError("Annotations must uniquely identify assigned trials")
            for item in annotations:
                if item.get("assignment_id") != assignment.id or item.get("reviewer_id") != assignment.reviewer_id or item.get("dataset_id") != bundle.get("dataset_id"):
                    raise ValueError("Annotation attribution does not match the assignment")
        else:
            allowed = {
                str((item.get("current") or {}).get("review_id") or item.get("pair_id"))
                if assignment.mode == "compare" else str(item.get("review_id"))
                for item in records
            }
            reviews = payload.get("reviews", {})
            if not set(reviews).issubset(allowed):
                raise ValueError("Reviews contain an identity outside the assigned bundle")
            if len({item["review_id"] for item in reviews.values()}) != len(reviews):
                raise ValueError("Review event identities must be unique")
            if any(item["reviewer_id"] != assignment.reviewer_id for item in reviews.values()):
                raise ValueError("Review attribution does not match the assignment")
            for item in reviews.values():
                validate_review_provenance(item)


class ProcessingService:
    def __init__(self, sessions: sessionmaker[Session], submission_root: Path, candidate_root: Path) -> None:
        self.sessions = sessions
        self.submission_root = submission_root.resolve()
        self.candidate_root = candidate_root.resolve()

    def recover_interrupted(self) -> int:
        with self.sessions.begin() as session:
            jobs = list(session.scalars(select(ProcessingJob).where(ProcessingJob.status == "running")))
            for job in jobs:
                job.status = "queued"
                job.started_at = None
                job.safe_summary = "Recovered after worker restart."
            return len(jobs)

    def process_next(self) -> str | None:
        session = self.sessions()
        try:
            session.execute(text("BEGIN IMMEDIATE"))
            job = session.scalar(
                select(ProcessingJob).where(ProcessingJob.status == "queued").order_by(ProcessingJob.created_at, ProcessingJob.id).limit(1)
            )
            if job is None:
                session.commit()
                return None
            job.status = "running"
            job.started_at = timestamp(utc_now())
            job_id = job.id
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
        try:
            self._run(job_id)
        except Exception as exc:
            with self.sessions.begin() as session:
                job = session.get(ProcessingJob, job_id)
                if job is not None:
                    job.status = "failed"
                    job.finished_at = timestamp(utc_now())
                    job.safe_summary = f"Processing failed: {type(exc).__name__}. The immutable receipt remains available."
                    assignment = session.get(ReviewAssignment, job.assignment_id)
                    if assignment is not None:
                        assignment.status = "failed"
            return job_id
        return job_id

    def _run(self, job_id: str) -> None:
        with self.sessions() as session:
            job = session.get(ProcessingJob, job_id)
            if job is None:
                raise RuntimeError("Unknown processing job")
            if job.job_kind == "combined_candidate":
                selected = tuple(job.selected_submission_ids or ())
                recipe = job.recipe
                assignment_id = job.assignment_id
            else:
                selected = ()
                recipe = ""
                assignment_id = ""
        if selected:
            self._run_combined(job_id, selected, recipe, assignment_id)
            return
        with self.sessions() as session:
            job = session.get(ProcessingJob, job_id)
            if job is None:
                raise RuntimeError("Unknown processing job")
            submission = session.get(ReviewSubmission, job.submission_id)
            if submission is None:
                raise RuntimeError("Processing job has no submission")
            source = self.submission_root / submission.export_path
            raw = source.read_bytes()
            if _digest(raw) != submission.export_digest:
                raise ValueError("Stored submission digest mismatch")
            payload = json.loads(raw)
            item_field = "annotations" if "annotations" in payload else "reviews"
            item_count = len(payload[item_field])
            audit = {
                "schema": "musparql.candidate-audit.v1",
                "job_id": job.id,
                "receipt_id": submission.id,
                "assignment_id": submission.assignment_id,
                "revision": submission.revision,
                "recipe": job.recipe,
                "source_digest": submission.export_digest,
                "item_count": item_count,
                "validated": True,
                "created_at": timestamp(utc_now()),
            }
        relative = f"{job_id}/audit.json"
        _atomic_write(self.candidate_root / job_id, "audit.json", _canonical_bytes(audit))
        with self.sessions.begin() as session:
            job = session.get(ProcessingJob, job_id)
            if job is None:
                raise RuntimeError("Processing job disappeared")
            job.status = "succeeded"
            job.finished_at = timestamp(utc_now())
            job.safe_summary = f"Validated {item_count} {item_field}; isolated candidate audit is ready."
            job.candidate_output_path = relative
            assignment = session.get(ReviewAssignment, job.assignment_id)
            if assignment is not None:
                assignment.status = "ready_for_owner_review"

    def _run_combined(
        self, job_id: str, selected_ids: tuple[str, ...], recipe: str,
        assignment_id: str,
    ) -> None:
        with self.sessions() as session:
            submissions = [session.get(ReviewSubmission, receipt_id) for receipt_id in selected_ids]
            if any(item is None for item in submissions):
                raise ValueError("Combined candidate references an unknown receipt")
            selected = [item for item in submissions if item is not None]
            assignments = [session.get(ReviewAssignment, item.assignment_id) for item in selected]
            baselines = {item.previous_benchmark_path for item in assignments if item is not None}
            if len(baselines) != 1:
                raise ValueError("Combined candidate revisions do not share one immutable baseline")
            records = []
            for submission in selected:
                raw = (self.submission_root / submission.export_path).read_bytes()
                if _digest(raw) != submission.export_digest:
                    raise ValueError("Stored submission digest mismatch")
                records.append({
                    "receipt_id": submission.id,
                    "assignment_id": submission.assignment_id,
                    "revision": submission.revision,
                    "digest": submission.export_digest,
                })
            manifest = {
                "schema": "musparql.combined-candidate-audit.v1",
                "job_id": job_id,
                "recipe": recipe,
                "baseline": next(iter(baselines)),
                "selected_submissions": records,
                "validated": True,
                "created_at": timestamp(utc_now()),
            }
        relative = f"{job_id}/combined-candidate.json"
        _atomic_write(
            self.candidate_root / job_id, "combined-candidate.json",
            _canonical_bytes(manifest),
        )
        with self.sessions.begin() as session:
            job = session.get(ProcessingJob, job_id)
            if job is None:
                raise RuntimeError("Combined candidate job disappeared")
            job.status = "succeeded"
            job.finished_at = timestamp(utc_now())
            job.safe_summary = f"Combined {len(records)} exact submission revisions from one immutable baseline."
            job.candidate_output_path = relative

    def dashboard(self) -> list[tuple[ReviewSubmission, ProcessingJob, ReviewAssignment]]:
        with self.sessions() as session:
            return list(session.execute(
                select(ReviewSubmission, ProcessingJob, ReviewAssignment)
                .join(ProcessingJob, ProcessingJob.submission_id == ReviewSubmission.id)
                .join(ReviewAssignment, ReviewAssignment.id == ReviewSubmission.assignment_id)
                .where(ProcessingJob.job_kind == "submission")
                .order_by(ReviewSubmission.submitted_at.desc())
            ).all())

    def combined_jobs(self) -> list[ProcessingJob]:
        with self.sessions() as session:
            return list(session.scalars(
                select(ProcessingJob)
                .where(ProcessingJob.job_kind == "combined_candidate")
                .order_by(ProcessingJob.created_at.desc())
            ))

    def create_combined_candidate(self, receipt_ids: list[str]) -> str:
        if not receipt_ids or len(receipt_ids) != len(set(receipt_ids)) or len(receipt_ids) > 100:
            raise ValueError("Select one to one hundred unique revisions")
        with self.sessions.begin() as session:
            submissions = [session.get(ReviewSubmission, receipt_id) for receipt_id in receipt_ids]
            if any(item is None or item.inclusion_status != "included" for item in submissions):
                raise ValueError("Combined candidates require included revisions")
            selected = [item for item in submissions if item is not None]
            ready_receipts = set(session.scalars(
                select(ProcessingJob.submission_id).where(
                    ProcessingJob.job_kind == "submission",
                    ProcessingJob.status == "succeeded",
                    ProcessingJob.submission_id.in_(receipt_ids),
                )
            ))
            if ready_receipts != set(receipt_ids):
                raise ValueError("Every selected revision must finish isolated processing")
            assignments = [session.get(ReviewAssignment, item.assignment_id) for item in selected]
            signatures = {
                (item.mode, item.processing_recipe, item.previous_benchmark_path)
                for item in assignments if item is not None
            }
            if len(signatures) != 1:
                raise ValueError("Selected revisions must share mode, recipe, and baseline")
            anchor = selected[0]
            assignment = session.get(ReviewAssignment, anchor.assignment_id)
            if assignment is None:
                raise LookupError("Anchor assignment is unavailable")
            job_id = "candidate-job-" + secrets.token_hex(16)
            session.add(ProcessingJob(
                id=job_id,
                assignment_id=anchor.assignment_id,
                submission_id=anchor.id,
                recipe=assignment.processing_recipe,
                status="queued",
                created_at=timestamp(utc_now()),
                job_kind="combined_candidate",
                selected_submission_ids=list(receipt_ids),
                approval_status="pending",
            ))
            return job_id

    def dashboard_items(self) -> dict[str, tuple[str, ...]]:
        with self.sessions() as session:
            submissions = list(session.scalars(select(ReviewSubmission)))
        result: dict[str, tuple[str, ...]] = {}
        for submission in submissions:
            try:
                payload = json.loads((self.submission_root / submission.export_path).read_bytes())
            except (OSError, json.JSONDecodeError):
                result[submission.id] = ()
                continue
            if "reviews" in payload:
                result[submission.id] = tuple(sorted(payload["reviews"]))
            else:
                result[submission.id] = tuple(
                    sorted(str(item["trial_id"]) for item in payload["annotations"])
                )
        return result

    def decide_inclusion(self, receipt_id: str, owner_id: str, decision: str, reason: str) -> None:
        allowed = {"included", "revision_requested", "rejected"}
        if decision not in allowed or (decision != "included" and not reason.strip()) or len(reason) > 500:
            raise ValueError("Invalid inclusion decision")
        with self.sessions.begin() as session:
            submission = session.get(ReviewSubmission, receipt_id)
            if submission is None:
                raise LookupError("Unknown receipt")
            submission.inclusion_status = decision
            submission.inclusion_reason = reason.strip() or None
            submission.decided_by_reviewer_id = owner_id
            submission.decided_at = timestamp(utc_now())
            session.add(OwnerProcessingDecision(
                id="owner-decision-" + secrets.token_hex(16),
                owner_reviewer_id=owner_id,
                target_type="submission",
                submission_id=receipt_id,
                job_id=None,
                item_id=None,
                decision=decision,
                reason=reason.strip() or None,
                created_at=timestamp(utc_now()),
            ))

    def decide_item(
        self, receipt_id: str, item_id: str, owner_id: str, decision: str, reason: str
    ) -> None:
        if decision not in {"included", "omitted", "revision_requested"}:
            raise ValueError("Invalid item decision")
        if decision != "included" and not reason.strip():
            raise ValueError("Item omission and revision require a reason")
        if not item_id or len(item_id) > 500 or len(reason) > 500:
            raise ValueError("Invalid item decision")
        with self.sessions.begin() as session:
            submission = session.get(ReviewSubmission, receipt_id)
            if submission is None:
                raise LookupError("Unknown receipt")
            payload = json.loads((self.submission_root / submission.export_path).read_bytes())
            allowed = (
                set(payload["reviews"])
                if "reviews" in payload
                else {str(item["trial_id"]) for item in payload["annotations"]}
            )
            if item_id not in allowed:
                raise LookupError("Unknown submitted item")
            session.add(OwnerProcessingDecision(
                id="owner-decision-" + secrets.token_hex(16),
                owner_reviewer_id=owner_id,
                target_type="item",
                submission_id=receipt_id,
                job_id=None,
                item_id=item_id,
                decision=decision,
                reason=reason.strip() or None,
                created_at=timestamp(utc_now()),
            ))

    def decide_candidate(self, job_id: str, owner_id: str, decision: str, reason: str) -> None:
        if decision not in {"approved", "rejected"} or (decision == "rejected" and not reason.strip()) or len(reason) > 500:
            raise ValueError("Invalid candidate decision")
        with self.sessions.begin() as session:
            job = session.get(ProcessingJob, job_id)
            if job is None or job.status != "succeeded":
                raise LookupError("Candidate is not ready")
            if job.job_kind != "combined_candidate":
                raise ValueError("Only a combined candidate can pass the promotion gate")
            submission = session.get(ReviewSubmission, job.submission_id)
            selected_submissions = (
                [session.get(ReviewSubmission, receipt_id) for receipt_id in (job.selected_submission_ids or [])]
                if job.job_kind == "combined_candidate" else [submission]
            )
            if decision == "approved" and any(
                item is None or item.inclusion_status != "included"
                for item in selected_submissions
            ):
                raise ValueError("Include every selected submission before approving its candidate")
            decision_id = "owner-decision-" + secrets.token_hex(16)
            if decision == "approved" and all(item is not None for item in selected_submissions):
                concrete = [item for item in selected_submissions if item is not None]
                item_history = list(session.scalars(
                    select(OwnerProcessingDecision)
                    .where(
                        OwnerProcessingDecision.submission_id.in_([item.id for item in concrete]),
                        OwnerProcessingDecision.target_type == "item",
                    )
                    .order_by(OwnerProcessingDecision.created_at, OwnerProcessingDecision.id)
                ))
                latest_items: dict[str, dict[str, dict[str, str | None]]] = {}
                for item in item_history:
                    if item.submission_id is not None and item.item_id is not None:
                        latest_items.setdefault(item.submission_id, {})[item.item_id] = {
                            "decision": item.decision, "reason": item.reason
                        }
                assignment = session.get(ReviewAssignment, job.assignment_id)
                manifest = {
                    "schema": "musparql.candidate-promotion-manifest.v1",
                    "job_id": job.id,
                    "approval_decision_id": decision_id,
                    "baseline": assignment.previous_benchmark_path if assignment else None,
                    "selected_submissions": [{
                        "receipt_id": item.id,
                        "assignment_id": item.assignment_id,
                        "revision": item.revision,
                        "digest": item.export_digest,
                    } for item in concrete],
                    "item_overrides": latest_items,
                    "approved_by": owner_id,
                    "approved_at": timestamp(utc_now()),
                }
                _atomic_write(
                    self.candidate_root / job.id, f"promotion-{decision_id}.json",
                    _canonical_bytes(manifest),
                )
                job.candidate_output_path = f"{job.id}/promotion-{decision_id}.json"
            job.approval_status = decision
            job.approval_reason = reason.strip() or None
            job.approved_by_reviewer_id = owner_id
            job.approved_at = timestamp(utc_now())
            assignment = session.get(ReviewAssignment, job.assignment_id)
            if assignment is not None:
                assignment.status = "approved" if decision == "approved" else "ready_for_owner_review"
            if job.job_kind == "combined_candidate":
                for selected in selected_submissions:
                    selected_assignment = (
                        session.get(ReviewAssignment, selected.assignment_id)
                        if selected is not None else None
                    )
                    if selected_assignment is not None:
                        selected_assignment.status = (
                            "approved" if decision == "approved" else "ready_for_owner_review"
                        )
            session.add(OwnerProcessingDecision(
                id=decision_id,
                owner_reviewer_id=owner_id,
                target_type="candidate",
                submission_id=None,
                job_id=job_id,
                item_id=None,
                decision=decision,
                reason=reason.strip() or None,
                created_at=timestamp(utc_now()),
            ))
