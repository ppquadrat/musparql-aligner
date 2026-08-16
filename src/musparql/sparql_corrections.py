"""Automatic SPARQL-correction triage and append-only review application."""
from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
from urllib.parse import urlsplit, urlunsplit
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Sequence

from musparql.sparql_versions import available_sparql_versions, resolve_sparql_version, sparql_hash, validate_execution_versions
from musparql.reviewer_provenance import validate_reviewer_id, validate_review_provenance


CANDIDATE_SCHEMA = "musparql.sparql-correction-candidate.v1"
REVIEW_EXPORT_SCHEMA = "musparql.sparql-correction-review-export.v2"
DECISIONS = {"approve_edit", "no_edit", "defer"}
EDIT_TYPES = {
    "syntax_correction",
    "endpoint_dialect_adaptation",
    "parameter_instantiation",
    "benchmark_specialization",
    "federation_rewrite",
    "performance_optimization",
    "other",
}
PROPOSAL_ORIGINS = {"human", "agent", "source_artifact"}
SHA256_RE = re.compile(r"sha256:[0-9a-f]{64}")


def safe_endpoint(value: Any) -> str | None:
    if not isinstance(value, str) or not value:
        return None
    parts = urlsplit(value)
    host = parts.hostname or ""
    if parts.port:
        host += f":{parts.port}"
    return urlunsplit((parts.scheme, host, parts.path, "", ""))


def safe_error(value: Any, limit: int = 500) -> str | None:
    if value is None:
        return None
    text = re.sub(r"(?i)bearer\s+[a-z0-9._~+/=-]+", "Bearer <redacted>", str(value))
    text = re.sub(r"https?://[^\s/@]+:[^\s/@]+@", lambda m: m.group(0).split("://", 1)[0] + "://<redacted>@", text)
    text = re.sub(r"(?i)(api[_-]?key|token|password|secret)=([^\s&]+)", r"\1=<redacted>", text)
    # Strip every URL query/fragment, including provider-specific signed URL
    # parameters whose names cannot be exhaustively enumerated.
    text = re.sub(r"https?://[^\s<>\"']+", lambda m: safe_endpoint(m.group(0)) or "<redacted-url>", text, flags=re.I)
    return text[:limit]


def safe_execution_projection(value: Mapping[str, Any]) -> Dict[str, Any]:
    result = {
        key: deepcopy(value.get(key))
        for key in (
            "status", "skip_reason", "http_status", "content_type", "effective_sparql_hash",
            "observed_at", "ran_at", "duration_ms", "result_count",
        )
        if value.get(key) is not None
    }
    endpoint = safe_endpoint(value.get("endpoint"))
    if endpoint is not None:
        result["endpoint"] = endpoint
    graph = safe_endpoint(value.get("graph"))
    if graph is not None:
        result["graph"] = graph
    error = safe_error(value.get("error") or value.get("error_line"))
    if error is not None:
        result["error"] = error
    return result


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def retained_sparql_edit_count(record: Mapping[str, Any]) -> int:
    """Return a validated retained-edit count; malformed state fails closed."""
    edits = record.get("sparql_edits")
    if edits is None:
        return 0
    if not isinstance(edits, list):
        raise ValueError("sparql_edits must be a list")
    # Full resolution validates contiguity, text, notes, and the source hash.
    resolve_sparql_version(record, "latest")
    return len(edits)


def correction_diagnostics(
    record: Mapping[str, Any], resolved: Mapping[str, Any]
) -> list[Dict[str, Any]]:
    """Return static diagnostics only while their pinned version remains latest."""
    raw_diagnostics = record.get("sparql_diagnostics")
    if raw_diagnostics is None:
        raw_diagnostics = []
    if not isinstance(raw_diagnostics, list):
        raise ValueError("sparql_diagnostics must be a list")
    latest = resolve_sparql_version(record, "latest")
    selected_version = resolved.get("sparql_version")
    selected_hash = resolved.get("sparql_hash")
    actionable = (
        selected_version == latest["sparql_version"]
        and selected_hash == latest["sparql_hash"]
    )
    result: list[Dict[str, Any]] = []
    seen: set[tuple[int, str, str, str]] = set()
    for diagnostic in raw_diagnostics:
        if not isinstance(diagnostic, Mapping):
            raise ValueError("sparql_diagnostics entries must be objects")
        version = diagnostic.get("sparql_version")
        digest = diagnostic.get("sparql_hash")
        code = diagnostic.get("code")
        source = diagnostic.get("source")
        message = diagnostic.get("message")
        if isinstance(version, bool) or not isinstance(version, int) or version < 0:
            raise ValueError("SPARQL diagnostic requires a non-negative sparql_version")
        if not isinstance(digest, str) or SHA256_RE.fullmatch(digest) is None:
            raise ValueError("SPARQL diagnostic requires a valid sparql_hash")
        if not all(
            isinstance(value, str) and value.strip()
            for value in (code, source, message)
        ):
            raise ValueError("SPARQL diagnostic requires nonempty code, source, and message")
        pinned = resolve_sparql_version(record, version)
        if pinned["sparql_hash"] != digest:
            raise ValueError("SPARQL diagnostic version/hash does not resolve")
        key = (version, digest, code, source)
        if key in seen:
            raise ValueError("Duplicate SPARQL diagnostic")
        seen.add(key)
        if actionable and version == selected_version and digest == selected_hash:
            result.append(deepcopy(dict(diagnostic)))
    return result


def sparql_provenance(record: Mapping[str, Any], resolved: Mapping[str, Any]) -> Dict[str, Any]:
    count = retained_sparql_edit_count(record)
    result = {
        "retained_edit_count": count,
        "selected_version": resolved["sparql_version"],
        "selected_hash": resolved["sparql_hash"],
        "history_digest": "sha256:" + hashlib.sha256(
            canonical_review_payload(record.get("sparql_correction_history") or [])
        ).hexdigest(),
        "execution_observation": execution_observation(record, resolved),
    }
    if resolved["sparql_version"] > 0:
        provenance = resolved.get("provenance") or {}
        public_fields = (
            "candidate_id", "candidate_digest", "decision", "edit_type", "rationale",
            "evidence_ids", "proposal_origin", "proposal_model", "reviewer_id", "reviewed_at",
            "review_export_hash", "approved_sparql_version", "approved_sparql_hash",
        )
        result["selected_edit"] = {
            key: deepcopy(provenance.get(key)) for key in public_fields if provenance.get(key) is not None
        }
        suggestion = provenance.get("agent_suggestion")
        if isinstance(suggestion, Mapping):
            result["selected_edit"]["agent_provenance"] = {
                key: deepcopy(suggestion.get(key)) for key in (
                    "suggestion_id", "suggestion_digest", "model", "request_id", "prompt_hash",
                    "schema_hash", "input_hash", "proposed_sparql_hash", "bundle_digest",
                ) if suggestion.get(key) is not None
            }
        attempts = provenance.get("ui_execution_attempts") or []
        if isinstance(attempts, list):
            result["selected_edit"]["ui_execution_observations"] = [
                {
                    key: deepcopy(attempt.get(key)) for key in (
                        "attempt_id", "attempt_digest", "target", "status", "sparql_hash",
                        "effective_sparql_hash", "duration_ms", "result_count", "ran_at",
                    ) if attempt.get(key) is not None
                }
                for attempt in attempts if isinstance(attempt, Mapping)
            ]
    return result


def execution_observation(record: Mapping[str, Any], resolved: Mapping[str, Any]) -> Dict[str, Any]:
    """Describe the newest matching attempt without turning execution into eligibility."""
    observations = record.get("execution_history") or record.get("run_history") or []
    matches = [
        item for item in observations
        if isinstance(item, Mapping)
        and item.get("sparql_version") == resolved["sparql_version"]
        and item.get("sparql_hash") == resolved["sparql_hash"]
    ]
    if not matches:
        return {"status": "not_attempted", "attempted": False}
    observation = matches[-1]
    return {
        "status": str(observation.get("status") or "unknown"),
        "attempted": True,
        "observed_at": observation.get("ran_at"),
        "execution_digest": "sha256:" + hashlib.sha256(
            canonical_review_payload(observation)
        ).hexdigest(),
        "endpoint": safe_endpoint(observation.get("endpoint")),
        "graph": safe_endpoint(observation.get("graph")),
        "duration_ms": observation.get("duration_ms"),
        "result_count": observation.get("result_count"),
    }


def assert_input_provenance_current(
    inputs: Iterable[Mapping[str, Any]], canonical_records: Sequence[Mapping[str, Any]]
) -> None:
    """Fail closed when frozen/generated input provenance is stale or incomplete."""
    canonical = {
        (str(item.get("kg_id") or ""), str(item.get("query_id") or "")): item
        for item in canonical_records
    }
    for item in inputs:
        key = (str(item.get("kg_id") or ""), str(item.get("query_id") or ""))
        record = canonical.get(key)
        if record is None:
            raise ValueError(f"Input pair is absent from canonical queries: {key[0]}/{key[1]}")
        version = item.get("sparql_version")
        resolved = resolve_sparql_version(record, version if isinstance(version, int) else "latest")
        expected = sparql_provenance(record, resolved)
        if item.get("sparql_hash") != resolved["sparql_hash"] or item.get("sparql_provenance") != expected:
            raise ValueError(f"Stale or incomplete SPARQL provenance for {key[0]}/{key[1]}")


def holdout_ineligible_reason(record: Mapping[str, Any]) -> str | None:
    if retained_sparql_edit_count(record) > 0:
        return "retained_sparql_edit_history"
    return None


def classify_failure(
    failure: Mapping[str, Any], diagnostics: Sequence[Mapping[str, Any]] | None = None
) -> Dict[str, Any]:
    status = str(failure.get("status") or "unknown")
    skip_reason = str(failure.get("skip_reason") or "")
    http_status = failure.get("http_status")
    if diagnostics:
        codes = ", ".join(
            sorted({str(item.get("code") or "unknown") for item in diagnostics})
        )
        return {
            "reason_code": "static_sparql_validation",
            "category": "likely_correction",
            "priority": "high",
            "summary": f"Static extraction validation flagged the retained SPARQL ({codes}).",
        }
    if status == "not_attempted":
        return {
            "reason_code": "not_attempted",
            "category": "needs_observation",
            "priority": "medium",
            "summary": "This retained version has no execution observation; it remains eligible for correction review.",
        }
    if status in {"ok", "empty"}:
        return {
            "reason_code": f"execution_{status}",
            "category": "general_review",
            "priority": "normal",
            "summary": "Execution completed, but semantic correction review remains available.",
        }
    if status in {"parse_error", "query_error"}:
        return {
            "reason_code": "endpoint_rejected_query",
            "category": "likely_correction",
            "priority": "high",
            "summary": "The endpoint classified the submitted text as a query or parse error.",
        }
    if status == "http_error" and http_status in {400, 422}:
        return {
            "reason_code": "endpoint_rejected_query",
            "category": "likely_correction",
            "priority": "high",
            "summary": "The submitted request received a 400/422 response; endpoint health and query validity still require review.",
        }
    if status == "skipped_local_query" and skip_reason == "parameterized_template":
        return {
            "reason_code": "parameterized_template",
            "category": "instantiation_required",
            "priority": "medium",
            "summary": "The query contains unresolved runtime placeholders and needs parameter instantiation.",
        }
    if status == "skipped_local_query" and skip_reason in {
        "requires_sparql_anything",
        "requires_local_file",
    }:
        return {
            "reason_code": skip_reason,
            "category": "runtime_environment",
            "priority": "informational",
            "summary": "The query requires a specialised or local execution environment, not necessarily an edit.",
        }
    if status in {"skipped_endpoint_unavailable", "skipped_no_endpoint"}:
        return {
            "reason_code": status,
            "category": "infrastructure",
            "priority": "informational",
            "summary": "Execution infrastructure was unavailable; this is not evidence of invalid SPARQL.",
        }
    if status == "http_error" and http_status == 500:
        return {
            "reason_code": "endpoint_or_query_failure",
            "category": "investigate",
            "priority": "medium",
            "summary": "The server failed while processing the query; endpoint, federation, cost, and query causes remain possible.",
        }
    return {
        "reason_code": "execution_failure",
        "category": "investigate",
        "priority": "medium",
        "summary": "Execution failed, but the observation alone does not establish that the SPARQL needs correction.",
    }


def candidate_id(failure: Mapping[str, Any], triage: Mapping[str, Any]) -> str:
    identity = "\n".join(
        [
            str(failure.get("kg_id") or ""),
            str(failure.get("query_id") or ""),
            str(failure.get("sparql_version") if failure.get("sparql_version") is not None else 0),
            str(failure.get("sparql_hash") or ""),
            str(triage.get("reason_code") or ""),
        ]
    )
    return "sc-" + hashlib.sha256(identity.encode("utf-8")).hexdigest()[:20]


def build_candidate(
    failure: Mapping[str, Any], query: Mapping[str, Any], *, captured_at: str | None = None
) -> Dict[str, Any]:
    version = failure.get("sparql_version")
    if isinstance(version, bool) or not isinstance(version, int):
        version = 0
    resolved = resolve_sparql_version(query, version)
    if failure.get("sparql_hash") and failure.get("sparql_hash") != resolved["sparql_hash"]:
        raise ValueError(f"Failure SPARQL hash does not resolve for {query.get('query_label')}")
    diagnostics = correction_diagnostics(query, resolved)
    triage = classify_failure(failure, diagnostics)
    execution = safe_execution_projection(failure)
    item = {
        "schema": CANDIDATE_SCHEMA,
        "candidate_id": candidate_id(failure, triage),
        "captured_at": captured_at or str(failure.get("observed_at") or utc_now()),
        "capture_provenance": {"producer": "run_queries.py", "automatic": True},
        "kg_id": query.get("kg_id"),
        "query_id": query.get("query_id"),
        "query_label": query.get("query_label"),
        "base_sparql_version": resolved["sparql_version"],
        "base_sparql_hash": resolved["sparql_hash"],
        "base_sparql": resolved["sparql"],
        "sparql_provenance": sparql_provenance(query, resolved),
        "triage": triage,
        "execution": execution,
        "evidence": deepcopy(query.get("evidence") or []),
        "existing_edits": deepcopy(query.get("sparql_edits") or []),
        "correction_history": deepcopy(query.get("sparql_correction_history") or []),
        "retained_versions": [
            {
                "sparql_version": version["sparql_version"],
                "sparql_hash": version["sparql_hash"],
                "sparql": version["sparql"],
                "note": version.get("note"),
            }
            for version in available_sparql_versions(query)
        ],
    }
    if diagnostics:
        item["sparql_diagnostics"] = diagnostics
    return item


def candidate_digest(item: Mapping[str, Any]) -> str:
    canonical = {key: value for key, value in item.items() if key != "candidate_digest"}
    return "sha256:" + hashlib.sha256(canonical_review_payload(canonical)).hexdigest()


def validate_candidate(item: Mapping[str, Any]) -> None:
    if item.get("schema") != CANDIDATE_SCHEMA:
        raise ValueError("Unsupported SPARQL correction candidate schema")
    for field in (
        "candidate_id", "kg_id", "query_id", "base_sparql", "base_sparql_hash",
        "captured_at",
    ):
        if not isinstance(item.get(field), str) or not str(item[field]).strip():
            raise ValueError(f"Correction candidate requires {field}")
    version = item.get("base_sparql_version")
    if isinstance(version, bool) or not isinstance(version, int) or version < 0:
        raise ValueError("Correction candidate requires a non-negative base_sparql_version")
    if SHA256_RE.fullmatch(str(item["base_sparql_hash"])) is None:
        raise ValueError("Correction candidate has an invalid base_sparql_hash")
    if sparql_hash(str(item["base_sparql"])) != item["base_sparql_hash"]:
        raise ValueError("Correction candidate base SPARQL hash mismatch")
    execution = item.get("execution")
    triage = item.get("triage")
    if not isinstance(execution, Mapping) or not isinstance(triage, Mapping):
        raise ValueError("Correction candidate requires execution and triage objects")
    if dict(execution) != safe_execution_projection(execution):
        raise ValueError("Correction candidate execution is not safely projected; regenerate the candidate ledger")
    reconstructed = {
        **dict(execution), "kg_id": item["kg_id"], "query_id": item["query_id"],
        "sparql_version": version, "sparql_hash": item["base_sparql_hash"],
    }
    diagnostics = item.get("sparql_diagnostics")
    if diagnostics is None:
        diagnostics = []
    if not isinstance(diagnostics, list) or not all(
        isinstance(entry, Mapping) for entry in diagnostics
    ):
        raise ValueError("Correction candidate sparql_diagnostics must be a list of objects")
    diagnostic_keys: set[tuple[Any, Any, Any, Any]] = set()
    for diagnostic in diagnostics:
        if (
            diagnostic.get("sparql_version") != version
            or diagnostic.get("sparql_hash") != item["base_sparql_hash"]
        ):
            raise ValueError("Correction candidate SPARQL diagnostic does not match its base pins")
        if not all(
            isinstance(diagnostic.get(field), str) and str(diagnostic[field]).strip()
            for field in ("code", "source", "message")
        ):
            raise ValueError("Correction candidate SPARQL diagnostic is incomplete")
        diagnostic_key = tuple(
            diagnostic.get(field) for field in ("sparql_version", "sparql_hash", "code", "source")
        )
        if diagnostic_key in diagnostic_keys:
            raise ValueError("Duplicate correction candidate SPARQL diagnostic")
        diagnostic_keys.add(diagnostic_key)
    expected_triage = classify_failure(reconstructed, diagnostics)
    if dict(triage) != expected_triage or candidate_id(reconstructed, expected_triage) != item["candidate_id"]:
        raise ValueError("Correction candidate identity or triage is inconsistent")
    evidence = item.get("evidence") or []
    if not isinstance(evidence, list):
        raise ValueError("Correction candidate evidence must be a list")
    if not all(isinstance(entry, Mapping) for entry in evidence):
        raise ValueError("Correction candidate evidence entries must be objects")
    evidence_ids = [entry.get("evidence_id") for entry in evidence]
    if any(not isinstance(value, str) or not value for value in evidence_ids) or len(evidence_ids) != len(set(evidence_ids)):
        raise ValueError("Correction candidate evidence IDs must be nonempty and unique")
    provenance = item.get("sparql_provenance")
    if not isinstance(provenance, Mapping) or provenance.get("selected_version") != version or provenance.get("selected_hash") != item["base_sparql_hash"]:
        raise ValueError("Correction candidate SPARQL provenance does not match its base pins")


def merge_candidates(
    existing: Iterable[Mapping[str, Any]],
    failures: Iterable[Mapping[str, Any]],
    queries: Sequence[Mapping[str, Any]],
    job_keys: set[tuple[str, str, int]],
) -> list[Dict[str, Any]]:
    """Replace only candidate rows in the current scoped run."""
    query_by_id = {
        (str(item.get("kg_id") or ""), str(item.get("query_id") or "")): item for item in queries
    }
    latest_job_pairs = {
        (kg_id, query_id)
        for kg_id, query_id, version in job_keys
        if (query := query_by_id.get((kg_id, query_id))) is not None
        and resolve_sparql_version(query, "latest")["sparql_version"] == version
    }
    retained = [
        deepcopy(dict(item))
        for item in existing
        if (str(item.get("kg_id") or ""), str(item.get("query_id") or ""))
        not in latest_job_pairs
        and (
            str(item.get("kg_id") or ""),
            str(item.get("query_id") or ""),
            int(item.get("base_sparql_version") or 0),
        ) not in job_keys
    ]
    for failure in failures:
        kg_id, query_id = str(failure.get("kg_id") or ""), str(failure.get("query_id") or "")
        version = failure.get("sparql_version")
        normalized_version = version if isinstance(version, int) and not isinstance(version, bool) else 0
        if (kg_id, query_id, normalized_version) not in job_keys:
            continue
        query = query_by_id.get((kg_id, query_id))
        if query is None:
            raise ValueError(f"No query record for correction candidate {query_id}")
        latest = resolve_sparql_version(query, "latest")
        if (
            normalized_version != latest["sparql_version"]
            or failure.get("sparql_hash") != latest["sparql_hash"]
        ):
            continue
        retained.append(build_candidate(failure, query))
    retained.sort(
        key=lambda item: (
            str(item.get("kg_id") or ""),
            str(item.get("query_label") or ""),
            int(item.get("base_sparql_version") or 0),
            str(item.get("candidate_id") or ""),
        )
    )
    return retained


def exclude_candidate_pairs(
    candidates: Iterable[Mapping[str, Any]], forbidden_pairs: set[tuple[str, str]]
) -> list[Dict[str, Any]]:
    return [
        deepcopy(dict(item)) for item in candidates
        if (str(item.get("kg_id") or ""), str(item.get("query_id") or "")) not in forbidden_pairs
    ]


def load_jsonl(path: Path) -> list[Dict[str, Any]]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def write_jsonl(path: Path, rows: Iterable[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            for row in rows:
                stream.write(json.dumps(row, ensure_ascii=False) + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp_name, path)
    except BaseException:
        try:
            os.unlink(temp_name)
        except FileNotFoundError:
            pass
        raise


def canonical_review_payload(payload: Mapping[str, Any]) -> bytes:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def validate_review_export(payload: Mapping[str, Any]) -> None:
    if payload.get("schema") != REVIEW_EXPORT_SCHEMA or payload.get("mode") != "sparql_correction":
        raise ValueError("Not a supported SPARQL correction review export")
    reviews = payload.get("reviews")
    if not isinstance(reviews, list):
        raise ValueError("Correction review export requires a reviews list")
    for field in ("reviewer_id", "dataset_id", "exported_at"):
        if not isinstance(payload.get(field), str) or not str(payload[field]).strip():
            raise ValueError(f"Correction review export requires {field}")
    try:
        datetime.fromisoformat(str(payload["exported_at"]).replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("Correction review export requires an ISO exported_at timestamp") from exc
    validate_reviewer_id(payload.get("reviewer_id"))
    for review in reviews:
        if not isinstance(review, Mapping):
            raise ValueError("Each correction review must be an object")
        decision = review.get("decision")
        if decision not in DECISIONS:
            raise ValueError(f"Unsupported correction decision: {decision!r}")
        for field in ("review_id", "candidate_id", "candidate_digest", "kg_id", "query_id", "base_sparql_hash", "reviewer_id", "reviewed_at"):
            if not isinstance(review.get(field), str) or not str(review[field]).strip():
                raise ValueError(f"Correction review requires {field}")
        validate_review_provenance(review)
        if review.get("reviewer_id") != payload.get("reviewer_id"):
            raise ValueError("Correction review reviewer_id does not match the export reviewer_id")
        if not isinstance(review.get("candidate_reason_code"), str) or not str(
            review["candidate_reason_code"]
        ).strip():
            raise ValueError("Correction review requires candidate_reason_code")
        version = review.get("base_sparql_version")
        if isinstance(version, bool) or not isinstance(version, int) or version < 0:
            raise ValueError("Correction review requires a non-negative base_sparql_version")
        rationale = review.get("rationale")
        if decision == "approve_edit":
            proposed = review.get("proposed_sparql")
            if not isinstance(proposed, str) or not proposed.strip():
                raise ValueError("approve_edit requires proposed_sparql")
            if review.get("edit_type") not in EDIT_TYPES and not (
                isinstance(rationale, str) and rationale.strip()
            ):
                raise ValueError("approve_edit requires either a supported edit_type or rationale")
        origin = review.get("proposal_origin")
        if origin not in PROPOSAL_ORIGINS:
            raise ValueError("Correction review requires a supported proposal_origin")
        if origin == "agent" and not isinstance(review.get("agent_suggestion"), Mapping):
            raise ValueError("Agent proposals require retained agent_suggestion provenance")
        try:
            datetime.fromisoformat(str(review["reviewed_at"]).replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError("Correction review requires an ISO reviewed_at timestamp") from exc
        evidence_ids = review.get("evidence_ids") or []
        if not isinstance(evidence_ids, list) or not all(isinstance(item, str) for item in evidence_ids):
            raise ValueError("evidence_ids must be a list of strings")


def apply_reviews(
    records: list[Dict[str, Any]],
    payload: Mapping[str, Any],
    *,
    export_path: str,
    candidates: Sequence[Mapping[str, Any]],
    candidate_path: str = "<in-memory>",
    forbidden_pairs: set[tuple[str, str]] | None = None,
    authoritative_suggestions: Sequence[Mapping[str, Any]] = (),
    authoritative_executions: Sequence[Mapping[str, Any]] = (),
    authoritative_bundle_digest: str | None = None,
) -> Dict[str, int]:
    validate_review_export(payload)
    if authoritative_bundle_digest is not None and payload.get("bundle_digest") != authoritative_bundle_digest:
        raise ValueError("Correction review export does not match the authoritative browser bundle")
    forbidden_pairs = forbidden_pairs or set()
    by_id = {(str(record.get("kg_id") or ""), str(record.get("query_id") or "")): record for record in records}
    candidate_by_id: Dict[str, Mapping[str, Any]] = {}
    for item in candidates:
        candidate_pair = (str(item.get("kg_id") or ""), str(item.get("query_id") or ""))
        if candidate_pair in forbidden_pairs:
            continue
        validate_candidate(item)
        identifier = str(item["candidate_id"])
        if identifier in candidate_by_id:
            raise ValueError(f"Duplicate correction candidate {identifier}")
        candidate_by_id[identifier] = item
    export_hash = "sha256:" + hashlib.sha256(canonical_review_payload(payload)).hexdigest()
    suggestion_by_id = {str(item.get("suggestion_id") or ""): item for item in authoritative_suggestions}
    execution_by_id = {str(item.get("attempt_id") or ""): item for item in authoritative_executions}
    stats = {"approved": 0, "no_edit": 0, "deferred": 0}
    seen_candidates: set[str] = set()
    for review in payload["reviews"]:
        if authoritative_bundle_digest is not None and review.get("bundle_digest") != authoritative_bundle_digest:
            raise ValueError("Correction review is not pinned to the authoritative browser bundle")
        candidate = str(review["candidate_id"])
        if candidate in seen_candidates:
            raise ValueError(f"Duplicate correction review for {candidate}")
        seen_candidates.add(candidate)
        kg_id, query_id = str(review["kg_id"]), str(review["query_id"])
        record = by_id.get((kg_id, query_id))
        if record is None:
            raise ValueError(f"Unknown correction query_id {query_id}")
        pair = (kg_id, query_id)
        if pair in forbidden_pairs:
            raise ValueError(f"Refusing to review or edit holdout-selected pair {pair[0]}/{pair[1]}")
        anchored = candidate_by_id.get(candidate)
        if anchored is None:
            raise ValueError(f"Correction candidate is absent from the authoritative ledger: {candidate}")
        if candidate_digest(anchored) != review["candidate_digest"]:
            raise ValueError("Correction candidate digest does not match the authoritative ledger")
        evidence = record.get("evidence") or []
        valid_evidence_ids = {
            str(item.get("evidence_id"))
            for item in evidence
            if isinstance(item, Mapping) and isinstance(item.get("evidence_id"), str)
        }
        unknown_evidence = sorted(set(review.get("evidence_ids") or []) - valid_evidence_ids)
        if review.get("decision") == "approve_edit" and unknown_evidence:
            raise ValueError(
                f"Unknown correction evidence IDs for {record.get('query_label')}: {unknown_evidence}"
            )
        suggestion = review.get("agent_suggestion")
        if review.get("proposal_origin") == "agent":
            suggestion_id = str(suggestion.get("suggestion_id") or "") if isinstance(suggestion, Mapping) else ""
            authoritative_suggestion = suggestion_by_id.get(suggestion_id)
            if authoritative_suggestion is None or dict(authoritative_suggestion) != dict(suggestion or {}):
                raise ValueError("Agent suggestion does not match the authoritative service log")
            if (
                authoritative_suggestion.get("candidate_id") != candidate
                or authoritative_suggestion.get("kg_id") != kg_id
                or authoritative_suggestion.get("query_id") != query_id
                or (
                    review.get("decision") == "approve_edit"
                    and authoritative_suggestion.get("proposed_sparql_hash") != sparql_hash(str(review.get("proposed_sparql") or ""))
                )
            ):
                raise ValueError("Agent suggestion identity or proposal hash is stale")
        for attempt in review.get("execution_attempts") or []:
            if not isinstance(attempt, Mapping):
                raise ValueError("execution_attempts entries must be objects")
            authoritative_attempt = execution_by_id.get(str(attempt.get("attempt_id") or ""))
            if authoritative_attempt is None or dict(authoritative_attempt) != dict(attempt):
                raise ValueError("UI execution attempt does not match the authoritative service log")
            if authoritative_attempt.get("candidate_id") != candidate or (
                authoritative_attempt.get("kg_id"), authoritative_attempt.get("query_id")
            ) != (kg_id, query_id):
                raise ValueError("UI execution attempt identity is inconsistent")
        latest = resolve_sparql_version(record, "latest")
        if (
            latest["sparql_version"] != review["base_sparql_version"]
            or latest["sparql_hash"] != review["base_sparql_hash"]
        ):
            raise ValueError(f"Stale correction proposal for {record.get('query_label')}")
        if any(
            anchored.get(field) != review.get(field)
            for field in (
                "kg_id", "query_id", "base_sparql_version", "base_sparql_hash",
            )
        ):
            raise ValueError("Correction review identity/base pins do not match the authoritative candidate")
        expected_diagnostics = correction_diagnostics(record, latest)
        if list(anchored.get("sparql_diagnostics") or []) != expected_diagnostics:
            raise ValueError("Correction candidate SPARQL diagnostics are stale")
        execution_snapshot = anchored["execution"]
        candidate_failure = {
            **dict(execution_snapshot),
            "kg_id": record.get("kg_id"),
            "query_id": query_id,
            "sparql_version": latest["sparql_version"],
            "sparql_hash": latest["sparql_hash"],
        }
        triage = classify_failure(candidate_failure, expected_diagnostics)
        if triage["reason_code"] != review["candidate_reason_code"]:
            raise ValueError("Correction candidate reason no longer matches its execution snapshot")
        if candidate_id(candidate_failure, triage) != candidate:
            raise ValueError("Correction candidate identity does not match its pinned trigger")
        observed_at = str(execution_snapshot.get("observed_at") or "")
        observations = record.get("execution_history") or record.get("run_history") or []
        execution_match = any(
            isinstance(item, Mapping)
            and item.get("sparql_version") == latest["sparql_version"]
            and item.get("sparql_hash") == latest["sparql_hash"]
            and item.get("status") == execution_snapshot.get("status")
            and item.get("endpoint") == execution_snapshot.get("endpoint")
            and (not observed_at or str(item.get("ran_at") or "") == observed_at)
            and item.get("http_status") == execution_snapshot.get("http_status")
            and item.get("skip_reason") == execution_snapshot.get("skip_reason")
            for item in observations
        )
        if not execution_match and execution_snapshot.get("status") != "not_attempted":
            raise ValueError("Correction candidate trigger does not match retained execution history")
        history = record.setdefault("sparql_correction_history", [])
        if not isinstance(history, list):
            raise ValueError("sparql_correction_history must be a list")
        if any(isinstance(item, Mapping) and item.get("candidate_id") == candidate for item in history):
            raise ValueError(f"Correction candidate already applied: {candidate}")
        reviewed_at = str(review["reviewed_at"])
        provenance = {
            "review_id": review.get("review_id"),
            "candidate_id": candidate,
            "base_sparql_version": latest["sparql_version"],
            "base_sparql_hash": latest["sparql_hash"],
            "decision": review["decision"],
            "edit_type": review.get("edit_type"),
            "rationale": review.get("rationale"),
            "evidence_ids": list(review.get("evidence_ids") or []),
            "proposal_origin": review.get("proposal_origin"),
            "proposal_model": review.get("proposal_model"),
            "agent_suggestion": deepcopy(review.get("agent_suggestion")) if review.get("proposal_origin") == "agent" else None,
            "ui_execution_attempts": deepcopy(review.get("execution_attempts") or []),
            "bundle_digest": review.get("bundle_digest"),
            "candidate_digest": candidate_digest(anchored),
            "candidate_ledger": candidate_path,
            "candidate_captured_at": anchored.get("captured_at"),
            "candidate_capture_provenance": deepcopy(anchored.get("capture_provenance") or {}),
            "candidate_execution": deepcopy(execution_snapshot),
            "reviewed_at": reviewed_at,
            "reviewer_id": review.get("reviewer_id"),
            "prior_review_ids": list(review.get("prior_review_ids") or []),
            "authored_formulation_ids": list(review.get("authored_formulation_ids") or []),
            "approved_formulation_ids": list(review.get("approved_formulation_ids") or []),
            "review_export": export_path,
            "review_export_hash": export_hash,
            "reviewer_note": review.get("reviewer_note"),
        }
        decision = review["decision"]
        if decision == "approve_edit":
            proposed = str(review["proposed_sparql"]).strip()
            proposed_hash = sparql_hash(proposed)
            if proposed_hash == latest["sparql_hash"]:
                raise ValueError(f"Approved correction is unchanged for {record.get('query_label')}")
            edits = record.setdefault("sparql_edits", [])
            if not isinstance(edits, list):
                raise ValueError("sparql_edits must be a list")
            approved_version = latest["sparql_version"] + 1
            provenance["approved_sparql_version"] = approved_version
            provenance["approved_sparql_hash"] = proposed_hash
            note = str(review.get("rationale") or "").strip() or str(review.get("edit_type") or "SPARQL correction")
            edit = {
                "version": approved_version,
                "sparql": proposed,
                "note": note,
                "edit_type": review.get("edit_type"),
                "evidence_ids": list(review.get("evidence_ids") or []),
                "provenance": deepcopy(provenance),
            }
            edits.append(edit)
            promoted = []
            for attempt in review.get("execution_attempts") or []:
                if (
                    isinstance(attempt, Mapping)
                    and attempt.get("target") == "proposal"
                    and attempt.get("sparql_hash") == proposed_hash
                ):
                    promoted.append({
                        key: deepcopy(attempt.get(key)) for key in (
                            "status", "pipeline_status", "endpoint", "graph", "duration_ms",
                            "result_count", "effective_sparql_hash", "error", "ran_at",
                        ) if attempt.get(key) is not None
                    } | {
                        "sparql_version": approved_version,
                        "sparql_hash": proposed_hash,
                        "source": "correction_ui",
                        "source_attempt_id": attempt.get("attempt_id"),
                        "source_attempt_digest": attempt.get("attempt_digest"),
                    })
            if promoted:
                execution_history = record.get("execution_history")
                if not isinstance(execution_history, list):
                    execution_history = list(record.get("run_history") or [])
                execution_history.extend(promoted)
                record["execution_history"] = execution_history
                record["run_history"] = execution_history
            stats["approved"] += 1
        elif decision == "no_edit":
            stats["no_edit"] += 1
        else:
            stats["deferred"] += 1
        history.append(provenance)
        validate_execution_versions(record)
    return stats
