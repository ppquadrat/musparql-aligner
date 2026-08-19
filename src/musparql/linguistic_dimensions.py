"""Contracts and deterministic construction for Phase 6b linguistic trials."""
from __future__ import annotations

import hashlib
import json
import random
from typing import Any, Iterable, Sequence


BUNDLE_SCHEMA = "musparql.linguistic-stimulus-bundle.v1"
EXPORT_SCHEMA = "musparql.linguistic-annotation-export.v1"
TASK_DESIGN_VERSION = "phase-6b-v1"


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def text_digest(value: str) -> str:
    return "sha256:" + hashlib.sha256(value.encode("utf-8")).hexdigest()


def validate_stimulus(record: dict[str, Any]) -> None:
    if _contains_holdout_marker(record):
        raise ValueError("Linguistic stimulus contains a holdout marker")
    required_text = (
        "trial_id", "kg_id", "query_id", "query_label", "sparql", "sparql_version",
        "sparql_digest", "sampling_stratum", "contrast_id",
    )
    for field in required_text:
        if not isinstance(record.get(field), str) or not record[field]:
            raise ValueError(f"Linguistic stimulus requires {field}")
    if record["sparql_digest"] != text_digest(record["sparql"]):
        raise ValueError("SPARQL digest does not match the frozen text")
    if record.get("presentation_arity") != 3:
        raise ValueError("The Phase 6b main task requires presentation_arity 3")
    if record.get("non_holdout") is not True or record.get("eligible") is not True:
        raise ValueError("Linguistic stimuli must be eligible and explicitly non-holdout")
    literal = record.get("literal")
    if not isinstance(literal, dict) or literal.get("validated") is not True:
        raise ValueError("A pre-validated literal reference is required")
    _validate_formulation(literal, literal=True)
    candidates = record.get("candidates")
    if not isinstance(candidates, list) or len(candidates) != 2:
        raise ValueError("Exactly two non-literal candidates are required")
    for candidate in candidates:
        _validate_formulation(candidate, literal=False)
    ids = [str(candidate["formulation_id"]) for candidate in candidates]
    if len(set(ids)) != 2 or literal["formulation_id"] in ids:
        raise ValueError("Literal and candidate formulation IDs must be distinct")


def _contains_holdout_marker(value: Any) -> bool:
    if isinstance(value, list):
        return any(_contains_holdout_marker(item) for item in value)
    if not isinstance(value, dict):
        return False
    for key, item in value.items():
        normalized = str(key).casefold()
        if normalized in {"holdout", "is_holdout"} and (
            item is True or str(item).casefold() in {"true", "yes", "holdout"}
        ):
            return True
        if normalized in {"split", "benchmark_split"} and str(item).casefold() in {
            "holdout", "private_holdout"
        }:
            return True
        if _contains_holdout_marker(item):
            return True
    return False


def _validate_formulation(value: Any, *, literal: bool) -> None:
    if not isinstance(value, dict):
        raise ValueError("Formulation must be an object")
    for field in ("formulation_id", "version", "text", "digest"):
        if not isinstance(value.get(field), str) or not value[field]:
            raise ValueError(f"Formulation requires {field}")
    if value["digest"] != text_digest(value["text"]):
        raise ValueError("Formulation digest does not match its frozen text")
    if literal and not isinstance(value.get("validation_provenance"), dict):
        raise ValueError("Literal validation provenance is required")
    if not literal and not isinstance(value.get("provenance"), dict):
        raise ValueError("Candidate provenance is required")


def validate_bundle(payload: dict[str, Any]) -> None:
    if payload.get("schema") != BUNDLE_SCHEMA or payload.get("mode") != "linguistic":
        raise ValueError("Bundle must use the Phase 6b linguistic contract")
    if not isinstance(payload.get("dataset_id"), str) or not payload["dataset_id"]:
        raise ValueError("Linguistic bundle requires dataset_id")
    if payload.get("holdout_input_policy") not in {
        "no_holdout", "identity_visible_selectors", "identity_private_filtered_upstream"
    }:
        raise ValueError("Bundle lacks an approved holdout-exclusion policy")
    randomization = payload.get("randomization")
    if (
        not isinstance(randomization, dict)
        or set(randomization) != {"algorithm", "seed"}
        or randomization.get("algorithm") != "python-mt19937-v1"
        or not isinstance(randomization.get("seed"), str)
        or not randomization["seed"]
    ):
        raise ValueError("Linguistic bundle randomization metadata is invalid")
    records = payload.get("records")
    if not isinstance(records, list) or payload.get("record_count") != len(records):
        raise ValueError("Linguistic bundle record count is invalid")
    if not records:
        raise ValueError("Linguistic bundle must contain at least one trial")
    sampling = payload.get("sampling")
    if not isinstance(sampling, dict) or set(sampling) != {
        "target_trials", "available_trials", "strata"
    }:
        raise ValueError("Linguistic bundle sampling metadata is invalid")
    target_trials = sampling.get("target_trials")
    available_trials = sampling.get("available_trials")
    strata = sampling.get("strata")
    if (
        not isinstance(target_trials, int)
        or isinstance(target_trials, bool)
        or target_trials != len(records)
        or not isinstance(available_trials, int)
        or isinstance(available_trials, bool)
        or available_trials < target_trials
        or not isinstance(strata, list)
        or not strata
        or any(not isinstance(item, str) or not item for item in strata)
        or len(set(strata)) != len(strata)
    ):
        raise ValueError("Linguistic bundle sampling metadata is invalid")
    trial_ids: set[str] = set()
    for record in records:
        if not isinstance(record, dict):
            raise ValueError("Linguistic trial must be an object")
        validate_stimulus(record)
        if record["trial_id"] in trial_ids:
            raise ValueError("Duplicate linguistic trial_id")
        trial_ids.add(record["trial_id"])
        if record["sampling_stratum"] not in strata:
            raise ValueError("Linguistic trial uses an undeclared sampling stratum")
    if payload.get("task_design_version") != TASK_DESIGN_VERSION:
        raise ValueError("Unsupported linguistic task design version")


def build_bundle(
    records: Iterable[dict[str, Any]], *, dataset_id: str, seed: str,
    target_trials: int | None = None, holdout_input_policy: str = "no_holdout",
) -> dict[str, Any]:
    """Validate, deterministically balance, and order an ordinary stimulus pool."""
    if not dataset_id or not seed:
        raise ValueError("dataset_id and a recorded randomization seed are required")
    pool = [dict(record) for record in records]
    trial_ids: set[str] = set()
    for record in pool:
        validate_stimulus(record)
        if record["trial_id"] in trial_ids:
            raise ValueError("Duplicate linguistic trial_id")
        trial_ids.add(record["trial_id"])
    ordered = sorted(pool, key=lambda item: (item["sampling_stratum"], item["trial_id"]))
    rng = random.Random(seed)
    by_stratum: dict[str, list[dict[str, Any]]] = {}
    for record in ordered:
        by_stratum.setdefault(record["sampling_stratum"], []).append(record)
    for rows in by_stratum.values():
        rng.shuffle(rows)
    selected: list[dict[str, Any]] = []
    strata = sorted(by_stratum)
    limit = len(pool) if target_trials is None else target_trials
    if limit < 1 or limit > len(pool):
        raise ValueError("target_trials must be between one and the pool size")
    while len(selected) < limit:
        progressed = False
        for stratum in strata:
            rows = by_stratum[stratum]
            if rows and len(selected) < limit:
                selected.append(rows.pop())
                progressed = True
        if not progressed:
            break
    rng.shuffle(selected)
    payload = {
        "schema": BUNDLE_SCHEMA,
        "mode": "linguistic",
        "task_design_version": TASK_DESIGN_VERSION,
        "dataset_id": dataset_id,
        "holdout_input_policy": holdout_input_policy,
        "randomization": {"algorithm": "python-mt19937-v1", "seed": seed},
        "sampling": {
            "target_trials": limit,
            "available_trials": len(pool),
            "strata": strata,
        },
        "record_count": len(selected),
        "records": selected,
    }
    validate_bundle(payload)
    return payload


def normalized_trial(
    stimulus: dict[str, Any], *, assignment_id: str, dataset_id: str,
    reviewer_id: str, display_order: Sequence[str], outcome: str,
    started_at: str, completed_at: str, ratings: dict[str, Any] | None = None,
    reason: str | None = None, comment: str | None = None,
    proposed_literal: str | None = None,
) -> dict[str, Any]:
    """Create an analysis-ready record after authoritative stimulus validation."""
    validate_stimulus(stimulus)
    candidate_ids = [item["formulation_id"] for item in stimulus["candidates"]]
    if sorted(display_order) != sorted(candidate_ids):
        raise ValueError("Display order must contain the frozen candidate set")
    if outcome not in {"rated", "cannot_assess", "literal_inaccurate"}:
        raise ValueError("Invalid linguistic trial outcome")
    record: dict[str, Any] = {
        "schema": "musparql.linguistic-trial-annotation.v1",
        "assignment_id": assignment_id,
        "dataset_id": dataset_id,
        "trial_id": stimulus["trial_id"],
        "reviewer_id": reviewer_id,
        "query_id": stimulus["query_id"],
        "sparql_version": stimulus["sparql_version"],
        "sparql_digest": stimulus["sparql_digest"],
        "literal": {key: stimulus["literal"][key] for key in ("formulation_id", "version", "digest")},
        "display_order": list(display_order),
        "displayed_formulations": [
            {key: item[key] for key in ("formulation_id", "version", "digest")}
            for item in stimulus["candidates"]
        ],
        "presentation_arity": 3,
        "outcome": outcome,
        "started_at": started_at,
        "completed_at": completed_at,
        "submitted_at": completed_at,
        "task_design_version": TASK_DESIGN_VERSION,
    }
    if outcome == "rated":
        if not isinstance(ratings, dict) or set(ratings) != set(candidate_ids):
            raise ValueError("Rated trials require both frozen candidates")
        dimensions = {"naturalness", "pragmatism", "interpretation_room"}
        for candidate_ratings in ratings.values():
            if not isinstance(candidate_ratings, dict) or set(candidate_ratings) != dimensions:
                raise ValueError("Rated trials require all three dimensions")
            for value in candidate_ratings.values():
                if not isinstance(value, int) or isinstance(value, bool) or not -100 <= value <= 100:
                    raise ValueError("Ratings must be integers from -100 to 100")
        record["ratings"] = ratings
    elif ratings is not None:
        raise ValueError("Non-rating outcomes cannot retain linguistic ratings")
    if outcome == "cannot_assess":
        if reason:
            record["reason"] = reason
        if comment:
            record["comment"] = comment
    if outcome == "literal_inaccurate":
        if proposed_literal:
            record["proposed_literal"] = proposed_literal
        if comment:
            record["comment"] = comment
    return record
