#!/usr/bin/env python3
"""Validate benchmark snapshot membership, schema, sidecars, and manifest counts."""
from __future__ import annotations

import argparse
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

from build_benchmark import (
    ALTERNATIVES_FILE,
    BENCHMARK_DISPOSITIONS,
    INCLUDED_FILE,
    LINGUISTIC_ANNOTATIONS_FILE,
    PIPELINE_ASSESSMENTS,
    read_json,
    read_jsonl,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from sparql_versions import resolve_sparql_version, sparql_hash


def key(record: Dict[str, Any]) -> Tuple[str, str]:
    return str(record.get("kg_id") or ""), str(record.get("query_id") or "")


def duplicate_values(records: Iterable[Dict[str, Any]], field: str) -> List[str]:
    counts = Counter(str(record.get(field) or "") for record in records)
    return sorted(value for value, count in counts.items() if not value or count != 1)


def snapshot_number(manifest: Dict[str, Any], snapshot: Path) -> int:
    value = str(manifest.get("benchmark_version") or snapshot.name)
    return int(value[1:]) if value.startswith("v") and value[1:].isdigit() else 0


def audit_version_pins(
    *,
    snapshot: Path,
    manifest: Dict[str, Any],
    partitions: Iterable[Tuple[str, List[Dict[str, Any]]]],
) -> List[str]:
    if snapshot_number(manifest, snapshot) < 8:
        return []
    errors: List[str] = []
    query_path = REPO_ROOT / "kg_queries.jsonl"
    if not query_path.exists():
        return ["cannot resolve v8 SPARQL pins because kg_queries.jsonl is missing"]
    canonical = {
        (str(row.get("kg_id") or ""), str(row.get("query_id") or "")): row
        for row in read_jsonl(query_path)
    }
    latest_required = manifest.get("sparql_version_policy") == "latest_retained"
    included: List[Dict[str, Any]] = []
    for name, records in partitions:
        if name == "included":
            included = records
        for row in records:
            label = str(row.get("query_label") or row.get("benchmark_id") or "")
            version = row.get("sparql_version")
            digest = row.get("sparql_hash")
            text = row.get("sparql")
            if not isinstance(version, int) or not isinstance(digest, str) or not isinstance(text, str):
                errors.append(f"{name} record lacks a complete SPARQL version pin: {label}")
                continue
            try:
                if sparql_hash(text) != digest:
                    errors.append(f"{name} SPARQL text/hash mismatch: {label}")
                    continue
            except ValueError as exc:
                errors.append(f"{name} has invalid SPARQL: {label}: {exc}")
                continue
            query = canonical.get(key(row))
            if query is None:
                errors.append(f"{name} SPARQL pin has no canonical query record: {label}")
                continue
            try:
                resolved = resolve_sparql_version(query, version)
                latest = resolve_sparql_version(query, "latest")
            except ValueError as exc:
                errors.append(f"{name} SPARQL pin does not resolve: {label}: {exc}")
                continue
            if resolved["sparql_hash"] != digest or resolved["sparql"] != text:
                errors.append(f"{name} SPARQL pin differs from the retained version: {label}")
            if latest_required and latest["sparql_version"] != version:
                errors.append(f"{name} does not select the latest retained SPARQL: {label}")

    execution = manifest.get("execution_snapshot")
    if not isinstance(execution, dict):
        errors.append("manifest is missing execution_snapshot")
        return errors
    cutoff = str(execution.get("captured_through") or "")
    statuses: Counter[str] = Counter()
    matched = 0
    for row in included:
        query = canonical.get(key(row))
        if query is None:
            continue
        observations = [
            item
            for item in query.get("execution_history") or []
            if isinstance(item, dict)
            and item.get("sparql_version") == row.get("sparql_version")
            and item.get("sparql_hash") == row.get("sparql_hash")
            and str(item.get("ran_at") or "") <= cutoff
        ]
        if not observations:
            errors.append(f"execution snapshot has no matching observation: {row.get('query_label')}")
            continue
        observation = max(observations, key=lambda item: str(item.get("ran_at") or ""))
        matched += 1
        statuses[str(observation.get("status"))] += 1
    if execution.get("selected_queries") != len(included):
        errors.append("execution_snapshot selected_queries does not match included records")
    if execution.get("selected_versions_with_execution") != matched:
        errors.append("execution_snapshot selected_versions_with_execution is incorrect")
    if execution.get("status_counts") != dict(sorted(statuses.items())):
        errors.append("execution_snapshot status_counts do not match pinned observations")
    return errors


def audit_snapshot(snapshot: Path) -> List[str]:
    errors: List[str] = []
    manifest = read_json(snapshot / "manifest.json")
    benchmark = read_jsonl(snapshot / "benchmark.jsonl")
    included = read_jsonl(snapshot / INCLUDED_FILE)
    dismissed = read_jsonl(snapshot / "dismissed.jsonl")
    holdout = read_jsonl(snapshot / "holdout.jsonl")
    alternatives = read_jsonl(snapshot / ALTERNATIVES_FILE)
    annotations = read_jsonl(snapshot / LINGUISTIC_ANNOTATIONS_FILE)

    for field in ("benchmark_id", "query_id"):
        duplicates = duplicate_values(benchmark, field)
        if duplicates:
            errors.append(f"benchmark has missing or duplicate {field}: {duplicates[:5]}")
    if len(benchmark) != len(included):
        errors.append(f"benchmark/included count mismatch: {len(benchmark)} != {len(included)}")
    if {key(row) for row in benchmark} != {key(row) for row in included}:
        errors.append("benchmark and included pair sets differ")

    all_partition_keys: List[Tuple[str, str]] = []
    for name, records, expected in (
        ("included", included, "included"),
        ("dismissed", dismissed, "excluded"),
        ("holdout", holdout, "withheld"),
    ):
        for row in records:
            disposition = row.get("benchmark_disposition")
            assessment = row.get("pipeline_assessment")
            if disposition != expected or disposition not in BENCHMARK_DISPOSITIONS:
                errors.append(f"{name} record has invalid disposition: {row.get('benchmark_id')}")
            if assessment is not None and assessment not in PIPELINE_ASSESSMENTS:
                errors.append(f"{name} record has invalid assessment: {row.get('benchmark_id')}")
            if name == "included" and assessment is None:
                errors.append(f"included record has no pipeline assessment: {row.get('benchmark_id')}")
            if name == "dismissed" and assessment is not None:
                errors.append(f"excluded record has a pipeline assessment: {row.get('benchmark_id')}")
            if name == "included" and not str(row.get("gold_question") or "").strip():
                errors.append(f"included record has no canonical question: {row.get('benchmark_id')}")
            all_partition_keys.append(key(row))
    if len(all_partition_keys) != len(set(all_partition_keys)):
        errors.append("included, dismissed, and holdout partitions overlap")

    included_by_key = {key(row): row for row in included}
    for row in alternatives:
        included_row = included_by_key.get(key(row))
        if included_row is None:
            errors.append(f"alternative has no included referent: {row.get('benchmark_id')}")
            continue
        if row.get("interpretive_annotations") or row.get("interpretive"):
            errors.append(f"alternative contains linguistic ratings: {row.get('benchmark_id')}")
        for item in row.get("accepted_alternatives", []):
            if not isinstance(item, dict) or item.get("acceptance") != "human_accepted":
                errors.append(f"alternative lacks acceptance provenance: {row.get('benchmark_id')}")

    counts = manifest.get("counts") if isinstance(manifest.get("counts"), dict) else {}
    expected_counts = {
        "benchmark": len(benchmark),
        "included": len(included),
        "dismissed": len(dismissed),
        "holdout": len(holdout),
        "alternatives": len(alternatives),
        "linguistic_annotations": len(annotations),
    }
    for field, actual in expected_counts.items():
        if field not in counts:
            errors.append(f"manifest is missing core count: {field}")
        elif counts[field] != actual:
            errors.append(f"manifest count {field}={counts[field]} but file has {actual}")
    assessment_counts = dict(sorted(Counter(str(row.get("pipeline_assessment")) for row in included).items()))
    disposition_counts = dict(
        sorted(Counter(str(row.get("benchmark_disposition")) for row in included + dismissed + holdout).items())
    )
    if counts.get("pipeline_assessment_counts") != assessment_counts:
        errors.append("manifest pipeline_assessment_counts do not match included records")
    if counts.get("benchmark_disposition_counts") != disposition_counts:
        errors.append("manifest benchmark_disposition_counts do not match snapshot partitions")
    errors.extend(
        audit_version_pins(
            snapshot=snapshot,
            manifest=manifest,
            partitions=(("included", included), ("dismissed", dismissed), ("holdout", holdout)),
        )
    )
    return errors


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("snapshot")
    args = parser.parse_args()
    errors = audit_snapshot(Path(args.snapshot))
    if errors:
        raise SystemExit("Snapshot audit failed:\n- " + "\n- ".join(errors))
    print(f"Snapshot audit passed: {args.snapshot}")


if __name__ == "__main__":
    main()
