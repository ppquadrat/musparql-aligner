#!/usr/bin/env python3
"""Validate benchmark snapshot membership, schema, sidecars, and manifest counts."""
from __future__ import annotations

import argparse
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


def key(record: Dict[str, Any]) -> Tuple[str, str]:
    return str(record.get("kg_id") or ""), str(record.get("query_id") or "")


def duplicate_values(records: Iterable[Dict[str, Any]], field: str) -> List[str]:
    counts = Counter(str(record.get(field) or "") for record in records)
    return sorted(value for value, count in counts.items() if not value or count != 1)


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
