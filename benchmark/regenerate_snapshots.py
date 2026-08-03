#!/usr/bin/env python3
"""Deterministically regenerate scoring files, alternatives, and counts from internal snapshots."""
from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterable, Tuple

from audit_snapshot import audit_snapshot
from build_benchmark import (
    ALTERNATIVES_FILE,
    INCLUDED_FILE,
    LINGUISTIC_ANNOTATIONS_FILE,
    add_formulation,
    alternatives_record_has_content,
    benchmark_gold_records,
    make_rephrasing_entry,
    normalize_rephrasing_text,
    read_json,
    read_jsonl,
    sort_sidecar_records,
    update_sidecar_identity,
    write_json,
    write_jsonl,
)


def pair_key(record: Dict[str, Any]) -> Tuple[str, str]:
    return str(record.get("kg_id") or ""), str(record.get("query_id") or "")


def sort_records(records: Iterable[Dict[str, Any]]) -> list[Dict[str, Any]]:
    return sorted(records, key=lambda row: (str(row.get("kg_id") or ""), str(row.get("query_label") or "")))


def add_accepted_model_alternative(sidecar: Dict[str, Any], record: Dict[str, Any]) -> None:
    if record.get("pipeline_assessment") != "accepted":
        return
    model_question = str(record.get("model_output", {}).get("nl_question") or "").strip()
    canonical = str(record.get("gold_question") or "").strip()
    if not model_question or normalize_rephrasing_text(model_question) == normalize_rephrasing_text(canonical):
        return
    review = record.get("review") if isinstance(record.get("review"), dict) else {}
    run = record.get("run") if isinstance(record.get("run"), dict) else {}
    entry = make_rephrasing_entry(
        text=model_question,
        source_type="model_output",
        review=review,
        review_id=str(review.get("review_id") or record.get("benchmark_id") or ""),
        review_path=None,
        dataset_id=str(review.get("dataset_id") or ""),
        record={"generation_run_id": run.get("generation_run_id") or run.get("run_id")},
    )
    if entry and run.get("model"):
        entry["model"] = run.get("model")
    add_formulation(sidecar, "accepted_alternatives", entry)


def regenerate_snapshot(snapshot: Path) -> Dict[str, Any]:
    version = snapshot.name
    manifest = read_json(snapshot / "manifest.json")
    built_at = str(manifest.get("built_at") or "")
    included = sort_records(read_jsonl(snapshot / INCLUDED_FILE))
    dismissed = sort_records(read_jsonl(snapshot / "dismissed.jsonl"))
    holdout = sort_records(read_jsonl(snapshot / "holdout.jsonl"))
    annotations = read_jsonl(snapshot / LINGUISTIC_ANNOTATIONS_FILE)
    alternatives_by_key = {pair_key(row): row for row in read_jsonl(snapshot / ALTERNATIVES_FILE)}

    for record in included:
        sidecar = alternatives_by_key.get(pair_key(record), {})
        for entry in sidecar.get("accepted_alternatives", []):
            if isinstance(entry, dict):
                entry["acceptance"] = "human_accepted"
        add_accepted_model_alternative(sidecar, record)
        if alternatives_record_has_content(sidecar):
            update_sidecar_identity(
                sidecar,
                benchmark_record=record,
                benchmark_version=version,
                built_at=built_at,
            )
            alternatives_by_key[pair_key(record)] = sidecar
    alternatives = sort_sidecar_records(list(alternatives_by_key.values()))
    benchmark = benchmark_gold_records(
        included=included,
        benchmark_version=version,
        built_at=built_at,
    )

    assessment_counts = Counter(str(row.get("pipeline_assessment") or "none") for row in included)
    disposition_counts = Counter(
        str(row.get("benchmark_disposition")) for row in included + dismissed + holdout
    )
    counts = manifest.setdefault("counts", {})
    counts.update(
        {
            "benchmark": len(benchmark),
            "included": len(included),
            "dismissed": len(dismissed),
            "holdout": len(holdout),
            "alternatives": len(alternatives),
            "linguistic_annotations": len(annotations),
            "pipeline_assessment_counts": dict(sorted(assessment_counts.items())),
            "benchmark_disposition_counts": dict(sorted(disposition_counts.items())),
        }
    )
    applied_assessments = counts.get("applied_pipeline_assessment_counts")
    if isinstance(applied_assessments, dict):
        cleaned_applied_assessments = {
            key: value
            for key, value in applied_assessments.items()
            if key in {"accepted", "prompt_improvement_recommended", "input_data_improvement_recommended", "not_applicable"}
        }
        counts["applied_pipeline_assessment_counts"] = cleaned_applied_assessments
        applied_total = counts.get("applied_compare_reviews", counts.get("applied_normal_reviews"))
        if isinstance(applied_total, int):
            included_applied = sum(cleaned_applied_assessments.values())
            applied_dispositions = {"included": included_applied}
            if applied_total > included_applied:
                applied_dispositions["excluded"] = applied_total - included_applied
            counts["applied_benchmark_disposition_counts"] = applied_dispositions
    manifest["files"] = {
        "benchmark": "benchmark.jsonl",
        "included": INCLUDED_FILE,
        "dismissed": "dismissed.jsonl",
        "holdout": "holdout.jsonl",
        "alternatives": ALTERNATIVES_FILE,
        "linguistic_annotations_internal": LINGUISTIC_ANNOTATIONS_FILE,
    }
    manifest["release_boundary"] = {
        "public_release_builder": "benchmark/build_public_release.py",
        "working_snapshot_is_not_a_release_archive": True,
        "public_release_files": ["manifest.json", "benchmark.jsonl", ALTERNATIVES_FILE],
        "internal_only_files": [INCLUDED_FILE, LINGUISTIC_ANNOTATIONS_FILE, "dismissed.jsonl", "holdout.jsonl"],
    }

    write_json(snapshot / "manifest.json", manifest)
    write_jsonl(snapshot / "benchmark.jsonl", benchmark)
    write_jsonl(snapshot / ALTERNATIVES_FILE, alternatives)
    errors = audit_snapshot(snapshot)
    if errors:
        raise ValueError(f"{version} failed after regeneration: {errors}")
    return {"version": version, "included": len(included), "alternatives": len(alternatives)}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--benchmark-root", default="benchmark")
    args = parser.parse_args()
    root = Path(args.benchmark_root)
    for snapshot in sorted(root.glob("v[0-9]*"), key=lambda path: int(path.name[1:])):
        print(json.dumps(regenerate_snapshot(snapshot), sort_keys=True))


if __name__ == "__main__":
    main()
