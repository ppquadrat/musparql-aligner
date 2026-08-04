#!/usr/bin/env python3
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

from build_benchmark import (
    ALTERNATIVES_FILE,
    INCLUDED_FILE,
    LINGUISTIC_ANNOTATIONS_FILE,
    add_interpretive_annotation,
    add_formulation,
    alternatives_record_has_content,
    assert_no_private_reviews,
    assert_non_holdout_export,
    benchmark_disposition,
    benchmark_gold_records,
    literal_wording,
    make_rephrasing_entry,
    pipeline_assessment,
    read_json,
    read_review_bundle,
    sort_sidecar_records,
    update_sidecar_identity,
    write_json,
    write_jsonl,
)
from update_benchmark import (
    included_records_path,
    make_benchmark_record,
    pair_key,
    read_jsonl,
)


def sort_records(records: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return sorted(records, key=lambda rec: (str(rec.get("kg_id") or ""), str(rec.get("query_label") or "")))


def source_run(bundle: Dict[str, Any]) -> Dict[str, Any]:
    runs = bundle.get("runs")
    if isinstance(runs, list) and len(runs) == 1 and isinstance(runs[0], dict):
        return runs[0]
    current_run = bundle.get("current_run")
    if isinstance(current_run, dict):
        return current_run
    return {}


def main() -> None:
    parser = argparse.ArgumentParser(description="Add initial-review decisions to an existing benchmark snapshot.")
    parser.add_argument("--previous-benchmark", required=True, help="Previous benchmark/vN directory.")
    parser.add_argument("--bundle", default="review/review_data.js", help="Normal review bundle.")
    parser.add_argument("--reviews", required=True, help="Exported initial-review decisions.")
    parser.add_argument("--outdir", required=True, help="Output benchmark/vN directory.")
    args = parser.parse_args()

    previous_dir = Path(args.previous_benchmark)
    bundle_path = Path(args.bundle)
    review_path = Path(args.reviews)
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    bundle = read_review_bundle(bundle_path)
    if bundle.get("mode") == "compare":
        raise ValueError("Initial-review update requires an initial-review bundle, not comparative mode.")
    review_export = read_json(review_path)
    assert_non_holdout_export(review_export)
    if review_export.get("mode") == "compare":
        raise ValueError("Initial-review update requires an initial-review export, not comparative mode.")
    if bundle.get("dataset_id") and review_export.get("dataset_id") and bundle.get("dataset_id") != review_export.get("dataset_id"):
        raise ValueError(
            f"Dataset mismatch: bundle has {bundle.get('dataset_id')}, review export has {review_export.get('dataset_id')}"
        )

    reviews = review_export.get("reviews")
    if not isinstance(reviews, dict):
        raise ValueError("Review export missing reviews object")
    assert_no_private_reviews(reviews)
    bundle_records = bundle.get("records")
    if not isinstance(bundle_records, list):
        raise ValueError("Review bundle missing records list")
    records_by_review_id = {
        str(record.get("review_id") or ""): record
        for record in bundle_records
        if isinstance(record, dict) and record.get("review_id")
    }

    included_by_key = {pair_key(rec): rec for rec in read_jsonl(included_records_path(previous_dir))}
    dismissed_by_key = {pair_key(rec): rec for rec in read_jsonl(previous_dir / "dismissed.jsonl")}
    alternatives_by_key = {pair_key(rec): rec for rec in read_jsonl(previous_dir / ALTERNATIVES_FILE)}
    annotations_by_key = {pair_key(rec): rec for rec in read_jsonl(previous_dir / LINGUISTIC_ANNOTATIONS_FILE)}
    existing_keys = set(included_by_key) | set(dismissed_by_key)

    current_run = source_run(bundle)
    dataset_id = str(review_export.get("dataset_id") or bundle.get("dataset_id") or "")
    assessment_counts: Counter[str] = Counter()
    disposition_counts: Counter[str] = Counter()
    applied = 0
    missing_records: List[str] = []
    overlaps: List[Tuple[str, Tuple[str, str]]] = []

    for review_id, review in reviews.items():
        if not isinstance(review, dict):
            continue
        disposition = benchmark_disposition(review)
        assessment = pipeline_assessment(review)
        if not disposition:
            continue
        record = records_by_review_id.get(str(review_id))
        if record is None:
            missing_records.append(str(review_id))
            continue
        key = pair_key(record)
        if key in existing_keys:
            overlaps.append((str(review_id), key))
            continue

        next_record = make_benchmark_record(
            record=record,
            review=review,
            review_id=str(review_id),
            review_path=review_path,
            dataset_id=dataset_id,
            current_run=current_run,
        )
        if disposition == "excluded":
            dismissed_by_key[key] = next_record
        else:
            included_by_key[key] = next_record

        if disposition == "included":
            alternatives: Dict[str, Any] = {}
            annotations: Dict[str, Any] = {}
            add_interpretive_annotation(
                annotations,
                review=review,
                review_id=str(review_id),
                review_path=review_path,
                dataset_id=dataset_id,
                record=record,
            )
            if assessment == "accepted":
                preferred = str(review.get("preferred_question") or "").strip()
                model_question = str(record.get("output", {}).get("nl_question") or "").strip()
                if preferred and preferred.casefold() != model_question.casefold():
                    add_formulation(
                        alternatives,
                        "accepted_alternatives",
                        make_rephrasing_entry(
                            text=model_question,
                            source_type="model_output",
                            review=review,
                            review_id=str(review_id),
                            review_path=review_path,
                            dataset_id=dataset_id,
                            record=record,
                        ),
                    )
            add_formulation(
                alternatives,
                "literal_formulations",
                make_rephrasing_entry(
                    text=literal_wording(review),
                    source_type="literal_sparql_wording",
                    review=review,
                    review_id=str(review_id),
                    review_path=review_path,
                    dataset_id=dataset_id,
                    record=record,
                ),
            )
            update_sidecar_identity(
                alternatives,
                benchmark_record=next_record,
                benchmark_version=outdir.name,
                built_at="",
            )
            update_sidecar_identity(
                annotations,
                benchmark_record=next_record,
                benchmark_version=outdir.name,
                built_at="",
            )
            if alternatives_record_has_content(alternatives):
                alternatives_by_key[key] = alternatives
            if annotations.get("interpretive_annotations"):
                annotations_by_key[key] = annotations

        disposition_counts[disposition] += 1
        if assessment:
            assessment_counts[assessment] += 1
        applied += 1

    if missing_records:
        raise ValueError(f"{len(missing_records)} reviewed records were not present in the bundle: {missing_records[:5]}")
    if overlaps:
        examples = ", ".join(f"{review_id} -> {key[0]}/{key[1]}" for review_id, key in overlaps[:5])
        raise ValueError(
            "Normal-review update encountered already-reviewed pairs. "
            "Adjudicate or use a conflict-aware merge before updating. "
            f"Examples: {examples}"
        )

    included = sort_records(included_by_key.values())
    dismissed = sort_records(dismissed_by_key.values())
    previous_manifest = read_json(previous_dir / "manifest.json") if (previous_dir / "manifest.json").exists() else {}

    built_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    benchmark_version = outdir.name
    alternatives_records = sort_sidecar_records(list(alternatives_by_key.values()))
    linguistic_annotation_records = sort_sidecar_records(list(annotations_by_key.values()))
    for sidecar in alternatives_records + linguistic_annotation_records:
        sidecar["benchmark_version"] = benchmark_version
        sidecar["benchmark_built_at"] = built_at
    benchmark_records = benchmark_gold_records(
        included=included,
        benchmark_version=benchmark_version,
        built_at=built_at,
    )

    manifest = {
        "benchmark_version": benchmark_version,
        "built_at": built_at,
        "update_type": "normal_review_additive_update",
        "previous_benchmark": str(previous_dir),
        "previous_benchmark_version": previous_manifest.get("benchmark_version"),
        "source_bundle": str(bundle_path),
        "source_review_export": str(review_path),
        "dataset_id": dataset_id,
        "current_run": current_run,
        "counts": {
            "benchmark": len(benchmark_records),
            "included": len(included),
            "dismissed": len(dismissed),
            "alternatives": len(alternatives_records),
            "linguistic_annotations": len(linguistic_annotation_records),
            "pipeline_assessment_counts": dict(Counter(str(row.get("pipeline_assessment")) for row in included)),
            "benchmark_disposition_counts": dict(Counter(str(row.get("benchmark_disposition")) for row in included + dismissed)),
            "applied_normal_reviews": applied,
            "applied_pipeline_assessment_counts": dict(assessment_counts),
            "applied_benchmark_disposition_counts": dict(disposition_counts),
            "overlapping_reviewed_pairs": 0,
        },
        "files": {
            "benchmark": "benchmark.jsonl",
            "alternatives": ALTERNATIVES_FILE,
        },
        "working_files": {
            "included": INCLUDED_FILE,
            "dismissed": "dismissed.jsonl",
            "linguistic_annotations_internal": LINGUISTIC_ANNOTATIONS_FILE,
        },
        "gold_question_policy": {
            "benchmark_contains_all_human_confirmed_included_pairs": True,
            "preferred_question_used_when_present": True,
            "approved_model_output_used_otherwise": True,
            "unchanged_previous_items_carried_forward": True,
        },
        "conflict_policy": {
            "overlapping_normal_reviews": "fail_until_adjudicated",
        },
        "release_boundary": {
            "public_release_files": ["manifest.json", "benchmark.jsonl", ALTERNATIVES_FILE],
            "internal_only_files": [INCLUDED_FILE, LINGUISTIC_ANNOTATIONS_FILE, "dismissed.jsonl"],
        },
    }

    write_json(outdir / "manifest.json", manifest)
    write_jsonl(outdir / "benchmark.jsonl", benchmark_records)
    write_jsonl(outdir / INCLUDED_FILE, included)
    write_jsonl(outdir / "dismissed.jsonl", dismissed)
    write_jsonl(outdir / ALTERNATIVES_FILE, alternatives_records)
    write_jsonl(outdir / LINGUISTIC_ANNOTATIONS_FILE, linguistic_annotation_records)

    print(f"Wrote manifest to {outdir / 'manifest.json'}")
    print(f"Wrote {len(benchmark_records)} benchmark records to {outdir / 'benchmark.jsonl'}")
    print(f"Applied {applied} initial-review decisions")


if __name__ == "__main__":
    main()
