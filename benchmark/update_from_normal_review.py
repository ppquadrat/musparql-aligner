#!/usr/bin/env python3
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

from build_benchmark import (
    AMBIGUITY_FILE,
    add_interpretive_annotation,
    add_rephrasing,
    ambiguity_record_has_content,
    benchmark_gold_records,
    is_private_holdout,
    literal_wording,
    make_rephrasing_entry,
    read_json,
    read_review_bundle,
    sort_ambiguity_records,
    update_ambiguity_identity,
    write_json,
    write_jsonl,
)
from update_benchmark import (
    approved_records_path,
    holdout_records_path,
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
    parser = argparse.ArgumentParser(description="Add normal-review decisions to an existing benchmark snapshot.")
    parser.add_argument("--previous-benchmark", required=True, help="Previous benchmark/vN directory.")
    parser.add_argument("--bundle", default="review/review_data.js", help="Normal review bundle.")
    parser.add_argument("--reviews", required=True, help="Exported normal-review decisions.")
    parser.add_argument("--outdir", required=True, help="Output benchmark/vN directory.")
    args = parser.parse_args()

    previous_dir = Path(args.previous_benchmark)
    bundle_path = Path(args.bundle)
    review_path = Path(args.reviews)
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    bundle = read_review_bundle(bundle_path)
    if bundle.get("mode") == "compare":
        raise ValueError("Normal-review update requires a normal review bundle, not compare mode.")
    review_export = read_json(review_path)
    if review_export.get("mode") == "compare":
        raise ValueError("Normal-review update requires a normal review export, not compare mode.")
    if bundle.get("dataset_id") and review_export.get("dataset_id") and bundle.get("dataset_id") != review_export.get("dataset_id"):
        raise ValueError(
            f"Dataset mismatch: bundle has {bundle.get('dataset_id')}, review export has {review_export.get('dataset_id')}"
        )

    reviews = review_export.get("reviews")
    if not isinstance(reviews, dict):
        raise ValueError("Review export missing reviews object")
    bundle_records = bundle.get("records")
    if not isinstance(bundle_records, list):
        raise ValueError("Review bundle missing records list")
    records_by_review_id = {
        str(record.get("review_id") or ""): record
        for record in bundle_records
        if isinstance(record, dict) and record.get("review_id")
    }

    approved_by_key = {pair_key(rec): rec for rec in read_jsonl(approved_records_path(previous_dir))}
    pending_by_key = {pair_key(rec): rec for rec in read_jsonl(previous_dir / "pending.jsonl")}
    dismissed_by_key = {pair_key(rec): rec for rec in read_jsonl(previous_dir / "dismissed.jsonl")}
    holdout_by_key = {pair_key(rec): rec for rec in read_jsonl(holdout_records_path(previous_dir))}
    ambiguity_by_key = {pair_key(rec): rec for rec in read_jsonl(previous_dir / AMBIGUITY_FILE)}
    existing_keys = set(approved_by_key) | set(pending_by_key) | set(dismissed_by_key) | set(holdout_by_key)

    current_run = source_run(bundle)
    dataset_id = str(review_export.get("dataset_id") or bundle.get("dataset_id") or "")
    status_counts: Counter[str] = Counter()
    applied = 0
    missing_records: List[str] = []
    overlaps: List[Tuple[str, Tuple[str, str]]] = []

    for review_id, review in reviews.items():
        if not isinstance(review, dict):
            continue
        status = str(review.get("status") or "")
        if not status:
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
        if is_private_holdout(review):
            holdout_by_key[key] = next_record
        elif status == "approve":
            approved_by_key[key] = next_record
        elif status == "dismiss":
            dismissed_by_key[key] = next_record
        else:
            pending_by_key[key] = next_record

        if not is_private_holdout(review) and status != "dismiss":
            ambiguity: Dict[str, Any] = {}
            add_interpretive_annotation(
                ambiguity,
                review=review,
                review_id=str(review_id),
                review_path=review_path,
                dataset_id=dataset_id,
                record=record,
            )
            if status == "approve":
                preferred = str(review.get("preferred_question") or "").strip()
                model_question = str(record.get("output", {}).get("nl_question") or "").strip()
                if preferred and preferred.casefold() != model_question.casefold():
                    add_rephrasing(
                        ambiguity,
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
            add_rephrasing(
                ambiguity,
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
            update_ambiguity_identity(
                ambiguity,
                benchmark_record=next_record,
                benchmark_version=outdir.name,
                built_at="",
            )
            if ambiguity_record_has_content(ambiguity):
                ambiguity_by_key[key] = ambiguity

        status_counts[status] += 1
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

    approved = sort_records(approved_by_key.values())
    pending = sort_records(pending_by_key.values())
    dismissed = sort_records(dismissed_by_key.values())
    holdout = sort_records(holdout_by_key.values())
    previous_manifest = read_json(previous_dir / "manifest.json") if (previous_dir / "manifest.json").exists() else {}

    built_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    benchmark_version = outdir.name
    ambiguity_records = sort_ambiguity_records(list(ambiguity_by_key.values()))
    for ambiguity in ambiguity_records:
        ambiguity["benchmark_version"] = benchmark_version
        ambiguity["benchmark_built_at"] = built_at
    benchmark_records = benchmark_gold_records(
        approved=approved,
        pending=pending,
        benchmark_version=benchmark_version,
        built_at=built_at,
        source_bundle=str(bundle_path),
        source_review_export=str(review_path),
        dataset_id=dataset_id,
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
            "approved": len(approved),
            "pending": len(pending),
            "dismissed": len(dismissed),
            "holdout": len(holdout),
            "ambiguity": len(ambiguity_records),
            "applied_normal_reviews": applied,
            "applied_status_counts": dict(status_counts),
            "overlapping_reviewed_pairs": 0,
        },
        "files": {
            "benchmark": "benchmark.jsonl",
            "approved": "approved.jsonl",
            "pending": "pending.jsonl",
            "dismissed": "dismissed.jsonl",
            "holdout": "holdout.jsonl",
            "ambiguity": AMBIGUITY_FILE,
        },
        "gold_question_policy": {
            "benchmark_includes_approved": True,
            "benchmark_includes_pending_with_reviewer_rewrite": True,
            "preferred_question_used_when_present": True,
            "approved_model_output_used_otherwise": True,
            "unchanged_previous_items_carried_forward": True,
        },
        "conflict_policy": {
            "overlapping_normal_reviews": "fail_until_adjudicated",
        },
    }

    write_json(outdir / "manifest.json", manifest)
    write_jsonl(outdir / "benchmark.jsonl", benchmark_records)
    write_jsonl(outdir / "approved.jsonl", approved)
    write_jsonl(outdir / "pending.jsonl", pending)
    write_jsonl(outdir / "dismissed.jsonl", dismissed)
    write_jsonl(outdir / "holdout.jsonl", holdout)
    write_jsonl(outdir / AMBIGUITY_FILE, ambiguity_records)

    print(f"Wrote manifest to {outdir / 'manifest.json'}")
    print(f"Wrote {len(benchmark_records)} benchmark records to {outdir / 'benchmark.jsonl'}")
    print(f"Applied {applied} normal-review decisions")


if __name__ == "__main__":
    main()
