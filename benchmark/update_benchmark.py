#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from build_benchmark import (
    has_query_specific_evidence,
    read_json,
    read_review_bundle,
    source_evidence_types,
    write_json,
    write_jsonl,
)


def read_jsonl(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    records: List[Dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        item = json.loads(line)
        if isinstance(item, dict):
            records.append(item)
    return records


def pair_key(record: Dict[str, Any]) -> Tuple[str, str]:
    return (str(record.get("kg_id") or ""), str(record.get("query_id") or ""))


def sort_records(records: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return sorted(records, key=lambda rec: (str(rec.get("kg_id") or ""), str(rec.get("query_label") or "")))


def make_benchmark_record(
    *,
    record: Dict[str, Any],
    review: Dict[str, Any],
    review_id: str,
    review_path: Path,
    dataset_id: str,
    current_run: Dict[str, Any],
) -> Dict[str, Any]:
    evidence = record.get("input", {}).get("evidence", [])
    if not isinstance(evidence, list):
        evidence = []
    evidence_types = source_evidence_types(evidence)
    preferred = str(review.get("preferred_question") or "").strip()
    model_question = str(record.get("output", {}).get("nl_question") or "").strip()
    gold_question = preferred or model_question
    gold_source = "reviewer_rewrite" if preferred else "approved_model_output"
    status = str(review.get("status") or "")

    return {
        "benchmark_id": review_id,
        "kg_id": record.get("kg_id"),
        "query_id": record.get("query_id"),
        "query_label": record.get("query_label"),
        "sparql": record.get("input", {}).get("sparql_clean"),
        "gold_question": gold_question,
        "gold_question_source": gold_source,
        "review_status": status,
        "review": {
            "review_id": review_id,
            "review_export": str(review_path),
            "dataset_id": dataset_id,
            "run_id": current_run.get("run_id") or record.get("run_id"),
            "note": review.get("note") or "",
            "updated_at": review.get("updated_at"),
            "copied_from_review_id": review.get("copied_from_review_id"),
        },
        "run": {
            "run_id": record.get("run_id"),
            "run_manifest": record.get("run_manifest"),
            "run_label": record.get("run_label"),
            "source_file": record.get("source_file"),
            "model": record.get("output_meta", {}).get("model"),
            "run_signature": record.get("output_meta", {}).get("run_signature"),
        },
        "model_output": {
            "nl_question": model_question,
            "origin_mode": record.get("output", {}).get("nl_question_origin", {}).get("mode"),
            "confidence": record.get("output", {}).get("confidence"),
            "confidence_rationale": record.get("output", {}).get("confidence_rationale"),
            "needs_review": record.get("output", {}).get("needs_review"),
            "retained_evidence_phrases": record.get("output", {}).get("ranked_evidence_phrases", []),
        },
        "evidence_summary": {
            "evidence_count": len(evidence),
            "evidence_types": evidence_types,
            "has_source_evidence": bool(evidence_types),
            "has_query_specific_evidence": has_query_specific_evidence(evidence_types),
        },
    }


def current_record(pair: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    current = pair.get("current")
    if not isinstance(current, dict):
        return None
    record = current.get("record")
    return record if isinstance(record, dict) else None


def current_review_id(pair: Dict[str, Any]) -> str:
    current = pair.get("current")
    if isinstance(current, dict) and current.get("review_id"):
        return str(current.get("review_id"))
    return str(pair.get("pair_id") or "")


def main() -> None:
    parser = argparse.ArgumentParser(description="Apply compare-review decisions to a previous benchmark snapshot.")
    parser.add_argument("--previous-benchmark", required=True, help="Previous benchmark/vN directory.")
    parser.add_argument("--bundle", default="review/review_data.js", help="Compare review bundle.")
    parser.add_argument("--reviews", required=True, help="Exported compare-review decisions.")
    parser.add_argument("--outdir", required=True, help="Output benchmark/vN directory.")
    args = parser.parse_args()

    previous_dir = Path(args.previous_benchmark)
    bundle_path = Path(args.bundle)
    review_path = Path(args.reviews)
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    bundle = read_review_bundle(bundle_path)
    if bundle.get("mode") != "compare":
        raise ValueError("Benchmark update requires a compare-mode review bundle.")
    review_export = read_json(review_path)
    if review_export.get("mode") != "compare":
        raise ValueError("Benchmark update requires a compare-mode review export.")
    if bundle.get("dataset_id") and review_export.get("dataset_id") and bundle.get("dataset_id") != review_export.get("dataset_id"):
        raise ValueError(
            f"Dataset mismatch: bundle has {bundle.get('dataset_id')}, review export has {review_export.get('dataset_id')}"
        )

    reviews = review_export.get("reviews")
    if not isinstance(reviews, dict):
        raise ValueError("Review export missing reviews object")
    pairs = bundle.get("records")
    if not isinstance(pairs, list):
        raise ValueError("Compare bundle missing records list")

    approved_by_key = {pair_key(rec): rec for rec in read_jsonl(previous_dir / "benchmark.jsonl")}
    pending_by_key = {pair_key(rec): rec for rec in read_jsonl(previous_dir / "pending.jsonl")}
    dismissed_by_key = {pair_key(rec): rec for rec in read_jsonl(previous_dir / "dismissed.jsonl")}
    current_run = bundle.get("current_run") if isinstance(bundle.get("current_run"), dict) else {}
    dataset_id = str(review_export.get("dataset_id") or bundle.get("dataset_id") or "")
    status_counts: Counter[str] = Counter()
    applied = 0

    for pair in pairs:
        if not isinstance(pair, dict):
            continue
        review_id = current_review_id(pair)
        review = reviews.get(review_id)
        if not isinstance(review, dict):
            continue
        status = str(review.get("status") or "")
        if not status:
            continue
        record = current_record(pair)
        key = (str(pair.get("kg_id") or ""), str(pair.get("query_id") or ""))
        approved_by_key.pop(key, None)
        pending_by_key.pop(key, None)
        dismissed_by_key.pop(key, None)
        status_counts[status] += 1
        applied += 1

        if record is None:
            if status == "dismiss":
                dismissed_by_key[key] = {
                    "benchmark_id": review_id,
                    "kg_id": pair.get("kg_id"),
                    "query_id": pair.get("query_id"),
                    "query_label": pair.get("query_label"),
                    "review_status": status,
                    "review": {
                        "review_id": review_id,
                        "review_export": str(review_path),
                        "dataset_id": dataset_id,
                        "note": review.get("note") or "",
                        "updated_at": review.get("updated_at"),
                    },
                    "pair_status": pair.get("pair_status"),
                }
            continue

        next_record = make_benchmark_record(
            record=record,
            review=review,
            review_id=review_id,
            review_path=review_path,
            dataset_id=dataset_id,
            current_run=current_run,
        )
        if status == "approve":
            approved_by_key[key] = next_record
        elif status == "dismiss":
            dismissed_by_key[key] = next_record
        else:
            pending_by_key[key] = next_record

    approved = sort_records(approved_by_key.values())
    pending = sort_records(pending_by_key.values())
    dismissed = sort_records(dismissed_by_key.values())
    previous_manifest = read_json(previous_dir / "manifest.json") if (previous_dir / "manifest.json").exists() else {}

    manifest = {
        "benchmark_version": outdir.name,
        "built_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "update_type": "compare_review_update",
        "previous_benchmark": str(previous_dir),
        "previous_benchmark_version": previous_manifest.get("benchmark_version"),
        "source_bundle": str(bundle_path),
        "source_review_export": str(review_path),
        "dataset_id": dataset_id,
        "previous_run": bundle.get("previous_run"),
        "current_run": bundle.get("current_run"),
        "counts": {
            "approved": len(approved),
            "pending": len(pending),
            "dismissed": len(dismissed),
            "applied_compare_reviews": applied,
            "applied_status_counts": dict(status_counts),
        },
        "files": {
            "benchmark": "benchmark.jsonl",
            "pending": "pending.jsonl",
            "dismissed": "dismissed.jsonl",
        },
        "gold_question_policy": {
            "preferred_question_used_when_present": True,
            "approved_model_output_used_otherwise": True,
            "unchanged_previous_items_carried_forward": True,
        },
    }

    write_json(outdir / "manifest.json", manifest)
    write_jsonl(outdir / "benchmark.jsonl", approved)
    write_jsonl(outdir / "pending.jsonl", pending)
    write_jsonl(outdir / "dismissed.jsonl", dismissed)

    print(f"Wrote manifest to {outdir / 'manifest.json'}")
    print(f"Wrote {len(approved)} approved records to {outdir / 'benchmark.jsonl'}")
    print(f"Wrote {len(pending)} pending records to {outdir / 'pending.jsonl'}")
    print(f"Wrote {len(dismissed)} dismissed records to {outdir / 'dismissed.jsonl'}")
    print(f"Applied {applied} compare-review decisions")


if __name__ == "__main__":
    main()
