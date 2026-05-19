#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from build_benchmark import (
    benchmark_gold_records,
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


def approved_records_path(benchmark_dir: Path) -> Path:
    preferred = benchmark_dir / "approved.jsonl"
    if preferred.exists():
        return preferred
    return benchmark_dir / "benchmark.jsonl"


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
            "generation_run_id": current_run.get("generation_run_id") or current_run.get("run_id") or record.get("generation_run_id") or record.get("run_id"),
            "note": review.get("note") or "",
            "updated_at": review.get("updated_at"),
            "copied_from_review_id": review.get("copied_from_review_id"),
        },
        "run": {
            "run_id": record.get("run_id"),
            "generation_run_id": record.get("generation_run_id") or record.get("run_id"),
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


def previous_record(pair: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    previous = pair.get("previous")
    if not isinstance(previous, dict):
        return None
    record = previous.get("record")
    return record if isinstance(record, dict) else None


def record_pair_key(record: Dict[str, Any]) -> Tuple[str, str]:
    return (str(record.get("kg_id") or ""), str(record.get("query_id") or ""))


def replacement_sparql_key(text: Any) -> str:
    if not isinstance(text, str):
        return ""
    kept: List[str] = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if stripped.lower().startswith("order by"):
            continue
        kept.append(stripped)
    return " ".join(" ".join(kept).split()).lower()


def pop_key_from_all(
    key: Tuple[str, str],
    approved_by_key: Dict[Tuple[str, str], Dict[str, Any]],
    pending_by_key: Dict[Tuple[str, str], Dict[str, Any]],
    dismissed_by_key: Dict[Tuple[str, str], Dict[str, Any]],
) -> bool:
    removed = False
    for records in (approved_by_key, pending_by_key, dismissed_by_key):
        if key in records:
            records.pop(key, None)
            removed = True
    return removed


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

    approved_by_key = {pair_key(rec): rec for rec in read_jsonl(approved_records_path(previous_dir))}
    pending_by_key = {pair_key(rec): rec for rec in read_jsonl(previous_dir / "pending.jsonl")}
    dismissed_by_key = {pair_key(rec): rec for rec in read_jsonl(previous_dir / "dismissed.jsonl")}
    removed_records: List[Dict[str, Any]] = []
    removed_by_label: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
    removed_by_sparql: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
    for pair in pairs:
        if not isinstance(pair, dict) or pair.get("pair_status") != "removed":
            continue
        record = previous_record(pair)
        if record is None:
            continue
        removed_records.append(record)
        label_key = (str(record.get("kg_id") or ""), str(record.get("query_label") or ""))
        removed_by_label.setdefault(label_key, []).append(record)
        sparql_key = replacement_sparql_key(record.get("input", {}).get("sparql_clean"))
        if sparql_key:
            removed_by_sparql.setdefault((str(record.get("kg_id") or ""), sparql_key), []).append(record)
    current_run = bundle.get("current_run") if isinstance(bundle.get("current_run"), dict) else {}
    dataset_id = str(review_export.get("dataset_id") or bundle.get("dataset_id") or "")
    status_counts: Counter[str] = Counter()
    applied = 0
    superseded_removed = 0

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

        if pair.get("pair_status") == "added":
            label_key = (str(record.get("kg_id") or ""), str(record.get("query_label") or ""))
            sparql_key = replacement_sparql_key(record.get("input", {}).get("sparql_clean"))
            candidates = list(removed_by_label.get(label_key, []))
            if sparql_key:
                candidates.extend(removed_by_sparql.get((str(record.get("kg_id") or ""), sparql_key), []))
            seen_candidate_keys = set()
            for candidate in candidates:
                candidate_key = record_pair_key(candidate)
                if candidate_key in seen_candidate_keys:
                    continue
                seen_candidate_keys.add(candidate_key)
                if candidate_key == key:
                    continue
                if pop_key_from_all(candidate_key, approved_by_key, pending_by_key, dismissed_by_key):
                    superseded_removed += 1

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

    built_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    benchmark_version = outdir.name
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
        "update_type": "compare_review_update",
        "previous_benchmark": str(previous_dir),
        "previous_benchmark_version": previous_manifest.get("benchmark_version"),
        "source_bundle": str(bundle_path),
        "source_review_export": str(review_path),
        "dataset_id": dataset_id,
        "previous_run": bundle.get("previous_run"),
        "current_run": bundle.get("current_run"),
        "counts": {
            "benchmark": len(benchmark_records),
            "approved": len(approved),
            "pending": len(pending),
            "dismissed": len(dismissed),
            "applied_compare_reviews": applied,
            "applied_status_counts": dict(status_counts),
            "superseded_removed_previous_items": superseded_removed,
        },
        "files": {
            "benchmark": "benchmark.jsonl",
            "approved": "approved.jsonl",
            "pending": "pending.jsonl",
            "dismissed": "dismissed.jsonl",
        },
        "gold_question_policy": {
            "benchmark_includes_approved": True,
            "benchmark_includes_pending_with_reviewer_rewrite": True,
            "preferred_question_used_when_present": True,
            "approved_model_output_used_otherwise": True,
            "unchanged_previous_items_carried_forward": True,
        },
    }

    write_json(outdir / "manifest.json", manifest)
    write_jsonl(outdir / "benchmark.jsonl", benchmark_records)
    write_jsonl(outdir / "approved.jsonl", approved)
    write_jsonl(outdir / "pending.jsonl", pending)
    write_jsonl(outdir / "dismissed.jsonl", dismissed)

    print(f"Wrote manifest to {outdir / 'manifest.json'}")
    print(f"Wrote {len(benchmark_records)} benchmark records to {outdir / 'benchmark.jsonl'}")
    print(f"Wrote {len(approved)} approved records to {outdir / 'approved.jsonl'}")
    print(f"Wrote {len(pending)} pending records to {outdir / 'pending.jsonl'}")
    print(f"Wrote {len(dismissed)} dismissed records to {outdir / 'dismissed.jsonl'}")
    print(f"Applied {applied} compare-review decisions")


if __name__ == "__main__":
    main()
