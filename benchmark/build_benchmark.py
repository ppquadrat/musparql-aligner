#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple


def read_review_bundle(path: Path) -> Dict[str, Any]:
    text = path.read_text(encoding="utf-8")
    prefix = "window.REVIEW_DATA = "
    if text.startswith(prefix):
        text = text[len(prefix):]
    text = text.rstrip().rstrip(";")
    data = json.loads(text)
    if not isinstance(data, dict):
        raise ValueError(f"Bad review bundle format: {path}")
    return data


def read_json(path: Path) -> Dict[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"Expected JSON object in {path}")
    return data


def write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def write_jsonl(path: Path, records: List[Dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as fh:
        for rec in records:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")


def source_evidence_types(evidence: List[Dict[str, Any]]) -> List[str]:
    return sorted({str(ev.get("type")) for ev in evidence if isinstance(ev, dict) and ev.get("type")})


def has_query_specific_evidence(evidence_types: List[str]) -> bool:
    return any(t in {"query_comment", "readme_query_desc", "doc_query_desc", "web_query_desc"} for t in evidence_types)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build a benchmark snapshot from a review bundle and reviewer export.")
    parser.add_argument("--bundle", default="review/review_data.js")
    parser.add_argument("--reviews", required=True)
    parser.add_argument("--outdir", required=True)
    args = parser.parse_args()

    bundle_path = Path(args.bundle)
    review_path = Path(args.reviews)
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    bundle = read_review_bundle(bundle_path)
    review_export = read_json(review_path)
    bundle_dataset_id = str(bundle.get("dataset_id") or "")
    review_dataset_id = str(review_export.get("dataset_id") or "")
    if bundle_dataset_id and review_dataset_id and bundle_dataset_id != review_dataset_id:
        raise ValueError(
            f"Dataset mismatch: bundle has {bundle_dataset_id}, review export has {review_dataset_id}"
        )

    bundle_records = bundle.get("records")
    if not isinstance(bundle_records, list):
        raise ValueError("Review bundle missing records list")
    review_map = review_export.get("reviews")
    if not isinstance(review_map, dict):
        raise ValueError("Review export missing reviews object")

    approved: List[Dict[str, Any]] = []
    pending: List[Dict[str, Any]] = []
    dismissed: List[Dict[str, Any]] = []
    status_counts: Counter[str] = Counter()

    for record in bundle_records:
        if not isinstance(record, dict):
            continue
        review_id = str(record.get("review_id") or "")
        review = review_map.get(review_id)
        if not isinstance(review, dict):
            continue
        status = str(review.get("status") or "")
        if not status:
            continue
        status_counts[status] += 1

        evidence = record.get("input", {}).get("evidence", [])
        if not isinstance(evidence, list):
            evidence = []
        evidence_types = source_evidence_types(evidence)
        preferred = str(review.get("preferred_question") or "").strip()
        model_question = str(record.get("output", {}).get("nl_question") or "").strip()
        gold_question = preferred or model_question
        gold_source = "reviewer_rewrite" if preferred else "approved_model_output"

        base = {
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
                "dataset_id": review_dataset_id or bundle_dataset_id,
                "note": review.get("note") or "",
                "updated_at": review.get("updated_at"),
            },
            "run": {
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

        if status == "approve":
            approved.append(base)
        elif status == "dismiss":
            dismissed.append(base)
        else:
            pending.append(base)

    approved.sort(key=lambda rec: (str(rec.get("kg_id")), str(rec.get("query_label"))))
    pending.sort(key=lambda rec: (str(rec.get("kg_id")), str(rec.get("query_label"))))
    dismissed.sort(key=lambda rec: (str(rec.get("kg_id")), str(rec.get("query_label"))))

    manifest = {
        "benchmark_version": outdir.name,
        "built_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "source_bundle": str(bundle_path),
        "source_review_export": str(review_path),
        "dataset_id": review_dataset_id or bundle_dataset_id,
        "counts": {
            "approved": len(approved),
            "pending": len(pending),
            "dismissed": len(dismissed),
            "reviewed_total": sum(status_counts.values()),
            "status_counts": dict(status_counts),
        },
        "files": {
            "benchmark": "benchmark.jsonl",
            "pending": "pending.jsonl",
            "dismissed": "dismissed.jsonl",
        },
        "gold_question_policy": {
            "preferred_question_used_when_present": True,
            "approved_model_output_used_otherwise": True,
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


if __name__ == "__main__":
    main()
