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


def run_metadata(record: Dict[str, Any]) -> Dict[str, Any]:
    output_meta = record.get("output_meta", {})
    if not isinstance(output_meta, dict):
        output_meta = {}
    request_config = output_meta.get("request_config")
    response_metadata = output_meta.get("response_metadata")
    meta = {
        "run_id": record.get("run_id"),
        "generation_run_id": record.get("generation_run_id") or record.get("run_id"),
        "run_manifest": record.get("run_manifest"),
        "run_label": record.get("run_label"),
        "source_file": record.get("source_file"),
        "model": output_meta.get("model"),
        "run_signature": output_meta.get("run_signature"),
    }
    if output_meta.get("requested_model") is not None:
        meta["requested_model"] = output_meta.get("requested_model")
    if output_meta.get("response_model") is not None:
        meta["response_model"] = output_meta.get("response_model")
    if output_meta.get("generation_parameters") is not None:
        meta["generation_parameters"] = output_meta.get("generation_parameters")
    if isinstance(request_config, dict):
        meta["request_config"] = request_config
    if isinstance(response_metadata, dict):
        meta["response_metadata"] = response_metadata
    return meta


def benchmark_gold_records(
    *,
    approved: List[Dict[str, Any]],
    pending: List[Dict[str, Any]],
    benchmark_version: str,
    built_at: str,
    source_bundle: str,
    source_review_export: str,
    dataset_id: str,
) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    for status_group, source_records in (("approved", approved), ("pending", pending)):
        for rec in source_records:
            gold_question = str(rec.get("gold_question") or "").strip()
            if not gold_question:
                continue
            if status_group == "pending" and rec.get("gold_question_source") != "reviewer_rewrite":
                continue
            records.append(
                {
                    "benchmark_version": benchmark_version,
                    "benchmark_built_at": built_at,
                    "benchmark_status_group": status_group,
                    "benchmark_id": rec.get("benchmark_id"),
                    "kg_id": rec.get("kg_id"),
                    "query_id": rec.get("query_id"),
                    "query_label": rec.get("query_label"),
                    "sparql": rec.get("sparql"),
                    "gold_question": gold_question,
                    "gold_question_source": rec.get("gold_question_source"),
                    "review_status": rec.get("review_status"),
                    "review": rec.get("review"),
                    "run": rec.get("run"),
                    "evidence_summary": rec.get("evidence_summary"),
                    "source_bundle": source_bundle,
                    "source_review_export": source_review_export,
                    "dataset_id": dataset_id,
                }
            )
    return sorted(records, key=lambda rec: (str(rec.get("kg_id")), str(rec.get("query_label"))))


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
    bundle_run_ids = [str(run_id) for run_id in bundle.get("run_ids", []) if str(run_id)]
    review_run_id = str(review_export.get("run_id") or "")
    if bundle_dataset_id and review_dataset_id and bundle_dataset_id != review_dataset_id:
        raise ValueError(
            f"Dataset mismatch: bundle has {bundle_dataset_id}, review export has {review_dataset_id}"
        )
    if review_run_id and bundle_run_ids and review_run_id not in bundle_run_ids:
        raise ValueError(f"Run mismatch: review export points to {review_run_id}, bundle has {bundle_run_ids}")

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
                "run_id": review_run_id or record.get("run_id"),
                "generation_run_id": review_run_id or record.get("generation_run_id") or record.get("run_id"),
                "note": review.get("note") or "",
                "updated_at": review.get("updated_at"),
            },
            "run": {
                **run_metadata(record),
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

    built_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    benchmark_version = outdir.name
    dataset_id = review_dataset_id or bundle_dataset_id
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
        "source_bundle": str(bundle_path),
        "source_review_export": str(review_path),
        "dataset_id": dataset_id,
        "run_id": review_run_id or (bundle_run_ids[0] if len(bundle_run_ids) == 1 else None),
        "counts": {
            "benchmark": len(benchmark_records),
            "approved": len(approved),
            "pending": len(pending),
            "dismissed": len(dismissed),
            "reviewed_total": sum(status_counts.values()),
            "status_counts": dict(status_counts),
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


if __name__ == "__main__":
    main()
