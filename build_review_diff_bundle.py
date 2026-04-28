#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from build_review_bundle import (
    build_input_index,
    infer_run_manifest,
    load_json,
    load_json_records,
    sha256_text,
    signature_token,
    stable_json_dumps,
)


PairKey = Tuple[str, str]


def pair_key(record: Dict[str, Any]) -> PairKey:
    return (str(record.get("kg_id") or ""), str(record.get("query_id") or ""))


def review_id_for(record: Dict[str, Any], idx: int) -> str:
    kg_id = str(record.get("kg_id") or "")
    query_label = str(record.get("query_label") or "")
    return f"{kg_id}::{query_label}::{signature_token(record, idx)}"


def pair_id_for(key: PairKey) -> str:
    return f"{key[0]}::{key[1]}"


def infer_run_file(output_path: Path, filename: str) -> Optional[Path]:
    candidate = output_path.parent / filename
    return candidate if candidate.exists() else None


def run_summary(output_path: Path, explicit_manifest: Optional[Path]) -> Dict[str, Any]:
    manifest_path = infer_run_manifest(output_path, explicit_manifest)
    manifest: Dict[str, Any] = {}
    if manifest_path is not None:
        manifest = load_json(manifest_path)
    return {
        "run_id": manifest.get("run_id") or output_path.parent.name,
        "run_label": output_path.parent.name if output_path.parent.name != "." else output_path.stem,
        "output_path": str(output_path),
        "manifest_path": str(manifest_path) if manifest_path is not None else None,
        "purpose": manifest.get("purpose"),
        "created_at": manifest.get("created_at"),
    }


def load_reviews(path: Optional[Path]) -> Dict[str, Any]:
    if path is None:
        return {}
    payload = load_json(path)
    reviews = payload.get("reviews")
    if not isinstance(reviews, dict):
        raise ValueError(f"Review export has no reviews object: {path}")
    return reviews


def record_payload(
    output_record: Dict[str, Any],
    input_index: Dict[Tuple[str, str, str], Dict[str, Any]],
    review_id: str,
    run: Dict[str, Any],
) -> Dict[str, Any]:
    kg_id = str(output_record.get("kg_id") or "")
    query_id = str(output_record.get("query_id") or "")
    query_label = str(output_record.get("query_label") or "")
    source_input = input_index.get((kg_id, query_id, query_label), {})
    return {
        "review_id": review_id,
        "run_id": run.get("run_id"),
        "run_label": run.get("run_label"),
        "source_file": run.get("output_path"),
        "run_manifest": run.get("manifest_path"),
        "kg_id": kg_id,
        "query_id": query_id,
        "query_label": query_label,
        "input": {
            "sparql_clean": source_input.get("sparql_clean"),
            "schema_ref": source_input.get("schema_ref"),
            "evidence": source_input.get("evidence", []),
        },
        "output": output_record.get("llm_output"),
        "output_meta": {
            "model": output_record.get("model"),
            "elapsed_ms": output_record.get("elapsed_ms"),
            "generated_at": output_record.get("generated_at"),
            "run_signature": output_record.get("run_signature"),
        },
    }


def index_outputs(records: Iterable[Dict[str, Any]]) -> Dict[PairKey, Tuple[int, Dict[str, Any]]]:
    indexed: Dict[PairKey, Tuple[int, Dict[str, Any]]] = {}
    for idx, rec in enumerate(records, start=1):
        indexed[pair_key(rec)] = (idx, rec)
    return indexed


def comparable_record(record: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not record:
        return {}
    return {
        "sparql": record.get("input", {}).get("sparql_clean"),
        "evidence": record.get("input", {}).get("evidence", []),
        "output": record.get("output"),
        "model": record.get("output_meta", {}).get("model"),
    }


def evidence_signature(record: Optional[Dict[str, Any]]) -> Dict[str, str]:
    evidence = (record or {}).get("input", {}).get("evidence", [])
    result: Dict[str, str] = {}
    for idx, item in enumerate(evidence):
        evidence_id = str(item.get("evidence_id") or f"idx-{idx}")
        result[evidence_id] = stable_json_dumps(item)
    return result


def ranked_signature(record: Optional[Dict[str, Any]]) -> str:
    output = (record or {}).get("output") or {}
    return stable_json_dumps(output.get("ranked_evidence_phrases") or [])


def change_flags(previous: Optional[Dict[str, Any]], current: Optional[Dict[str, Any]]) -> List[str]:
    if previous is None:
        return ["new_pair"]
    if current is None:
        return ["removed_pair"]

    flags: List[str] = []
    prev_output = previous.get("output") or {}
    curr_output = current.get("output") or {}
    if prev_output.get("nl_question") != curr_output.get("nl_question"):
        flags.append("question_changed")
    if prev_output.get("confidence") != curr_output.get("confidence"):
        flags.append("confidence_changed")
    if prev_output.get("confidence_rationale") != curr_output.get("confidence_rationale"):
        flags.append("rationale_changed")
    if prev_output.get("nl_question_origin") != curr_output.get("nl_question_origin"):
        flags.append("origin_changed")
    if ranked_signature(previous) != ranked_signature(current):
        flags.append("retained_evidence_changed")
    if previous.get("input", {}).get("sparql_clean") != current.get("input", {}).get("sparql_clean"):
        flags.append("sparql_changed")
    if previous.get("output_meta", {}).get("model") != current.get("output_meta", {}).get("model"):
        flags.append("model_changed")

    prev_evidence = evidence_signature(previous)
    curr_evidence = evidence_signature(current)
    prev_ids = set(prev_evidence)
    curr_ids = set(curr_evidence)
    if curr_ids - prev_ids:
        flags.append("input_evidence_added")
    if prev_ids - curr_ids:
        flags.append("input_evidence_removed")
    if any(prev_evidence[eid] != curr_evidence[eid] for eid in prev_ids & curr_ids):
        flags.append("input_evidence_changed")
    return flags


def evidence_diffs(previous: Optional[Dict[str, Any]], current: Optional[Dict[str, Any]]) -> Dict[str, List[str]]:
    prev_evidence = evidence_signature(previous)
    curr_evidence = evidence_signature(current)
    prev_ids = set(prev_evidence)
    curr_ids = set(curr_evidence)
    return {
        "added": sorted(curr_ids - prev_ids),
        "removed": sorted(prev_ids - curr_ids),
        "changed": sorted(eid for eid in prev_ids & curr_ids if prev_evidence[eid] != curr_evidence[eid]),
    }


def pair_status(previous: Optional[Dict[str, Any]], current: Optional[Dict[str, Any]], flags: List[str]) -> str:
    if previous is None:
        return "added"
    if current is None:
        return "removed"
    return "changed" if flags else "unchanged"


def main() -> None:
    parser = argparse.ArgumentParser(description="Build a browser review bundle comparing two LLM review runs.")
    parser.add_argument("--previous-outputs", required=True)
    parser.add_argument("--current-outputs", required=True)
    parser.add_argument("--previous-inputs", default="", help="Defaults to llm_inputs.jsonl beside previous outputs, then ./llm_inputs.jsonl.")
    parser.add_argument("--current-inputs", default="", help="Defaults to llm_inputs.jsonl beside current outputs, then ./llm_inputs.jsonl.")
    parser.add_argument("--previous-reviews", default="")
    parser.add_argument("--out", default="review/review_data.js")
    parser.add_argument("--previous-run-manifest", default="")
    parser.add_argument("--current-run-manifest", default="")
    parser.add_argument("--include-unchanged", action="store_true")
    args = parser.parse_args()

    previous_outputs_path = Path(args.previous_outputs)
    current_outputs_path = Path(args.current_outputs)
    previous_inputs_path = Path(args.previous_inputs) if args.previous_inputs else infer_run_file(previous_outputs_path, "llm_inputs.jsonl") or Path("llm_inputs.jsonl")
    current_inputs_path = Path(args.current_inputs) if args.current_inputs else infer_run_file(current_outputs_path, "llm_inputs.jsonl") or Path("llm_inputs.jsonl")
    previous_manifest = Path(args.previous_run_manifest) if args.previous_run_manifest else None
    current_manifest = Path(args.current_run_manifest) if args.current_run_manifest else None
    previous_reviews_path = Path(args.previous_reviews) if args.previous_reviews else None

    previous_run = run_summary(previous_outputs_path, previous_manifest)
    current_run = run_summary(current_outputs_path, current_manifest)
    previous_inputs = build_input_index(load_json_records(previous_inputs_path))
    current_inputs = build_input_index(load_json_records(current_inputs_path))
    previous_outputs = index_outputs(load_json_records(previous_outputs_path))
    current_outputs = index_outputs(load_json_records(current_outputs_path))
    previous_reviews = load_reviews(previous_reviews_path)

    records: List[Dict[str, Any]] = []
    for key in sorted(set(previous_outputs) | set(current_outputs)):
        previous_record = None
        previous_review_id = None
        if key in previous_outputs:
            previous_idx, previous_output = previous_outputs[key]
            previous_review_id = review_id_for(previous_output, previous_idx)
            previous_record = record_payload(previous_output, previous_inputs, previous_review_id, previous_run)

        current_record = None
        current_review_id = None
        if key in current_outputs:
            current_idx, current_output = current_outputs[key]
            current_review_id = review_id_for(current_output, current_idx)
            current_record = record_payload(current_output, current_inputs, current_review_id, current_run)

        flags = change_flags(previous_record, current_record)
        status = pair_status(previous_record, current_record, flags)
        if status == "unchanged" and not args.include_unchanged:
            continue

        display_record = current_record or previous_record or {}
        records.append(
            {
                "pair_id": pair_id_for(key),
                "kg_id": key[0],
                "query_id": key[1],
                "query_label": display_record.get("query_label") or key[1],
                "pair_status": status,
                "change_flags": flags,
                "evidence_diff": evidence_diffs(previous_record, current_record),
                "previous": {
                    "review_id": previous_review_id,
                    "record": previous_record,
                    "review": previous_reviews.get(previous_review_id or "", {}),
                },
                "current": {
                    "review_id": current_review_id or f"{key[0]}::{key[1]}::removed",
                    "record": current_record,
                },
            }
        )

    payload = {
        "mode": "compare",
        "dataset_id": sha256_text(
            stable_json_dumps(
                {
                    "previous": str(previous_outputs_path),
                    "current": str(current_outputs_path),
                    "records": [
                        {
                            "pair_id": rec["pair_id"],
                            "pair_status": rec["pair_status"],
                            "change_flags": rec["change_flags"],
                        }
                        for rec in records
                    ],
                }
            )
        )[:16],
        "built_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "previous_run": previous_run,
        "current_run": current_run,
        "previous_reviews_path": str(previous_reviews_path) if previous_reviews_path else None,
        "previous_inputs_path": str(previous_inputs_path),
        "current_inputs_path": str(current_inputs_path),
        "record_count": len(records),
        "summary": {
            "changed": sum(1 for rec in records if rec["pair_status"] == "changed"),
            "added": sum(1 for rec in records if rec["pair_status"] == "added"),
            "removed": sum(1 for rec in records if rec["pair_status"] == "removed"),
        },
        "records": records,
    }

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(
        "window.REVIEW_DATA = " + json.dumps(payload, ensure_ascii=False, indent=2) + ";\n",
        encoding="utf-8",
    )
    print(f"Wrote {len(records)} comparison records to {out_path}")


if __name__ == "__main__":
    main()
