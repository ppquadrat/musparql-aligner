#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from scripts.build_review_bundle import (
    build_input_index,
    infer_run_manifest,
    load_selector_keys,
    load_json,
    load_json_records,
    sha256_text,
    signature_token,
    stable_json_dumps,
    without_reviewer_ids,
)
from scripts.benchmark.build_benchmark import literal_wording, review_comments
from musparql.holdout_selectors import add_holdout_filter_arguments, holdout_input_policy
from musparql.sparql_corrections import assert_input_provenance_current
from musparql.reviewer_provenance import validate_reviewer_id


PairKey = Tuple[str, str]
HOLDOUT_SPLIT = "private_holdout"


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
        "generation_run_id": manifest.get("generation_run_id") or manifest.get("run_id") or output_path.parent.name,
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
    if payload.get("kind") != "non_holdout_review_export":
        raise ValueError("Comparative review tools require kind='non_holdout_review_export'")
    reviews = payload.get("reviews")
    if not isinstance(reviews, dict):
        raise ValueError(f"Review export has no reviews object: {path}")
    if any(
        isinstance(review, dict)
        and (
            review.get("split") == HOLDOUT_SPLIT
            or review.get("benchmark_disposition") == "withheld"
        )
        for review in reviews.values()
    ):
        raise ValueError("Comparative review tools require a sanitized public review export")
    return reviews


def load_jsonl(path: Path) -> List[Dict[str, Any]]:
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


def benchmark_review_for_record(record: Dict[str, Any], group: str, source_benchmark: Path) -> Dict[str, Any]:
    review = record.get("review") if isinstance(record.get("review"), dict) else {}
    gold_source = str(record.get("gold_question_source") or "")
    public_comment, internal_comment = review_comments(review)
    provenance = record.get("provenance") if isinstance(record.get("provenance"), dict) else {}
    return {
        "review_id": review.get("review_id")
        or provenance.get("approval_review_id")
        or record.get("benchmark_id"),
        "benchmark_disposition": group,
        "pipeline_assessment": str(record.get("pipeline_assessment") or ""),
        "preferred_question": record.get("gold_question") if gold_source == "reviewer_rewrite" else "",
        "literal_wording": literal_wording(review),
        "public_comment": public_comment,
        "internal_comment": internal_comment,
        "reviewer_id": review.get("reviewer_id") or provenance.get("reviewer_id"),
        "reviewed_at": review.get("reviewed_at") or review.get("updated_at"),
        "prior_review_ids": review.get("prior_review_ids", []),
        "authored_formulation_ids": review.get("authored_formulation_ids", []),
        "approved_formulation_ids": review.get("approved_formulation_ids", []),
        "benchmark_id": record.get("benchmark_id"),
        "source_benchmark": str(source_benchmark),
    }


def load_benchmark_reviews(path: Optional[Path]) -> Tuple[Dict[PairKey, Dict[str, Any]], set[PairKey]]:
    if path is None:
        return {}, set()
    expected_paths = [path / "benchmark.jsonl", path / "included.jsonl", path / "dismissed.jsonl"]
    if not any(candidate.exists() for candidate in expected_paths):
        raise FileNotFoundError(f"Previous benchmark has no review records: {path}")
    review_by_pair: Dict[PairKey, Dict[str, Any]] = {}
    benchmark_pairs: set[PairKey] = set()
    for group, filename in (
        ("included", "benchmark.jsonl"),
        ("included", "included.jsonl"),
        ("excluded", "dismissed.jsonl"),
    ):
        for record in load_jsonl(path / filename):
            key = pair_key(record)
            if group == "included":
                benchmark_pairs.add(key)
            review_by_pair[key] = benchmark_review_for_record(record, group, path)
    return review_by_pair, benchmark_pairs


def previous_pipeline_assessment_by_pair(
    previous_outputs: Dict[PairKey, Tuple[int, Dict[str, Any]]],
    previous_reviews: Dict[str, Any],
) -> Dict[PairKey, str]:
    assessments: Dict[PairKey, str] = {}
    for key, (idx, output) in previous_outputs.items():
        review = previous_reviews.get(review_id_for(output, idx))
        if not isinstance(review, dict):
            continue
        assessment = review.get("pipeline_assessment")
        if isinstance(assessment, str) and assessment:
            assessments[key] = assessment
    return assessments


def previous_review_by_pair(
    previous_outputs: Dict[PairKey, Tuple[int, Dict[str, Any]]],
    previous_reviews: Dict[str, Any],
) -> Dict[PairKey, Dict[str, Any]]:
    mapped: Dict[PairKey, Dict[str, Any]] = {}
    for key, (idx, output) in previous_outputs.items():
        review = previous_reviews.get(review_id_for(output, idx))
        if isinstance(review, dict):
            mapped[key] = review
    return mapped


def previous_review_split_by_pair(
    previous_outputs: Dict[PairKey, Tuple[int, Dict[str, Any]]],
    previous_reviews: Dict[str, Any],
) -> Dict[PairKey, str]:
    splits: Dict[PairKey, str] = {}
    for key, (idx, output) in previous_outputs.items():
        review = previous_reviews.get(review_id_for(output, idx))
        if not isinstance(review, dict):
            continue
        split = review.get("split")
        if isinstance(split, str) and split:
            splits[key] = split
    return splits


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
    request_config = output_record.get("request_config")
    response_metadata = output_record.get("response_metadata")
    output_meta = {
        "model": output_record.get("model"),
        "elapsed_ms": output_record.get("elapsed_ms"),
        "generated_at": output_record.get("generated_at"),
        "run_signature": output_record.get("run_signature"),
    }
    if isinstance(request_config, dict):
        output_meta["request_config"] = request_config
        output_meta["requested_model"] = request_config.get("requested_model")
        output_meta["generation_parameters"] = request_config.get("generation_parameters")
    if isinstance(response_metadata, dict):
        output_meta["response_metadata"] = response_metadata
        output_meta["response_model"] = response_metadata.get("model")
    return {
        "review_id": review_id,
        "run_id": run.get("run_id"),
        "generation_run_id": run.get("generation_run_id") or run.get("run_id"),
        "run_label": run.get("run_label"),
        "source_file": run.get("output_path"),
        "run_manifest": run.get("manifest_path"),
        "kg_id": kg_id,
        "query_id": query_id,
        "query_label": query_label,
        "input": {
            "sparql_clean": source_input.get("sparql_clean"),
            "sparql_version": source_input.get("sparql_version"),
            "sparql_hash": source_input.get("sparql_hash"),
            "schema_ref": source_input.get("schema_ref"),
            "evidence": source_input.get("evidence", []),
            "sparql_provenance": source_input.get("sparql_provenance"),
        },
        "output": output_record.get("llm_output"),
        "output_meta": output_meta,
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
        "sparql_version": record.get("input", {}).get("sparql_version"),
        "sparql_hash": record.get("input", {}).get("sparql_hash"),
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
    if previous.get("input", {}).get("sparql_version") != current.get("input", {}).get("sparql_version"):
        flags.append("sparql_version_changed")
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


REVIEW_WORTHY_FLAGS = {
    "question_changed",
    "origin_changed",
    "retained_evidence_changed",
    "sparql_changed",
}


def has_review_worthy_change(status: str, flags: List[str]) -> bool:
    if status in {"added", "removed"}:
        return True
    return any(flag in REVIEW_WORTHY_FLAGS for flag in flags)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build a browser review bundle comparing two LLM review runs.")
    identity = parser.add_mutually_exclusive_group(required=True)
    identity.add_argument("--reviewer-id", help="Anonymous reviewer ID for the legacy local workflow.")
    identity.add_argument(
        "--reviewer-neutral",
        action="store_true",
        help="Omit reviewer identity so the hosted portal can attribute an assignment after authentication.",
    )
    parser.add_argument("--previous-outputs", required=True)
    parser.add_argument("--current-outputs", required=True)
    parser.add_argument("--previous-inputs", default="", help="Defaults to llm_inputs.jsonl beside previous outputs, then ./llm_inputs.jsonl.")
    parser.add_argument("--current-inputs", default="", help="Defaults to llm_inputs.jsonl beside current outputs, then ./llm_inputs.jsonl.")
    parser.add_argument("--kg-queries", default="var/queries/kg_queries.jsonl", help="Current canonical query records used to validate SPARQL provenance.")
    parser.add_argument("--previous-reviews", default="")
    parser.add_argument(
        "--previous-benchmark",
        default="",
        help="Previous benchmark/vN directory. When provided, carried-forward benchmark decisions are used as previous review context.",
    )
    add_holdout_filter_arguments(parser)
    parser.add_argument(
        "--benchmark-only",
        action="store_true",
        help="Only include pairs present in the previous benchmark's public benchmark.jsonl set.",
    )
    parser.add_argument("--out", default="review/review_data.js")
    parser.add_argument("--previous-run-manifest", default="")
    parser.add_argument("--current-run-manifest", default="")
    parser.add_argument("--include-unchanged", action="store_true")
    parser.add_argument(
        "--include-dismissed",
        action="store_true",
        help="Include pairs that were dismissed in the previous review export. By default they are excluded from comparative review.",
    )
    parser.add_argument(
        "--include-metadata-only",
        action="store_true",
        help="Include pairs whose only changes are confidence, rationale, model, or full-input evidence metadata.",
    )
    parser.add_argument(
        "--assert-complete-review-provenance",
        action="store_true",
        help="Human assertion that supplied benchmark/review sources cover every prior reviewer decision; required to enable holdout selection.",
    )
    args = parser.parse_args()
    reviewer_id = validate_reviewer_id(args.reviewer_id) if args.reviewer_id else None

    previous_outputs_path = Path(args.previous_outputs)
    current_outputs_path = Path(args.current_outputs)
    previous_inputs_path = Path(args.previous_inputs) if args.previous_inputs else infer_run_file(previous_outputs_path, "llm_inputs.jsonl") or Path("var/llm/inputs.jsonl")
    current_inputs_path = Path(args.current_inputs) if args.current_inputs else infer_run_file(current_outputs_path, "llm_inputs.jsonl") or Path("var/llm/inputs.jsonl")
    previous_manifest = Path(args.previous_run_manifest) if args.previous_run_manifest else None
    current_manifest = Path(args.current_run_manifest) if args.current_run_manifest else None
    previous_reviews_path = Path(args.previous_reviews) if args.previous_reviews else None
    previous_benchmark_path = Path(args.previous_benchmark) if args.previous_benchmark else None
    holdout_selector_keys = load_selector_keys(Path(args.holdout_selectors) if args.holdout_selectors else None)

    previous_run = run_summary(previous_outputs_path, previous_manifest)
    current_run = run_summary(current_outputs_path, current_manifest)
    previous_inputs = build_input_index(load_json_records(previous_inputs_path))
    current_inputs = build_input_index(load_json_records(current_inputs_path))
    previous_outputs = index_outputs(load_json_records(previous_outputs_path))
    current_outputs = index_outputs(load_json_records(current_outputs_path))
    if args.assert_complete_review_provenance:
        canonical_records = load_json_records(Path(args.kg_queries))
        assert_input_provenance_current(previous_inputs.values(), canonical_records)
        assert_input_provenance_current(current_inputs.values(), canonical_records)
        for label, inputs in (("previous", previous_inputs), ("current", current_inputs)):
            invalid = [
                key
                for key, item in inputs.items()
                if not isinstance(item.get("sparql_provenance"), dict)
                or isinstance(item["sparql_provenance"].get("retained_edit_count"), bool)
                or not isinstance(item["sparql_provenance"].get("retained_edit_count"), int)
                or item["sparql_provenance"]["retained_edit_count"] < 0
            ]
            if invalid:
                raise ValueError(
                    f"Cannot enable holdout selection: {label} inputs lack valid SPARQL edit provenance"
                )
    previous_reviews = load_reviews(previous_reviews_path)
    previous_review_map = previous_review_by_pair(previous_outputs, previous_reviews)
    benchmark_review_map, benchmark_pairs = load_benchmark_reviews(previous_benchmark_path)
    if benchmark_review_map:
        previous_review_map.update(benchmark_review_map)
    previous_dispositions = {
        key: str(review.get("benchmark_disposition"))
        for key, review in previous_review_map.items()
        if review.get("benchmark_disposition")
    }
    previous_splits = {key: str(review.get("split")) for key, review in previous_review_map.items() if review.get("split")}

    records: List[Dict[str, Any]] = []
    dismissed_excluded = 0
    holdout_excluded = 0
    non_benchmark_excluded = 0
    metadata_only_excluded = 0
    for key in sorted(set(previous_outputs) | set(current_outputs)):
        if key in holdout_selector_keys:
            holdout_excluded += 1
            continue
        if args.benchmark_only and previous_benchmark_path is not None and key not in benchmark_pairs:
            non_benchmark_excluded += 1
            continue
        if previous_splits.get(key) == HOLDOUT_SPLIT:
            holdout_excluded += 1
            continue
        if previous_dispositions.get(key) == "excluded" and not args.include_dismissed:
            dismissed_excluded += 1
            continue

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
        if status == "changed" and not has_review_worthy_change(status, flags) and not args.include_metadata_only:
            metadata_only_excluded += 1
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
                    "review": previous_review_map.get(key, {}),
                },
                "current": {
                    "review_id": current_review_id or f"{key[0]}::{key[1]}::removed",
                    "record": current_record,
                },
            }
        )

    payload = {
        "schema": "musparql.review-bundle.v2",
        "mode": "compare",
        "dataset_id": sha256_text(
            stable_json_dumps(
                {
                    "previous": str(previous_outputs_path),
                    "current": str(current_outputs_path),
                    "previous_benchmark": str(previous_benchmark_path) if previous_benchmark_path else None,
                    "benchmark_only": bool(args.benchmark_only),
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
        "previous_benchmark_path": str(previous_benchmark_path) if previous_benchmark_path else None,
        "benchmark_only": bool(args.benchmark_only),
        "holdout_review_provenance_complete": bool(args.assert_complete_review_provenance),
        "holdout_input_policy": holdout_input_policy(args),
        "previous_inputs_path": str(previous_inputs_path),
        "current_inputs_path": str(current_inputs_path),
        "record_count": len(records),
        "summary": {
            "changed": sum(1 for rec in records if rec["pair_status"] == "changed"),
            "added": sum(1 for rec in records if rec["pair_status"] == "added"),
            "removed": sum(1 for rec in records if rec["pair_status"] == "removed"),
            "dismissed_excluded": dismissed_excluded,
            "holdout_excluded": holdout_excluded,
            "non_benchmark_excluded": non_benchmark_excluded,
            "metadata_only_excluded": metadata_only_excluded,
            "benchmark_included": sum(1 for rec in records if rec.get("previous", {}).get("review", {}).get("benchmark_disposition") == "included"),
        },
        "records": records,
    }
    if reviewer_id is None:
        payload = without_reviewer_ids(payload)
    else:
        payload["reviewer_id"] = reviewer_id

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(
        "window.REVIEW_DATA = " + json.dumps(payload, ensure_ascii=False, indent=2) + ";\n",
        encoding="utf-8",
    )
    print(f"Wrote {len(records)} comparison records to {out_path}")
    if dismissed_excluded:
        print(f"Excluded {dismissed_excluded} previously dismissed pairs")
    if holdout_excluded:
        print(f"Excluded {holdout_excluded} private holdout pairs")
    if non_benchmark_excluded:
        print(f"Excluded {non_benchmark_excluded} non-benchmark pairs")
    if metadata_only_excluded:
        print(f"Excluded {metadata_only_excluded} metadata-only changed pairs")


if __name__ == "__main__":
    main()
