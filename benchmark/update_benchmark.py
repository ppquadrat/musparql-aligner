#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from build_benchmark import (
    ALTERNATIVES_FILE,
    HOLDOUT_SPLIT,
    INCLUDED_FILE,
    LINGUISTIC_ANNOTATIONS_FILE,
    add_interpretive_annotation,
    add_formulation,
    alternatives_record_has_content,
    benchmark_disposition,
    benchmark_gold_records,
    has_query_specific_evidence,
    is_private_holdout,
    literal_wording,
    make_rephrasing_entry,
    normalize_rephrasing_text,
    pipeline_assessment,
    read_json,
    read_review_bundle,
    review_comments,
    run_metadata,
    sort_sidecar_records,
    source_evidence_types,
    update_sidecar_identity,
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


def included_records_path(benchmark_dir: Path) -> Path:
    preferred = benchmark_dir / INCLUDED_FILE
    if preferred.exists():
        return preferred
    return benchmark_dir / "benchmark.jsonl"


def holdout_records_path(benchmark_dir: Path) -> Path:
    return benchmark_dir / "holdout.jsonl"


def sort_records(records: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return sorted(records, key=lambda rec: (str(rec.get("kg_id") or ""), str(rec.get("query_label") or "")))


def previous_gold_rephrasing(record: Dict[str, Any], dataset_id: str) -> Dict[str, Any] | None:
    text = " ".join(str(record.get("gold_question") or "").split())
    if not text:
        return None
    review = record.get("review") if isinstance(record.get("review"), dict) else {}
    run = record.get("run") if isinstance(record.get("run"), dict) else {}
    return {
        "text": text,
        "normalized_text": normalize_rephrasing_text(text),
        "source_type": "previous_canonical_question",
        "review_id": review.get("review_id") or record.get("benchmark_id"),
        "review_export": review.get("review_export"),
        "dataset_id": review.get("dataset_id") or dataset_id,
        "run_id": review.get("run_id") or run.get("run_id"),
        "generation_run_id": review.get("generation_run_id") or run.get("generation_run_id") or run.get("run_id"),
        "model": run.get("model"),
        "updated_at": review.get("updated_at"),
        "run": run,
    }


def merge_alternative_records(
    target: Dict[str, Any] | None,
    source: Dict[str, Any] | None,
) -> Dict[str, Any] | None:
    if not source:
        return target
    if not target:
        return source
    for field in ("accepted_alternatives", "literal_formulations"):
        for entry in source.get(field, []):
            if isinstance(entry, dict):
                add_formulation(target, field, entry)
    return target


def merge_annotation_records(
    target: Dict[str, Any] | None,
    source: Dict[str, Any] | None,
) -> Dict[str, Any] | None:
    if not source:
        return target
    if not target:
        return source
    annotations = target.setdefault("interpretive_annotations", [])
    for annotation in source.get("interpretive_annotations", []):
        if isinstance(annotation, dict) and annotation not in annotations:
            annotations.append(annotation)
    return target


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
    disposition = benchmark_disposition(review)
    assessment = pipeline_assessment(review)

    return {
        "benchmark_id": review_id,
        "kg_id": record.get("kg_id"),
        "query_id": record.get("query_id"),
        "query_label": record.get("query_label"),
        "sparql": record.get("input", {}).get("sparql_clean"),
        "sparql_version": record.get("input", {}).get("sparql_version"),
        "sparql_hash": record.get("input", {}).get("sparql_hash"),
        "gold_question": gold_question,
        "gold_question_source": gold_source,
        "benchmark_disposition": disposition,
        "pipeline_assessment": assessment or None,
        "split": HOLDOUT_SPLIT if is_private_holdout(review) else "public",
        "review": {
            "review_id": review_id,
            "review_export": str(review_path),
            "dataset_id": dataset_id,
            "run_id": current_run.get("run_id") or record.get("run_id"),
            "generation_run_id": current_run.get("generation_run_id") or current_run.get("run_id") or record.get("generation_run_id") or record.get("run_id"),
            "literal_wording": literal_wording(review),
            "public_comment": review_comments(review)[0],
            "internal_comment": review_comments(review)[1],
            "updated_at": review.get("updated_at"),
            "copied_from_review_id": review.get("copied_from_review_id"),
        },
        "run": run_metadata(record),
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
    included_by_key: Dict[Tuple[str, str], Dict[str, Any]],
    dismissed_by_key: Dict[Tuple[str, str], Dict[str, Any]],
    holdout_by_key: Dict[Tuple[str, str], Dict[str, Any]],
) -> bool:
    removed = False
    for records in (included_by_key, dismissed_by_key, holdout_by_key):
        if key in records:
            records.pop(key, None)
            removed = True
    return removed


def main() -> None:
    parser = argparse.ArgumentParser(description="Apply comparative-review decisions to a previous benchmark snapshot.")
    parser.add_argument("--previous-benchmark", required=True, help="Previous benchmark/vN directory.")
    parser.add_argument("--bundle", default="review/review_data.js", help="Comparative-review bundle.")
    parser.add_argument("--reviews", required=True, help="Exported comparative-review decisions.")
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

    included_by_key = {pair_key(rec): rec for rec in read_jsonl(included_records_path(previous_dir))}
    dismissed_by_key = {pair_key(rec): rec for rec in read_jsonl(previous_dir / "dismissed.jsonl")}
    holdout_by_key = {pair_key(rec): rec for rec in read_jsonl(holdout_records_path(previous_dir))}
    alternatives_by_key = {pair_key(rec): rec for rec in read_jsonl(previous_dir / ALTERNATIVES_FILE)}
    annotations_by_key = {pair_key(rec): rec for rec in read_jsonl(previous_dir / LINGUISTIC_ANNOTATIONS_FILE)}
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
    assessment_counts: Counter[str] = Counter()
    disposition_counts: Counter[str] = Counter()
    applied = 0
    superseded_removed = 0

    for pair in pairs:
        if not isinstance(pair, dict):
            continue
        review_id = current_review_id(pair)
        review = reviews.get(review_id)
        if not isinstance(review, dict):
            continue
        disposition = benchmark_disposition(review)
        assessment = pipeline_assessment(review)
        if not disposition:
            continue
        record = current_record(pair)
        key = (str(pair.get("kg_id") or ""), str(pair.get("query_id") or ""))
        previous_public_record = included_by_key.get(key)
        current_alternatives = alternatives_by_key.pop(key, None)
        current_annotations = annotations_by_key.pop(key, None)
        included_by_key.pop(key, None)
        dismissed_by_key.pop(key, None)
        holdout_by_key.pop(key, None)
        disposition_counts[disposition] += 1
        if assessment:
            assessment_counts[assessment] += 1
        applied += 1

        if record is None:
            if disposition == "excluded":
                dismissed_by_key[key] = {
                    "benchmark_id": review_id,
                    "kg_id": pair.get("kg_id"),
                    "query_id": pair.get("query_id"),
                    "query_label": pair.get("query_label"),
                    "benchmark_disposition": "excluded",
                    "pipeline_assessment": None,
                    "review": {
                        "review_id": review_id,
                        "review_export": str(review_path),
                        "dataset_id": dataset_id,
                        "public_comment": review_comments(review)[0],
                        "internal_comment": review_comments(review)[1],
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
                if pop_key_from_all(candidate_key, included_by_key, dismissed_by_key, holdout_by_key):
                    superseded_removed += 1
                    current_alternatives = merge_alternative_records(current_alternatives, alternatives_by_key.pop(candidate_key, None))
                    current_annotations = merge_annotation_records(current_annotations, annotations_by_key.pop(candidate_key, None))

        next_record = make_benchmark_record(
            record=record,
            review=review,
            review_id=review_id,
            review_path=review_path,
            dataset_id=dataset_id,
            current_run=current_run,
        )
        if disposition == "withheld":
            holdout_by_key[key] = next_record
        elif disposition == "excluded":
            dismissed_by_key[key] = next_record
        else:
            included_by_key[key] = next_record

        if disposition == "included":
            current_alternatives = current_alternatives or {}
            current_annotations = current_annotations or {}
            if previous_public_record:
                old_gold = str(previous_public_record.get("gold_question") or "").strip()
                new_gold = str(next_record.get("gold_question") or "").strip()
                if old_gold and normalize_rephrasing_text(old_gold) != normalize_rephrasing_text(new_gold):
                    add_formulation(current_alternatives, "accepted_alternatives", previous_gold_rephrasing(previous_public_record, dataset_id))
            add_interpretive_annotation(
                current_annotations,
                review=review,
                review_id=review_id,
                review_path=review_path,
                dataset_id=dataset_id,
                record=record,
            )
            if assessment == "accepted":
                preferred = str(review.get("preferred_question") or "").strip()
                model_question = str(record.get("output", {}).get("nl_question") or "").strip()
                if preferred and normalize_rephrasing_text(preferred) != normalize_rephrasing_text(model_question):
                    add_formulation(
                        current_alternatives,
                        "accepted_alternatives",
                        make_rephrasing_entry(
                            text=model_question,
                            source_type="model_output",
                            review=review,
                            review_id=review_id,
                            review_path=review_path,
                            dataset_id=dataset_id,
                            record=record,
                        ),
                    )
            add_formulation(
                current_alternatives,
                "literal_formulations",
                make_rephrasing_entry(
                    text=literal_wording(review),
                    source_type="literal_sparql_wording",
                    review=review,
                    review_id=review_id,
                    review_path=review_path,
                    dataset_id=dataset_id,
                    record=record,
                ),
            )
            update_sidecar_identity(
                current_alternatives,
                benchmark_record=next_record,
                benchmark_version=outdir.name,
                built_at="",
            )
            update_sidecar_identity(
                current_annotations,
                benchmark_record=next_record,
                benchmark_version=outdir.name,
                built_at="",
            )
            if alternatives_record_has_content(current_alternatives):
                alternatives_by_key[key] = current_alternatives
            if current_annotations.get("interpretive_annotations"):
                annotations_by_key[key] = current_annotations

    included = sort_records(included_by_key.values())
    dismissed = sort_records(dismissed_by_key.values())
    holdout = sort_records(holdout_by_key.values())
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
            "included": len(included),
            "dismissed": len(dismissed),
            "holdout": len(holdout),
            "alternatives": len(alternatives_records),
            "linguistic_annotations": len(linguistic_annotation_records),
            "pipeline_assessment_counts": dict(Counter(str(row.get("pipeline_assessment")) for row in included)),
            "benchmark_disposition_counts": dict(Counter(str(row.get("benchmark_disposition")) for row in included + dismissed + holdout)),
            "applied_compare_reviews": applied,
            "applied_pipeline_assessment_counts": dict(assessment_counts),
            "applied_benchmark_disposition_counts": dict(disposition_counts),
            "superseded_removed_previous_items": superseded_removed,
        },
        "files": {
            "benchmark": "benchmark.jsonl",
            "included": INCLUDED_FILE,
            "dismissed": "dismissed.jsonl",
            "holdout": "holdout.jsonl",
            "alternatives": ALTERNATIVES_FILE,
            "linguistic_annotations_internal": LINGUISTIC_ANNOTATIONS_FILE,
        },
        "gold_question_policy": {
            "benchmark_contains_all_human_confirmed_included_pairs": True,
            "preferred_question_used_when_present": True,
            "approved_model_output_used_otherwise": True,
            "unchanged_previous_items_carried_forward": True,
        },
        "holdout_policy": {
            "review_split": HOLDOUT_SPLIT,
            "excluded_from_public_benchmark": True,
            "excluded_from_normal_autoeval": True,
            "excluded_from_future_generation_inputs": True,
            "unchanged_previous_holdout_items_carried_forward": True,
        },
        "release_boundary": {
            "public_release_files": ["manifest.json", "benchmark.jsonl", ALTERNATIVES_FILE],
            "internal_only_files": [INCLUDED_FILE, LINGUISTIC_ANNOTATIONS_FILE, "dismissed.jsonl", "holdout.jsonl"],
        },
    }

    write_json(outdir / "manifest.json", manifest)
    write_jsonl(outdir / "benchmark.jsonl", benchmark_records)
    write_jsonl(outdir / INCLUDED_FILE, included)
    write_jsonl(outdir / "dismissed.jsonl", dismissed)
    write_jsonl(outdir / "holdout.jsonl", holdout)
    write_jsonl(outdir / ALTERNATIVES_FILE, alternatives_records)
    write_jsonl(outdir / LINGUISTIC_ANNOTATIONS_FILE, linguistic_annotation_records)

    print(f"Wrote manifest to {outdir / 'manifest.json'}")
    print(f"Wrote {len(benchmark_records)} benchmark records to {outdir / 'benchmark.jsonl'}")
    print(f"Wrote {len(included)} included records to {outdir / INCLUDED_FILE}")
    print(f"Wrote {len(dismissed)} dismissed records to {outdir / 'dismissed.jsonl'}")
    print(f"Wrote {len(holdout)} private holdout records to {outdir / 'holdout.jsonl'}")
    print(f"Wrote {len(alternatives_records)} alternative-formulation records to {outdir / ALTERNATIVES_FILE}")
    print(f"Wrote {len(linguistic_annotation_records)} internal linguistic-annotation records to {outdir / LINGUISTIC_ANNOTATIONS_FILE}")
    print(f"Applied {applied} comparative-review decisions")


if __name__ == "__main__":
    main()
