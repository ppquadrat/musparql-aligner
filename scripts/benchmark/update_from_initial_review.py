#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

from scripts.benchmark.build_benchmark import (
    ALTERNATIVES_FILE,
    INCLUDED_FILE,
    LINGUISTIC_ANNOTATIONS_FILE,
    PUBLIC_RELEASE_FILES,
    add_interpretive_annotation,
    add_formulation,
    alternatives_record_has_content,
    assert_no_private_reviews,
    assert_non_holdout_export,
    benchmark_disposition,
    benchmark_gold_records,
    canonical_execution_snapshot,
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
from scripts.benchmark.update_benchmark import (
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


def is_reviewed_sparql_replacement(record: Dict[str, Any], previous: Dict[str, Any]) -> bool:
    """Allow a re-reviewed SPARQL revision to replace the same benchmark identity."""
    if record.get("has_prior_pair_review") is not True or record.get("review_scope") != "new":
        return False
    current_input = record.get("input") if isinstance(record.get("input"), dict) else {}
    current_hash = str(current_input.get("sparql_hash") or "").removeprefix("sha256:")
    previous_hash = str(previous.get("sparql_hash") or "").removeprefix("sha256:")
    if current_hash and previous_hash:
        return current_hash != previous_hash
    current_version = current_input.get("sparql_version")
    previous_version = previous.get("sparql_version")
    if isinstance(current_version, int) and isinstance(previous_version, int):
        return current_version != previous_version
    return str(current_input.get("sparql_clean") or "").strip() != str(previous.get("sparql") or "").strip()


def apply_stage(
    *,
    bundle_path: Path,
    review_path: Path,
    decisions_by_pair: Dict[str, Dict[str, Any]],
    included_by_key: Dict[Tuple[str, str], Dict[str, Any]],
    dismissed_by_key: Dict[Tuple[str, str], Dict[str, Any]],
    alternatives_by_key: Dict[Tuple[str, str], Dict[str, Any]],
    annotations_by_key: Dict[Tuple[str, str], Dict[str, Any]],
    existing_keys: set[Tuple[str, str]],
    outdir: Path,
) -> Dict[str, Any]:
    """Apply one bundle/export pair to the accumulating snapshot state.

    A review round can produce several exports -- one per reviewing team
    plus a synthesized meta-review, each pinned to its own dataset's
    bundle -- and they are all one update to one snapshot. Each call records
    what it applied so the manifest can name every export, rather than the
    build chaining through throwaway snapshots whose provenance is lost.
    """
    assessment_counts: Counter[str] = Counter()
    disposition_counts: Counter[str] = Counter()
    applied = 0
    missing_records: List[str] = []
    overlaps: List[Tuple[str, Tuple[str, str]]] = []
    replaced_keys: set[Tuple[str, str]] = set()
    replaced_sparql_revisions = 0
    meta_alternatives = 0
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

    current_run = source_run(bundle)
    dataset_id = str(review_export.get("dataset_id") or bundle.get("dataset_id") or "")

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
            previous_record = included_by_key.get(key) or dismissed_by_key.get(key)
            if (
                key not in replaced_keys
                and isinstance(previous_record, dict)
                and is_reviewed_sparql_replacement(record, previous_record)
            ):
                included_by_key.pop(key, None)
                dismissed_by_key.pop(key, None)
                alternatives_by_key.pop(key, None)
                annotations_by_key.pop(key, None)
                replaced_keys.add(key)
                replaced_sparql_revisions += 1
            else:
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
            for alternative in (
                (decisions_by_pair.get(str(review_id), {}) or {}).get("alternative_questions") or []
            ):
                entry = make_rephrasing_entry(
                    text=alternative,
                    source_type="meta_review_alternative",
                    review=review,
                    review_id=str(review_id),
                    review_path=review_path,
                    dataset_id=dataset_id,
                    record=record,
                )
                if entry:
                    # No indexing needed any more: alternative_id is derived
                    # from the wording itself, so two alternatives of the same
                    # pair already differ.
                    add_formulation(alternatives, "accepted_alternatives", entry)
                    meta_alternatives += 1
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

        existing_keys.add(key)
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

    # A merged export's filename carries the reviewing group's ID, so the
    # tracked manifest names it by content digest instead. The path itself is
    # retained per record in the ignored included.jsonl.
    export_digest = "sha256:" + hashlib.sha256(review_path.read_bytes()).hexdigest()
    return {
        "mode": "initial",
        "source_bundle": str(bundle_path),
        "source_review_export_digest": export_digest,
        "dataset_id": dataset_id,
        "current_run": current_run,
        "applied_reviews": applied,
        "applied_meta_review_alternatives": meta_alternatives,
        "replaced_sparql_revisions": replaced_sparql_revisions,
        "applied_pipeline_assessment_counts": dict(assessment_counts),
        "applied_benchmark_disposition_counts": dict(disposition_counts),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Add initial-review decisions to an existing benchmark snapshot.")
    parser.add_argument("--previous-benchmark", required=True, help="Previous benchmark/vN directory.")
    parser.add_argument(
        "--bundle",
        action="append",
        default=None,
        help=(
            "Normal review bundle. Repeat it, paired in order with --reviews, to apply a round "
            "whose reviews were exported per team and per dataset as one update."
        ),
    )
    parser.add_argument(
        "--reviews",
        action="append",
        required=True,
        help="Exported initial-review decisions; repeat it once per --bundle, in the same order.",
    )
    parser.add_argument(
        "--decisions",
        default=None,
        help=(
            "Optional meta-review decisions JSONL (catalog/curated/meta_review_decisions.jsonl). "
            "Alternative wordings a meta-reviewer chose to keep are recorded as accepted "
            "alternatives; without it only the model output can become one."
        ),
    )
    parser.add_argument("--outdir", required=True, help="Output benchmark/vN directory.")
    parser.add_argument(
        "--kg-queries",
        default="var/queries/kg_queries.jsonl",
        help="Canonical query catalogue used to summarize execution observations.",
    )
    args = parser.parse_args()

    previous_dir = Path(args.previous_benchmark)
    bundle_paths = [Path(value) for value in (args.bundle or ["review/review_data.js"])]
    review_paths = [Path(value) for value in args.reviews]
    if len(bundle_paths) != len(review_paths):
        raise ValueError(
            f"--bundle and --reviews must be paired in order: {len(bundle_paths)} bundles "
            f"against {len(review_paths)} exports"
        )
    outdir = Path(args.outdir)
    query_path = Path(args.kg_queries)
    outdir.mkdir(parents=True, exist_ok=True)

    decisions_by_pair: Dict[str, Dict[str, Any]] = {}
    if args.decisions:
        for line in Path(args.decisions).read_text(encoding="utf-8").splitlines():
            if line.strip():
                decision = json.loads(line)
                decisions_by_pair[str(decision.get("pair_review_id") or "")] = decision

    included_by_key = {pair_key(rec): rec for rec in read_jsonl(included_records_path(previous_dir))}
    dismissed_by_key = {pair_key(rec): rec for rec in read_jsonl(previous_dir / "dismissed.jsonl")}
    alternatives_by_key = {pair_key(rec): rec for rec in read_jsonl(previous_dir / ALTERNATIVES_FILE)}
    annotations_by_key = {pair_key(rec): rec for rec in read_jsonl(previous_dir / LINGUISTIC_ANNOTATIONS_FILE)}
    existing_keys = set(included_by_key) | set(dismissed_by_key)

    stages: List[Dict[str, Any]] = []
    for bundle_path, review_path in zip(bundle_paths, review_paths):
        stages.append(
            apply_stage(
                bundle_path=bundle_path,
                review_path=review_path,
                decisions_by_pair=decisions_by_pair,
                included_by_key=included_by_key,
                dismissed_by_key=dismissed_by_key,
                alternatives_by_key=alternatives_by_key,
                annotations_by_key=annotations_by_key,
                existing_keys=existing_keys,
                outdir=outdir,
            )
        )

    applied = sum(stage["applied_reviews"] for stage in stages)
    meta_alternatives = sum(stage["applied_meta_review_alternatives"] for stage in stages)
    replaced_sparql_revisions = sum(stage["replaced_sparql_revisions"] for stage in stages)
    assessment_counts: Counter[str] = Counter()
    disposition_counts: Counter[str] = Counter()
    for stage in stages:
        assessment_counts.update(stage["applied_pipeline_assessment_counts"])
        disposition_counts.update(stage["applied_benchmark_disposition_counts"])
    dataset_ids = sorted({str(stage["dataset_id"]) for stage in stages if stage["dataset_id"]})
    # The round-level aggregate comes from merge_adjudicated_reviews, which is
    # the only step that sees the original submissions. Counting the merged
    # exports here instead would describe the merge artefacts, not the cohort.
    review_cohort = None
    for review_path in review_paths:
        summary_path = review_path.parent / "round-summary.json"
        if summary_path.exists():
            review_cohort = read_json(summary_path)
            break
    single = len(stages) == 1

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

    # Carry forward every assertion the previous snapshot made about its own
    # content. v9 and v10 dropped v8's source_prompt policy key while still
    # carrying its 20 LinkedMusic records, which left a fifth of the snapshot
    # resting on a statement the snapshot no longer made.
    gold_question_policy = dict(previous_manifest.get("gold_question_policy") or {})
    gold_question_policy.update(
        {
            "benchmark_contains_all_human_confirmed_included_pairs": True,
            "preferred_question_used_when_present": True,
            "approved_model_output_used_otherwise": True,
            "unchanged_previous_items_carried_forward": True,
        }
    )

    manifest = {
        "benchmark_version": benchmark_version,
        "built_at": built_at,
        "update_type": "normal_review_update" if single else "multi_export_review_update",
        "previous_benchmark": str(previous_dir),
        "previous_benchmark_version": previous_manifest.get("benchmark_version"),
        # A single-export update keeps the scalar fields it has always had. A
        # round exported per team and per dataset names every export instead,
        # so no stage's provenance is dropped.
        "source_bundle": stages[0]["source_bundle"] if single else [s["source_bundle"] for s in stages],
        "source_review_export_digest": (
            stages[0]["source_review_export_digest"]
            if single
            else [s["source_review_export_digest"] for s in stages]
        ),
        "dataset_id": dataset_ids[0] if len(dataset_ids) == 1 else dataset_ids,
        "current_run": stages[0]["current_run"] if single else [s["current_run"] for s in stages],
        "review_stages": [
            {
                "mode": stage["mode"],
                "source_bundle": stage["source_bundle"],
                "source_review_export_digest": stage["source_review_export_digest"],
                "dataset_id": stage["dataset_id"],
                "applied_reviews": stage["applied_reviews"],
            }
            for stage in stages
        ],
        "sparql_version_policy": "latest_retained",
        **({"review_cohort": review_cohort} if review_cohort else {}),
        "execution_snapshot": canonical_execution_snapshot(
            included,
            query_path=query_path,
            captured_through=built_at,
        ),
        "counts": {
            "benchmark": len(benchmark_records),
            "included": len(included),
            "dismissed": len(dismissed),
            "alternatives": len(alternatives_records),
            "linguistic_annotations": len(linguistic_annotation_records),
            "pipeline_assessment_counts": dict(Counter(str(row.get("pipeline_assessment")) for row in included)),
            "benchmark_disposition_counts": dict(Counter(str(row.get("benchmark_disposition")) for row in included + dismissed)),
            "applied_normal_reviews": applied,
            "applied_meta_review_alternatives": meta_alternatives,
            "replaced_sparql_revisions": replaced_sparql_revisions,
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
        "gold_question_policy": gold_question_policy,
        "conflict_policy": {
            "overlapping_normal_reviews": "replace_only_attested_re_reviewed_sparql_revisions; otherwise fail",
        },
        "release_boundary": {
            "public_release_files": list(PUBLIC_RELEASE_FILES),
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
