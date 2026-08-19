#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from musparql.holdout_selectors import add_holdout_filter_arguments, holdout_input_policy, validate_selector_record
from musparql.reviewer_provenance import validate_reviewer_id
from scripts.runs.build_run_snapshot import create_run_snapshot
from musparql.sparql_corrections import assert_input_provenance_current

BENCHMARK_FILES = {
    "included": "included.jsonl",
    "excluded": "dismissed.jsonl",
}


def stable_json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def load_json_records(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")
    raw = path.read_text(encoding="utf-8", errors="ignore")
    stripped = raw.lstrip("\ufeff").lstrip()
    if not stripped:
        return []
    if stripped.startswith("["):
        data = json.loads(stripped)
        if not isinstance(data, list):
            raise ValueError(f"Expected JSON array in {path}")
        return [item for item in data if isinstance(item, dict)]

    rows: List[Dict[str, Any]] = []
    for line in raw.splitlines():
        line = line.strip().lstrip("\ufeff")
        if not line:
            continue
        item = json.loads(line)
        if isinstance(item, dict):
            rows.append(item)
    return rows


def load_optional_json_records(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    return load_json_records(path)


def load_selector_keys(path: Optional[Path]) -> set[Tuple[str, str]]:
    """Load annotation-free holdout selectors under the identity-visible policy."""
    if path is None:
        return set()
    selectors: set[Tuple[str, str]] = set()
    for record in load_json_records(path):
        selectors.add(validate_selector_record(record))
    return selectors


def load_previous_benchmark(path: Optional[Path]) -> Dict[Tuple[str, str], Dict[str, Any]]:
    if path is None:
        return {}
    expected_paths = [path / "benchmark.jsonl", *(path / filename for filename in BENCHMARK_FILES.values())]
    if not any(candidate.exists() for candidate in expected_paths):
        raise FileNotFoundError(f"Previous benchmark has no review records: {path}")
    records: Dict[Tuple[str, str], Dict[str, Any]] = {}
    sources = [("included", path / "benchmark.jsonl")]
    sources.extend((disposition, path / filename) for disposition, filename in BENCHMARK_FILES.items())
    for disposition, source_path in sources:
        for rec in load_optional_json_records(source_path):
            kg_id = str(rec.get("kg_id") or "")
            query_id = str(rec.get("query_id") or "")
            if not kg_id or not query_id:
                continue
            assessment = str(rec.get("pipeline_assessment") or "")
            split = str(rec.get("split") or "")
            records[(kg_id, query_id)] = {
                "reviewed": True,
                "benchmark_id": rec.get("benchmark_id"),
                "review_id": (
                    rec.get("review", {}).get("review_id")
                    if isinstance(rec.get("review"), dict) else None
                ) or (
                    rec.get("provenance", {}).get("approval_review_id")
                    if isinstance(rec.get("provenance"), dict) else None
                ) or rec.get("benchmark_id"),
                "reviewer_id": rec.get("provenance", {}).get("reviewer_id")
                if isinstance(rec.get("provenance"), dict) else None,
                "benchmark_disposition": disposition,
                "pipeline_assessment": assessment,
                "split": split,
                "source_benchmark": str(path),
                "sparql": rec.get("sparql"),
                "sparql_version": rec.get("sparql_version"),
                "sparql_hash": rec.get("sparql_hash"),
            }
    return records


def canonical_sparql(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    return "\n".join(line.rstrip() for line in value.replace("\r\n", "\n").split("\n")).strip()


def previous_review_matches(previous: Dict[str, Any], source_input: Dict[str, Any]) -> bool:
    """Only reuse a prior decision when it assessed the same retained SPARQL."""
    previous_hash = previous.get("sparql_hash")
    current_hash = source_input.get("sparql_hash")
    if isinstance(previous_hash, str) and isinstance(current_hash, str):
        if previous_hash != current_hash:
            return False
        previous_version = previous.get("sparql_version")
        current_version = source_input.get("sparql_version")
        return previous_version is None or current_version is None or previous_version == current_version
    previous_text = canonical_sparql(previous.get("sparql"))
    if previous_text:
        return previous_text == canonical_sparql(source_input.get("sparql_clean"))
    # Old snapshots that stored neither text nor hashes cannot be disambiguated;
    # retain their historical ID-based behavior.
    return previous_hash is None


def signature_token(record: Dict[str, Any], idx: int) -> str:
    signature = record.get("run_signature")
    if isinstance(signature, dict) and signature:
        return sha256_text(stable_json_dumps(signature))[:12]
    model = str(record.get("model") or "unknown")
    return f"{model}-{idx:04d}"


def output_meta(record: Dict[str, Any]) -> Dict[str, Any]:
    request_config = record.get("request_config")
    response_metadata = record.get("response_metadata")
    meta = {
        "model": record.get("model"),
        "elapsed_ms": record.get("elapsed_ms"),
        "generated_at": record.get("generated_at"),
        "run_signature": record.get("run_signature"),
    }
    if isinstance(request_config, dict):
        meta["request_config"] = request_config
        meta["requested_model"] = request_config.get("requested_model")
        meta["generation_parameters"] = request_config.get("generation_parameters")
    if isinstance(response_metadata, dict):
        meta["response_metadata"] = response_metadata
        meta["response_model"] = response_metadata.get("model")
    return meta


def build_input_index(records: Iterable[Dict[str, Any]]) -> Dict[Tuple[str, str, str], Dict[str, Any]]:
    index: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
    for rec in records:
        key = (
            str(rec.get("kg_id") or ""),
            str(rec.get("query_id") or ""),
            str(rec.get("query_label") or ""),
        )
        index[key] = rec
    return index


def load_json(path: Path) -> Dict[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"Expected JSON object in {path}")
    return data


def resolve_manifest_file(manifest_path: Path, manifest: Dict[str, Any], key: str) -> Path:
    files = manifest.get("files")
    entry = files.get(key) if isinstance(files, dict) else None
    filename = entry.get("filename") if isinstance(entry, dict) else None
    if not isinstance(filename, str) or not filename:
        raise ValueError(f"Run manifest has no {key} file: {manifest_path}")
    path = (manifest_path.parent / filename).resolve()
    if not path.is_relative_to(manifest_path.parent.resolve()):
        raise ValueError(f"Run manifest file escapes its run directory: {manifest_path}")
    if not path.exists():
        raise FileNotFoundError(f"Run manifest file is missing: {path}")
    return path


def resolve_latest_frozen_run(runs_root: Path) -> Tuple[Path, Path, Path]:
    candidates: List[Tuple[str, str, Path, Dict[str, Any]]] = []
    for manifest_path in runs_root.glob("*/manifest.json"):
        manifest = load_json(manifest_path)
        created_at = str(manifest.get("created_at") or "")
        run_id = str(manifest.get("generation_run_id") or manifest.get("run_id") or manifest_path.parent.name)
        if created_at:
            candidates.append((created_at, run_id, manifest_path.resolve(), manifest))
    if not candidates:
        raise FileNotFoundError(f"No frozen run manifests found under {runs_root}")
    _, run_id, manifest_path, manifest = max(candidates, key=lambda item: (item[0], item[1]))
    inputs_path = resolve_manifest_file(manifest_path, manifest, "llm_inputs")
    outputs_path = resolve_manifest_file(manifest_path, manifest, "llm_outputs")
    if not load_json_records(outputs_path):
        raise ValueError(f"Latest frozen run has empty outputs: {run_id}")
    return inputs_path, outputs_path, manifest_path


def infer_run_manifest(output_path: Path, explicit_manifest: Optional[Path]) -> Optional[Path]:
    candidates: List[Path] = []
    if explicit_manifest is not None:
        candidates.append(explicit_manifest)
    candidates.extend(
        [
            output_path.parent / "manifest.json",
            output_path.parent.parent / "manifest.json",
        ]
    )
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def infer_errors_path(output_path: Path) -> Optional[Path]:
    name = output_path.name
    if name.endswith(".jsonl"):
        candidate = output_path.with_name(name[:-6] + ".errors.jsonl")
        if candidate.exists():
            return candidate
    return None


def slugify(text: str) -> str:
    cleaned = "".join(ch.lower() if ch.isalnum() else "-" for ch in text)
    while "--" in cleaned:
        cleaned = cleaned.replace("--", "-")
    return cleaned.strip("-") or "run"


def default_run_id(output_path: Path) -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d-%H%M%S")
    return f"{stamp}-{slugify(output_path.stem)}"


def ensure_single_run_manifest(
    *,
    output_paths: List[Path],
    inputs_path: Path,
    prompt_path: Path,
    schema_path: Path,
    examples_path: Optional[Path],
    kgs_path: Optional[Path],
    kg_queries_path: Optional[Path],
    explicit_run_manifest: Optional[Path],
    explicit_run_id: str,
    freeze_enabled: bool,
) -> Optional[Path]:
    manifests = []
    for output_path in output_paths:
        manifest = infer_run_manifest(output_path, explicit_run_manifest)
        if manifest is not None:
            manifests.append(manifest.resolve())
    unique_manifests = sorted({str(path) for path in manifests})
    if len(unique_manifests) == 1:
        return Path(unique_manifests[0])
    if len(unique_manifests) > 1:
        raise ValueError(f"Review bundle spans multiple runs: {unique_manifests}")
    if not freeze_enabled:
        return None
    if len(output_paths) != 1:
        raise ValueError("Automatic run freezing currently requires exactly one output file.")

    output_path = output_paths[0]
    run_id = explicit_run_id or default_run_id(output_path)
    outdir = create_run_snapshot(
        run_id=run_id,
        inputs=inputs_path,
        outputs=output_path,
        errors=infer_errors_path(output_path),
        prompt=prompt_path,
        schema=schema_path,
        examples=examples_path,
        kgs=kgs_path,
        kg_queries=kg_queries_path,
        purpose="review bundle source",
        notes="Auto-frozen by build_review_bundle.py",
        outroot=Path("var/runs"),
    )
    return outdir / "manifest.json"


def main() -> None:
    parser = argparse.ArgumentParser(description="Build a browser review bundle from LLM inputs and outputs.")
    identity = parser.add_mutually_exclusive_group(required=True)
    identity.add_argument("--reviewer-id", help="Anonymous reviewer ID for the legacy local workflow.")
    identity.add_argument(
        "--reviewer-neutral",
        action="store_true",
        help="Omit reviewer identity so the hosted portal can attribute an assignment after authentication.",
    )
    parser.add_argument("--inputs", default=None)
    parser.add_argument("--outputs", nargs="+", default=None)
    parser.add_argument(
        "--latest-run",
        action="store_true",
        help="Resolve inputs, outputs, and manifest from the newest frozen run under --runs-root.",
    )
    parser.add_argument("--runs-root", default="var/runs", help="Frozen-run directory used by --latest-run.")
    parser.add_argument("--out", default="review/review_data.js")
    parser.add_argument("--prompt", default="prompts/llm_nl_generation.prompt.txt")
    parser.add_argument("--schema", default="schemas/llm_output.schema.json")
    parser.add_argument("--examples", default="prompts/llm_nl_generation.examples.jsonl")
    parser.add_argument("--kgs", default="catalog/kgs.jsonl")
    parser.add_argument("--kg-queries", default="var/queries/kg_queries.jsonl")
    parser.add_argument(
        "--kg-id",
        action="append",
        default=[],
        help="Include every reviewable record for this KG; may be repeated.",
    )
    parser.add_argument(
        "--query-label",
        action="append",
        default=[],
        help="Include this query label; may be repeated and is combined with --kg-id as a union.",
    )
    parser.add_argument("--run-manifest", default="", help="Optional run manifest to attach explicit run metadata.")
    parser.add_argument("--run-id", default="", help="Optional run id to use when auto-freezing a run.")
    parser.add_argument("--no-freeze", action="store_true", help="Do not auto-freeze a run when no manifest is found.")
    parser.add_argument(
        "--previous-benchmark",
        default="",
        help="Optional benchmark directory used to exclude previously reviewed pairs from initial review.",
    )
    add_holdout_filter_arguments(parser)
    parser.add_argument(
        "--include-reviewed",
        action="store_true",
        help="Include non-holdout pairs already present in the previous benchmark.",
    )
    parser.add_argument(
        "--reveal-previous-decision",
        action="store_true",
        help="When reviewed pairs are included, expose their previous judgement in the UI bundle.",
    )
    parser.add_argument(
        "--assert-complete-review-provenance",
        action="store_true",
        help="Human assertion that supplied benchmark/review sources cover every prior reviewer decision; required to enable holdout selection.",
    )
    args = parser.parse_args()
    reviewer_id = validate_reviewer_id(args.reviewer_id) if args.reviewer_id else None

    if args.latest_run:
        if args.inputs is not None or args.outputs is not None or args.run_manifest:
            raise ValueError("--latest-run cannot be combined with --inputs, --outputs, or --run-manifest")
        inputs_path, latest_output_path, latest_manifest_path = resolve_latest_frozen_run(Path(args.runs_root))
        output_paths = [latest_output_path]
        explicit_run_manifest = latest_manifest_path
        print(f"Using latest frozen run: {latest_manifest_path.parent.name}")
    else:
        inputs_path = Path(args.inputs or "var/llm/inputs.jsonl")
        output_paths = [Path(p) for p in (args.outputs or ["var/llm/outputs.jsonl"])]
        explicit_run_manifest = Path(args.run_manifest) if args.run_manifest else None
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    prompt_path = Path(args.prompt)
    schema_path = Path(args.schema)
    examples_path = Path(args.examples) if args.examples else None
    kgs_path = Path(args.kgs) if args.kgs else None
    kg_queries_path = Path(args.kg_queries) if args.kg_queries else None
    previous_benchmark_path = Path(args.previous_benchmark) if args.previous_benchmark else None
    previous_benchmark = load_previous_benchmark(previous_benchmark_path)
    holdout_selector_keys = load_selector_keys(Path(args.holdout_selectors) if args.holdout_selectors else None)
    selected_kg_ids = {str(value) for value in args.kg_id if str(value)}
    selected_query_labels = {str(value) for value in args.query_label if str(value)}
    scope_counts = {
        "new_records": 0,
        "previously_reviewed_records": 0,
        "previously_reviewed_excluded": 0,
        "holdout_excluded": 0,
    }

    run_manifest_for_bundle = ensure_single_run_manifest(
        output_paths=output_paths,
        inputs_path=inputs_path,
        prompt_path=prompt_path,
        schema_path=schema_path,
        examples_path=examples_path,
        kgs_path=kgs_path,
        kg_queries_path=kg_queries_path,
        explicit_run_manifest=explicit_run_manifest,
        explicit_run_id=args.run_id,
        freeze_enabled=not args.no_freeze,
    )

    input_records = load_json_records(inputs_path)
    if args.assert_complete_review_provenance:
        if kg_queries_path is None:
            raise ValueError("Canonical --kg-queries is required for holdout provenance checks")
        assert_input_provenance_current(input_records, load_json_records(kg_queries_path))
    input_index = build_input_index(input_records)

    review_records: List[Dict[str, Any]] = []
    run_summaries: Dict[str, Dict[str, Any]] = {}
    for output_path in output_paths:
        output_records = load_json_records(output_path)
        run_label = output_path.stem
        run_manifest_path = infer_run_manifest(output_path, run_manifest_for_bundle)
        run_manifest: Dict[str, Any] = {}
        run_id = ""
        if run_manifest_path is not None:
            run_manifest = load_json(run_manifest_path)
            run_id = str(run_manifest.get("generation_run_id") or run_manifest.get("run_id") or "")
            if run_id:
                run_summaries[run_id] = {
                    "run_id": run_id,
                    "generation_run_id": run_id,
                    "manifest_path": str(run_manifest_path),
                    "purpose": run_manifest.get("purpose"),
                    "created_at": run_manifest.get("created_at"),
                }
        for idx, rec in enumerate(output_records, start=1):
            kg_id = str(rec.get("kg_id") or "")
            query_id = str(rec.get("query_id") or "")
            query_label = str(rec.get("query_label") or "")
            if (selected_kg_ids or selected_query_labels) and not (
                kg_id in selected_kg_ids or query_label in selected_query_labels
            ):
                continue
            key = (kg_id, query_id, query_label)
            source_input = input_index.get(key, {})
            previous_candidate = previous_benchmark.get((kg_id, query_id))
            if (kg_id, query_id) in holdout_selector_keys:
                scope_counts["holdout_excluded"] += 1
                continue
            previous_review = (
                previous_candidate
                if previous_candidate and previous_review_matches(previous_candidate, source_input)
                else None
            )
            has_prior_pair_review = previous_candidate is not None
            if previous_review and not args.include_reviewed:
                scope_counts["previously_reviewed_excluded"] += 1
                continue
            review_scope = "previously_reviewed" if previous_review else "new"
            if review_scope == "previously_reviewed":
                scope_counts["previously_reviewed_records"] += 1
            else:
                scope_counts["new_records"] += 1
            previous_review_payload: Optional[Dict[str, Any]] = None
            if previous_review:
                previous_review_payload = {
                    "reviewed": True,
                    "source_benchmark": previous_review.get("source_benchmark"),
                }
                if args.reveal_previous_decision:
                    previous_review_payload.update(
                        {
                            "benchmark_id": previous_review.get("benchmark_id"),
                            "benchmark_disposition": previous_review.get("benchmark_disposition"),
                            "pipeline_assessment": previous_review.get("pipeline_assessment"),
                            "split": previous_review.get("split"),
                        }
                    )
            token = signature_token(rec, idx)
            review_id = f"{kg_id}::{query_label}::{token}"
            review_record = {
                "review_id": review_id,
                "prior_review_ids": [str(previous_candidate["review_id"])]
                if previous_candidate and previous_candidate.get("review_id") else [],
                "review_scope": review_scope,
                "has_prior_pair_review": has_prior_pair_review,
                "run_id": run_id or None,
                "generation_run_id": run_id or None,
                "run_label": run_label,
                "source_file": str(output_path),
                "run_manifest": str(run_manifest_path) if run_manifest_path is not None else None,
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
                "output": rec.get("llm_output"),
                "output_meta": output_meta(rec),
            }
            if previous_review_payload:
                review_record["previous_review"] = previous_review_payload
            review_records.append(review_record)

    review_records.sort(
        key=lambda rec: (
            str(rec.get("kg_id") or ""),
            str(rec.get("query_label") or ""),
            str(rec.get("run_label") or ""),
        )
    )

    if args.assert_complete_review_provenance:
        missing_edit_provenance = [
            rec["review_id"]
            for rec in review_records
            if not isinstance(rec.get("input", {}).get("sparql_provenance"), dict)
            or isinstance(rec["input"]["sparql_provenance"].get("retained_edit_count"), bool)
            or not isinstance(rec["input"]["sparql_provenance"].get("retained_edit_count"), int)
            or rec["input"]["sparql_provenance"]["retained_edit_count"] < 0
        ]
        if missing_edit_provenance:
            raise ValueError(
                "Cannot enable holdout selection: SPARQL edit provenance is missing or invalid for "
                + ", ".join(missing_edit_provenance[:5])
            )

    dataset_payload = {
        "schema": "musparql.review-bundle.v2",
        "dataset_id": sha256_text(
            stable_json_dumps(
                {
                    "inputs": str(inputs_path),
                    "outputs": [str(p) for p in output_paths],
                    "previous_benchmark": str(previous_benchmark_path) if previous_benchmark_path is not None else "",
                    "include_reviewed": args.include_reviewed,
                    "reveal_previous_decision": args.reveal_previous_decision,
                    "records": [
                        {
                            "review_id": rec["review_id"],
                            "query_id": rec["query_id"],
                            "run_label": rec["run_label"],
                        }
                        for rec in review_records
                    ],
                }
            )
        )[:16],
        "built_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "inputs_path": str(inputs_path),
        "output_paths": [str(p) for p in output_paths],
        "run_ids": sorted(run_summaries.keys()),
        "generation_run_ids": sorted(run_summaries.keys()),
        "single_run_id": sorted(run_summaries.keys())[0] if len(run_summaries) == 1 else None,
        "single_generation_run_id": sorted(run_summaries.keys())[0] if len(run_summaries) == 1 else None,
        "runs": [run_summaries[run_id] for run_id in sorted(run_summaries.keys())],
        "record_count": len(review_records),
        "review_scope_policy": {
            "previous_benchmark_path": str(previous_benchmark_path) if previous_benchmark_path is not None else None,
            "include_reviewed": bool(args.include_reviewed),
            "reveal_previous_decision": bool(args.reveal_previous_decision),
            "default_scope": "new" if previous_benchmark_path is not None else "all",
            "counts": scope_counts,
        },
        "holdout_review_provenance_complete": bool(args.assert_complete_review_provenance),
        "holdout_input_policy": holdout_input_policy(args),
        "pipeline_assessment_definitions": {
            "accepted": "The candidate formulation is acceptable.",
            "prompt_improvement_recommended": "The canonical pair is valid, but prompt or model behaviour should improve.",
            "input_data_improvement_recommended": "The canonical pair is valid, but generation inputs are wrong, incomplete, noisy, or missing key signals.",
            "not_applicable": "No generated formulation was assessed.",
        },
        "records": review_records,
    }
    if reviewer_id is not None:
        dataset_payload["reviewer_id"] = reviewer_id

    out_path.write_text(
        "window.REVIEW_DATA = " + json.dumps(dataset_payload, ensure_ascii=False, indent=2) + ";\n",
        encoding="utf-8",
    )
    print(f"Wrote {len(review_records)} review records to {out_path}")


if __name__ == "__main__":
    main()
