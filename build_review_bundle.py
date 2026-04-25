#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from runs.build_run_snapshot import create_run_snapshot


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


def signature_token(record: Dict[str, Any], idx: int) -> str:
    signature = record.get("run_signature")
    if isinstance(signature, dict) and signature:
        return sha256_text(stable_json_dumps(signature))[:12]
    model = str(record.get("model") or "unknown")
    return f"{model}-{idx:04d}"


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
        outroot=Path("runs"),
    )
    return outdir / "manifest.json"


def main() -> None:
    parser = argparse.ArgumentParser(description="Build a browser review bundle from LLM inputs and outputs.")
    parser.add_argument("--inputs", default="llm_inputs.jsonl")
    parser.add_argument("--outputs", nargs="+", default=["llm_outputs.jsonl"])
    parser.add_argument("--out", default="review/review_data.js")
    parser.add_argument("--prompt", default="prompts/llm_nl_generation.prompt.txt")
    parser.add_argument("--schema", default="schemas/llm_output.schema.json")
    parser.add_argument("--examples", default="prompts/llm_nl_generation.examples.jsonl")
    parser.add_argument("--kgs", default="kgs.jsonl")
    parser.add_argument("--kg-queries", default="kg_queries.jsonl")
    parser.add_argument("--run-manifest", default="", help="Optional run manifest to attach explicit run metadata.")
    parser.add_argument("--run-id", default="", help="Optional run id to use when auto-freezing a run.")
    parser.add_argument("--no-freeze", action="store_true", help="Do not auto-freeze a run when no manifest is found.")
    args = parser.parse_args()

    inputs_path = Path(args.inputs)
    output_paths = [Path(p) for p in args.outputs]
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    explicit_run_manifest = Path(args.run_manifest) if args.run_manifest else None
    prompt_path = Path(args.prompt)
    schema_path = Path(args.schema)
    examples_path = Path(args.examples) if args.examples else None
    kgs_path = Path(args.kgs) if args.kgs else None
    kg_queries_path = Path(args.kg_queries) if args.kg_queries else None

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
            run_id = str(run_manifest.get("run_id") or "")
            if run_id:
                run_summaries[run_id] = {
                    "run_id": run_id,
                    "manifest_path": str(run_manifest_path),
                    "purpose": run_manifest.get("purpose"),
                    "created_at": run_manifest.get("created_at"),
                }
        for idx, rec in enumerate(output_records, start=1):
            kg_id = str(rec.get("kg_id") or "")
            query_id = str(rec.get("query_id") or "")
            query_label = str(rec.get("query_label") or "")
            key = (kg_id, query_id, query_label)
            source_input = input_index.get(key, {})
            token = signature_token(rec, idx)
            review_id = f"{kg_id}::{query_label}::{token}"
            review_records.append(
                {
                    "review_id": review_id,
                    "run_id": run_id or None,
                    "run_label": run_label,
                    "source_file": str(output_path),
                    "run_manifest": str(run_manifest_path) if run_manifest_path is not None else None,
                    "kg_id": kg_id,
                    "query_id": query_id,
                    "query_label": query_label,
                    "input": {
                        "sparql_clean": source_input.get("sparql_clean"),
                        "schema_ref": source_input.get("schema_ref"),
                        "evidence": source_input.get("evidence", []),
                    },
                    "output": rec.get("llm_output"),
                    "output_meta": {
                        "model": rec.get("model"),
                        "elapsed_ms": rec.get("elapsed_ms"),
                        "generated_at": rec.get("generated_at"),
                        "run_signature": rec.get("run_signature"),
                    },
                }
            )

    review_records.sort(
        key=lambda rec: (
            str(rec.get("kg_id") or ""),
            str(rec.get("query_label") or ""),
            str(rec.get("run_label") or ""),
        )
    )

    dataset_payload = {
        "dataset_id": sha256_text(
            stable_json_dumps(
                {
                    "inputs": str(inputs_path),
                    "outputs": [str(p) for p in output_paths],
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
        "single_run_id": sorted(run_summaries.keys())[0] if len(run_summaries) == 1 else None,
        "runs": [run_summaries[run_id] for run_id in sorted(run_summaries.keys())],
        "record_count": len(review_records),
        "review_status_definitions": {
            "approve": "Keep this example in the benchmark as-is.",
            "dismiss": "Exclude this example from the benchmark going forward.",
            "needs_prompt_fix": "The example is valid, but the model behavior should improve through prompt changes.",
            "needs_data_fix": "The example may be valid, but the model inputs are wrong, incomplete, noisy, or missing key signals.",
        },
        "records": review_records,
    }

    out_path.write_text(
        "window.REVIEW_DATA = " + json.dumps(dataset_payload, ensure_ascii=False, indent=2) + ";\n",
        encoding="utf-8",
    )
    print(f"Wrote {len(review_records)} review records to {out_path}")


if __name__ == "__main__":
    main()
