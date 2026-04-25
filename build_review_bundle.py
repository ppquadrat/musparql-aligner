#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


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


def main() -> None:
    parser = argparse.ArgumentParser(description="Build a browser review bundle from LLM inputs and outputs.")
    parser.add_argument("--inputs", default="llm_inputs.jsonl")
    parser.add_argument("--outputs", nargs="+", default=["llm_outputs.jsonl"])
    parser.add_argument("--out", default="review/review_data.js")
    args = parser.parse_args()

    inputs_path = Path(args.inputs)
    output_paths = [Path(p) for p in args.outputs]
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    input_records = load_json_records(inputs_path)
    input_index = build_input_index(input_records)

    review_records: List[Dict[str, Any]] = []
    for output_path in output_paths:
        output_records = load_json_records(output_path)
        run_label = output_path.stem
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
                    "run_label": run_label,
                    "source_file": str(output_path),
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
