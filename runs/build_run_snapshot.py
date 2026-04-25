#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        while True:
            chunk = fh.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def load_json_records(path: Path) -> List[Dict[str, Any]]:
    raw = path.read_text(encoding="utf-8", errors="ignore")
    stripped = raw.lstrip("\ufeff").lstrip()
    if not stripped:
        return []
    if stripped.startswith("["):
        data = json.loads(stripped)
        if not isinstance(data, list):
            return []
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


def copy_file(src: Path, dest: Path) -> Dict[str, Any]:
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dest)
    return {
        "filename": dest.name,
        "source_path": str(src),
        "sha256": sha256_file(dest),
        "size_bytes": dest.stat().st_size,
    }


def infer_models(output_records: List[Dict[str, Any]]) -> List[str]:
    models = sorted({str(rec.get("model")) for rec in output_records if rec.get("model")})
    return models


def create_run_snapshot(
    *,
    run_id: str,
    inputs: Path,
    outputs: Path,
    errors: Optional[Path],
    prompt: Path,
    schema: Path,
    examples: Optional[Path],
    kgs: Optional[Path],
    kg_queries: Optional[Path],
    purpose: str,
    notes: str,
    outroot: Path,
) -> Path:
    outdir = outroot / run_id
    outdir.mkdir(parents=True, exist_ok=True)

    files: Dict[str, Dict[str, Any]] = {}
    files["llm_inputs"] = copy_file(inputs, outdir / "llm_inputs.jsonl")
    files["llm_outputs"] = copy_file(outputs, outdir / "llm_outputs.jsonl")
    files["prompt"] = copy_file(prompt, outdir / "prompt.txt")
    files["schema"] = copy_file(schema, outdir / "schema.json")
    if errors and errors.exists():
        files["llm_outputs_errors"] = copy_file(errors, outdir / "llm_outputs.errors.jsonl")
    if examples and examples.exists():
        files["examples"] = copy_file(examples, outdir / "examples.jsonl")
    if kgs and kgs.exists():
        files["kgs"] = copy_file(kgs, outdir / "kgs.jsonl")
    if kg_queries and kg_queries.exists():
        files["kg_queries"] = copy_file(kg_queries, outdir / "kg_queries.jsonl")

    output_records = load_json_records(outputs)
    error_records = load_json_records(errors) if errors and errors.exists() else []
    manifest = {
        "run_id": run_id,
        "created_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "purpose": purpose or None,
        "notes": notes or None,
        "record_counts": {
            "outputs": len(output_records),
            "errors": len(error_records),
        },
        "models": infer_models(output_records),
        "files": files,
    }
    (outdir / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return outdir


def main() -> None:
    parser = argparse.ArgumentParser(description="Create a frozen run snapshot under runs/<run-id>/")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--inputs", required=True)
    parser.add_argument("--outputs", required=True)
    parser.add_argument("--errors", default="")
    parser.add_argument("--prompt", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--examples", default="")
    parser.add_argument("--kgs", default="")
    parser.add_argument("--kg-queries", default="")
    parser.add_argument("--purpose", default="")
    parser.add_argument("--notes", default="")
    parser.add_argument("--outroot", default="runs")
    args = parser.parse_args()

    outdir = create_run_snapshot(
        run_id=args.run_id,
        inputs=Path(args.inputs),
        outputs=Path(args.outputs),
        errors=Path(args.errors) if args.errors else None,
        prompt=Path(args.prompt),
        schema=Path(args.schema),
        examples=Path(args.examples) if args.examples else None,
        kgs=Path(args.kgs) if args.kgs else None,
        kg_queries=Path(args.kg_queries) if args.kg_queries else None,
        purpose=args.purpose,
        notes=args.notes,
        outroot=Path(args.outroot),
    )
    print(f"Wrote run snapshot to {outdir}")


if __name__ == "__main__":
    main()
