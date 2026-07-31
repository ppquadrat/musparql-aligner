#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

from build_benchmark import AMBIGUITY_FILE, benchmark_gold_records, read_json, sort_ambiguity_records, write_json, write_jsonl
from update_benchmark import approved_records_path, holdout_records_path, pair_key, read_jsonl


def sort_records(records: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return sorted(records, key=lambda rec: (str(rec.get("kg_id") or ""), str(rec.get("query_label") or "")))


def normalize_sparql(text: str) -> str:
    return "\n".join(line.rstrip() for line in text.strip().splitlines()).strip()


def parse_curated_markdown(path: Path) -> List[Dict[str, Any]]:
    text = path.read_text(encoding="utf-8")
    challenge = ""
    entries: List[Dict[str, Any]] = []
    for match in re.finditer(r"(?ms)^(# Challenge .+?)\n(.*?)(?=^# Challenge |\Z)", text):
        challenge = " ".join(match.group(1).lstrip("#").strip().split())
        section = match.group(2)
        for cq_match in re.finditer(r"(?ms)^## CQ\s+(.+?)\n(.*?)(?=^## CQ |\Z)", section):
            cq = cq_match.group(1).strip()
            body = cq_match.group(2)
            prompt_match = re.search(r"\*\*Prompt:\*\*\s*(.*)", body)
            dataset_match = re.search(r"\*\*Dataset:\*\*\s*(.*)", body)
            rows_match = re.search(r"\*\*Number of result rows:\*\*\s*(.*)", body)
            sparql_match = re.search(r"```sparql\s*(.*?)```", body, re.S)
            prompt = prompt_match.group(1).strip() if prompt_match else ""
            sparql = normalize_sparql(sparql_match.group(1)) if sparql_match else ""
            if not prompt or prompt.casefold() == "nan" or not sparql:
                continue
            entries.append(
                {
                    "challenge": challenge,
                    "cq": cq,
                    "prompt": prompt,
                    "dataset": dataset_match.group(1).strip() if dataset_match else "",
                    "reported_result_rows": rows_match.group(1).strip() if rows_match else "",
                    "sparql": sparql,
                }
            )
    return entries


def make_query_id(sparql: str) -> str:
    return "linkedmusic__sha256:" + hashlib.sha256(sparql.encode("utf-8")).hexdigest()


def make_token(*parts: str) -> str:
    return hashlib.sha256("\n".join(parts).encode("utf-8")).hexdigest()[:12]


def make_record(entry: Dict[str, Any], index: int, source_path: Path, dataset_id: str) -> Dict[str, Any]:
    query_label = f"linkedmusic-{index:04d}"
    query_id = make_query_id(entry["sparql"])
    benchmark_id = f"linkedmusic::{query_label}::{make_token(query_id, entry['prompt'])}"
    return {
        "benchmark_id": benchmark_id,
        "kg_id": "linkedmusic",
        "query_id": query_id,
        "query_label": query_label,
        "sparql": entry["sparql"],
        "gold_question": entry["prompt"],
        "gold_question_source": "source_prompt",
        "review_status": "approve",
        "split": "public",
        "review": {
            "review_id": benchmark_id,
            "review_export": None,
            "dataset_id": dataset_id,
            "run_id": None,
            "generation_run_id": None,
            "note": "Public LinkedMusic prompt paired with manually corrected SPARQL; imported without a review UI pass.",
            "literal_wording": "",
            "updated_at": None,
        },
        "run": {
            "run_id": None,
            "generation_run_id": None,
            "run_manifest": None,
            "run_label": "curated_source",
            "source_file": str(source_path),
            "model": None,
            "run_signature": None,
        },
        "model_output": {
            "nl_question": None,
            "origin_mode": "source_prompt",
            "confidence": None,
            "confidence_rationale": None,
            "needs_review": False,
            "retained_evidence_phrases": [],
        },
        "evidence_summary": {
            "evidence_count": 1,
            "evidence_types": ["curated_source_prompt"],
            "has_source_evidence": True,
            "has_query_specific_evidence": True,
        },
        "source": {
            "source_type": "curated_linkedmusic_example",
            "source_file": str(source_path),
            "prompt_source": "public LinkedMusic challenge prompt",
            "sparql_source": "manually corrected local SPARQL",
            "curated_source_file": str(source_path),
            "challenge": entry["challenge"],
            "cq": entry["cq"],
            "dataset": entry["dataset"],
            "reported_result_rows": entry["reported_result_rows"],
            "execution_note": "SPARQL is taken from the local corrected LinkedMusic source file. Some examples may still fail against live/federated endpoints; inclusion is based on the checked NL-SPARQL pairing.",
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Add curated LinkedMusic prompt/SPARQL pairs to a benchmark snapshot.")
    parser.add_argument("--previous-benchmark", required=True, help="Previous benchmark/vN directory.")
    parser.add_argument("--source", default="curated_sources/LinkedMusic_Queries_Corrected.md")
    parser.add_argument("--outdir", required=True, help="Output benchmark/vN directory.")
    parser.add_argument("--dataset-id", default="linkedmusic-curated-2026-07-14")
    args = parser.parse_args()

    previous_dir = Path(args.previous_benchmark)
    source_path = Path(args.source)
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    entries = parse_curated_markdown(source_path)
    if len(entries) != 20:
        raise ValueError(f"Expected 20 LinkedMusic examples, found {len(entries)}")

    approved_by_key = {pair_key(rec): rec for rec in read_jsonl(approved_records_path(previous_dir))}
    pending_by_key = {pair_key(rec): rec for rec in read_jsonl(previous_dir / "pending.jsonl")}
    dismissed_by_key = {pair_key(rec): rec for rec in read_jsonl(previous_dir / "dismissed.jsonl")}
    holdout_by_key = {pair_key(rec): rec for rec in read_jsonl(holdout_records_path(previous_dir))}
    ambiguity_by_key = {pair_key(rec): rec for rec in read_jsonl(previous_dir / AMBIGUITY_FILE)}
    existing_keys = set(approved_by_key) | set(pending_by_key) | set(dismissed_by_key) | set(holdout_by_key)

    dataset_counts: Counter[str] = Counter()
    added = 0
    for index, entry in enumerate(entries, start=1):
        record = make_record(entry, index, source_path, args.dataset_id)
        key = pair_key(record)
        if key in existing_keys:
            raise ValueError(f"LinkedMusic record overlaps an existing benchmark key: {key}")
        approved_by_key[key] = record
        existing_keys.add(key)
        dataset_counts[entry["dataset"]] += 1
        added += 1

    approved = sort_records(approved_by_key.values())
    pending = sort_records(pending_by_key.values())
    dismissed = sort_records(dismissed_by_key.values())
    holdout = sort_records(holdout_by_key.values())
    previous_manifest = read_json(previous_dir / "manifest.json") if (previous_dir / "manifest.json").exists() else {}

    built_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    benchmark_version = outdir.name
    ambiguity_records = sort_ambiguity_records(list(ambiguity_by_key.values()))
    for ambiguity in ambiguity_records:
        ambiguity["benchmark_version"] = benchmark_version
        ambiguity["benchmark_built_at"] = built_at
    benchmark_records = benchmark_gold_records(
        approved=approved,
        pending=pending,
        benchmark_version=benchmark_version,
        built_at=built_at,
        source_bundle=str(source_path),
        source_review_export=str(source_path),
        dataset_id=args.dataset_id,
    )

    manifest = {
        "benchmark_version": benchmark_version,
        "built_at": built_at,
        "update_type": "curated_linkedmusic_addition",
        "previous_benchmark": str(previous_dir),
        "previous_benchmark_version": previous_manifest.get("benchmark_version"),
        "source_curated_file": str(source_path),
        "dataset_id": args.dataset_id,
        "counts": {
            "benchmark": len(benchmark_records),
            "approved": len(approved),
            "pending": len(pending),
            "dismissed": len(dismissed),
            "holdout": len(holdout),
            "ambiguity": len(ambiguity_records),
            "added_curated_linkedmusic": added,
            "linkedmusic_dataset_counts": dict(dataset_counts),
        },
        "files": {
            "benchmark": "benchmark.jsonl",
            "approved": "approved.jsonl",
            "pending": "pending.jsonl",
            "dismissed": "dismissed.jsonl",
            "holdout": "holdout.jsonl",
            "ambiguity": AMBIGUITY_FILE,
        },
        "gold_question_policy": {
            "benchmark_includes_approved": True,
            "benchmark_includes_pending_with_reviewer_rewrite": True,
            "preferred_question_used_when_present": True,
            "approved_model_output_used_otherwise": True,
            "source_prompt_used_for_curated_linkedmusic": True,
            "unchanged_previous_items_carried_forward": True,
        },
    }

    write_json(outdir / "manifest.json", manifest)
    write_jsonl(outdir / "benchmark.jsonl", benchmark_records)
    write_jsonl(outdir / "approved.jsonl", approved)
    write_jsonl(outdir / "pending.jsonl", pending)
    write_jsonl(outdir / "dismissed.jsonl", dismissed)
    write_jsonl(outdir / "holdout.jsonl", holdout)
    write_jsonl(outdir / AMBIGUITY_FILE, ambiguity_records)

    print(f"Wrote manifest to {outdir / 'manifest.json'}")
    print(f"Wrote {len(benchmark_records)} benchmark records to {outdir / 'benchmark.jsonl'}")
    print(f"Added {added} curated LinkedMusic records")


if __name__ == "__main__":
    main()
