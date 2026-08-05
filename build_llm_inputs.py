#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, Iterable, List, Set, Tuple

from holdout_selectors import add_holdout_filter_arguments, validate_selector_record, validate_selectors_current
from sparql_corrections import sparql_provenance
from sparql_versions import resolve_sparql_version


SPARQL_BLOCK_EVIDENCE_TYPES = {
    "repo_file",
    "md_pre",
    "md_fence",
    "doc_pre",
    "doc_fence",
    "doc_pdf",
    "curated_query",
}


def load_jsonl(path: Path) -> List[Dict[str, object]]:
    records: List[Dict[str, object]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            records.append(json.loads(line))
    return records


def load_query_pairs(path: Path) -> Set[Tuple[str, str]]:
    pairs: Set[Tuple[str, str]] = set()
    if not path.exists():
        return pairs
    for rec in load_jsonl(path):
        kg_id, query_id = rec.get("kg_id"), rec.get("query_id")
        if isinstance(kg_id, str) and kg_id and isinstance(query_id, str) and query_id:
            pairs.add((kg_id, query_id))
    return pairs


def load_excluded_pairs(path: Path) -> Set[Tuple[str, str]]:
    if path.is_dir():
        return load_query_pairs(path / "dismissed.jsonl")
    if not path.exists():
        raise FileNotFoundError(f"Benchmark exclusion records not found: {path}")
    return load_query_pairs(path)


def load_dismissed_pairs(path: Path) -> Set[Tuple[str, str]]:
    return load_excluded_pairs(path)


def load_exclusion_policy(path: Path) -> Tuple[List[Dict[str, object]], Set[Tuple[str, str]]]:
    """Load version-aware public dismissal records."""
    if path.is_dir():
        dismissed_path = path / "dismissed.jsonl"
        dismissed = load_jsonl(dismissed_path) if dismissed_path.exists() else []
        return dismissed, set()
    if not path.exists():
        raise FileNotFoundError(f"Benchmark exclusion records not found: {path}")
    records = load_jsonl(path)
    if any(
        item.get("split") == "private_holdout"
        or item.get("benchmark_disposition") in {"withheld", "holdout"}
        for item in records
    ):
        raise ValueError("Private annotations are not valid dismissal inputs; use an annotation-free selector file")
    return records, set()


def load_holdout_selectors(path: Path) -> Set[Tuple[str, str]]:
    selectors: Set[Tuple[str, str]] = set()
    if not path.exists():
        raise FileNotFoundError(f"Holdout selector records not found: {path}")
    for item in load_jsonl(path):
        selectors.add(validate_selector_record(item))
    return selectors


def canonical_sparql(value: object) -> str:
    if not isinstance(value, str):
        return ""
    return "\n".join(line.rstrip() for line in value.replace("\r\n", "\n").split("\n")).strip()


def dismissed_record_matches(payload: Dict[str, object], dismissed: Dict[str, object]) -> bool:
    if (
        payload.get("query_id") != dismissed.get("query_id")
        or payload.get("kg_id") != dismissed.get("kg_id")
    ):
        return False
    old_hash = dismissed.get("sparql_hash")
    new_hash = payload.get("sparql_hash")
    if isinstance(old_hash, str) and isinstance(new_hash, str):
        if old_hash != new_hash:
            return False
        old_version = dismissed.get("sparql_version")
        new_version = payload.get("sparql_version")
        return old_version is None or new_version is None or old_version == new_version
    old_sparql = canonical_sparql(dismissed.get("sparql"))
    if old_sparql:
        return old_sparql == canonical_sparql(payload.get("sparql_clean"))
    return True


def iter_evidence(
    evidence: Iterable[object],
    include_sparql_blocks: bool,
) -> List[Dict[str, object]]:
    out: List[Dict[str, object]] = []
    for ev in evidence:
        if not isinstance(ev, dict):
            continue
        ev_type = ev.get("type")
        snippet = ev.get("snippet")
        evidence_id = ev.get("evidence_id")
        if not isinstance(ev_type, str) or not isinstance(snippet, str) or not isinstance(evidence_id, str):
            continue
        if not snippet.strip():
            continue
        if not include_sparql_blocks and ev_type in SPARQL_BLOCK_EVIDENCE_TYPES:
            continue
        out.append(
            {
                "evidence_id": evidence_id,
                "type": ev_type,
                "snippet": snippet,
                "source_path": ev.get("source_path", ""),
                "source_url": ev.get("source_url", ""),
            }
        )
    return out


def build_prompt_input(
    rec: Dict[str, object],
    include_raw: bool,
    include_sparql_blocks: bool,
    sparql_version: str = "latest",
) -> Dict[str, object]:
    resolved = resolve_sparql_version(rec, sparql_version)
    payload: Dict[str, object] = {
        "query_id": rec.get("query_id"),
        "query_label": rec.get("query_label"),
        "kg_id": rec.get("kg_id"),
        "sparql_clean": resolved["sparql"],
        "sparql_version": resolved["sparql_version"],
        "sparql_hash": resolved["sparql_hash"],
        "sparql_provenance": sparql_provenance(rec, resolved),
        "evidence": iter_evidence(rec.get("evidence", []) or [], include_sparql_blocks),
        "schema_ref": "schemas/llm_output.schema.json",
    }
    if include_raw:
        payload["sparql_raw"] = rec.get("sparql_raw")
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(description="Build LLM prompt inputs from kg_queries.jsonl.")
    parser.add_argument(
        "--input",
        default="kg_queries.jsonl",
        help="Input query records JSONL (default: kg_queries.jsonl)",
    )
    parser.add_argument(
        "--output",
        default="llm_inputs.jsonl",
        help="Output LLM input JSONL (default: llm_inputs.jsonl)",
    )
    parser.add_argument(
        "--include-raw-sparql",
        action="store_true",
        help="Include sparql_raw in each payload.",
    )
    parser.add_argument(
        "--include-sparql-evidence",
        action="store_true",
        help="Keep evidence types that are likely full SPARQL blocks (repo_file/md_pre/doc_pre/etc).",
    )
    parser.add_argument(
        "--exclude-dismissed-benchmark",
        default="",
        help="Benchmark directory, or public dismissal JSONL file, whose dismissed records should be excluded from prompt inputs.",
    )
    add_holdout_filter_arguments(parser)
    parser.add_argument(
        "--sparql-version",
        default="latest",
        help="SPARQL version to put in prompt inputs: original, latest, or a non-negative integer.",
    )
    args = parser.parse_args()

    in_path = Path(args.input)
    out_path = Path(args.output)
    records = load_jsonl(in_path)
    dismissed_records, holdout_pairs = (
        load_exclusion_policy(Path(args.exclude_dismissed_benchmark))
        if args.exclude_dismissed_benchmark
        else ([], set())
    )
    selector_rows = load_jsonl(Path(args.holdout_selectors)) if args.holdout_selectors else []
    holdout_selector_keys = validate_selectors_current(selector_rows, records) if selector_rows else set()

    written = 0
    skipped_excluded = 0
    with out_path.open("w", encoding="utf-8") as f:
        for rec in records:
            raw_key = (str(rec.get("kg_id") or ""), str(rec.get("query_id") or ""))
            if raw_key in holdout_selector_keys:
                skipped_excluded += 1
                continue
            payload = build_prompt_input(
                rec,
                include_raw=args.include_raw_sparql,
                include_sparql_blocks=args.include_sparql_evidence,
                sparql_version=args.sparql_version,
            )
            query_id = payload.get("query_id")
            if isinstance(query_id, str) and (
                (str(payload.get("kg_id") or ""), query_id) in holdout_pairs
                or any(dismissed_record_matches(payload, item) for item in dismissed_records)
            ):
                skipped_excluded += 1
                continue
            if not isinstance(payload.get("query_id"), str) or not isinstance(payload.get("kg_id"), str):
                continue
            if not isinstance(payload.get("sparql_clean"), str) or not payload["sparql_clean"].strip():
                continue
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")
            written += 1

    print(f"Wrote {written} prompt payloads to {out_path.resolve()}")
    if skipped_excluded:
        print(f"Skipped {skipped_excluded} dismissed/holdout benchmark records")


if __name__ == "__main__":
    main()
