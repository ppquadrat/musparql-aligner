#!/usr/bin/env python3
"""Attach normalized catalog source identifiers to existing pipeline artefacts."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from musparql.source_catalog import load_hydrated_seeds, load_source_catalog, source_for_locator


def read_jsonl(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8-sig").splitlines() if line.strip()]


def write_jsonl(path: Path, records: Iterable[Dict[str, Any]]) -> None:
    path.write_text(
        "".join(json.dumps(record, ensure_ascii=False) + "\n" for record in records),
        encoding="utf-8",
    )


def public_provenance(source: Dict[str, Any]) -> Dict[str, Any]:
    return {
        field: source[field]
        for field in ("type", "title", "url", "local_path", "derived_from", "description")
        if source.get(field) not in (None, "", [])
    }


def cached_source_url(path: Path) -> str:
    """Return the captured URL from a text snapshot without reading its body."""
    try:
        with path.open(encoding="utf-8") as handle:
            header = handle.readline().strip()
    except (OSError, UnicodeError):
        return ""
    return header.removeprefix("SOURCE: ").strip() if header.startswith("SOURCE: ") else ""


def detail_matches_source(detail: Dict[str, Any], source: Dict[str, Any]) -> bool:
    if detail.get("source_id") == source.get("source_id"):
        return True
    catalogue = {str(source.get("source_id")): source}
    paths = (detail.get("pipeline_path"), detail.get("source_path"))
    urls = (detail.get("source_url"), detail.get("resolved_url"))
    return any(
        source_for_locator(path, url, catalogue) is not None
        for path in paths
        for url in urls
    )


def source_file_for_record(
    source: Dict[str, Any], source_files: List[str], used_files: set[str]
) -> Optional[str]:
    local_path = str(source.get("local_path") or "")
    if local_path and local_path in source_files and local_path not in used_files:
        return local_path
    catalogue = {str(source.get("source_id")): source}
    for filename in source_files:
        if filename in used_files:
            continue
        captured_url = cached_source_url(Path(filename))
        if captured_url and source_for_locator(filename, captured_url, catalogue) is not None:
            return filename
    return None


def normalize_kgs(path: Path, seeds_path: Path, sources_path: Path) -> int:
    records = read_jsonl(path)
    hydrated = {str(item.get("kg_id")): item for item in load_hydrated_seeds(seeds_path, sources_path)}
    changed = 0
    for record in records:
        seed = hydrated.get(str(record.get("kg_id")))
        if seed is None:
            continue
        for field in ("seed_version", "review_domains", "familiarity_scopes"):
            if record.get(field) != seed.get(field):
                record[field] = seed.get(field)
                changed += 1
        source_records = seed.get("source_records") or []
        source_ids = [source.get("source_id") for source in source_records]
        if record.get("source_ids") != source_ids:
            record["source_ids"] = source_ids
            changed += 1
        existing_details = record.get("source_details")
        if not isinstance(existing_details, list):
            existing_details = []
        unmatched = [detail for detail in existing_details if isinstance(detail, dict)]
        source_files = [str(item) for item in record.get("source_files") or [] if isinstance(item, str)]
        used_files: set[str] = set()
        normalized_details: List[Dict[str, Any]] = []
        for source in source_records:
            match = next((detail for detail in unmatched if detail_matches_source(detail, source)), None)
            detail = dict(match) if match is not None else {}
            if match is not None:
                unmatched.remove(match)
                existing_pipeline_path = match.get("pipeline_path")
                if isinstance(existing_pipeline_path, str) and existing_pipeline_path:
                    used_files.add(existing_pipeline_path)
            before = json.dumps(detail, sort_keys=True, ensure_ascii=False)
            detail["source_id"] = source.get("source_id")
            detail["catalog_provenance"] = public_provenance(source)
            local_path = source.get("local_path")
            if local_path:
                detail["pipeline_path"] = local_path
                detail["source_path"] = local_path
                detail["is_local_file"] = True
                if source.get("url"):
                    detail["source_url"] = source.get("url")
                    detail["resolved_url"] = source.get("url")
            elif not detail.get("pipeline_path"):
                source_file = source_file_for_record(source, source_files, used_files)
                if source_file:
                    detail["pipeline_path"] = source_file
                    used_files.add(source_file)
            normalized_details.append(detail)
            if before != json.dumps(detail, sort_keys=True, ensure_ascii=False):
                changed += 1
        if existing_details != normalized_details:
            record["source_details"] = normalized_details
            changed += 1
    write_jsonl(path, records)
    return changed


def normalize_query_catalog(path: Path, catalogue: Dict[str, Dict[str, Any]]) -> int:
    records = read_jsonl(path)
    changed = 0
    for record in records:
        evidence_items = record.get("evidence")
        if not isinstance(evidence_items, list):
            continue
        for evidence in evidence_items:
            if not isinstance(evidence, dict):
                continue
            source = source_for_locator(evidence.get("source_path"), evidence.get("source_url"), catalogue)
            if source is None:
                continue
            before = json.dumps(evidence, sort_keys=True, ensure_ascii=False)
            evidence["source_id"] = source.get("source_id")
            if source.get("url"):
                evidence["source_catalog_url"] = source.get("url")
            captured_url = cached_source_url(Path(str(evidence.get("source_path") or "")))
            if captured_url:
                evidence["source_url"] = captured_url
            if before != json.dumps(evidence, sort_keys=True, ensure_ascii=False):
                changed += 1
    if records:
        write_jsonl(path, records)
    return changed


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sources", default="catalog/sources.yaml")
    parser.add_argument("--seeds", default="catalog/seeds.yaml")
    parser.add_argument("--kgs", default="catalog/kgs.jsonl")
    parser.add_argument("--query-catalog", action="append", default=[])
    args = parser.parse_args()

    sources_path = Path(args.sources)
    catalogue = load_source_catalog(sources_path)
    print(f"{args.kgs}: normalized {normalize_kgs(Path(args.kgs), Path(args.seeds), sources_path)} fields")
    query_paths = [Path("var/queries/kg_queries.jsonl")]
    query_paths.extend(Path(path) for path in args.query_catalog)
    for path in query_paths:
        if path.exists():
            print(f"{path}: normalized {normalize_query_catalog(path, catalogue)} evidence records")


if __name__ == "__main__":
    main()
