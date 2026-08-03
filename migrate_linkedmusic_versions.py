#!/usr/bin/env python3
"""Migrate LinkedMusic's corrected standalone queries into versioned official records."""
from __future__ import annotations

import argparse
import json
import re
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Tuple

from extract_queries import build_query_record, normalize_query, sha256_hash
from sparql_versions import (
    backfill_legacy_execution_versions,
    sparql_hash,
    validate_execution_versions,
)


KG_ID = "linkedmusic"
OFFICIAL_SOURCE_ID = "linkedmusic-public-query-database"
EDIT_SOURCE_ID = "linkedmusic-corrected-examples-working-copy"
OFFICIAL_URL = (
    "https://docs.google.com/spreadsheets/d/"
    "1Bsb1fZXPUGgqAMfNQeCf4J5l0p5rLhTQMCwRc0ZVrFs/edit?gid=1479578801#gid=1479578801"
)


def read_jsonl(path: Path) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8-sig").splitlines(), start=1):
        if not line.strip():
            continue
        value = json.loads(line)
        if not isinstance(value, dict):
            raise ValueError(f"Expected object at {path}:{line_number}")
        records.append(value)
    return records


def write_jsonl(path: Path, records: Iterable[Mapping[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")


def normalize_prompt(value: Any) -> str:
    return " ".join(str(value or "").split()).casefold()


def parse_corrected_markdown(path: Path) -> List[Dict[str, Any]]:
    text = path.read_text(encoding="utf-8")
    entries: List[Dict[str, Any]] = []
    for challenge_match in re.finditer(r"(?ms)^# Challenge\s+(\d+)(?:[^\n]*)\n(.*?)(?=^# Challenge |\Z)", text):
        challenge = int(challenge_match.group(1))
        section = challenge_match.group(2)
        for cq_match in re.finditer(r"(?ms)^## CQ\s+(\d+)\s*\n(.*?)(?=^## CQ |\Z)", section):
            cq = int(cq_match.group(1))
            body = cq_match.group(2)
            prompt_match = re.search(r"\*\*Prompt:\*\*\s*(.*)", body)
            sparql_match = re.search(r"```sparql\s*(.*?)```", body, re.S | re.I)
            prompt = prompt_match.group(1).strip() if prompt_match else ""
            sparql = sparql_match.group(1).strip() if sparql_match else ""
            if not prompt or prompt.casefold() == "nan" or not sparql:
                continue
            entries.append(
                {
                    "challenge": challenge,
                    "cq": cq,
                    "prompt": prompt,
                    "sparql": normalize_query(sparql),
                }
            )
    if len(entries) != 20:
        raise ValueError(f"Expected 20 corrected LinkedMusic queries, found {len(entries)}")
    return entries


def official_key(record: Mapping[str, Any]) -> Tuple[int, int]:
    return int(record["challenge"]), int(record["cq"])


def record_source_ids(record: Mapping[str, Any]) -> set[str]:
    evidence = record.get("evidence")
    if not isinstance(evidence, list):
        return set()
    return {
        str(item.get("source_id"))
        for item in evidence
        if isinstance(item, dict) and item.get("source_id")
    }


def tag_execution(execution: Any, version: int, digest: str) -> Any:
    if not isinstance(execution, dict):
        return execution
    result = deepcopy(execution)
    result["sparql_version"] = version
    result["sparql_hash"] = digest
    return result


def migrate_execution_state(source: Mapping[str, Any], target: Dict[str, Any], version: int, digest: str) -> None:
    history = source.get("execution_history")
    if not isinstance(history, list):
        history = source.get("run_history")
    tagged_history = [tag_execution(item, version, digest) for item in history] if isinstance(history, list) else []
    target["execution_history"] = tagged_history
    target["run_history"] = deepcopy(tagged_history)

    latest = source.get("latest_execution")
    if not isinstance(latest, dict):
        latest = source.get("latest_run")
    tagged_latest = tag_execution(latest, version, digest) if isinstance(latest, dict) else None
    target["latest_execution"] = tagged_latest
    target["latest_run"] = deepcopy(tagged_latest)

    successful = source.get("latest_successful_execution")
    if not isinstance(successful, dict):
        successful = source.get("latest_successful_run")
    tagged_successful = tag_execution(successful, version, digest) if isinstance(successful, dict) else None
    target["latest_successful_execution"] = tagged_successful
    target["latest_successful_run"] = deepcopy(tagged_successful)


def build_canonical_record(
    official: Mapping[str, Any],
    corrected: Mapping[str, Any],
    previous: Mapping[str, Any],
    label: str,
    extracted_at: str,
) -> Dict[str, Any]:
    raw = str(official["sparql"]).strip()
    clean = normalize_query(raw, inject_missing_prefixes=False)
    clean_hash = sha256_hash(clean)
    record = build_query_record(
        kg_id=KG_ID,
        query_label=label,
        query_type="select",
        raw_query=raw,
        clean_query=clean,
        raw_hash=sha256_hash(raw),
        clean_hash=clean_hash,
    )
    edit_sparql = str(corrected["sparql"])
    edit_hash = sparql_hash(edit_sparql)
    source_is_canonical = OFFICIAL_SOURCE_ID in record_source_ids(previous)
    first_edit = {
        "version": 1,
        "sparql": edit_sparql,
        "note": "Musparql correction retained in curated_sources/LinkedMusic_Queries_Corrected.md.",
        "source_id": EDIT_SOURCE_ID,
    }
    previous_edits = previous.get("sparql_edits")
    if source_is_canonical and isinstance(previous_edits, list) and previous_edits:
        if not isinstance(previous_edits[0], Mapping):
            raise ValueError(f"Existing version 1 is not an object for {label}")
        if previous_edits[0].get("version") != 1 or previous_edits[0].get("sparql") != edit_sparql:
            raise ValueError(f"Existing version 1 no longer matches corrected source for {label}")
        retained_edits = deepcopy(previous_edits)
        retained_edits[0]["source_id"] = EDIT_SOURCE_ID
        record["sparql_edits"] = retained_edits
    else:
        record["sparql_edits"] = [first_edit]
    record["nl_question"] = {
        "text": str(official["prompt"]).strip(),
        "source": OFFICIAL_SOURCE_ID,
        "generated_at": None,
        "generator": None,
    }
    record["curated_query"] = {
        field: official.get(field)
        for field in ("challenge", "cq", "databases", "reported_result_rows")
    }
    record["evidence"] = [
        {
            "evidence_id": "e1",
            "type": "curated_query",
            "source_url": OFFICIAL_URL,
            "source_id": OFFICIAL_SOURCE_ID,
            "source_path": "curated_sources/LinkedMusic_Queries_Official.jsonl",
            "source_snapshot_metadata": "curated_sources/LinkedMusic_Queries_Official.meta.json",
            "repo_commit": "",
            "snippet": raw,
            "extracted_at": extracted_at,
            "extractor_version": "migrate_linkedmusic_versions.py@v1",
            "challenge": official["challenge"],
            "cq": official["cq"],
        }
    ]
    if source_is_canonical:
        source_fields = {
            field: deepcopy(record[field])
            for field in (
                "query_label",
                "query_id",
                "kg_id",
                "query_type",
                "sparql_raw",
                "sparql_clean",
                "sparql_hash",
                "sparql_edits",
                "raw_hash",
                "nl_question",
                "curated_query",
            )
        }
        retained = deepcopy(previous)
        retained.update(source_fields)
        # Evidence was already created from this official source. Retaining it avoids
        # timestamp churn while preserving any later provenance annotations.
        if not retained.get("evidence"):
            retained["evidence"] = record["evidence"]
        else:
            for item in retained["evidence"]:
                if isinstance(item, dict) and item.get("source_id") == OFFICIAL_SOURCE_ID:
                    item["source_snapshot_metadata"] = (
                        "curated_sources/LinkedMusic_Queries_Official.meta.json"
                    )
        record = retained
    else:
        source_fields = {
            field: deepcopy(record[field])
            for field in (
                "query_label",
                "query_id",
                "kg_id",
                "query_type",
                "sparql_raw",
                "sparql_clean",
                "sparql_hash",
                "sparql_edits",
                "raw_hash",
                "evidence",
                "nl_question",
                "curated_query",
            )
        }
        retained = deepcopy(previous)
        retained.update(source_fields)
        record = retained
        migrate_execution_state(previous, record, 1, edit_hash)
    return record


def migrate_records(
    records: List[Dict[str, Any]],
    official_records: List[Dict[str, Any]],
    corrected_records: List[Dict[str, Any]],
    *,
    extracted_at: str,
) -> tuple[List[Dict[str, Any]], Dict[str, str]]:
    if len(official_records) != 20:
        raise ValueError(f"Expected 20 official LinkedMusic queries, found {len(official_records)}")
    official_by_key = {official_key(item): item for item in official_records}
    corrected_by_key = {official_key(item): item for item in corrected_records}
    if len(official_by_key) != 20 or len(corrected_by_key) != 20:
        raise ValueError("LinkedMusic challenge/CQ keys must be unique")
    if set(official_by_key) != set(corrected_by_key):
        raise ValueError("Official and corrected LinkedMusic challenge/CQ keys do not match")
    for key in sorted(official_by_key):
        if normalize_prompt(official_by_key[key].get("prompt")) != normalize_prompt(corrected_by_key[key].get("prompt")):
            raise ValueError(f"Prompt mismatch for LinkedMusic challenge/CQ {key}")

    old_corrected = [
        record
        for record in records
        if record.get("kg_id") == KG_ID and EDIT_SOURCE_ID in record_source_ids(record)
    ]
    existing_canonical = [
        record
        for record in records
        if record.get("kg_id") == KG_ID and OFFICIAL_SOURCE_ID in record_source_ids(record)
    ]
    if old_corrected and existing_canonical:
        raise ValueError("Found both standalone corrected and canonical official LinkedMusic records")
    if len(old_corrected) not in {0, 20} or len(existing_canonical) not in {0, 20}:
        raise ValueError(
            f"Expected either 20 corrected or 20 canonical LinkedMusic records; found {len(old_corrected)} and {len(existing_canonical)}"
        )

    previous_by_edit_hash: Dict[str, Dict[str, Any]] = {}
    for record in old_corrected:
        digest = record.get("sparql_hash")
        if isinstance(digest, str):
            previous_by_edit_hash[digest] = record
    canonical_by_original_hash = {
        str(record.get("sparql_hash")): record for record in existing_canonical if record.get("sparql_hash")
    }

    replacements: List[Dict[str, Any]] = []
    old_to_new: Dict[str, str] = {}
    used_labels: set[str] = set()
    for index, key in enumerate(sorted(official_by_key), start=1):
        official = official_by_key[key]
        corrected = corrected_by_key[key]
        edit_digest = sparql_hash(str(corrected["sparql"]))
        original_clean = normalize_query(str(official["sparql"]), inject_missing_prefixes=False)
        original_digest = sparql_hash(original_clean)
        previous = previous_by_edit_hash.get(edit_digest) or canonical_by_original_hash.get(original_digest)
        if previous is None:
            raise ValueError(f"No current LinkedMusic record matches challenge/CQ {key}")
        label = str(previous.get("query_label") or f"linkedmusic-{50 + index:04d}")
        if label in used_labels:
            raise ValueError(f"Duplicate LinkedMusic query label during migration: {label}")
        used_labels.add(label)
        replacement = build_canonical_record(official, corrected, previous, label, extracted_at)
        backfill_legacy_execution_versions(replacement)
        validate_execution_versions(replacement)
        replacements.append(replacement)
        old_id = previous.get("query_id")
        if isinstance(old_id, str):
            old_to_new[old_id] = str(replacement["query_id"])

    removed_ids = {
        str(record.get("query_id"))
        for record in old_corrected + existing_canonical
        if record.get("query_id")
    }
    result = [record for record in records if str(record.get("query_id")) not in removed_ids]
    for record in result:
        if isinstance(record.get("sparql_clean"), str):
            backfill_legacy_execution_versions(record)
            validate_execution_versions(record)
    result.extend(replacements)
    if sum(1 for record in result if record.get("kg_id") == KG_ID) != 70:
        raise ValueError("LinkedMusic migration must retain exactly 70 total query records")
    if sum(1 for record in result if OFFICIAL_SOURCE_ID in record_source_ids(record)) != 20:
        raise ValueError("LinkedMusic migration must produce exactly 20 canonical official records")
    if any(EDIT_SOURCE_ID in record_source_ids(record) for record in result):
        raise ValueError("Standalone corrected LinkedMusic records remain after migration")
    return result, old_to_new


def migrate_failures(
    failures: List[Dict[str, Any]],
    old_to_new: Mapping[str, str],
    canonical_records: Iterable[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    by_id = {str(record.get("query_id")): record for record in canonical_records}
    migrated: List[Dict[str, Any]] = []
    for failure in failures:
        record = deepcopy(failure)
        old_id = record.get("query_id")
        new_id = old_to_new.get(str(old_id))
        if new_id and new_id != old_id:
            canonical = by_id[new_id]
            edit = canonical["sparql_edits"][0]
            record["query_id"] = new_id
            record["sparql_version"] = 1
            record["sparql_hash"] = sparql_hash(edit["sparql"])
        migrated.append(record)
    return migrated


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--queries", default="kg_queries.jsonl")
    parser.add_argument("--official", default="curated_sources/LinkedMusic_Queries_Official.jsonl")
    parser.add_argument("--corrected", default="curated_sources/LinkedMusic_Queries_Corrected.md")
    parser.add_argument("--failures", default="runnable_queries.failures.jsonl")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    queries_path = Path(args.queries)
    failures_path = Path(args.failures)
    official = read_jsonl(Path(args.official))
    corrected = parse_corrected_markdown(Path(args.corrected))
    records = read_jsonl(queries_path)
    extracted_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    migrated, old_to_new = migrate_records(records, official, corrected, extracted_at=extracted_at)
    failures = read_jsonl(failures_path) if failures_path.exists() else []
    migrated_failures = migrate_failures(failures, old_to_new, migrated)

    if not args.dry_run:
        write_jsonl(queries_path, migrated)
        write_jsonl(failures_path, migrated_failures)
    print(
        f"Validated {len(official)} official LinkedMusic queries and {len(corrected)} version-1 edits; "
        f"migrated {len(old_to_new)} query IDs ({'dry run' if args.dry_run else 'written'})."
    )


if __name__ == "__main__":
    main()
