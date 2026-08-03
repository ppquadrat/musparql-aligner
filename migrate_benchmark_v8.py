#!/usr/bin/env python3
"""Create the adjudicated, version-pinned v8 snapshot from v7."""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

from sparql_versions import resolve_sparql_version, validate_execution_versions


SOURCE_SNAPSHOT = Path("benchmark/v7")
TARGET_SNAPSHOT = Path("benchmark/v8")
EDIT_SOURCE_ID = "musparql-manual-correction-2026-08-03"

CORRECTIONS = {
    "organs-0002": {
        "value": "organs:Part15_021AlphenaandenRijn",
        "question": "What is the organ at the Maranathakerk in Alphen aan den Rijn called?",
    },
    "organs-0003": {
        "value": "organs:Part12_063UtrechtMaria",
        "question": "When was the organ at the former Old Catholic Church of St Mary Minor in Utrecht built?",
    },
    "organs-0004": {
        "value": "organs:OI-ee4e8485b89314b0555e15c5b0b7181c",
        "question": "What external website URL is recorded for the organ at St Crispinus and Crispinianus in Saarlouis/Lisdorf?",
    },
    "organs-0005": {
        "value": "organs:FR-59350-LILLE-TEMPLE1-X",
        "question": "Which other organs share the same city or builder as the organ at the Protestant church in Lille?",
    },
    "organs-0006": {
        "value": None,
        "question": "Where and when was the organ at the Gereformeerde Gemeente in Rhenen most recently located?",
    },
    "organs-0008": {
        "value": "organs:FR-67462-SELES-STFOYY1-X",
        "question": "What are the URL and caption of the primary image for the organ at Sainte-Foy church in Sélestat?",
    },
}


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]


def write_jsonl(path: Path, rows: Iterable[Mapping[str, Any]]) -> None:
    path.write_text(
        "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows),
        encoding="utf-8",
    )


def make_token(*parts: str) -> str:
    return hashlib.sha256("\n".join(parts).encode("utf-8")).hexdigest()[:12]


def one_value_query(query: str, value: str) -> str:
    pattern = re.compile(r"(?is)(\bVALUES\s+\?organ_query_iri\s*\{).*?(\})")
    result, count = pattern.subn(rf"\1\n {value}\n \2", query)
    if count != 1:
        raise ValueError(f"Expected one organ VALUES block, found {count}")
    return result


def add_query_edits(records: list[dict[str, Any]]) -> None:
    by_label = {str(row.get("query_label")): row for row in records}
    for label, correction in CORRECTIONS.items():
        value = correction["value"]
        if value is None:
            continue
        record = by_label[label]
        edited = one_value_query(str(record["sparql_clean"]), str(value))
        expected = {
            "version": 1,
            "sparql": edited,
            "note": "Reduce an illustrative multi-identifier VALUES list to one concrete organ so the instantiated entity can be named naturally in the benchmark question.",
            "source_id": EDIT_SOURCE_ID,
        }
        existing = record.get("sparql_edits") or []
        if existing and existing != [expected]:
            raise ValueError(f"Unexpected existing SPARQL edits for {label}")
        record["sparql_edits"] = [expected]
        validate_execution_versions(record)


def canonical_query(
    row: Mapping[str, Any],
    *,
    by_id: Mapping[tuple[str, str], dict[str, Any]],
    by_label: Mapping[tuple[str, str], dict[str, Any]],
    linkedmusic_by_cq: Mapping[tuple[int, int], dict[str, Any]],
) -> dict[str, Any]:
    kg_id = str(row.get("kg_id") or "")
    query_id = str(row.get("query_id") or "")
    match = by_id.get((kg_id, query_id))
    if match is not None:
        return match
    if kg_id == "linkedmusic":
        source = row.get("source") if isinstance(row.get("source"), Mapping) else {}
        challenge_match = re.search(r"\d+", str(source.get("challenge") or ""))
        cq_match = re.search(r"\d+", str(source.get("cq") or ""))
        if challenge_match and cq_match:
            match = linkedmusic_by_cq.get((int(challenge_match.group()), int(cq_match.group())))
            if match is not None:
                return match
    match = by_label.get((kg_id, str(row.get("query_label") or "")))
    if match is None:
        raise ValueError(f"No canonical query record for {kg_id}/{query_id}")
    return match


def pin_latest_version(row: dict[str, Any], query: Mapping[str, Any]) -> None:
    resolved = resolve_sparql_version(query, "latest")
    row["query_id"] = query["query_id"]
    row["query_label"] = query["query_label"]
    row["sparql"] = resolved["sparql"]
    row["sparql_version"] = resolved["sparql_version"]
    row["sparql_hash"] = resolved["sparql_hash"]


def update_identity(row: dict[str, Any], query: Mapping[str, Any]) -> None:
    old_id = str(row.get("benchmark_id") or "")
    pin_latest_version(row, query)
    if str(row.get("kg_id")) == "linkedmusic":
        question = str(row.get("gold_question") or row.get("canonical_question") or "")
        new_id = f"linkedmusic::{query['query_label']}::{make_token(str(query['query_id']), question)}"
        row["benchmark_id"] = new_id
        review = row.get("review")
        if isinstance(review, dict) and review.get("review_id") == old_id:
            review["review_id"] = new_id
            review["internal_comment"] = (
                "Public LinkedMusic prompt paired with canonical source SPARQL version 0 "
                "and the attributed Musparql edit selected as version 1; live "
                "comparison found no improvement in coarse execution outcomes."
            )
        run = row.get("run")
        if isinstance(run, dict):
            run["source_file"] = "curated_sources/LinkedMusic_Queries_Official.jsonl"
        curated = query.get("curated_query") if isinstance(query.get("curated_query"), Mapping) else {}
        row["source"] = {
            "source_type": "curated_linkedmusic_example",
            "source_id": "linkedmusic-public-query-database",
            "source_file": "curated_sources/LinkedMusic_Queries_Official.jsonl",
            "source_url": "https://docs.google.com/spreadsheets/d/1Bsb1fZXPUGgqAMfNQeCf4J5l0p5rLhTQMCwRc0ZVrFs/edit?gid=1479578801#gid=1479578801",
            "prompt_source": "public LinkedMusic query database",
            "sparql_source": "selected retained version 1",
            "sparql_edit_source_id": "linkedmusic-corrected-examples-working-copy",
            "sparql_edit_source_file": "curated_sources/LinkedMusic_Queries_Corrected.md",
            "challenge": curated.get("challenge"),
            "cq": curated.get("cq"),
            "dataset": curated.get("databases"),
            "reported_result_rows": curated.get("reported_result_rows"),
            "execution_note": "Execution is tracked by SPARQL version and hash in kg_queries.jsonl.",
        }


def update_snapshot(included: list[dict[str, Any]], query_records: list[dict[str, Any]], adjudicated_at: str) -> None:
    queries = {str(row.get("query_label")): row for row in query_records}
    found: set[str] = set()
    for row in included:
        label = str(row.get("query_label"))
        if label not in CORRECTIONS:
            continue
        found.add(label)
        query_record = queries[label]
        pin_latest_version(row, query_record)
        row["gold_question"] = CORRECTIONS[label]["question"]
        row["gold_question_source"] = "reviewer_rewrite"
        row["pipeline_assessment"] = "input_data_improvement_recommended"
        row["adjudication"] = {
            "type": "manual_nl_sparql_alignment",
            "adjudicated_at": adjudicated_at,
            "previous_benchmark_version": "v7",
            "reason": "Name the concrete VALUES instance in the NL question; reduce illustrative multi-identifier lists to one organ where necessary.",
        }
    if found != set(CORRECTIONS):
        raise ValueError(f"Missing v7 included records: {sorted(set(CORRECTIONS) - found)}")


def migrate(*, dry_run: bool = False) -> None:
    query_path = Path("kg_queries.jsonl")
    query_records = read_jsonl(query_path)
    add_query_edits(query_records)
    if dry_run:
        return

    write_jsonl(query_path, query_records)
    TARGET_SNAPSHOT.mkdir(parents=True, exist_ok=True)

    by_id = {(str(row["kg_id"]), str(row["query_id"])): row for row in query_records}
    by_label = {(str(row["kg_id"]), str(row["query_label"])): row for row in query_records}
    linkedmusic_by_cq = {
        (int(row["curated_query"]["challenge"]), int(row["curated_query"]["cq"])): row
        for row in query_records
        if row.get("kg_id") == "linkedmusic"
        and isinstance(row.get("curated_query"), dict)
        and row.get("sparql_edits")
    }

    existing_manifest_path = TARGET_SNAPSHOT / "manifest.json"
    existing_manifest = (
        json.loads(existing_manifest_path.read_text(encoding="utf-8"))
        if existing_manifest_path.exists()
        else {}
    )
    adjudicated_at = str(existing_manifest.get("built_at") or "")
    if existing_manifest.get("benchmark_version") != "v8" or not adjudicated_at:
        adjudicated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    included = read_jsonl(SOURCE_SNAPSHOT / "included.jsonl")
    for row in included:
        query = canonical_query(
            row, by_id=by_id, by_label=by_label, linkedmusic_by_cq=linkedmusic_by_cq
        )
        update_identity(row, query)
    update_snapshot(included, query_records, adjudicated_at)
    write_jsonl(TARGET_SNAPSHOT / "included.jsonl", included)

    for filename in ("dismissed.jsonl", "holdout.jsonl"):
        rows = read_jsonl(SOURCE_SNAPSHOT / filename)
        for row in rows:
            query = canonical_query(
                row, by_id=by_id, by_label=by_label, linkedmusic_by_cq=linkedmusic_by_cq
            )
            update_identity(row, query)
        write_jsonl(TARGET_SNAPSHOT / filename, rows)

    alternatives_path = TARGET_SNAPSHOT / "alternatives.jsonl"
    alternatives = [
        row
        for row in read_jsonl(SOURCE_SNAPSHOT / "alternatives.jsonl")
        if str(row.get("query_label")) not in CORRECTIONS
    ]
    for row in alternatives:
        query = canonical_query(
            row, by_id=by_id, by_label=by_label, linkedmusic_by_cq=linkedmusic_by_cq
        )
        update_identity(row, query)
    write_jsonl(alternatives_path, alternatives)

    annotations = read_jsonl(SOURCE_SNAPSHOT / "linguistic_annotations.jsonl")
    for row in annotations:
        query = canonical_query(
            row, by_id=by_id, by_label=by_label, linkedmusic_by_cq=linkedmusic_by_cq
        )
        update_identity(row, query)
    write_jsonl(TARGET_SNAPSHOT / "linguistic_annotations.jsonl", annotations)

    manifest_path = TARGET_SNAPSHOT / "manifest.json"
    manifest = json.loads((SOURCE_SNAPSHOT / "manifest.json").read_text(encoding="utf-8"))
    statuses: Counter[str] = Counter()
    execution_times: list[str] = []
    for row in included:
        query = by_id[(str(row["kg_id"]), str(row["query_id"]))]
        execution = query.get("latest_execution")
        if not isinstance(execution, Mapping):
            raise ValueError(f"No latest execution for {row['query_label']}")
        if (
            execution.get("sparql_version") != row["sparql_version"]
            or execution.get("sparql_hash") != row["sparql_hash"]
        ):
            raise ValueError(f"Latest execution is not for selected SPARQL: {row['query_label']}")
        statuses[str(execution.get("status"))] += 1
        execution_times.append(str(execution.get("ran_at") or ""))
    manifest.update(
        {
            "benchmark_version": "v8",
            "built_at": adjudicated_at,
            "update_type": "versioned_sparql_and_nl_alignment",
            "previous_benchmark": "benchmark/v7",
            "previous_benchmark_version": "v7",
            "curation_note": "All benchmark pairs are pinned to the latest retained SPARQL version. LinkedMusic uses canonical official-source identities and edited version 1 queries; live comparison found no improvement in coarse execution outcomes. Concrete organ VALUES instances are named in canonical questions and illustrative lists are reduced to one organ.",
            "sparql_version_policy": "latest_retained",
            "execution_snapshot": {
                "source": "kg_queries.jsonl",
                "captured_through": max(execution_times),
                "selected_queries": len(included),
                "selected_versions_with_execution": len(included),
                "status_counts": dict(sorted(statuses.items())),
                "note": "Execution history remains canonical in kg_queries.jsonl and is not duplicated in public NL-SPARQL records.",
            },
        }
    )
    manifest.pop("source_curated_file", None)
    manifest.pop("dataset_id", None)
    counts = manifest.get("counts")
    if isinstance(counts, dict):
        counts.pop("added_curated_linkedmusic", None)
        counts.pop("linkedmusic_dataset_counts", None)
    gold_policy = manifest.get("gold_question_policy")
    if isinstance(gold_policy, dict):
        gold_policy.pop("corrected_linkedmusic_sparql_used", None)
        gold_policy["versioned_linkedmusic_sparql_used"] = True
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    benchmark_dir = str(Path("benchmark").resolve())
    if benchmark_dir not in sys.path:
        sys.path.insert(0, benchmark_dir)
    from regenerate_snapshots import regenerate_snapshot

    regenerate_snapshot(TARGET_SNAPSHOT)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    migrate(dry_run=args.dry_run)
    print("Validated benchmark v8 migration" + (" (dry run)." if args.dry_run else " and wrote benchmark/v8."))


if __name__ == "__main__":
    main()
