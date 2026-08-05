#!/usr/bin/env python3
"""Seed the durable approved-edit archive from a version-pinned benchmark."""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any, Mapping

from musparql.approved_sparql_edits import (
    APPROVED_EDIT_SCHEMA,
    archive_rows_from_records,
    restore_approved_edits,
)
from musparql.sparql_corrections import load_jsonl, write_jsonl
from musparql.sparql_versions import resolve_sparql_version, sparql_hash


def recovery_note(row: Mapping[str, Any]) -> tuple[str, str, str]:
    kg_id = str(row["kg_id"])
    label = str(row.get("query_label") or row["query_id"])
    if kg_id == "linkedmusic":
        return (
            "Use the manually corrected LinkedMusic working-copy query approved in benchmark v8.",
            "linkedmusic-corrected-examples-working-copy",
            "other",
        )
    if kg_id == "jazzontology":
        return (
            "Instantiate the source query template with the concrete resource approved in benchmark v8.",
            "benchmark-v8-approved-sparql",
            "parameter_instantiation",
        )
    if kg_id == "organs" and label == "organs-0011":
        return (
            "Use the valid boolean comparison approved in benchmark v8.",
            "benchmark-v8-approved-sparql",
            "syntax_correction",
        )
    if kg_id == "organs":
        return (
            "Reduce the illustrative VALUES list to the concrete organ approved in benchmark v8.",
            "musparql-manual-correction-2026-08-03",
            "benchmark_specialization",
        )
    raise ValueError(f"No recovery policy for {kg_id}/{label}")


def benchmark_archive_rows(
    queries: list[dict[str, Any]], benchmark: list[dict[str, Any]], benchmark_path: Path
) -> list[dict[str, Any]]:
    by_key = {(str(row["kg_id"]), str(row["query_id"])): row for row in queries}
    rows: list[dict[str, Any]] = []
    for benchmark_row in benchmark:
        version = benchmark_row.get("sparql_version", 0)
        if version == 0:
            continue
        if version != 1:
            raise ValueError("This recovery migration requires benchmark version-1 edits")
        key = (str(benchmark_row["kg_id"]), str(benchmark_row["query_id"]))
        query = by_key.get(key)
        if query is None:
            raise ValueError(f"Benchmark correction does not resolve: {key[0]}/{key[1]}")
        base = resolve_sparql_version(query, 0)
        corrected = str(benchmark_row["sparql"])
        corrected_hash = sparql_hash(corrected)
        if corrected_hash != benchmark_row.get("sparql_hash"):
            raise ValueError(f"Benchmark SPARQL hash mismatch: {key[0]}/{key[1]}")
        note, source_id, edit_type = recovery_note(benchmark_row)
        rows.append(
            {
                "schema": APPROVED_EDIT_SCHEMA,
                "kg_id": key[0],
                "query_id": key[1],
                "query_label": query.get("query_label"),
                "base_sparql_hash": base["sparql_hash"],
                "version": 1,
                "sparql": corrected,
                "sparql_hash": corrected_hash,
                "note": note,
                "source_id": source_id,
                "edit_type": edit_type,
                "evidence_ids": [],
                "provenance": {
                    "approval_source": "human_curated_benchmark",
                    "benchmark_version": "v8",
                    "reconstructed_from": str(benchmark_path),
                },
            }
        )
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--queries", default="var/queries/kg_queries.jsonl")
    parser.add_argument("--benchmark", default="benchmark/v8/benchmark.jsonl")
    parser.add_argument("--out", default="catalog/curated/Approved_SPARQL_Edits.jsonl")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    query_path = Path(args.queries)
    benchmark_path = Path(args.benchmark)
    records = load_jsonl(query_path)
    recovered = benchmark_archive_rows(records, load_jsonl(benchmark_path), benchmark_path)
    restored = restore_approved_edits(records, recovered)
    archive = archive_rows_from_records(records)
    if len(archive) != len(recovered):
        raise ValueError(
            "Working catalogue contains approved edits not represented by the recovery benchmark"
        )
    if not args.dry_run:
        write_jsonl(Path(args.out), archive)
        write_jsonl(query_path, records)
    suffix = " (dry run)" if args.dry_run else ""
    print(f"Validated {len(archive)} approved SPARQL edits; restored {restored}{suffix}")


if __name__ == "__main__":
    main()
