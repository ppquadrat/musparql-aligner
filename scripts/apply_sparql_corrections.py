#!/usr/bin/env python3
"""Apply a human-reviewed SPARQL correction export append-only."""
from __future__ import annotations

import argparse
import json
import re
import hashlib
from pathlib import Path

from musparql.approved_sparql_edits import archive_rows_from_records
from musparql.holdout_selectors import add_holdout_filter_arguments, validate_selector_record, validate_selectors_current
from musparql.sparql_corrections import apply_reviews, load_jsonl, write_jsonl


def selector_keys(path: Path | None) -> set[tuple[str, str]]:
    if path is None:
        return set()
    return {validate_selector_record(item) for item in load_jsonl(path)}


def bundle_digest(path: Path) -> str:
    text = path.read_text(encoding="utf-8")
    match = re.search(r"window\.SPARQL_CORRECTION_DATA\s*=\s*(\{.*\})\s*;\s*$", text, re.S)
    if not match:
        raise ValueError(f"Unsupported correction bundle: {path}")
    payload = json.loads(match.group(1))
    value = payload.get("bundle_digest")
    if not isinstance(value, str) or not value.startswith("sha256:"):
        raise ValueError("Correction bundle is missing its authoritative digest")
    unsigned = dict(payload)
    unsigned.pop("bundle_digest", None)
    expected = "sha256:" + hashlib.sha256(
        json.dumps(unsigned, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    if value != expected:
        raise ValueError("Correction bundle digest does not match its contents")
    return value


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("review_export")
    parser.add_argument("--queries", default="var/queries/kg_queries.jsonl")
    parser.add_argument("--candidates", default="var/queries/sparql_correction_candidates.jsonl")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--suggestion-log", default="var/review/workbench/suggestions.jsonl")
    parser.add_argument("--execution-log", default="var/review/workbench/execution_attempts.jsonl")
    parser.add_argument("--bundle", default="review/sparql_correction_data.js")
    parser.add_argument(
        "--approved-edits",
        default="catalog/curated/Approved_SPARQL_Edits.jsonl",
        help="Tracked, public-safe archive of approved SPARQL versions.",
    )
    add_holdout_filter_arguments(parser)
    args = parser.parse_args()
    if args.holdout_filtered_upstream:
        raise ValueError(
            "--holdout-filtered-upstream is invalid when applying to the full canonical query file"
        )
    export_path = Path(args.review_export)
    query_path = Path(args.queries)
    payload = json.loads(export_path.read_text(encoding="utf-8"))
    records = load_jsonl(query_path)
    selector_rows = load_jsonl(Path(args.holdout_selectors)) if args.holdout_selectors else []
    selected_holdouts = validate_selectors_current(selector_rows, records) if selector_rows else set()
    stats = apply_reviews(
        records,
        payload,
        export_path=str(export_path),
        candidates=load_jsonl(Path(args.candidates)),
        candidate_path=str(Path(args.candidates)),
        forbidden_pairs=selected_holdouts,
        authoritative_suggestions=load_jsonl(Path(args.suggestion_log)),
        authoritative_executions=load_jsonl(Path(args.execution_log)),
        authoritative_bundle_digest=bundle_digest(Path(args.bundle)),
    )
    if not args.dry_run:
        # Write the durable archive first. If the working-catalogue replacement
        # then fails, the next extraction restores the approved version rather
        # than losing it.
        write_jsonl(Path(args.approved_edits), archive_rows_from_records(records))
        write_jsonl(query_path, records)
    suffix = " (dry run)" if args.dry_run else ""
    print(
        f"Validated correction review export{suffix}: approved={stats['approved']}, "
        f"no_edit={stats['no_edit']}, deferred={stats['deferred']}"
    )


if __name__ == "__main__":
    main()
