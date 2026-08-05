#!/usr/bin/env python3
"""Apply a human-reviewed SPARQL correction export append-only."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from holdout_selectors import add_holdout_filter_arguments, validate_selector_record
from sparql_corrections import apply_reviews, load_jsonl, write_jsonl


def selector_keys(path: Path | None) -> set[tuple[str, str]]:
    if path is None:
        return set()
    return {validate_selector_record(item) for item in load_jsonl(path)}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("review_export")
    parser.add_argument("--queries", default="kg_queries.jsonl")
    parser.add_argument("--candidates", default="sparql_correction_candidates.jsonl")
    parser.add_argument("--dry-run", action="store_true")
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
    stats = apply_reviews(
        records,
        payload,
        export_path=str(export_path),
        candidates=load_jsonl(Path(args.candidates)),
        candidate_path=str(Path(args.candidates)),
        forbidden_pairs=selector_keys(
            Path(args.holdout_selectors) if args.holdout_selectors else None
        ),
    )
    if not args.dry_run:
        write_jsonl(query_path, records)
    suffix = " (dry run)" if args.dry_run else ""
    print(
        f"Validated correction review export{suffix}: approved={stats['approved']}, "
        f"no_edit={stats['no_edit']}, deferred={stats['deferred']}"
    )


if __name__ == "__main__":
    main()
