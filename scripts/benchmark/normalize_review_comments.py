#!/usr/bin/env python3
"""Normalize legacy reviewer notes into public and internal comment fields."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, Iterable

sys.path.insert(0, str(Path(__file__).resolve().parent))

from scripts.benchmark.build_benchmark import literal_wording, read_jsonl, review_comments, write_json, write_jsonl


def normalize_review(review: Dict[str, Any], *, reclassify_existing_public: bool = False) -> bool:
    before = json.dumps(review, ensure_ascii=False, sort_keys=True)
    literal = literal_wording(review)
    public_comment, internal_comment = review_comments(review)
    if reclassify_existing_public and public_comment:
        internal_comment = "\n\n".join(
            text for text in (internal_comment, public_comment) if text
        )
        public_comment = ""
    review.pop("note", None)
    review["public_comment"] = public_comment
    review["internal_comment"] = internal_comment
    if literal:
        review["literal_wording"] = literal
    return before != json.dumps(review, ensure_ascii=False, sort_keys=True)


def normalize_records(
    records: Iterable[Dict[str, Any]], *, reclassify_existing_public: bool = False
) -> int:
    changed = 0
    for record in records:
        review = record.get("review")
        if isinstance(review, dict) and normalize_review(
            review, reclassify_existing_public=reclassify_existing_public
        ):
            changed += 1
    return changed


def normalize_snapshot(snapshot: Path, *, reclassify_existing_public: bool = False) -> int:
    changed = 0
    public_by_benchmark_id: Dict[str, str] = {}
    for filename in ("included.jsonl", "dismissed.jsonl"):
        path = snapshot / filename
        if not path.exists():
            continue
        records = read_jsonl(path)
        changed += normalize_records(
            records, reclassify_existing_public=reclassify_existing_public
        )
        for record in records:
            review = record.get("review")
            if isinstance(review, dict):
                public_by_benchmark_id[str(record.get("benchmark_id") or "")] = str(
                    review.get("public_comment") or ""
                )
        write_jsonl(path, records)
    benchmark_path = snapshot / "benchmark.jsonl"
    if benchmark_path.exists():
        benchmark_records = read_jsonl(benchmark_path)
        for record in benchmark_records:
            benchmark_id = str(record.get("benchmark_id") or "")
            if benchmark_id not in public_by_benchmark_id:
                continue
            provenance = record.get("provenance")
            if not isinstance(provenance, dict):
                provenance = {}
                record["provenance"] = provenance
            before = json.dumps(provenance, ensure_ascii=False, sort_keys=True)
            public_comment = public_by_benchmark_id[benchmark_id]
            if public_comment:
                provenance["reviewer_comment"] = public_comment
            else:
                provenance.pop("reviewer_comment", None)
            if before != json.dumps(provenance, ensure_ascii=False, sort_keys=True):
                changed += 1
        write_jsonl(benchmark_path, benchmark_records)
    return changed


def normalize_export(path: Path, *, reclassify_existing_public: bool = False) -> int:
    payload = json.loads(path.read_text(encoding="utf-8"))
    reviews = payload.get("reviews")
    if not isinstance(reviews, dict):
        return 0
    changed = sum(
        1
        for review in reviews.values()
        if isinstance(review, dict)
        and normalize_review(review, reclassify_existing_public=reclassify_existing_public)
    )
    if changed:
        write_json(path, payload)
    return changed


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--benchmark-root", default="benchmark")
    parser.add_argument(
        "--exports",
        default="",
        help="Explicit sanitized non-holdout review-export directory. Raw/private exports are never scanned by default.",
    )
    parser.add_argument(
        "--reclassify-existing-public-as-legacy",
        action="store_true",
        help="One-time repair for comments promoted by the earlier unsafe migration",
    )
    args = parser.parse_args()

    total = 0
    for snapshot in sorted(Path(args.benchmark_root).glob("v[0-9]*")):
        changed = normalize_snapshot(
            snapshot,
            reclassify_existing_public=args.reclassify_existing_public_as_legacy,
        )
        total += changed
        print(f"{snapshot}: normalized {changed} review records")
    exports = Path(args.exports) if args.exports else None
    if exports is not None and exports.exists():
        for path in sorted(exports.glob("*.json")):
            changed = normalize_export(
                path,
                reclassify_existing_public=args.reclassify_existing_public_as_legacy,
            )
            total += changed
            print(f"{path}: normalized {changed} exported reviews")
    print(f"Normalized {total} review records in total")


if __name__ == "__main__":
    main()
