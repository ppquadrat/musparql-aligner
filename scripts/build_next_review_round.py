#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path
from typing import List

from musparql.holdout_selectors import add_holdout_filter_arguments


def output_path(value: str) -> Path:
    path = Path(value)
    if path.is_dir():
        return path / "llm_outputs.jsonl"
    return path


def main() -> None:
    parser = argparse.ArgumentParser(description="Build the next comparative-review bundle from a previous reviewed run and current outputs.")
    parser.add_argument("--previous-run", required=True, help="Previous run directory or previous llm_outputs.jsonl.")
    parser.add_argument("--previous-reviews", required=True, help="Previous review export JSON.")
    parser.add_argument(
        "--previous-benchmark",
        default="",
        help="Previous benchmark/vN directory. Use this for normal benchmark comparison rounds so carried-forward decisions are shown.",
    )
    parser.add_argument("--current-run", default="var/llm/outputs.jsonl", help="Current run directory or current llm_outputs.jsonl.")
    add_holdout_filter_arguments(parser)
    parser.add_argument("--out", default="review/review_data.js")
    parser.add_argument(
        "--benchmark-only",
        action="store_true",
        help="Only include pairs present in the previous benchmark public evaluation set.",
    )
    parser.add_argument("--include-unchanged", action="store_true")
    parser.add_argument(
        "--include-dismissed",
        action="store_true",
        help="Include pairs dismissed in the previous review export. By default they are excluded.",
    )
    parser.add_argument(
        "--include-metadata-only",
        action="store_true",
        help="Include pairs whose only changes are confidence, rationale, model, or full-input evidence metadata.",
    )
    parser.add_argument(
        "--assert-complete-review-provenance",
        action="store_true",
        help="Human assertion that the supplied prior-review sources cover every earlier reviewer decision; required to enable holdout selection.",
    )
    args = parser.parse_args()

    command: List[str] = [
        sys.executable,
        "-m",
        "scripts.build_review_diff_bundle",
        "--previous-outputs",
        str(output_path(args.previous_run)),
        "--current-outputs",
        str(output_path(args.current_run)),
        "--previous-reviews",
        args.previous_reviews,
        "--out",
        args.out,
    ]
    if args.previous_benchmark:
        command.extend(["--previous-benchmark", args.previous_benchmark])
    if args.holdout_selectors:
        command.extend(["--holdout-selectors", args.holdout_selectors])
    elif args.holdout_filtered_upstream:
        command.append("--holdout-filtered-upstream")
    else:
        command.append("--no-holdout")
    if args.benchmark_only:
        command.append("--benchmark-only")
    if args.include_unchanged:
        command.append("--include-unchanged")
    if args.include_dismissed:
        command.append("--include-dismissed")
    if args.include_metadata_only:
        command.append("--include-metadata-only")
    if args.assert_complete_review_provenance:
        command.append("--assert-complete-review-provenance")

    subprocess.run(command, check=True)


if __name__ == "__main__":
    main()
