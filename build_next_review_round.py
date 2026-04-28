#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path
from typing import List


def output_path(value: str) -> Path:
    path = Path(value)
    if path.is_dir():
        return path / "llm_outputs.jsonl"
    return path


def main() -> None:
    parser = argparse.ArgumentParser(description="Build the next compare-review bundle from a previous reviewed run and current outputs.")
    parser.add_argument("--previous-run", required=True, help="Previous run directory or previous llm_outputs.jsonl.")
    parser.add_argument("--previous-reviews", required=True, help="Previous review export JSON.")
    parser.add_argument("--current-run", default="llm_outputs.jsonl", help="Current run directory or current llm_outputs.jsonl.")
    parser.add_argument("--out", default="review/review_data.js")
    parser.add_argument("--include-unchanged", action="store_true")
    args = parser.parse_args()

    command: List[str] = [
        sys.executable,
        "build_review_diff_bundle.py",
        "--previous-outputs",
        str(output_path(args.previous_run)),
        "--current-outputs",
        str(output_path(args.current_run)),
        "--previous-reviews",
        args.previous_reviews,
        "--out",
        args.out,
    ]
    if args.include_unchanged:
        command.append("--include-unchanged")

    subprocess.run(command, check=True)


if __name__ == "__main__":
    main()
