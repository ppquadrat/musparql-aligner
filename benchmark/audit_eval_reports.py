#!/usr/bin/env python3
"""Check that saved evaluation reports refer to their declared benchmark snapshot."""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import List

from build_benchmark import read_json, read_jsonl


def audit_report(report_dir: Path, repository_root: Path) -> List[str]:
    errors: List[str] = []
    manifest = read_json(report_dir / "manifest.json")
    benchmark_name = manifest.get("benchmark")
    if not benchmark_name:
        return [f"{report_dir}: manifest has no benchmark path"]
    benchmark_dir = repository_root / str(benchmark_name)
    benchmark = read_jsonl(benchmark_dir / "benchmark.jsonl")
    by_id = {str(row.get("benchmark_id") or ""): row for row in benchmark}
    declared = manifest.get("benchmark_counts")
    if isinstance(declared, dict) and "included" in declared and declared["included"] != len(benchmark):
        errors.append(
            f"{report_dir}: declares {declared['included']} included records but {benchmark_dir} has {len(benchmark)}"
        )
    scores = read_jsonl(report_dir / "scores.jsonl")
    for score in scores:
        benchmark_id = str(score.get("benchmark_id") or "")
        referent = by_id.get(benchmark_id)
        if referent is None:
            errors.append(f"{report_dir}: score has no benchmark referent: {benchmark_id}")
            continue
        if score.get("query_id") != referent.get("query_id"):
            errors.append(f"{report_dir}: query ID differs for {benchmark_id}")
        if score.get("gold_question") != referent.get("gold_question"):
            errors.append(f"{report_dir}: canonical question differs for {benchmark_id}")
    return errors


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("reports_root", nargs="?", default="evals/reports")
    parser.add_argument("--repository-root", default=".")
    args = parser.parse_args()
    reports_root = Path(args.reports_root)
    repository_root = Path(args.repository_root)
    errors: List[str] = []
    for manifest_path in sorted(reports_root.glob("*/manifest.json")):
        errors.extend(audit_report(manifest_path.parent, repository_root))
    if errors:
        raise SystemExit("Evaluation report audit failed:\n- " + "\n- ".join(errors))
    print(f"Evaluation report audit passed: {reports_root}")


if __name__ == "__main__":
    main()
