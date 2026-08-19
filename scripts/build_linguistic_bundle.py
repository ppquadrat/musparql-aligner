#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from musparql.linguistic_dimensions import build_bundle


def main() -> None:
    parser = argparse.ArgumentParser(description="Build a deterministic Phase 6b stimulus bundle")
    parser.add_argument("--input", type=Path, required=True, help="ordinary non-holdout stimulus JSONL")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--dataset-id", required=True)
    parser.add_argument("--seed", required=True)
    parser.add_argument("--target-trials", type=int)
    args = parser.parse_args()
    records = [json.loads(line) for line in args.input.read_text(encoding="utf-8").splitlines() if line.strip()]
    payload = build_bundle(records, dataset_id=args.dataset_id, seed=args.seed, target_trials=args.target_trials)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
