#!/usr/bin/env python3
"""Build the public, synthetic-safe SPARQL correction review bundle."""
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

from holdout_selectors import add_holdout_filter_arguments, holdout_input_policy, validate_selector_record
from sparql_corrections import candidate_digest, load_jsonl, validate_candidate


def load_selector_keys(path: Path | None) -> set[tuple[str, str]]:
    if path is None:
        return set()
    if not path.exists():
        raise FileNotFoundError(f"Holdout selector records not found: {path}")
    return {validate_selector_record(item) for item in load_jsonl(path)}


def build_payload(
    candidates: list[Dict[str, Any]],
    *,
    selector_keys: set[tuple[str, str]],
    source_path: str,
    input_policy: str,
) -> Dict[str, Any]:
    filtered = []
    excluded = 0
    seen: set[str] = set()
    for item in candidates:
        validate_candidate(item)
        identifier = str(item["candidate_id"])
        if identifier in seen:
            raise ValueError(f"Duplicate SPARQL correction candidate {identifier}")
        seen.add(identifier)
        key = (str(item.get("kg_id") or ""), str(item.get("query_id") or ""))
        if key in selector_keys:
            excluded += 1
            continue
        filtered.append({**item, "candidate_digest": candidate_digest(item)})
    identity = json.dumps(
        [
            [item.get("candidate_id"), item.get("base_sparql_hash")]
            for item in filtered
        ],
        sort_keys=True,
        separators=(",", ":"),
    )
    return {
        "mode": "sparql_correction",
        "schema": "musparql.sparql-correction-bundle.v1",
        "dataset_id": hashlib.sha256(identity.encode("utf-8")).hexdigest()[:16],
        "built_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "source_path": source_path,
        "holdout_input_policy": input_policy,
        "holdout_excluded": excluded,
        "record_count": len(filtered),
        "records": filtered,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidates", default="sparql_correction_candidates.jsonl")
    parser.add_argument("--out", default="review/sparql_correction_data.js")
    add_holdout_filter_arguments(parser)
    args = parser.parse_args()
    candidates_path = Path(args.candidates)
    selector_keys = load_selector_keys(
        Path(args.holdout_selectors) if args.holdout_selectors else None
    )
    payload = build_payload(
        load_jsonl(candidates_path),
        selector_keys=selector_keys,
        source_path=str(candidates_path),
        input_policy=holdout_input_policy(args),
    )
    out_path = Path(args.out)
    out_path.write_text(
        "window.SPARQL_CORRECTION_DATA = "
        + json.dumps(payload, ensure_ascii=False, indent=2)
        + ";\n",
        encoding="utf-8",
    )
    print(
        f"Wrote {payload['record_count']} correction candidates to {out_path.resolve()} "
        f"(holdout_excluded={payload['holdout_excluded']})"
    )


if __name__ == "__main__":
    main()
