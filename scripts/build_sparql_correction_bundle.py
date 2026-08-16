#!/usr/bin/env python3
"""Build the public, synthetic-safe SPARQL correction review bundle."""
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

from musparql.holdout_selectors import add_holdout_filter_arguments, holdout_input_policy, validate_selector_record, validate_selectors_current
from musparql.sparql_corrections import candidate_digest, load_jsonl, validate_candidate
from musparql.reviewer_provenance import validate_reviewer_id


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
    reviewer_id: str = "reviewer-0001",
) -> Dict[str, Any]:
    reviewer_id = validate_reviewer_id(reviewer_id)
    filtered = []
    excluded = 0
    seen: set[str] = set()
    for item in candidates:
        key = (str(item.get("kg_id") or ""), str(item.get("query_id") or ""))
        if key in selector_keys:
            excluded += 1
            continue
        validate_candidate(item)
        identifier = str(item["candidate_id"])
        if identifier in seen:
            raise ValueError(f"Duplicate SPARQL correction candidate {identifier}")
        seen.add(identifier)
        filtered.append({**item, "candidate_digest": candidate_digest(item)})
    identity = json.dumps(
        [
            [item.get("candidate_id"), item.get("base_sparql_hash")]
            for item in filtered
        ],
        sort_keys=True,
        separators=(",", ":"),
    )
    payload = {
        "mode": "sparql_correction",
        "schema": "musparql.sparql-correction-bundle.v1",
        "reviewer_id": reviewer_id,
        "dataset_id": hashlib.sha256(identity.encode("utf-8")).hexdigest()[:16],
        "built_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "source_path": source_path,
        "holdout_input_policy": input_policy,
        "holdout_excluded": excluded,
        "record_count": len(filtered),
        "records": filtered,
    }
    payload["bundle_digest"] = "sha256:" + hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidates", default="var/queries/sparql_correction_candidates.jsonl")
    parser.add_argument("--out", default="review/sparql_correction_data.js")
    parser.add_argument("--queries", default="var/queries/kg_queries.jsonl")
    parser.add_argument("--reviewer-id", required=True, help="Anonymous reviewer ID, for example reviewer-0001.")
    add_holdout_filter_arguments(parser)
    args = parser.parse_args()
    candidates_path = Path(args.candidates)
    selector_rows = load_jsonl(Path(args.holdout_selectors)) if args.holdout_selectors else []
    selector_keys = validate_selectors_current(selector_rows, load_jsonl(Path(args.queries))) if selector_rows else set()
    payload = build_payload(
        load_jsonl(candidates_path),
        selector_keys=selector_keys,
        source_path=str(candidates_path),
        input_policy=holdout_input_policy(args),
        reviewer_id=args.reviewer_id,
    )
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
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
