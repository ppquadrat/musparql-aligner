#!/usr/bin/env python3
"""Re-triage the whole correction queue from the observations already recorded.

`merge_candidates` deliberately replaces only the rows in the current scoped
run, so a change to `classify_failure` does not reach candidates whose queries
were not re-executed. That is right for a run against endpoints -- an old row's
observation is still its own evidence -- but it means a corrected classifier
leaves most of the queue carrying verdicts the project no longer stands behind.

This recomputes every candidate from the canonical catalogue and the execution
observation already retained for each pair's latest SPARQL version. It reaches
no endpoint and records no new observation: the evidence is unchanged and only
the reading of it is recomputed. Each candidate keeps its observation's own
timestamp as `captured_at`.

A reclassified candidate gets a new `candidate_id`, because the reason code is
part of candidate identity. That is intended -- a different triage is a
different candidate to review -- but a pair whose earlier candidate was already
corrected may therefore reappear in the queue under its new reading. Such a
pair carries its own `correction_history`, so the workbench still shows what
was applied.

Usage:
    python -m scripts.retriage_sparql_corrections --holdout-selectors var/holdout/selectors.jsonl
    python -m scripts.retriage_sparql_corrections --no-holdout --dry-run
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Mapping

from musparql.holdout_selectors import (
    add_holdout_filter_arguments,
    holdout_input_policy,
    validate_selectors_current,
)
from musparql.sparql_corrections import (
    build_candidate,
    exclude_candidate_pairs,
    load_jsonl,
    validate_candidate,
    write_jsonl,
)
from musparql.sparql_versions import resolve_sparql_version

PRODUCER = "retriage_sparql_corrections.py"


def latest_observation(record: Mapping[str, Any], resolved: Mapping[str, Any]) -> Dict[str, Any]:
    """The last observation recorded for this pair's latest retained version."""
    observations = [
        item
        for item in (record.get("execution_history") or [])
        if isinstance(item, Mapping)
        and item.get("sparql_version") == resolved["sparql_version"]
        and item.get("sparql_hash") == resolved["sparql_hash"]
    ]
    if not observations:
        return {
            "status": "not_attempted",
            "ran_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        }
    return dict(observations[-1])


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--queries", default="var/queries/kg_queries.jsonl")
    parser.add_argument("--candidates", default="var/queries/sparql_correction_candidates.jsonl")
    parser.add_argument(
        "--include-unobserved",
        action="store_true",
        help=(
            "Also add pairs that have never been observed for their latest version, which "
            "merge_candidates never admitted to the ledger. They triage as not_attempted / "
            "needs_observation, a reading classify_failure has always had but which no pair "
            "has ever carried."
        ),
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Report the reclassification without writing."
    )
    add_holdout_filter_arguments(parser)
    args = parser.parse_args()
    if args.holdout_filtered_upstream:
        raise ValueError(
            "--holdout-filtered-upstream is invalid because this reads the canonical query file"
        )

    query_path = Path(args.queries)
    candidate_path = Path(args.candidates)
    records = load_jsonl(query_path)
    selector_rows = load_jsonl(Path(args.holdout_selectors)) if args.holdout_selectors else []
    forbidden = validate_selectors_current(selector_rows, records) if selector_rows else set()

    previous = {
        str(item.get("candidate_id") or ""): item
        for item in exclude_candidate_pairs(load_jsonl(candidate_path), forbidden)
    }
    previous_by_pair = {
        (str(item.get("kg_id") or ""), str(item.get("query_id") or "")): item
        for item in previous.values()
    }

    candidates: List[Dict[str, Any]] = []
    changes: List[tuple[str, str, str, str]] = []
    unobserved: List[str] = []
    before: Counter[str] = Counter()
    after: Counter[str] = Counter()
    for record in records:
        pair = (str(record.get("kg_id") or ""), str(record.get("query_id") or ""))
        if pair in forbidden:
            continue
        resolved = resolve_sparql_version(record, "latest")
        observation = latest_observation(record, resolved)
        if pair not in previous_by_pair and not args.include_unobserved:
            # Reclassifying what the queue holds is one question; whether the
            # queue should hold every unobserved pair is another, and silently
            # answering the second would hide it.
            unobserved.append(f"{record.get('kg_id')}/{record.get('query_label')}")
            continue
        failure = {
            **observation,
            "kg_id": record.get("kg_id"),
            "query_id": record.get("query_id"),
            "query_label": record.get("query_label"),
            "sparql_version": resolved["sparql_version"],
            "sparql_hash": resolved["sparql_hash"],
            "observed_at": observation.get("ran_at"),
        }
        candidate = build_candidate(failure, record)
        candidate["capture_provenance"] = {"producer": PRODUCER, "automatic": True}
        validate_candidate(candidate)
        candidates.append(candidate)
        old = previous_by_pair.get(pair)
        old_reason = str((old or {}).get("triage", {}).get("reason_code") or "absent")
        new_reason = str(candidate["triage"]["reason_code"])
        before[old_reason] += 1
        after[new_reason] += 1
        if old_reason != new_reason:
            changes.append(
                (str(record.get("kg_id") or ""), str(record.get("query_label") or ""), old_reason, new_reason)
            )

    candidates.sort(key=lambda item: (str(item.get("kg_id") or ""), str(item.get("query_label") or "")))
    print(f"Re-triaged {len(candidates)} candidates from retained observations (no endpoint was contacted).")
    print(f"Holdout policy: {holdout_input_policy(args)}")
    width = max((len(key) for key in set(before) | set(after)), default=0)
    print("\nreason_code".ljust(width + 4) + "before   after")
    for reason in sorted(set(before) | set(after)):
        print(f"{reason.ljust(width + 4)}{before.get(reason, 0):>6}{after.get(reason, 0):>8}")
    if unobserved:
        print(
            f"\n{len(unobserved)} pairs are absent from the ledger because no observation exists "
            f"for their latest version; merge_candidates only ever admitted pairs from a scoped "
            f"run. They are left out. Pass --include-unobserved to add them as "
            f"not_attempted / needs_observation."
        )
        shown = ", ".join(sorted(unobserved)[:8])
        print(f"  for example: {shown}{' ...' if len(unobserved) > 8 else ''}")
    if changes:
        print(f"\n{len(changes)} candidates change reading:")
        for kg_id, label, old_reason, new_reason in sorted(changes):
            print(f"  {kg_id}/{label}: {old_reason} -> {new_reason}")
    if args.dry_run:
        print("\nDry run: nothing written.")
        return 0
    write_jsonl(candidate_path, candidates)
    print(f"\nWrote {len(candidates)} candidates to {candidate_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
