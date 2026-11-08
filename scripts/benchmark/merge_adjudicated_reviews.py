#!/usr/bin/env python3
"""Turn a round of overlapping team reviews plus meta-review decisions into
review exports the benchmark builder can consume.

`update_from_initial_review` takes one bundle and one export per run and fails
closed when a pair has already been reviewed, so a round in which several teams
judged the same pair cannot be ingested directly. This script resolves the
overlap ahead of the build and leaves the builder's guard intact.

For each dataset it writes:

* `meta-review.json` — one synthesized export authored by the meta-reviewer,
  holding exactly the adjudicated pairs. The adjudicated canonical question
  becomes `preferred_question`, the rationale becomes `public_comment` (public
  provenance, per REVIEW_POLICY.md), the graph-context decision becomes
  `interpretive.requires_graph_context_knowledge`, and `prior_review_ids` names
  every team review the decision supersedes.
* `<group>.json` per reviewing team — that team's own export, filtered to the
  pairs it alone owns. Attribution is preserved: nothing a participant wrote is
  re-attributed to the meta-reviewer.

A multi-team pair that has no decision is resolved by a documented tie-break
(earliest `reviewed_at`, then review id) so one team's review stands and the
others are dropped; `--strict` turns that into an error instead. Those pairs
agree on disposition and wording by construction — they are the ones where every
team accepted the pipeline wording — but their `pipeline_assessment` may differ,
and the summary reports each one.

The resulting exports contain no pair twice, so build order does not matter.
Each one still has to be built against its own bundle, because the builder
requires the bundle and export `dataset_id` to match.

Two provenance notes about the synthesized export. Its `bundle_digest` is the
real digest of the bundle that was adjudicated, taken from the team exports for
that dataset, which all agree. Its `assignment_id` is borrowed from one of those
team exports so that the export satisfies `schemas/review_export.schema.json`; a
meta-review is not itself a reviewing assignment, so that field names a team's
assignment rather than the adjudication.

Pairs whose decision records `sparql_correction_required` are reported and, with
--skip-correction-required, left out: their SPARQL does not express the paired
question, so building them would put a known-defective pair in the benchmark.

Usage:
    python -m scripts.benchmark.merge_adjudicated_reviews \
        --exports var/review/exports/ipl-2026-09-16 \
        --bundles var/review/bundles/ipl-2026-20260916-seven-kg \
        --decisions catalog/curated/meta_review_decisions.jsonl \
        --meta-reviewer-id reviewer-NNNN \
        --outdir var/review/exports/ipl-2026-09-16-merged
"""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List

NOT_REVIEWED = "not reviewed"
SCHEMA = "musparql.review-export.v2"
KIND = "non_holdout_review_export"


def read_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def read_jsonl(path: Path) -> List[Dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def is_self_declared_non_review(review: Dict[str, Any]) -> bool:
    return (review.get("literal_wording") or "").strip().lower().startswith(NOT_REVIEWED)


def load_bundles(bundle_dir: Path) -> Dict[str, Dict[str, Any]]:
    """Map bundle review_id -> record, for every frozen package."""
    records: Dict[str, Dict[str, Any]] = {}
    for path in sorted(bundle_dir.glob("*.json")):
        if path.name == "manifest.json":
            continue
        bundle = read_json(path)
        for record in bundle.get("records", []):
            review_id = str(record.get("review_id") or "")
            if review_id:
                records[review_id] = record
    return records


def load_exports(export_dir: Path) -> List[Dict[str, Any]]:
    exports = []
    for path in sorted(export_dir.glob("*.json")):
        data = read_json(path)
        if data.get("kind") != KIND:
            continue
        data["_path"] = str(path)
        exports.append(data)
    return exports


def require_one_export_per_team(exports: Iterable[Dict[str, Any]]) -> None:
    seen: Dict[str, str] = {}
    for export in exports:
        team = str(export.get("review_group_id") or export.get("assignment_id") or export["_path"])
        if team in seen:
            raise SystemExit(
                "The export directory must hold one export per reviewing team (the latest valid "
                f"revision). {team} appears in both {seen[team]} and {export['_path']}."
            )
        seen[team] = export["_path"]


def synth_review(
    decision: Dict[str, Any],
    record: Dict[str, Any],
    meta_reviewer_id: str,
    reviewed_at: str,
) -> Dict[str, Any]:
    review_id = f"{decision['pair_review_id']}::{meta_reviewer_id}"
    canonical = (decision.get("canonical_question") or "").strip()
    pipeline = str(record.get("output", {}).get("nl_question") or "").strip()
    authored: List[str] = []
    approved: List[str] = []
    if canonical and canonical != pipeline:
        authored.append(f"{review_id}::formulation::preferred")
    elif canonical:
        approved.append(f"{review_id}::formulation::candidate")
    return {
        "review_id": review_id,
        "reviewer_id": meta_reviewer_id,
        "reviewed_at": reviewed_at,
        "prior_review_ids": list(decision.get("superseded_review_ids") or []),
        "authored_formulation_ids": authored,
        "approved_formulation_ids": approved,
        "benchmark_disposition": decision["benchmark_disposition"],
        "pipeline_assessment": decision.get("pipeline_assessment"),
        "preferred_question": canonical,
        "literal_wording": "",
        "public_comment": (decision.get("rationale") or "").strip(),
        "internal_comment": "",
        "split": "",
        "interpretive": {
            "naturalness": None,
            "pragmatism": None,
            "room_for_interpretation": None,
            "requires_graph_context_knowledge": bool(decision.get("graph_context_required")),
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--exports", type=Path, required=True, help="directory of team exports, one per team")
    parser.add_argument("--bundles", type=Path, required=True, help="frozen review bundle directory")
    parser.add_argument("--decisions", type=Path, required=True, help="meta-review decisions JSONL")
    parser.add_argument("--meta-reviewer-id", required=True, help="the meta-reviewer's pseudonymous reviewer-NNNN id")
    parser.add_argument("--outdir", type=Path, required=True, help="where to write the merged exports")
    parser.add_argument("--round", default=None, help="only use decisions from this round")
    parser.add_argument("--decided-at", default=None, help="override the reviewed_at timestamp (RFC 3339)")
    parser.add_argument("--strict", action="store_true", help="fail on a multi-team pair with no decision")
    parser.add_argument(
        "--skip-correction-required",
        action="store_true",
        help="omit pairs whose decision sets sparql_correction_required (always reported)",
    )
    parser.add_argument(
        "--keep-non-reviews",
        action="store_true",
        help='keep reviews self-marked "Not reviewed" in literal_wording (dropped by default)',
    )
    args = parser.parse_args()

    if not args.meta_reviewer_id.startswith("reviewer-") or not args.meta_reviewer_id[9:].isdigit():
        raise SystemExit("--meta-reviewer-id must use the pseudonymous form reviewer-NNNN")

    records = load_bundles(args.bundles)
    exports = load_exports(args.exports)
    if not exports:
        raise SystemExit(f"no review exports found under {args.exports}")
    require_one_export_per_team(exports)

    decisions = [
        d for d in read_jsonl(args.decisions)
        if args.round is None or d.get("round") == args.round
    ]
    needs_correction = [d for d in decisions if d.get("sparql_correction_required")]
    skipped_pairs: set[str] = set()
    if args.skip_correction_required:
        skipped_pairs = {str(d["pair_review_id"]) for d in needs_correction}
        decisions = [d for d in decisions if not d.get("sparql_correction_required")]
    by_pair_decision = {str(d["pair_review_id"]): d for d in decisions}

    # which teams reviewed each pair
    teams_by_pair: Dict[str, List[tuple[str, Dict[str, Any], Dict[str, Any]]]] = defaultdict(list)
    for export in exports:
        team = str(export.get("review_group_id") or export["_path"])
        for review_id, review in (export.get("reviews") or {}).items():
            if not args.keep_non_reviews and is_self_declared_non_review(review):
                continue
            teams_by_pair[str(review_id)].append((team, export, review))

    unknown = sorted(set(by_pair_decision) - set(teams_by_pair))
    if unknown:
        raise SystemExit(f"decisions reference pairs absent from the exports: {unknown[:5]}")

    # resolve every pair to exactly one owner
    owner: Dict[str, str] = {}
    tie_broken: List[tuple[str, List[str]]] = []
    for pair, entries in teams_by_pair.items():
        if pair in skipped_pairs:
            # left out entirely: falling back to a team review would build the
            # defective pair under that team's wording instead.
            continue
        if pair in by_pair_decision:
            owner[pair] = "__meta__"
        elif len(entries) == 1:
            owner[pair] = entries[0][0]
        else:
            chosen = sorted(entries, key=lambda e: (str(e[2].get("reviewed_at") or ""), str(e[2].get("review_id") or "")))[0]
            owner[pair] = chosen[0]
            assessments = sorted({str(e[2].get("pipeline_assessment")) for e in entries})
            tie_broken.append((pair, assessments))

    if tie_broken and args.strict:
        listed = ", ".join(pair for pair, _ in tie_broken[:5])
        raise SystemExit(
            f"{len(tie_broken)} multi-team pairs have no decision and --strict is set. "
            f"Adjudicate them or drop --strict to accept the tie-break. Examples: {listed}"
        )

    reviewed_at = args.decided_at or (
        f"{decisions[0].get('decided_at', '1970-01-01')}T00:00:00Z" if decisions else "1970-01-01T00:00:00Z"
    )
    args.outdir.mkdir(parents=True, exist_ok=True)
    written: List[Dict[str, Any]] = []

    # one synthesized meta-review export per dataset
    meta_by_dataset: Dict[str, Dict[str, Any]] = defaultdict(dict)
    for pair, decision in by_pair_decision.items():
        record = records.get(pair)
        if record is None:
            raise SystemExit(f"pair {pair} is not present in the frozen bundles under {args.bundles}")
        meta_by_dataset[str(decision["dataset_id"])][pair] = synth_review(
            decision, record, args.meta_reviewer_id, reviewed_at
        )

    for dataset_id, reviews in sorted(meta_by_dataset.items()):
        source = next(e for e in exports if e.get("dataset_id") == dataset_id)
        payload = {
            "schema": SCHEMA,
            "kind": KIND,
            "assignment_id": source["assignment_id"],
            "bundle_digest": source["bundle_digest"],
            "reviewer_id": args.meta_reviewer_id,
            "dataset_id": dataset_id,
            "exported_at": reviewed_at,
            "run_id": source.get("run_id"),
            "run_ids": list(source.get("run_ids") or []),
            "runs": list(source.get("runs") or []),
            "reviews": reviews,
        }
        path = args.outdir / f"{dataset_id}--meta-review.json"
        path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        written.append({"path": str(path), "dataset_id": dataset_id, "reviews": len(reviews), "author": "meta-review"})

    # each team's export, filtered to the pairs it owns
    for export in exports:
        team = str(export.get("review_group_id") or export["_path"])
        kept = {
            pair: review
            for pair, review in (export.get("reviews") or {}).items()
            if owner.get(str(pair)) == team
        }
        if not kept:
            continue
        payload = {k: v for k, v in export.items() if k not in {"_path", "reviews"}}
        payload["reviews"] = kept
        if "completion_item_count" in payload:
            payload["completion_item_count"] = min(int(payload["completion_item_count"]), len(kept))
        path = args.outdir / f"{export['dataset_id']}--{team}.json"
        path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        written.append({"path": str(path), "dataset_id": export["dataset_id"], "reviews": len(kept), "author": team})

    total = sum(item["reviews"] for item in written)
    print(f"teams: {len(exports)}   decided pairs: {len(by_pair_decision)}   pairs resolved: {len(owner)}")
    print(f"wrote {len(written)} exports under {args.outdir} covering {total} reviews")
    if total != len(owner):
        print(f"WARNING: {total} reviews written for {len(owner)} pairs — a pair may appear twice")
    for item in sorted(written, key=lambda i: (i["dataset_id"], i["author"])):
        print(f"  {item['dataset_id']:52} {item['author']:28} {item['reviews']:3} reviews")
    if needs_correction:
        labels = ", ".join(str(d.get("query_label")) for d in needs_correction)
        if args.skip_correction_required:
            print(f"\nomitted {len(needs_correction)} pairs awaiting SPARQL correction: {labels}")
        else:
            print(
                f"\nWARNING: {len(needs_correction)} adjudicated pairs are marked as needing SPARQL "
                f"correction and are INCLUDED in this merge: {labels}.\n"
                "Their SPARQL does not express the paired question. Correct them first, or re-run "
                "with --skip-correction-required."
            )
    if tie_broken:
        print(f"\n{len(tie_broken)} multi-team pairs had no decision and were resolved by tie-break:")
        for pair, assessments in tie_broken:
            flag = "  <- assessments differ" if len(assessments) > 1 else ""
            print(f"  {pair.split('::')[1]:22} {', '.join(assessments)}{flag}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
