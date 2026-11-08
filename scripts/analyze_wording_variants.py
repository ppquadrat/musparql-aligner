#!/usr/bin/env python3
"""Find NL/SPARQL pairs that several reviewing teams worded differently.

Input is a directory of canonical non-holdout review exports
(`musparql.review-export.v2`, one file per submission revision) plus,
optionally, the frozen review bundle directory that was reviewed, so that
examples can show the original pipeline question and the SPARQL.

The script keeps only the latest valid revision per reviewing team — the same
rule the IPL workshop report used — then groups reviews by NL/SPARQL pair and
reports:

  * pairs judged by at least two teams;
  * pairs where at least two teams supplied a non-empty `preferred_question`;
  * of those, pairs where the supplied wordings actually differ;
  * pairs where one team rewrote the question and another accepted the
    pipeline wording as it stood (a silent conflict under the current
    "preferred_question wins" gold-question policy).

It prints no reviewer names: exports carry pseudonymous reviewer IDs only.

Usage:
    python scripts/analyze_wording_variants.py EXPORT_DIR \
        [--bundles var/review/bundles/ipl-2026-20260916-seven-kg] \
        [--examples 5] [--json out.json]
"""

from __future__ import annotations

import argparse
import json
import re
import unicodedata
from collections import defaultdict
from pathlib import Path
from typing import Any

PAIR_KEY_PARTS = 2  # kg::query-id identifies the pair across bundles/runs


def pair_key(review_key: str) -> str:
    parts = review_key.split("::")
    return "::".join(parts[:PAIR_KEY_PARTS]) if len(parts) >= PAIR_KEY_PARTS else review_key


def normalise(text: str) -> str:
    """Compare wordings ignoring case, punctuation and whitespace noise."""
    text = unicodedata.normalize("NFKC", text or "").casefold().strip()
    text = re.sub(r"[\s]+", " ", text)
    text = re.sub(r"[^\w\s]", "", text)
    return text.strip()


def load_exports(export_dir: Path) -> list[dict[str, Any]]:
    exports = []
    for path in sorted(export_dir.rglob("*.json")):
        try:
            data = json.loads(path.read_text())
        except (OSError, json.JSONDecodeError) as exc:  # pragma: no cover
            print(f"skipped {path}: {exc}")
            continue
        if data.get("kind") != "non_holdout_review_export":
            continue
        data["_path"] = str(path)
        exports.append(data)
    return exports


def latest_per_team(exports: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Keep one export per reviewing team: highest revision, else latest export."""
    best: dict[str, dict[str, Any]] = {}
    for export in exports:
        team = export.get("review_group_id") or export.get("assignment_id") or export["_path"]
        stamp = (export.get("revision") or 0, export.get("exported_at") or "")
        current = best.get(team)
        if current is None or stamp > (current.get("revision") or 0, current.get("exported_at") or ""):
            best[team] = export
    return list(best.values())


def load_bundle_records(bundle_dir: Path | None) -> dict[str, dict[str, Any]]:
    records: dict[str, dict[str, Any]] = {}
    if bundle_dir is None:
        return records
    for path in sorted(bundle_dir.glob("*.json")):
        if path.name == "manifest.json":
            continue
        data = json.loads(path.read_text())
        for record in data.get("records", []):
            key = pair_key(str(record.get("review_id") or ""))
            if key:
                records[key] = record
    return records


NOT_REVIEWED_MARKER = "not reviewed"


def is_self_declared_non_review(review: dict[str, Any]) -> bool:
    """One workshop team used `literal_wording` as a status box.

    Items marked "Not reviewed" there carry the untouched form defaults
    (included/accepted), so they are not judgments and must not be counted as
    coverage or as one side of a disagreement.
    """
    return (review.get("literal_wording") or "").strip().lower().startswith(NOT_REVIEWED_MARKER)


def collect(
    exports: list[dict[str, Any]], exclude_non_reviews: bool = False
) -> dict[str, list[dict[str, Any]]]:
    by_pair: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for export in exports:
        team = export.get("review_group_id") or export.get("assignment_id") or "unknown-team"
        contributors = export.get("contributor_reviewer_ids") or [export.get("reviewer_id")]
        for review_key, review in (export.get("reviews") or {}).items():
            if exclude_non_reviews and is_self_declared_non_review(review):
                continue
            by_pair[pair_key(review_key)].append(
                {
                    "team": team,
                    "contributors": contributors,
                    "review_key": review_key,
                    "disposition": review.get("benchmark_disposition"),
                    "assessment": review.get("pipeline_assessment"),
                    "preferred_question": (review.get("preferred_question") or "").strip(),
                    "literal_wording": (review.get("literal_wording") or "").strip(),
                    "public_comment": (review.get("public_comment") or "").strip(),
                    "internal_comment": (review.get("internal_comment") or "").strip(),
                    "approved_formulation_ids": review.get("approved_formulation_ids") or [],
                    "authored_formulation_ids": review.get("authored_formulation_ids") or [],
                    "export": export["_path"],
                }
            )
    return by_pair


def classify(by_pair: dict[str, list[dict[str, Any]]]) -> dict[str, list[str]]:
    buckets: dict[str, list[str]] = {
        "pairs_reviewed": [],
        "pairs_multi_team": [],
        "pairs_multi_wording_supplied": [],
        "pairs_conflicting_wordings": [],
        "pairs_rewrite_vs_accept": [],
        "pairs_conflicting_disposition": [],
    }
    for key, reviews in by_pair.items():
        buckets["pairs_reviewed"].append(key)
        teams = {r["team"] for r in reviews}
        if len(teams) < 2:
            continue
        buckets["pairs_multi_team"].append(key)

        supplied = [r for r in reviews if r["preferred_question"]]
        supplied_teams = {r["team"] for r in supplied}
        if len(supplied_teams) >= 2:
            buckets["pairs_multi_wording_supplied"].append(key)
            if len({normalise(r["preferred_question"]) for r in supplied}) >= 2:
                buckets["pairs_conflicting_wordings"].append(key)
        if supplied_teams and len(teams - supplied_teams) >= 1:
            buckets["pairs_rewrite_vs_accept"].append(key)
        if len({r["disposition"] for r in reviews if r["disposition"]}) >= 2:
            buckets["pairs_conflicting_disposition"].append(key)
    return buckets


def render_example(key: str, reviews: list[dict[str, Any]], record: dict[str, Any] | None) -> str:
    lines = [f"## {key}", ""]
    if record:
        origin = record.get("output", {}).get("nl_question_origin") or {}
        lines += [
            f"KG: {record.get('kg_id')}   pass: {record.get('workshop_pass')}"
            f"   scope: {record.get('review_scope')}",
            "",
            "Evidence the pipeline worked from:",
        ]
        for item in record.get("input", {}).get("evidence", []):
            lines.append(f"  [{item.get('evidence_id')}] {item.get('snippet')!r}")
            lines.append(f"      source: {item.get('source_url') or item.get('source_path')}")
        lines += [
            "",
            f"Pipeline question ({origin.get('mode')} from {', '.join(origin.get('evidence_ids') or []) or 'n/a'}):",
            f"  {record.get('output', {}).get('nl_question')}",
            "",
            "SPARQL:",
        ]
        sparql = (record.get("input", {}).get("sparql_clean") or "").strip()
        lines += [f"    {line}" for line in sparql.splitlines()] or ["    (none)"]
        lines.append("")
    for index, review in enumerate(sorted(reviews, key=lambda r: r["team"]), start=1):
        lines += [
            f"Team {index} ({review['team']}, contributors: {', '.join(c for c in review['contributors'] if c)})",
            f"  disposition: {review['disposition']}   assessment: {review['assessment']}",
            f"  preferred_question: {review['preferred_question'] or '(none — pipeline wording kept)'}",
        ]
        if review["literal_wording"]:
            lines.append(f"  literal_wording: {review['literal_wording']}")
        for label in ("public_comment", "internal_comment"):
            text = review.get(label) or ""
            if not text:
                continue
            body = text.splitlines()
            lines.append(f"  {label}: {body[0]}")
            lines += [f"      {line}" for line in body[1:]]
        lines.append("")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("export_dir", type=Path, help="directory of non_holdout_review_export JSON files")
    parser.add_argument("--bundles", type=Path, default=None, help="frozen review bundle directory")
    parser.add_argument("--examples", type=int, default=5, help="how many conflicting pairs to print")
    parser.add_argument(
        "--exclude-non-reviews",
        action="store_true",
        help='drop reviews whose literal_wording begins "Not reviewed" (see is_self_declared_non_review)',
    )
    parser.add_argument("--json", type=Path, default=None, help="write the full grouping to this file")
    args = parser.parse_args()

    exports = load_exports(args.export_dir)
    if not exports:
        print(f"no review exports found under {args.export_dir}")
        return 1
    kept = latest_per_team(exports)
    by_pair = collect(kept, exclude_non_reviews=args.exclude_non_reviews)
    buckets = classify(by_pair)
    records = load_bundle_records(args.bundles)

    print(f"exports found:                      {len(exports)}")
    print(f"latest valid revision per team:     {len(kept)}")
    print(f"pair reviews (team x pair):         {sum(len(v) for v in by_pair.values())}")
    print(f"distinct pairs reviewed:            {len(buckets['pairs_reviewed'])}")
    print(f"pairs judged by >= 2 teams:         {len(buckets['pairs_multi_team'])}")
    print(f"  >= 2 teams supplied a wording:    {len(buckets['pairs_multi_wording_supplied'])}")
    print(f"  wordings actually differ:         {len(buckets['pairs_conflicting_wordings'])}")
    print(f"  rewrite vs pipeline-kept:         {len(buckets['pairs_rewrite_vs_accept'])}")
    print(f"  include/exclude disagreement:     {len(buckets['pairs_conflicting_disposition'])}")
    print()

    shown = buckets["pairs_conflicting_wordings"] or buckets["pairs_rewrite_vs_accept"]
    for key in shown[: args.examples]:
        print(render_example(key, by_pair[key], records.get(key)))
        print("-" * 72)

    if args.json:
        args.json.write_text(
            json.dumps({"buckets": {k: sorted(v) for k, v in buckets.items()}, "pairs": by_pair}, indent=2)
        )
        print(f"wrote {args.json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
