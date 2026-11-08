#!/usr/bin/env python3
"""Convert hand-written meta-review notes into the decisions JSONL.

The meta-reviewer works in a markdown file with one `## <query-label>` block per
contested pair and indented `slot : value` lines (see
`var/review/analysis/ipl-2026-09-16-meta-review-decisions.md`). This turns that
into `catalog/curated/meta_review_decisions.jsonl`, which
`merge_adjudicated_reviews` and `update_from_initial_review --decisions` read.

The notes format is a convenience, not a schema: this parser is deliberately
strict about what it recognises and silently skips any `##` heading that does
not name a pair in the bundles, plus any block with no decision recorded yet.
Re-run it after editing the notes; it rewrites the whole JSONL.

`record as alternative` accepts: `all` (the pipeline wording plus every team
rewrite — NOT the evidence label, which is a console example title rather than a
formulation), `all except evidence label` (same thing, kept for notes that say
it explicitly), `none`, `pipeline`, `only evidence, not pipeline`, or literal
text. The canonical wording is never also listed as an alternative.

`pipeline_assessment` is derived rather than recorded by hand: "accepted" only
where the pipeline wording survives as an acceptable alternative, so the
builder's own alternative handling agrees with the explicit choice. Override it
in the JSONL if that is wrong for a pair.
"""

from __future__ import annotations

import argparse
import glob
import json
import re
from pathlib import Path
from typing import Any, Dict, List

DECIDED_SLOTS = ("include/exclude", "canonical wording", "record as alternative", "rationale")
NOT_REVIEWED = "not reviewed"


def load_bundle_records(bundle_dir: Path) -> Dict[str, Dict[str, Any]]:
    records: Dict[str, Dict[str, Any]] = {}
    for path in sorted(bundle_dir.glob("*.json")):
        if path.name == "manifest.json":
            continue
        bundle = json.loads(path.read_text(encoding="utf-8"))
        for record in bundle.get("records", []):
            record["_dataset_id"] = bundle.get("dataset_id")
            records[str(record.get("query_label"))] = record
    return records


def load_team_reviews(export_dir: Path) -> tuple[Dict[str, List[str]], Dict[str, List[str]]]:
    review_ids: Dict[str, List[str]] = {}
    wordings: Dict[str, List[str]] = {}
    for path in sorted(export_dir.glob("*.json")):
        export = json.loads(path.read_text(encoding="utf-8"))
        for key, review in (export.get("reviews") or {}).items():
            if (review.get("literal_wording") or "").strip().lower().startswith(NOT_REVIEWED):
                continue
            label = key.split("::")[1]
            review_ids.setdefault(label, []).append(str(review.get("review_id")))
            preferred = (review.get("preferred_question") or "").strip()
            if preferred:
                wordings.setdefault(label, []).append(preferred)
    return review_ids, wordings


def parse_notes(path: Path, known_labels: set[str]) -> List[Dict[str, Any]]:
    blocks: List[Dict[str, Any]] = []
    current: Dict[str, Any] | None = None
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.startswith("## "):
            label = line[3:].strip()
            current = {"label": label, "slots": {}} if label in known_labels else None
            if current:
                blocks.append(current)
        elif current is not None and line.startswith("    ") and ":" in line:
            slot, _, value = line.strip().partition(":")
            current["slots"][slot.strip()] = value.strip()
    return blocks


def resolve_alternatives(spec: str, record: Dict[str, Any], team_wordings: List[str], canonical: str) -> List[str]:
    evidence = (record.get("input", {}).get("evidence") or [{}])[0].get("snippet", "")
    pipeline = record.get("output", {}).get("nl_question") or ""
    spec_l = spec.strip().lower()
    if spec_l in ("", "none"):
        chosen: List[str] = []
    elif spec_l == "all" or spec_l.startswith("all except evidence"):
        chosen = [pipeline] + team_wordings
    elif spec_l == "pipeline":
        chosen = [pipeline]
    elif spec_l.startswith("only evidence"):
        chosen = [evidence]
    else:
        chosen = [re.sub(r"\s*\((?:not canonical).*$", "", spec).strip()]
    out, seen = [], set()
    for text in chosen:
        text = (text or "").strip()
        if text and text != canonical.strip() and text not in seen:
            seen.add(text)
            out.append(text)
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--notes", type=Path, required=True, help="the hand-written decisions markdown")
    parser.add_argument("--bundles", type=Path, required=True, help="frozen review bundle directory")
    parser.add_argument("--exports", type=Path, required=True, help="team exports for the round")
    parser.add_argument("--out", type=Path, required=True, help="decisions JSONL to write")
    parser.add_argument("--round", required=True, help="round identifier, e.g. ipl-2026-09-16")
    parser.add_argument("--decided-at", required=True, help="date the decisions were made, YYYY-MM-DD")
    args = parser.parse_args()

    records = load_bundle_records(args.bundles)
    team_review_ids, team_wordings = load_team_reviews(args.exports)
    blocks = parse_notes(args.notes, set(records))

    out: List[Dict[str, Any]] = []
    skipped: List[str] = []
    for block in blocks:
        label, slots = block["label"], block["slots"]
        if not any(slots.get(slot) for slot in DECIDED_SLOTS):
            skipped.append(label)
            continue
        record = records[label]
        disposition = {"include": "included", "exclude": "excluded"}.get(slots.get("include/exclude", ""), "included")
        canonical = slots.get("canonical wording", "")
        alternatives = (
            resolve_alternatives(slots.get("record as alternative", ""), record, team_wordings.get(label, []), canonical)
            if disposition == "included"
            else []
        )
        pipeline = (record.get("output", {}).get("nl_question") or "").strip()
        stated = slots.get("pipeline assessment", "").strip()
        if disposition == "excluded":
            assessment = None
        elif stated:
            assessment = stated
        else:
            kept_pipeline = pipeline in [a.strip() for a in alternatives] or canonical.strip() == pipeline
            assessment = "accepted" if kept_pipeline else "prompt_improvement_recommended"
        context = slots.get("graph context required", "")
        out.append({
            "round": args.round,
            "dataset_id": record["_dataset_id"],
            "kg_id": record.get("kg_id"),
            "query_label": label,
            "pair_review_id": record.get("review_id"),
            "benchmark_disposition": disposition,
            "pipeline_assessment": assessment,
            "pipeline_assessment_derived": disposition == "included" and not stated,
            "canonical_question": canonical,
            "alternative_questions": alternatives,
            "graph_context_required": context.lower().startswith("yes"),
            "graph_context_note": context.split(":", 1)[1].strip() if context.lower().startswith("yes:") else "",
            "sparql_correction_required": slots.get("sparql correction", "").startswith("NEEDED"),
            "rationale": slots.get("rationale", ""),
            "superseded_review_ids": sorted(team_review_ids.get(label, [])),
            "decided_at": args.decided_at,
        })

    args.out.write_text("".join(json.dumps(r, ensure_ascii=False) + "\n" for r in out), encoding="utf-8")
    print(f"wrote {args.out} with {len(out)} decisions")
    if skipped:
        print(f"skipped {len(skipped)} blocks with no decision recorded yet: {', '.join(skipped)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
