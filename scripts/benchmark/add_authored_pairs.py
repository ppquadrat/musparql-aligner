#!/usr/bin/env python3
"""Admit pairs whose question is already settled, with no generation round.

`build_review_bundle` iterates over the generation outputs and decorates each
one from the inputs index, so a pair absent from the outputs file yields no
bundle record at all. A benchmark record is then made from a bundle record plus
a review export. That chain means a pair whose question is already settled --
authored by the source that published the SPARQL, or decided in meta-review --
could previously enter the benchmark only by generating a wording that would be
discarded and reviewing a pairing that had already been judged.

Nothing in the data model required that. The 20 LinkedMusic pairs admitted in
v8 and carried forward since are generation-free and review-free: a null model
output, a nulled run, a review block naming no reviewer,
`pipeline_assessment: not_applicable`, a `source` block in place of reviewer
attribution, and no `sparql_provenance`. Every consumer already accommodates
that shape -- `benchmark_gold_records` branches on `source.source_type` and
attributes to a reviewer only when one is named, and neither `audit_snapshot`
nor `build_public_release` requires a review block. What was missing was a
writer: v8's admission was a one-off migration that is no longer in the tree.

This is that writer. Three origins:

* `source_prompt` -- the source itself paired question and SPARQL, as the
  LinkedMusic public query database did. The pair spec carries the question and
  the source block, because that provenance lives outside this repository.
* `meta_review_decision` -- the wording was decided in meta-review. The pair
  spec carries only the identity; the question, disposition and alternatives are
  read from `catalog/curated/meta_review_decisions.jsonl`, so there is one
  source of truth and no wording is retyped.
* `retained_sparql_repin` -- the pair was already reviewed and is already in
  the previous snapshot, and only its SPARQL has changed since. The question
  and the review that approved it are carried forward untouched and just the
  version pin moves, so the pair keeps the reviewer attribution it earned.
  Re-admitting it as an authored pair would throw that away.

The decision's own `pipeline_assessment` judges the wording the pipeline
produced for that pair, which is not the wording being admitted here. It is
preserved in the `source` block as `meta_review_pipeline_assessment` rather
than written to the record's `pipeline_assessment`, which stays
`not_applicable`: there is no model output in this record to assess.

What is given up is recorded, not hidden. An authored pair carries no pipeline
assessment, and a meta-review wording has one expert's confirmation where a
review round has several. Both facts are legible from `gold_question_source`
and the `source` block.

The manifest's `gold_question_policy` is unioned with the previous snapshot's,
so an assertion made when content was admitted keeps travelling with every
later snapshot that carries that content forward. v9 and v10 dropped v8's
`source_prompt_used_for_curated_linkedmusic` key while still carrying its 20
records, which is the failure mode this avoids.

Usage:
    python -m scripts.benchmark.add_authored_pairs \\
      --previous-benchmark benchmark/v10 \\
      --pairs var/benchmark/authored-pairs-2026-11-09.jsonl \\
      --decisions catalog/curated/meta_review_decisions.jsonl \\
      --meta-reviewer-id reviewer-0001 \\
      --outdir benchmark/v11 \\
      --holdout-selectors var/holdout/selectors.jsonl
"""
from __future__ import annotations

import argparse
import hashlib
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

from musparql.holdout_selectors import (
    add_holdout_filter_arguments,
    holdout_input_policy,
    validate_selectors_current,
)
from musparql.reviewer_provenance import validate_public_reviewer_ids
from musparql.sparql_versions import resolve_sparql_version

from scripts.benchmark.build_benchmark import (
    ALTERNATIVES_FILE,
    INCLUDED_FILE,
    LINGUISTIC_ANNOTATIONS_FILE,
    PUBLIC_RELEASE_FILES,
    add_formulation,
    alternatives_record_has_content,
    benchmark_gold_records,
    canonical_execution_snapshot,
    make_rephrasing_entry,
    read_json,
    read_jsonl,
    source_evidence_types,
    sort_sidecar_records,
    update_sidecar_identity,
    write_json,
    write_jsonl,
)
from scripts.benchmark.update_benchmark import included_records_path, pair_key

ORIGINS = ("source_prompt", "meta_review_decision", "retained_sparql_repin")
UPDATE_TYPE = "authored_pair_admission"


def sort_records(records: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return sorted(
        records,
        key=lambda rec: (str(rec.get("kg_id") or ""), str(rec.get("query_label") or "")),
    )


def benchmark_identity(kg_id: str, query_label: str, query_id: str, sparql_hash: str, question: str) -> str:
    """Stable id for an authored pair, in the `kg::label::digest` shape v8 used."""
    digest = hashlib.sha256(
        "\u0000".join((kg_id, query_label, query_id, sparql_hash, question)).encode("utf-8")
    ).hexdigest()[:12]
    return f"{kg_id}::{query_label}::{digest}"


def evidence_summary(evidence: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Summarize the canonical record's evidence for an authored pair.

    `has_query_specific_evidence` is true by construction: an authored pair's
    question was written for this query, not inferred from KG-level material.
    v8's LinkedMusic records assert the same thing for `curated_source_prompt`,
    which `has_query_specific_evidence()` does not recognise.
    """
    types = source_evidence_types(evidence)
    return {
        "evidence_count": len(evidence),
        "evidence_types": types,
        "has_source_evidence": bool(types),
        "has_query_specific_evidence": True,
    }


def load_decisions(path: Path | None) -> Dict[Tuple[str, str], Dict[str, Any]]:
    if path is None:
        return {}
    decisions: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        decision = json.loads(line)
        key = (str(decision.get("kg_id") or ""), str(decision.get("query_label") or ""))
        if key in decisions:
            raise ValueError(f"Duplicate meta-review decision for {key[0]}/{key[1]}")
        decisions[key] = decision
    return decisions


def repinned_record(
    *, spec: Dict[str, Any], query: Dict[str, Any], previous: Dict[str, Any]
) -> Tuple[Dict[str, Any], str, List[str]]:
    """Re-pin an already reviewed pair to the latest retained SPARQL.

    The wording and the review that approved it are unchanged, so nothing is
    re-decided and nothing is generated: only the SPARQL pin moves. The
    original review block is preserved verbatim, which is why this is better
    than re-admitting the pair as an authored one -- re-admission would discard
    the reviewer attribution the pair already earned.
    """
    kg_id = str(query.get("kg_id") or "")
    query_label = str(query.get("query_label") or "")
    resolved = resolve_sparql_version(query, "latest")
    if str(previous.get("sparql_hash") or "") == resolved["sparql_hash"]:
        raise ValueError(
            f"{kg_id}/{query_label} is already pinned to the latest retained SPARQL; "
            "there is nothing to re-pin."
        )
    if not str(previous.get("gold_question") or "").strip():
        raise ValueError(f"{kg_id}/{query_label}: the previous record has no gold question to keep")
    record = dict(previous)
    record["sparql"] = resolved["sparql"]
    record["sparql_version"] = resolved["sparql_version"]
    record["sparql_hash"] = resolved["sparql_hash"]
    # The previous pin's edit provenance described a different version. Dropping
    # it is what lets the audit accept the record: audit_version_pins checks
    # edit provenance only when it is present, and no reviewer has looked at
    # provenance for this version.
    record.pop("sparql_provenance", None)
    source = dict(record.get("source") or {})
    # `source_type` is deliberately left as it was. A re-pin does not change
    # where the question came from, and a reviewed pair carries no `source`
    # block at all, so inventing one here would downgrade the record's
    # `human_review` provenance to a description of this bookkeeping step.
    source.update(
        {
            "sparql_source": f"selected retained version {resolved['sparql_version']}",
            "repinned_from_sparql_version": previous.get("sparql_version"),
            "repinned_from_sparql_hash": previous.get("sparql_hash"),
            "repin_note": (
                "The SPARQL was corrected after this pair was reviewed. The question and the "
                "review that approved it are unchanged; only the version pin moved."
            ),
            "execution_note": "Execution is tracked by SPARQL version and hash in kg_queries.jsonl.",
        }
    )
    record["source"] = source
    note = str(spec.get("note") or "").strip()
    if note:
        review = dict(record.get("review") or {})
        review["internal_comment"] = note
        record["review"] = review
    return record, str(record.get("benchmark_disposition") or "included"), []


def authored_record(
    *,
    spec: Dict[str, Any],
    query: Dict[str, Any],
    decision: Dict[str, Any] | None,
    pairs_path: Path,
    decisions_path: Path | None,
    meta_reviewer_id: str,
) -> Tuple[Dict[str, Any], str, List[str]]:
    """Build the included record, its disposition, and its alternative wordings."""
    origin = str(spec.get("origin") or "")
    kg_id = str(query.get("kg_id") or "")
    query_label = str(query.get("query_label") or "")
    query_id = str(query.get("query_id") or "")
    resolved = resolve_sparql_version(query, "latest")
    alternatives: List[str] = []
    note = str(spec.get("note") or "").strip()

    if origin == "meta_review_decision":
        if decision is None:
            raise ValueError(f"{kg_id}/{query_label}: no meta-review decision found")
        question = " ".join(str(decision.get("canonical_question") or "").split())
        if not question:
            raise ValueError(
                f"{kg_id}/{query_label}: the meta-review decision records no canonical question. "
                "Decide the wording in the notes first."
            )
        disposition = str(decision.get("benchmark_disposition") or "included")
        alternatives = [
            " ".join(str(text).split())
            for text in (decision.get("alternative_questions") or [])
            if str(text).strip()
        ]
        pair_review_id = str(decision.get("pair_review_id") or "")
        if not pair_review_id:
            raise ValueError(f"{kg_id}/{query_label}: the meta-review decision has no pair_review_id")
        # A public review event id must end in the approving reviewer's
        # pseudonym, the same shape the team review ids use.
        review_id = f"{pair_review_id}::{meta_reviewer_id}"
        source = {
            "source_type": "meta_review_decision",
            "source_id": "musparql-meta-review-decisions",
            "source_file": str(decisions_path) if decisions_path else None,
            "prompt_source": "Musparql meta-review",
            "meta_review_round": decision.get("round"),
            "meta_review_pair_review_id": decision.get("pair_review_id"),
            "meta_review_pipeline_assessment": decision.get("pipeline_assessment"),
            "meta_review_decided_at": decision.get("decided_at"),
            "superseded_review_count": int(decision.get("superseded_review_count") or 0),
            "confirmation": "single_meta_reviewer",
            "sparql_source": f"selected retained version {resolved['sparql_version']}",
            "execution_note": "Execution is tracked by SPARQL version and hash in kg_queries.jsonl.",
        }
        internal_comment = note or (
            "Admitted without a generation round: the canonical wording was decided in "
            "meta-review against the retained SPARQL version, so there is no model output "
            "to review. The meta-review rationale is public provenance in the decisions file."
        )
        reviewer_id: str | None = meta_reviewer_id
    elif origin == "source_prompt":
        question = " ".join(str(spec.get("gold_question") or "").split())
        if not question:
            raise ValueError(f"{kg_id}/{query_label}: source_prompt pairs must carry gold_question")
        source_spec = spec.get("source")
        if not isinstance(source_spec, dict) or not source_spec.get("source_id"):
            raise ValueError(
                f"{kg_id}/{query_label}: source_prompt pairs must carry a source block with source_id"
            )
        disposition = str(spec.get("benchmark_disposition") or "included")
        if spec.get("alternative_questions"):
            raise ValueError(
                f"{kg_id}/{query_label}: a source_prompt pair cannot carry alternative wordings. "
                "An accepted alternative needs an approving reviewer to attribute it to, and a "
                "source-authored pairing has none. Record further source wordings as their own "
                "pairs, or admit the alternatives through a review."
            )
        source = {
            "source_type": str(source_spec.get("source_type") or "curated_source_prompt"),
            "confirmation": "source_authored_pairing",
            "sparql_source": f"selected retained version {resolved['sparql_version']}",
            "execution_note": "Execution is tracked by SPARQL version and hash in kg_queries.jsonl.",
            **{k: v for k, v in source_spec.items() if k != "source_type"},
        }
        review_id = benchmark_identity(kg_id, query_label, query_id, resolved["sparql_hash"], question)
        internal_comment = note or (
            "Admitted without a generation round: the source published this question with this "
            "SPARQL, so the pairing has the source's own authority and there is no model output "
            "to review."
        )
        reviewer_id = None
    else:
        raise ValueError(f"{kg_id}/{query_label}: origin must be one of {ORIGINS}, not {origin!r}")

    if disposition not in ("included", "excluded"):
        raise ValueError(f"{kg_id}/{query_label}: benchmark_disposition must be included or excluded")

    record = {
        "review_submissions": (
            int(decision.get("superseded_review_count") or 0)
            if origin == "meta_review_decision" and decision
            else 0
        ),
        "adjudicated": bool(
            origin == "meta_review_decision"
            and decision
            and decision.get("superseded_review_count")
        ),
        "benchmark_id": benchmark_identity(
            kg_id, query_label, query_id, resolved["sparql_hash"], question
        ),
        "kg_id": kg_id,
        "query_id": query_id,
        "query_label": query_label,
        "sparql": resolved["sparql"],
        "sparql_version": resolved["sparql_version"],
        "sparql_hash": resolved["sparql_hash"],
        "gold_question": question,
        "gold_question_source": origin,
        "benchmark_disposition": disposition,
        "pipeline_assessment": "not_applicable",
        "split": "public",
        "evidence_summary": evidence_summary(
            [item for item in (query.get("evidence") or []) if isinstance(item, dict)]
        ),
        "source": source,
        "model_output": {
            "nl_question": None,
            "origin_mode": origin,
            "confidence": None,
            "confidence_rationale": None,
            "needs_review": False,
            "retained_evidence_phrases": [],
        },
        "run": {
            "run_id": None,
            "generation_run_id": None,
            "run_manifest": None,
            "run_label": "authored_pair",
            "source_file": str(pairs_path),
            "model": None,
            "run_signature": None,
        },
        "review": {
            "review_id": review_id,
            "reviewer_id": reviewer_id,
            "review_export": None,
            "dataset_id": None,
            "run_id": None,
            "generation_run_id": None,
            "literal_wording": "",
            "updated_at": None,
            "public_comment": "",
            "internal_comment": internal_comment,
        },
    }
    if reviewer_id is None:
        del record["review"]["reviewer_id"]
    return record, disposition, alternatives


def alternatives_sidecar(
    *,
    record: Dict[str, Any],
    texts: List[str],
    benchmark_version: str,
) -> Dict[str, Any] | None:
    review = record.get("review") or {}
    review_id = str(review.get("review_id") or record.get("benchmark_id") or "")
    sidecar: Dict[str, Any] = {}
    for text in texts:
        entry = make_rephrasing_entry(
            text=text,
            source_type=f"{record.get('gold_question_source')}_alternative",
            review={"review_id": review_id},
            review_id=review_id,
            review_path=None,
            dataset_id="",
            record={"query_id": record.get("query_id")},
        )
        if not entry:
            continue
        add_formulation(sidecar, "accepted_alternatives", entry)
    if not alternatives_record_has_content(sidecar):
        return None
    update_sidecar_identity(
        sidecar, benchmark_record=record, benchmark_version=benchmark_version, built_at=""
    )
    return sidecar


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--previous-benchmark", required=True, help="Previous benchmark/vN directory.")
    parser.add_argument("--pairs", required=True, help="JSONL of authored pair specs.")
    parser.add_argument("--outdir", required=True, help="Output benchmark/vN+1 directory.")
    parser.add_argument("--kg-queries", default="var/queries/kg_queries.jsonl")
    parser.add_argument(
        "--decisions",
        default=None,
        help="Meta-review decisions JSONL; required for meta_review_decision pairs.",
    )
    parser.add_argument(
        "--meta-reviewer-id",
        default="",
        help="The meta-reviewer's pseudonymous reviewer-NNNN id; required for meta_review_decision pairs.",
    )
    parser.add_argument(
        "--replace-existing",
        action="store_true",
        help=(
            "Allow an authored pair to replace the same pair already in the previous snapshot. "
            "A reviewed record replaced this way loses its reviewer attribution, so the swap is "
            "never implicit."
        ),
    )
    parser.add_argument("--dry-run", action="store_true", help="Report without writing the snapshot.")
    add_holdout_filter_arguments(parser)
    args = parser.parse_args()

    previous_dir = Path(args.previous_benchmark)
    pairs_path = Path(args.pairs)
    outdir = Path(args.outdir)
    query_path = Path(args.kg_queries)
    decisions_path = Path(args.decisions) if args.decisions else None

    specs = read_jsonl(pairs_path)
    if not specs:
        raise ValueError(f"No authored pairs in {pairs_path}")
    needs_meta = any(str(spec.get("origin")) == "meta_review_decision" for spec in specs)
    if needs_meta:
        if decisions_path is None:
            raise ValueError("--decisions is required for meta_review_decision pairs")
        if not args.meta_reviewer_id:
            raise ValueError("--meta-reviewer-id is required for meta_review_decision pairs")
        validate_public_reviewer_ids([{"reviewer_id": args.meta_reviewer_id}], "--meta-reviewer-id")

    queries = read_jsonl(query_path)
    by_label = {
        (str(row.get("kg_id") or ""), str(row.get("query_label") or "")): row for row in queries
    }
    selector_rows = read_jsonl(Path(args.holdout_selectors)) if args.holdout_selectors else []
    forbidden = validate_selectors_current(selector_rows, queries) if selector_rows else set()
    decisions = load_decisions(decisions_path)

    included_by_key = {pair_key(rec): rec for rec in read_jsonl(included_records_path(previous_dir))}
    dismissed_by_key = {pair_key(rec): rec for rec in read_jsonl(previous_dir / "dismissed.jsonl")}
    alternatives_by_key = {pair_key(rec): rec for rec in read_jsonl(previous_dir / ALTERNATIVES_FILE)}
    annotations_by_key = {
        pair_key(rec): rec for rec in read_jsonl(previous_dir / LINGUISTIC_ANNOTATIONS_FILE)
    }

    benchmark_version = outdir.name
    origin_counts: Counter[str] = Counter()
    replaced: List[str] = []
    repinned: List[str] = []
    for spec in specs:
        kg_id = str(spec.get("kg_id") or "")
        query_label = str(spec.get("query_label") or "")
        query = by_label.get((kg_id, query_label))
        if query is None:
            raise ValueError(f"Unknown pair in the canonical catalogue: {kg_id}/{query_label}")
        key = pair_key({"kg_id": kg_id, "query_id": query.get("query_id")})
        if key in forbidden:
            raise ValueError(f"Refusing to admit holdout-selected pair {kg_id}/{query_label}")
        previous_record = included_by_key.get(key) or dismissed_by_key.get(key)
        if str(spec.get("origin")) == "retained_sparql_repin":
            if previous_record is None:
                raise ValueError(
                    f"{kg_id}/{query_label} is not in {previous_dir}, so there is no pin to move. "
                    "Admit it as an authored pair instead."
                )
            record, disposition, alternative_texts = repinned_record(
                spec=spec, query=query, previous=previous_record
            )
            included_by_key.pop(key, None)
            dismissed_by_key.pop(key, None)
            if disposition == "excluded":
                dismissed_by_key[key] = record
            else:
                included_by_key[key] = record
                # The pair keeps whatever alternatives and annotations it had:
                # the wording did not change, so neither did its sidecars.
                for sidecars in (alternatives_by_key, annotations_by_key):
                    sidecar = sidecars.get(key)
                    if sidecar is not None:
                        update_sidecar_identity(
                            sidecar,
                            benchmark_record=record,
                            benchmark_version=benchmark_version,
                            built_at="",
                        )
            origin_counts["retained_sparql_repin"] += 1
            repinned.append(f"{kg_id}/{query_label}")
            continue
        record, disposition, alternative_texts = authored_record(
            spec=spec,
            query=query,
            decision=decisions.get((kg_id, query_label)),
            pairs_path=pairs_path,
            decisions_path=decisions_path,
            meta_reviewer_id=args.meta_reviewer_id,
        )
        if previous_record is not None:
            if not args.replace_existing:
                raise ValueError(
                    f"{kg_id}/{query_label} is already in {previous_dir}. "
                    "Pass --replace-existing to replace it, which discards its review attribution."
                )
            if str(previous_record.get("sparql_hash") or "") == record["sparql_hash"]:
                raise ValueError(
                    f"{kg_id}/{query_label} is already in {previous_dir} at the same SPARQL version; "
                    "there is nothing to admit."
                )
            included_by_key.pop(key, None)
            dismissed_by_key.pop(key, None)
            alternatives_by_key.pop(key, None)
            annotations_by_key.pop(key, None)
            replaced.append(f"{kg_id}/{query_label}")
        if disposition == "excluded":
            dismissed_by_key[key] = record
        else:
            included_by_key[key] = record
            sidecar = alternatives_sidecar(
                record=record, texts=alternative_texts, benchmark_version=benchmark_version
            )
            if sidecar is not None:
                alternatives_by_key[key] = sidecar
        origin_counts[str(record["gold_question_source"])] += 1

    included = sort_records(included_by_key.values())
    dismissed = sort_records(dismissed_by_key.values())
    previous_manifest = (
        read_json(previous_dir / "manifest.json") if (previous_dir / "manifest.json").exists() else {}
    )
    built_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    alternative_records = sort_sidecar_records(list(alternatives_by_key.values()))
    annotation_records = sort_sidecar_records(list(annotations_by_key.values()))
    for sidecar in alternative_records + annotation_records:
        sidecar["benchmark_version"] = benchmark_version
        sidecar["benchmark_built_at"] = built_at
    benchmark_records = benchmark_gold_records(
        included=included, benchmark_version=benchmark_version, built_at=built_at
    )

    # Carry forward every assertion the previous snapshot made about its own
    # content, so an admission policy does not quietly lapse on the next build.
    gold_question_policy = dict(previous_manifest.get("gold_question_policy") or {})
    gold_question_policy.update(
        {
            "benchmark_contains_all_human_confirmed_included_pairs": True,
            "unchanged_previous_items_carried_forward": True,
            "authored_pairs_admitted_without_generation_or_review": True,
        }
    )
    if origin_counts.get("source_prompt"):
        gold_question_policy["source_prompt_used_for_source_authored_pairs"] = True
    if origin_counts.get("meta_review_decision"):
        gold_question_policy["meta_review_canonical_question_used_for_authored_pairs"] = True
    if origin_counts.get("retained_sparql_repin"):
        gold_question_policy["previous_question_kept_when_sparql_was_repinned"] = True

    manifest = {
        "benchmark_version": benchmark_version,
        "built_at": built_at,
        "update_type": UPDATE_TYPE,
        "previous_benchmark": str(previous_dir),
        "previous_benchmark_version": previous_manifest.get("benchmark_version"),
        "source_bundle": None,
        "source_review_export": None,
        "authored_pairs": str(pairs_path),
        "meta_review_decisions": str(decisions_path) if decisions_path else None,
        # The adjudicator's pseudonym stays out of the tracked manifest; the
        # fact of adjudication is what a reader needs, and the ID is retained
        # in the ignored included.jsonl.
        "meta_review_adjudicated": bool(args.meta_reviewer_id),
        "holdout_input_policy": holdout_input_policy(args),
        "sparql_version_policy": "latest_retained",
        "execution_snapshot": canonical_execution_snapshot(
            included, query_path=query_path, captured_through=built_at
        ),
        "counts": {
            "benchmark": len(benchmark_records),
            "included": len(included),
            "dismissed": len(dismissed),
            "alternatives": len(alternative_records),
            "linguistic_annotations": len(annotation_records),
            "admitted_authored_pairs": sum(origin_counts.values()),
            "admitted_authored_pair_origins": dict(origin_counts),
            "replaced_existing_pairs": len(replaced),
            "repinned_existing_pairs": len(repinned),
            "pipeline_assessment_counts": dict(
                Counter(str(row.get("pipeline_assessment")) for row in included)
            ),
            "benchmark_disposition_counts": dict(
                Counter(str(row.get("benchmark_disposition")) for row in included + dismissed)
            ),
        },
        "files": {"benchmark": "benchmark.jsonl", "alternatives": ALTERNATIVES_FILE},
        "working_files": {
            "included": INCLUDED_FILE,
            "dismissed": "dismissed.jsonl",
            "linguistic_annotations_internal": LINGUISTIC_ANNOTATIONS_FILE,
        },
        "gold_question_policy": gold_question_policy,
        "authored_pair_policy": {
            "origins": list(ORIGINS),
            "pipeline_assessment": "not_applicable; an authored pair has no model output to assess",
            "confirmation": {
                "source_prompt": "source_authored_pairing",
                "meta_review_decision": "single_meta_reviewer",
                "retained_sparql_repin": "carried_forward_from_previous_review",
            },
            "replaced_existing_pairs": replaced,
            "repinned_existing_pairs": repinned,
            "repin": (
                "retained_sparql_repin moves an already reviewed pair to the latest retained "
                "SPARQL, keeping its question and its review; nothing is re-decided"
            ),
        },
        "release_boundary": {
            "public_release_files": list(PUBLIC_RELEASE_FILES),
            "internal_only_files": [INCLUDED_FILE, LINGUISTIC_ANNOTATIONS_FILE, "dismissed.jsonl"],
        },
    }

    summary = (
        f"{'Would admit' if args.dry_run else 'Admitted'} {sum(origin_counts.values())} authored pairs "
        f"({dict(origin_counts)}); benchmark {len(benchmark_records)} records, "
        f"included {len(included)}, dismissed {len(dismissed)}, alternatives {len(alternative_records)}"
    )
    if args.dry_run:
        print(summary)
        if replaced:
            print(f"Would replace: {', '.join(replaced)}")
        if repinned:
            print(f"Would re-pin: {', '.join(repinned)}")
        return 0

    outdir.mkdir(parents=True, exist_ok=True)
    write_json(outdir / "manifest.json", manifest)
    write_jsonl(outdir / "benchmark.jsonl", benchmark_records)
    write_jsonl(outdir / INCLUDED_FILE, included)
    write_jsonl(outdir / "dismissed.jsonl", dismissed)
    write_jsonl(outdir / ALTERNATIVES_FILE, alternative_records)
    write_jsonl(outdir / LINGUISTIC_ANNOTATIONS_FILE, annotation_records)
    print(f"Wrote manifest to {outdir / 'manifest.json'}")
    print(summary)
    if replaced:
        print(f"Replaced: {', '.join(replaced)}")
    if repinned:
        print(f"Re-pinned: {', '.join(repinned)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
