#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple


HOLDOUT_SPLIT = "private_holdout"
INCLUDED_FILE = "included.jsonl"
ALTERNATIVES_FILE = "alternatives.jsonl"
LINGUISTIC_ANNOTATIONS_FILE = "linguistic_annotations.jsonl"
PUBLIC_RELEASE_FILES = (
    "manifest.json",
    "benchmark.jsonl",
    ALTERNATIVES_FILE,
    "LICENSE",
    "THIRD_PARTY_NOTICES.md",
)

BENCHMARK_DISPOSITIONS = frozenset({"included", "excluded", "withheld"})
PIPELINE_ASSESSMENTS = frozenset(
    {
        "accepted",
        "prompt_improvement_recommended",
        "input_data_improvement_recommended",
        "not_applicable",
    }
)

PUBLIC_SPARQL_PROVENANCE_FIELDS = (
    "retained_edit_count",
    "selected_version",
    "selected_hash",
    "history_digest",
)
PUBLIC_EXECUTION_OBSERVATION_FIELDS = (
    "status",
    "attempted",
    "observed_at",
    "result_count",
)
PUBLIC_SELECTED_EDIT_FIELDS = (
    "decision",
    "edit_type",
    "rationale",
    "proposal_origin",
    "reviewed_at",
    "approved_sparql_version",
    "approved_sparql_hash",
)


def _selected_public_fields(value: Dict[str, Any], fields: Tuple[str, ...]) -> Dict[str, Any]:
    return {
        field: value[field]
        for field in fields
        if field in value and value[field] not in (None, "", [], {})
    }


def public_sparql_provenance(value: Any) -> Dict[str, Any] | None:
    """Project internal SPARQL correction state onto stable public provenance."""
    if not isinstance(value, dict):
        return None
    result = _selected_public_fields(value, PUBLIC_SPARQL_PROVENANCE_FIELDS)
    observation = value.get("execution_observation")
    if isinstance(observation, dict):
        public_observation = _selected_public_fields(observation, PUBLIC_EXECUTION_OBSERVATION_FIELDS)
        if public_observation:
            result["execution_observation"] = public_observation
    selected_edit = value.get("selected_edit")
    if isinstance(selected_edit, dict):
        public_edit = _selected_public_fields(selected_edit, PUBLIC_SELECTED_EDIT_FIELDS)
        if public_edit:
            result["selected_edit"] = public_edit
    return result or None


def normalized_review_decision(review: Dict[str, Any]) -> Tuple[str, str]:
    """Return and validate the benchmark disposition and pipeline assessment."""
    if review.get("status"):
        raise ValueError("Legacy review decisions are not supported; migrate the export before building")
    explicit_disposition = str(review.get("benchmark_disposition") or "")
    explicit_assessment = str(review.get("pipeline_assessment") or "")

    if review.get("split") == HOLDOUT_SPLIT:
        if explicit_disposition and explicit_disposition not in {"included", "withheld"}:
            raise ValueError("A private holdout cannot also be excluded")
        explicit_disposition = "withheld"

    if not explicit_disposition and explicit_assessment:
        explicit_disposition = "included"
    if explicit_disposition and explicit_disposition not in BENCHMARK_DISPOSITIONS:
        raise ValueError(f"Unknown benchmark disposition: {explicit_disposition!r}")
    if explicit_assessment and explicit_assessment not in PIPELINE_ASSESSMENTS:
        raise ValueError(f"Unknown pipeline assessment: {explicit_assessment!r}")
    if explicit_disposition == "included" and not explicit_assessment:
        raise ValueError("An included decision requires a pipeline assessment")
    if explicit_disposition == "excluded" and explicit_assessment:
        raise ValueError("An excluded decision cannot carry a pipeline assessment")
    if explicit_disposition in {"excluded", "withheld"} and explicit_assessment == "not_applicable":
        raise ValueError(f"{explicit_disposition!r} cannot use the included-source assessment 'not_applicable'")
    return explicit_disposition, explicit_assessment

def benchmark_disposition(review: Dict[str, Any]) -> str:
    return normalized_review_decision(review)[0]


def pipeline_assessment(review: Dict[str, Any]) -> str:
    return normalized_review_decision(review)[1]


def read_review_bundle(path: Path) -> Dict[str, Any]:
    text = path.read_text(encoding="utf-8")
    prefix = "window.REVIEW_DATA = "
    if text.startswith(prefix):
        text = text[len(prefix):]
    text = text.rstrip().rstrip(";")
    data = json.loads(text)
    if not isinstance(data, dict):
        raise ValueError(f"Bad review bundle format: {path}")
    return data


def read_json(path: Path) -> Dict[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"Expected JSON object in {path}")
    return data


def write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def neutral_execution_snapshot(records: List[Dict[str, Any]], *, captured_through: str) -> Dict[str, Any]:
    """Summarize neutral execution observations already pinned in record provenance."""
    statuses: Counter[str] = Counter()
    attempted = 0
    observed_times: List[str] = []
    for record in records:
        provenance = record.get("sparql_provenance") if isinstance(record.get("sparql_provenance"), dict) else {}
        observation = provenance.get("execution_observation") if isinstance(provenance, dict) else None
        if not isinstance(observation, dict):
            observation = {"status": "not_attempted", "attempted": False}
        status = str(observation.get("status") or "not_attempted")
        statuses[status] += 1
        if observation.get("attempted") is True:
            attempted += 1
        if observation.get("observed_at"):
            observed_times.append(str(observation["observed_at"]))
    return {
        "source": "pinned sparql_provenance.execution_observation",
        "captured_through": max(observed_times) if observed_times else captured_through,
        "selected_queries": len(records),
        "selected_versions_with_execution": attempted,
        "status_counts": dict(sorted(statuses.items())),
        "note": "Execution observations are trust provenance only; missing or failed observations do not affect inclusion.",
    }


def canonical_execution_snapshot(
    records: List[Dict[str, Any]],
    *,
    query_path: Path,
    captured_through: str,
) -> Dict[str, Any]:
    """Summarize canonical execution history for the selected SPARQL pins."""
    canonical = {pair_key(row): row for row in read_jsonl(query_path)}
    statuses: Counter[str] = Counter()
    attempted = 0
    for record in records:
        query = canonical.get(pair_key(record))
        observations = [
            item
            for item in ((query or {}).get("execution_history") or [])
            if isinstance(item, dict)
            and item.get("sparql_version") == record.get("sparql_version")
            and item.get("sparql_hash") == record.get("sparql_hash")
            and str(item.get("ran_at") or "") <= captured_through
        ]
        if not observations:
            statuses["not_attempted"] += 1
            continue
        observation = max(observations, key=lambda item: str(item.get("ran_at") or ""))
        attempted += 1
        statuses[str(observation.get("status") or "unknown")] += 1
    return {
        "source": str(query_path),
        "captured_through": captured_through,
        "selected_queries": len(records),
        "selected_versions_with_execution": attempted,
        "status_counts": dict(sorted(statuses.items())),
        "note": "Execution observations are trust provenance only; missing or failed observations do not affect inclusion.",
    }


def write_jsonl(path: Path, records: List[Dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as fh:
        for rec in records:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")


def read_jsonl(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    records: List[Dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        item = json.loads(line)
        if isinstance(item, dict):
            records.append(item)
    return records


def source_evidence_types(evidence: List[Dict[str, Any]]) -> List[str]:
    return sorted({str(ev.get("type")) for ev in evidence if isinstance(ev, dict) and ev.get("type")})


def has_query_specific_evidence(evidence_types: List[str]) -> bool:
    return any(t in {"query_comment", "readme_query_desc", "doc_query_desc", "web_query_desc"} for t in evidence_types)


def is_private_holdout(review: Dict[str, Any]) -> bool:
    return review.get("split") == HOLDOUT_SPLIT or benchmark_disposition(review) == "withheld"


def assert_no_private_reviews(reviews: Dict[str, Any]) -> None:
    """Reject mixed/private exports at the public benchmark boundary."""
    private_ids = [
        str(review_id)
        for review_id, review in reviews.items()
        if isinstance(review, dict) and is_private_holdout(review)
    ]
    if private_ids:
        raise ValueError(
            "Public benchmark tools cannot consume private holdout annotations. "
            "Use the browser's sanitized public export; private records must remain outside the repository."
        )


def assert_non_holdout_export(payload: Dict[str, Any]) -> None:
    """Require the browser's explicitly sanitized export format."""
    if payload.get("kind") != "non_holdout_review_export":
        raise ValueError(
            "Public benchmark tools require kind='non_holdout_review_export'; "
            "legacy, mixed, and private review exports are rejected"
        )


def pair_key(record: Dict[str, Any]) -> Tuple[str, str]:
    return (str(record.get("kg_id") or ""), str(record.get("query_id") or ""))


def normalize_rephrasing_text(text: Any) -> str:
    return " ".join(str(text or "").split()).casefold()


def remove_literal_from_comment(comment: Any, literal: Any) -> str:
    """Remove a legacy ``Literal: ...`` line once it is stored separately."""
    text = str(comment or "").replace("\r\n", "\n").replace("\r", "\n")
    expected = normalize_rephrasing_text(literal)
    if not text.strip() or not expected:
        return text.strip()
    kept: List[str] = []
    for line in text.splitlines():
        cleaned = line.strip().lstrip("\ufeff").replace("\xa0", " ")
        if cleaned.casefold().startswith("literal:"):
            candidate = cleaned.split(":", 1)[1]
            if normalize_rephrasing_text(candidate) == expected:
                continue
        kept.append(line.rstrip())
    while kept and not kept[0].strip():
        kept.pop(0)
    while kept and not kept[-1].strip():
        kept.pop()
    return "\n".join(kept)


def literal_wording(review: Dict[str, Any]) -> str:
    explicit = " ".join(str(review.get("literal_wording") or "").split())
    if explicit:
        return explicit
    note = str(review.get("note") or "")
    for line in note.splitlines():
        stripped = line.strip().lstrip("\ufeff").replace("\xa0", " ")
        if stripped.casefold().startswith("literal:"):
            return " ".join(stripped.split(":", 1)[1].split())
    return ""


def review_comments(review: Dict[str, Any]) -> Tuple[str, str]:
    """Return normalized public/internal comments with legacy-note support."""
    literal = literal_wording(review)
    if "public_comment" in review:
        public = review.get("public_comment") or ""
        internal = review.get("internal_comment") or ""
    else:
        # The legacy note field predated the public/private contract and was
        # excluded from public releases, so migration must fail closed.
        public = ""
        internal = review.get("internal_comment") or review.get("note") or ""
    return (
        remove_literal_from_comment(public, literal),
        remove_literal_from_comment(internal, literal),
    )


def normalize_interpretive(review: Dict[str, Any]) -> Dict[str, Any]:
    raw = review.get("interpretive")
    if not isinstance(raw, dict):
        return {}

    def score(value: Any) -> int | None:
        if value is None or value == "":
            return None
        try:
            number = int(value)
        except (TypeError, ValueError):
            return None
        return max(0, min(100, number))

    interpretive = {
        "naturalness": score(raw.get("naturalness")),
        "pragmatism": score(raw.get("pragmatism")),
        "room_for_interpretation": score(raw.get("room_for_interpretation")),
        "requires_graph_context_knowledge": bool(raw.get("requires_graph_context_knowledge")),
    }
    if (
        interpretive["naturalness"] is None
        and interpretive["pragmatism"] is None
        and interpretive["room_for_interpretation"] is None
        and not interpretive["requires_graph_context_knowledge"]
    ):
        return {}
    return interpretive


def run_metadata(record: Dict[str, Any]) -> Dict[str, Any]:
    output_meta = record.get("output_meta", {})
    if not isinstance(output_meta, dict):
        output_meta = {}
    request_config = output_meta.get("request_config")
    response_metadata = output_meta.get("response_metadata")
    meta = {
        "run_id": record.get("run_id"),
        "generation_run_id": record.get("generation_run_id") or record.get("run_id"),
        "run_manifest": record.get("run_manifest"),
        "run_label": record.get("run_label"),
        "source_file": record.get("source_file"),
        "model": output_meta.get("model"),
        "run_signature": output_meta.get("run_signature"),
    }
    if output_meta.get("requested_model") is not None:
        meta["requested_model"] = output_meta.get("requested_model")
    if output_meta.get("response_model") is not None:
        meta["response_model"] = output_meta.get("response_model")
    if output_meta.get("generation_parameters") is not None:
        meta["generation_parameters"] = output_meta.get("generation_parameters")
    if isinstance(request_config, dict):
        meta["request_config"] = request_config
    if isinstance(response_metadata, dict):
        meta["response_metadata"] = response_metadata
    return meta


def review_provenance(
    *,
    review: Dict[str, Any],
    review_id: str,
    review_path: Path | str | None,
    dataset_id: str,
    record: Dict[str, Any] | None,
) -> Dict[str, Any]:
    run = run_metadata(record or {})
    return {
        "review_id": review_id,
        "dataset_id": dataset_id,
        "generation_run_id": review.get("generation_run_id") or run.get("generation_run_id"),
        "model": run.get("model"),
    }


def make_rephrasing_entry(
    *,
    text: str,
    source_type: str,
    review: Dict[str, Any],
    review_id: str,
    review_path: Path | str | None,
    dataset_id: str,
    record: Dict[str, Any] | None,
) -> Dict[str, Any] | None:
    clean_text = " ".join(str(text or "").split())
    if not clean_text:
        return None
    return {
        "text": clean_text,
        "normalized_text": normalize_rephrasing_text(clean_text),
        "source_type": source_type,
        **review_provenance(
            review=review,
            review_id=review_id,
            review_path=review_path,
            dataset_id=dataset_id,
            record=record,
        ),
    }


def add_formulation(record: Dict[str, Any], field: str, entry: Dict[str, Any] | None) -> None:
    if not entry:
        return
    if field == "accepted_alternatives":
        entry["acceptance"] = "human_accepted"
    for item in record.get(field, []):
        if not isinstance(item, dict):
            continue
        if normalize_rephrasing_text(item.get("text")) != entry["normalized_text"]:
            continue
        for key in ("acceptance", "generation_run_id", "model"):
            if item.get(key) in (None, "") and entry.get(key) not in (None, ""):
                item[key] = entry[key]
        return
    record.setdefault(field, []).append(entry)


def update_sidecar_identity(
    sidecar: Dict[str, Any],
    *,
    benchmark_record: Dict[str, Any],
    benchmark_version: str,
    built_at: str,
) -> Dict[str, Any]:
    sidecar.update(
        {
            "benchmark_version": benchmark_version,
            "benchmark_built_at": built_at,
            "benchmark_id": benchmark_record.get("benchmark_id"),
            "kg_id": benchmark_record.get("kg_id"),
            "query_id": benchmark_record.get("query_id"),
            "query_label": benchmark_record.get("query_label"),
            "sparql": benchmark_record.get("sparql"),
            "sparql_version": benchmark_record.get("sparql_version"),
            "sparql_hash": benchmark_record.get("sparql_hash"),
            "canonical_question": benchmark_record.get("gold_question"),
            "canonical_question_source": benchmark_record.get("gold_question_source"),
        }
    )
    provenance = public_sparql_provenance(benchmark_record.get("sparql_provenance"))
    if provenance:
        sidecar["sparql_provenance"] = provenance
    else:
        sidecar.pop("sparql_provenance", None)
    return sidecar


def add_interpretive_annotation(
    annotations_record: Dict[str, Any],
    *,
    review: Dict[str, Any],
    review_id: str,
    review_path: Path | str | None,
    dataset_id: str,
    record: Dict[str, Any] | None,
) -> None:
    interpretive = normalize_interpretive(review)
    if not interpretive:
        return
    annotation = {
        "interpretive": interpretive,
        **review_provenance(
            review=review,
            review_id=review_id,
            review_path=review_path,
            dataset_id=dataset_id,
            record=record,
        ),
    }
    existing = annotations_record.setdefault("interpretive_annotations", [])
    if annotation not in existing:
        existing.append(annotation)


def alternatives_record_has_content(record: Dict[str, Any]) -> bool:
    return bool(record.get("accepted_alternatives") or record.get("literal_formulations"))


def sort_sidecar_records(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return sorted(records, key=lambda rec: (str(rec.get("kg_id") or ""), str(rec.get("query_label") or "")))


def sidecars_from_benchmark_record(
    *,
    benchmark_record: Dict[str, Any],
    review: Dict[str, Any],
    source_record: Dict[str, Any],
    review_id: str,
    review_path: Path | str,
    dataset_id: str,
    benchmark_version: str,
    built_at: str,
) -> Tuple[Dict[str, Any] | None, Dict[str, Any] | None]:
    if benchmark_record.get("benchmark_disposition") != "included":
        return None, None
    alternatives = update_sidecar_identity(
        {},
        benchmark_record=benchmark_record,
        benchmark_version=benchmark_version,
        built_at=built_at,
    )
    linguistic_annotations = update_sidecar_identity(
        {},
        benchmark_record=benchmark_record,
        benchmark_version=benchmark_version,
        built_at=built_at,
    )
    add_interpretive_annotation(
        linguistic_annotations,
        review=review,
        review_id=review_id,
        review_path=review_path,
        dataset_id=dataset_id,
        record=source_record,
    )
    if benchmark_record.get("pipeline_assessment") == "accepted":
        preferred = str(review.get("preferred_question") or "").strip()
        model_question = str(source_record.get("output", {}).get("nl_question") or "").strip()
        if preferred and normalize_rephrasing_text(preferred) != normalize_rephrasing_text(model_question):
            add_formulation(
                alternatives,
                "accepted_alternatives",
                make_rephrasing_entry(
                    text=model_question,
                    source_type="model_output",
                    review=review,
                    review_id=review_id,
                    review_path=review_path,
                    dataset_id=dataset_id,
                    record=source_record,
                ),
            )
    add_formulation(
        alternatives,
        "literal_formulations",
        make_rephrasing_entry(
            text=literal_wording(review),
            source_type="literal_sparql_wording",
            review=review,
            review_id=review_id,
            review_path=review_path,
            dataset_id=dataset_id,
            record=source_record,
        ),
    )
    if not alternatives_record_has_content(alternatives):
        alternatives = None
    if not linguistic_annotations.get("interpretive_annotations"):
        linguistic_annotations = None
    return alternatives, linguistic_annotations


def benchmark_gold_records(
    *,
    included: List[Dict[str, Any]],
    benchmark_version: str,
    built_at: str,
) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    for rec in included:
        gold_question = str(rec.get("gold_question") or "").strip()
        if not gold_question:
            raise ValueError(f"Included record has no canonical question: {rec.get('benchmark_id')}")
        source = rec.get("source") if isinstance(rec.get("source"), dict) else {}
        provenance = {
            "question_source": rec.get("gold_question_source"),
            "source_type": source.get("source_type") or "human_review",
        }
        review = rec.get("review") if isinstance(rec.get("review"), dict) else {}
        public_comment, _ = review_comments(review)
        if public_comment:
            provenance["reviewer_comment"] = public_comment
        for field in ("prompt_source", "challenge", "cq", "dataset", "reported_result_rows"):
            if source.get(field) not in (None, ""):
                provenance[field] = source.get(field)
        benchmark_record = {
            "benchmark_version": benchmark_version,
            "benchmark_built_at": built_at,
            "benchmark_id": rec.get("benchmark_id"),
            "kg_id": rec.get("kg_id"),
            "query_id": rec.get("query_id"),
            "query_label": rec.get("query_label"),
            "sparql": rec.get("sparql"),
            "sparql_version": rec.get("sparql_version"),
            "sparql_hash": rec.get("sparql_hash"),
            "gold_question": gold_question,
            "gold_question_source": rec.get("gold_question_source"),
            "evidence_summary": rec.get("evidence_summary"),
            "provenance": provenance,
        }
        sparql_provenance = public_sparql_provenance(rec.get("sparql_provenance"))
        if sparql_provenance:
            benchmark_record["sparql_provenance"] = sparql_provenance
        records.append(benchmark_record)
    return sorted(records, key=lambda rec: (str(rec.get("kg_id")), str(rec.get("query_label"))))


def main() -> None:
    parser = argparse.ArgumentParser(description="Build a benchmark snapshot from a review bundle and reviewer export.")
    parser.add_argument("--bundle", default="review/review_data.js")
    parser.add_argument("--reviews", required=True)
    parser.add_argument("--outdir", required=True)
    args = parser.parse_args()

    bundle_path = Path(args.bundle)
    review_path = Path(args.reviews)
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    bundle = read_review_bundle(bundle_path)
    review_export = read_json(review_path)
    assert_non_holdout_export(review_export)
    bundle_dataset_id = str(bundle.get("dataset_id") or "")
    review_dataset_id = str(review_export.get("dataset_id") or "")
    bundle_run_ids = [str(run_id) for run_id in bundle.get("run_ids", []) if str(run_id)]
    review_run_id = str(review_export.get("run_id") or "")
    if bundle_dataset_id and review_dataset_id and bundle_dataset_id != review_dataset_id:
        raise ValueError(
            f"Dataset mismatch: bundle has {bundle_dataset_id}, review export has {review_dataset_id}"
        )
    if review_run_id and bundle_run_ids and review_run_id not in bundle_run_ids:
        raise ValueError(f"Run mismatch: review export points to {review_run_id}, bundle has {bundle_run_ids}")

    bundle_records = bundle.get("records")
    if not isinstance(bundle_records, list):
        raise ValueError("Review bundle missing records list")
    review_map = review_export.get("reviews")
    if not isinstance(review_map, dict):
        raise ValueError("Review export missing reviews object")
    assert_no_private_reviews(review_map)

    included: List[Dict[str, Any]] = []
    dismissed: List[Dict[str, Any]] = []
    alternatives_records: List[Dict[str, Any]] = []
    linguistic_annotation_records: List[Dict[str, Any]] = []
    assessment_counts: Counter[str] = Counter()
    disposition_counts: Counter[str] = Counter()

    for record in bundle_records:
        if not isinstance(record, dict):
            continue
        review_id = str(record.get("review_id") or "")
        review = review_map.get(review_id)
        if not isinstance(review, dict):
            continue
        disposition = benchmark_disposition(review)
        assessment = pipeline_assessment(review)
        if not disposition:
            continue
        disposition_counts[disposition] += 1
        if assessment:
            assessment_counts[assessment] += 1
        split = "public"

        evidence = record.get("input", {}).get("evidence", [])
        if not isinstance(evidence, list):
            evidence = []
        evidence_types = source_evidence_types(evidence)
        preferred = str(review.get("preferred_question") or "").strip()
        model_question = str(record.get("output", {}).get("nl_question") or "").strip()
        gold_question = preferred or model_question
        gold_source = "reviewer_rewrite" if preferred else "approved_model_output"

        base = {
            "benchmark_id": review_id,
            "kg_id": record.get("kg_id"),
            "query_id": record.get("query_id"),
            "query_label": record.get("query_label"),
            "sparql": record.get("input", {}).get("sparql_clean"),
            "sparql_version": record.get("input", {}).get("sparql_version"),
            "sparql_hash": record.get("input", {}).get("sparql_hash"),
            "sparql_provenance": record.get("input", {}).get("sparql_provenance"),
            "gold_question": gold_question,
            "gold_question_source": gold_source,
            "benchmark_disposition": disposition,
            "pipeline_assessment": assessment or None,
            "split": split,
            "review": {
                "review_id": review_id,
                "review_export": str(review_path),
                "dataset_id": review_dataset_id or bundle_dataset_id,
                "run_id": review_run_id or record.get("run_id"),
                "generation_run_id": review_run_id or record.get("generation_run_id") or record.get("run_id"),
                "literal_wording": literal_wording(review),
                "public_comment": review_comments(review)[0],
                "internal_comment": review_comments(review)[1],
                "updated_at": review.get("updated_at"),
            },
            "run": {
                **run_metadata(record),
            },
            "model_output": {
                "nl_question": model_question,
                "origin_mode": record.get("output", {}).get("nl_question_origin", {}).get("mode"),
                "confidence": record.get("output", {}).get("confidence"),
                "confidence_rationale": record.get("output", {}).get("confidence_rationale"),
                "needs_review": record.get("output", {}).get("needs_review"),
                "retained_evidence_phrases": record.get("output", {}).get("ranked_evidence_phrases", []),
            },
            "evidence_summary": {
                "evidence_count": len(evidence),
                "evidence_types": evidence_types,
                "has_source_evidence": bool(evidence_types),
                "has_query_specific_evidence": has_query_specific_evidence(evidence_types),
            },
        }

        if disposition == "excluded":
            dismissed.append(base)
        else:
            if not gold_question:
                raise ValueError(f"Included record has no canonical question: {review_id}")
            included.append(base)

            alternatives, linguistic_annotations = sidecars_from_benchmark_record(
                benchmark_record=base,
                review=review,
                source_record=record,
                review_id=review_id,
                review_path=review_path,
                dataset_id=review_dataset_id or bundle_dataset_id,
                benchmark_version=outdir.name,
                built_at="",
            )
            if alternatives:
                alternatives_records.append(alternatives)
            if linguistic_annotations:
                linguistic_annotation_records.append(linguistic_annotations)

    included.sort(key=lambda rec: (str(rec.get("kg_id")), str(rec.get("query_label"))))
    dismissed.sort(key=lambda rec: (str(rec.get("kg_id")), str(rec.get("query_label"))))

    built_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    benchmark_version = outdir.name
    dataset_id = review_dataset_id or bundle_dataset_id
    for sidecar in alternatives_records + linguistic_annotation_records:
        sidecar["benchmark_version"] = benchmark_version
        sidecar["benchmark_built_at"] = built_at
    alternatives_records = sort_sidecar_records(alternatives_records)
    linguistic_annotation_records = sort_sidecar_records(linguistic_annotation_records)
    benchmark_records = benchmark_gold_records(
        included=included,
        benchmark_version=benchmark_version,
        built_at=built_at,
    )

    manifest = {
        "benchmark_version": benchmark_version,
        "built_at": built_at,
        "source_bundle": str(bundle_path),
        "source_review_export": str(review_path),
        "dataset_id": dataset_id,
        "run_id": review_run_id or (bundle_run_ids[0] if len(bundle_run_ids) == 1 else None),
        "sparql_version_policy": "latest_retained",
        "execution_snapshot": neutral_execution_snapshot(included, captured_through=built_at),
        "counts": {
            "benchmark": len(benchmark_records),
            "included": len(included),
            "dismissed": len(dismissed),
            "alternatives": len(alternatives_records),
            "linguistic_annotations": len(linguistic_annotation_records),
            "reviewed_total": sum(disposition_counts.values()),
            "pipeline_assessment_counts": dict(assessment_counts),
            "benchmark_disposition_counts": dict(disposition_counts),
        },
        "files": {
            "benchmark": "benchmark.jsonl",
            "alternatives": ALTERNATIVES_FILE,
        },
        "working_files": {
            "included": INCLUDED_FILE,
            "dismissed": "dismissed.jsonl",
            "linguistic_annotations_internal": LINGUISTIC_ANNOTATIONS_FILE,
        },
        "gold_question_policy": {
            "benchmark_contains_all_human_confirmed_included_pairs": True,
            "preferred_question_used_when_present": True,
            "approved_model_output_used_otherwise": True,
        },
        "holdout_policy": {
            "private_annotations_outside_repository": True,
            "mixed_private_exports_rejected": True,
        },
        "release_boundary": {
            "public_release_files": list(PUBLIC_RELEASE_FILES),
            "internal_only_files": [INCLUDED_FILE, LINGUISTIC_ANNOTATIONS_FILE, "dismissed.jsonl"],
        },
    }

    write_json(outdir / "manifest.json", manifest)
    write_jsonl(outdir / "benchmark.jsonl", benchmark_records)
    write_jsonl(outdir / INCLUDED_FILE, included)
    write_jsonl(outdir / "dismissed.jsonl", dismissed)
    write_jsonl(outdir / ALTERNATIVES_FILE, alternatives_records)
    write_jsonl(outdir / LINGUISTIC_ANNOTATIONS_FILE, linguistic_annotation_records)

    print(f"Wrote manifest to {outdir / 'manifest.json'}")
    print(f"Wrote {len(benchmark_records)} benchmark records to {outdir / 'benchmark.jsonl'}")
    print(f"Wrote {len(included)} included records to {outdir / INCLUDED_FILE}")
    print(f"Wrote {len(dismissed)} dismissed records to {outdir / 'dismissed.jsonl'}")
    print(f"Wrote {len(alternatives_records)} alternative-formulation records to {outdir / ALTERNATIVES_FILE}")
    print(f"Wrote {len(linguistic_annotation_records)} internal linguistic-annotation records to {outdir / LINGUISTIC_ANNOTATIONS_FILE}")


if __name__ == "__main__":
    main()
