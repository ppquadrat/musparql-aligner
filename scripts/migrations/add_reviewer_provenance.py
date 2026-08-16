#!/usr/bin/env python3
"""Migrate sanitized legacy review exports to pseudonymous reviewer provenance v2."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import tempfile
from typing import Any, Dict

from musparql.reviewer_provenance import formulation_id, validate_reviewer_id, validate_review_provenance


def _event_id(value: str, reviewer_id: str) -> str:
    return value if re.search(r"::reviewer-[0-9]{4,}$", value) else f"{value}::{reviewer_id}"


def _event_formulation_id(value: str, reviewer_id: str) -> str:
    marker = "::formulation::"
    if marker not in value or re.search(r"::reviewer-[0-9]{4,}::formulation::", value):
        return value
    base, role = value.rsplit(marker, 1)
    return f"{_event_id(base, reviewer_id)}{marker}{role}"


def _links(review_id: str, review: Dict[str, Any]) -> tuple[list[str], list[str]]:
    authored: list[str] = []
    approved: list[str] = []
    if str(review.get("preferred_question") or "").strip():
        authored.append(formulation_id(review_id, "preferred"))
    if str(review.get("literal_wording") or "").strip():
        authored.append(formulation_id(review_id, "literal"))
    disposition = str(review.get("benchmark_disposition") or "")
    assessment = str(review.get("pipeline_assessment") or "")
    if disposition in {"included", "withheld"} or assessment:
        approved.append(formulation_id(review_id, "preferred" if authored and authored[0].endswith("preferred") else "candidate"))
    return authored, approved


def migrate_review(review_id: str, review: Dict[str, Any], reviewer_id: str, fallback_time: str) -> Dict[str, Any]:
    result = dict(review)
    event_review_id = _event_id(str(result.get("review_id") or review_id), reviewer_id)
    result["review_id"] = event_review_id
    result["reviewer_id"] = reviewer_id
    result["reviewed_at"] = str(result.get("reviewed_at") or result.pop("updated_at", "") or fallback_time)
    result.pop("updated_at", None)
    result["prior_review_ids"] = [_event_id(str(value), reviewer_id) for value in (result.get("prior_review_ids") or [])]
    copied = str(result.get("copied_from_review_id") or "")
    if copied:
        copied = _event_id(copied, reviewer_id)
        result["copied_from_review_id"] = copied
    if copied and copied not in result["prior_review_ids"]:
        result["prior_review_ids"].append(copied)
    authored, approved = _links(event_review_id, result)
    result["authored_formulation_ids"] = [
        _event_formulation_id(str(value), reviewer_id)
        for value in (result.get("authored_formulation_ids") or authored)
    ]
    result["approved_formulation_ids"] = [
        _event_formulation_id(str(value), reviewer_id)
        for value in (result.get("approved_formulation_ids") or approved)
    ]
    validate_review_provenance(result)
    return result


def migrate_payload(payload: Dict[str, Any], reviewer_id: str) -> Dict[str, Any]:
    reviewer_id = validate_reviewer_id(reviewer_id)
    source_schema = str(payload.get("schema") or "")
    if source_schema in {
        "musparql.review-export.v2",
        "musparql.sparql-correction-review-export.v2",
    } and payload.get("reviewer_id") != reviewer_id:
        raise ValueError("Existing v2 export reviewer_id does not match --reviewer-id")
    if payload.get("schema") in {
        "musparql.sparql-correction-review-export.v1",
        "musparql.sparql-correction-review-export.v2",
    }:
        reviews = payload.get("reviews")
        if not isinstance(reviews, list) or not all(isinstance(review, dict) for review in reviews):
            raise ValueError("Correction review export requires a reviews array")
        result = dict(payload)
        result["schema"] = "musparql.sparql-correction-review-export.v2"
        result["reviewer_id"] = reviewer_id
        migrated_reviews = []
        for review in reviews:
            if source_schema.endswith(".v2") and review.get("reviewer_id") != reviewer_id:
                raise ValueError("Existing v2 review reviewer_id does not match --reviewer-id")
            migrated = dict(review)
            review_key = str(migrated.get("candidate_id") or "")
            if not review_key:
                raise ValueError("Correction review requires candidate_id")
            event_review_id = _event_id(str(migrated.get("review_id") or review_key), reviewer_id)
            migrated["review_id"] = event_review_id
            formulation = f"{event_review_id}::formulation::sparql"
            migrated["reviewer_id"] = reviewer_id
            migrated["prior_review_ids"] = [_event_id(str(value), reviewer_id) for value in (migrated.get("prior_review_ids") or [])]
            migrated["authored_formulation_ids"] = [_event_formulation_id(str(value), reviewer_id) for value in (
                migrated.get("authored_formulation_ids")
                or ([formulation] if migrated.get("decision") == "approve_edit" and migrated.get("proposal_origin") == "human" else [])
            )]
            migrated["approved_formulation_ids"] = [_event_formulation_id(str(value), reviewer_id) for value in (
                migrated.get("approved_formulation_ids")
                or ([formulation] if migrated.get("decision") == "approve_edit" else [])
            )]
            validate_review_provenance(migrated)
            migrated_reviews.append(migrated)
        result["reviews"] = migrated_reviews
        return result
    if payload.get("kind") != "non_holdout_review_export":
        raise ValueError("Only sanitized non-holdout or SPARQL-correction review exports may be migrated")
    reviews = payload.get("reviews")
    if not isinstance(reviews, dict):
        raise ValueError("Review export requires a reviews object")
    if source_schema.endswith(".v2") and any(
        isinstance(review, dict) and review.get("reviewer_id") != reviewer_id
        for review in reviews.values()
    ):
        raise ValueError("Existing v2 review reviewer_id does not match --reviewer-id")
    fallback_time = str(payload.get("exported_at") or "")
    result = dict(payload)
    result["schema"] = "musparql.review-export.v2"
    result["reviewer_id"] = reviewer_id
    result["reviews"] = {
        str(review_id): migrate_review(str(review_id), review, reviewer_id, fallback_time)
        for review_id, review in reviews.items()
        if isinstance(review, dict)
    }
    if len(result["reviews"]) != len(reviews):
        raise ValueError("Every review must be an object")
    return result


def migrate_query_catalog(records: list[Dict[str, Any]], reviewer_id: str) -> list[Dict[str, Any]]:
    reviewer_id = validate_reviewer_id(reviewer_id)
    migrated_records: list[Dict[str, Any]] = []
    for record in records:
        row = dict(record)
        history = row.get("sparql_correction_history")
        migrated_history: list[Dict[str, Any]] = []
        prior_ids: list[str] = []
        if isinstance(history, list):
            for item in history:
                if not isinstance(item, dict):
                    migrated_history.append(item)
                    continue
                migrated = dict(item)
                review_id = str(migrated.get("candidate_id") or "")
                if not review_id or not migrated.get("reviewed_at"):
                    migrated_history.append(migrated)
                    continue
                event_review_id = _event_id(str(migrated.get("review_id") or review_id), reviewer_id)
                migrated["review_id"] = event_review_id
                formulation = f"{event_review_id}::formulation::sparql"
                migrated["reviewer_id"] = reviewer_id
                migrated["prior_review_ids"] = [_event_id(str(value), reviewer_id) for value in (migrated.get("prior_review_ids") or prior_ids)]
                migrated["authored_formulation_ids"] = [_event_formulation_id(str(value), reviewer_id) for value in (
                    migrated.get("authored_formulation_ids")
                    or ([formulation] if migrated.get("decision") == "approve_edit" and migrated.get("proposal_origin") == "human" else [])
                )]
                migrated["approved_formulation_ids"] = [_event_formulation_id(str(value), reviewer_id) for value in (
                    migrated.get("approved_formulation_ids")
                    or ([formulation] if migrated.get("decision") == "approve_edit" else [])
                )]
                validate_review_provenance(migrated)
                migrated_history.append(migrated)
                prior_ids.append(event_review_id)
            row["sparql_correction_history"] = migrated_history

        edits = row.get("sparql_edits")
        if isinstance(edits, list):
            next_edits = []
            history_by_candidate = {
                str(item.get("candidate_id")): item
                for item in migrated_history if isinstance(item, dict) and item.get("candidate_id")
            }
            for edit in edits:
                if not isinstance(edit, dict) or not isinstance(edit.get("provenance"), dict):
                    next_edits.append(edit)
                    continue
                next_edit = dict(edit)
                provenance = dict(edit["provenance"])
                source = history_by_candidate.get(str(provenance.get("candidate_id") or ""), {})
                for field in (
                    "reviewer_id", "prior_review_ids", "authored_formulation_ids",
                    "approved_formulation_ids",
                ):
                    if field in source:
                        provenance[field] = source[field]
                next_edit["provenance"] = provenance
                next_edits.append(next_edit)
            row["sparql_edits"] = next_edits
        migrated_records.append(row)
    return migrated_records


def write_atomic(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(payload, stream, ensure_ascii=False, indent=2)
            stream.write("\n")
        os.replace(temporary, path)
    except BaseException:
        Path(temporary).unlink(missing_ok=True)
        raise


def write_jsonl_atomic(path: Path, records: list[Dict[str, Any]]) -> None:
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            for record in records:
                stream.write(json.dumps(record, ensure_ascii=False) + "\n")
        os.replace(temporary, path)
    except BaseException:
        Path(temporary).unlink(missing_ok=True)
        raise


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("paths", nargs="*", help="Explicit sanitized export paths under var/review/exports.")
    parser.add_argument("--reviewer-id", required=True)
    parser.add_argument("--kg-queries", default="", help="Optional explicit local query catalogue to backfill.")
    parser.add_argument("--write", action="store_true", help="Replace each file atomically; otherwise validate only.")
    args = parser.parse_args()
    if not args.paths and not args.kg_queries:
        parser.error("provide at least one sanitized export path or --kg-queries")
    root = Path.cwd().resolve()
    allowed = (root / "var" / "review" / "exports").resolve()
    for raw_path in args.paths:
        path = Path(raw_path).resolve()
        if not path.is_relative_to(allowed):
            raise ValueError(f"Migration input must be under {allowed}: {path}")
        payload = json.loads(path.read_text(encoding="utf-8"))
        migrated = migrate_payload(payload, args.reviewer_id)
        if args.write:
            write_atomic(path, migrated)
        print(f"{'Migrated' if args.write else 'Validated'} {path}")
    if args.kg_queries:
        query_path = Path(args.kg_queries).resolve()
        expected = (root / "var" / "queries" / "kg_queries.jsonl").resolve()
        if query_path != expected:
            raise ValueError(f"Query-catalogue migration is restricted to {expected}")
        records = [json.loads(line) for line in query_path.read_text(encoding="utf-8").splitlines() if line.strip()]
        migrated_records = migrate_query_catalog(records, args.reviewer_id)
        if args.write:
            write_jsonl_atomic(query_path, migrated_records)
        print(f"{'Migrated' if args.write else 'Validated'} {query_path}")


if __name__ == "__main__":
    main()
