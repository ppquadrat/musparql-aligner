"""Validation helpers for confidential reviewer data and public-safe IDs."""
from __future__ import annotations

from datetime import datetime
import re
from typing import Any, Mapping, Sequence


REVIEWER_ID_RE = re.compile(r"^reviewer-[0-9]{4,}$")
EXPERIENCE_LEVELS = frozenset({"none", "occasional", "regular", "expert"})
LANGUAGE_LEVELS = frozenset({"basic", "advanced", "fluent", "native"})
KG_FAMILIARITY_LEVELS = frozenset({"none", "inspected", "queried", "regular_user", "creator"})


def validate_reviewer_id(value: Any) -> str:
    reviewer_id = str(value or "")
    if not REVIEWER_ID_RE.fullmatch(reviewer_id):
        raise ValueError("reviewer_id must use the pseudonymous form reviewer-NNNN")
    return reviewer_id


def _iso_datetime(value: Any, field: str) -> str:
    text = str(value or "")
    if not text:
        raise ValueError(f"{field} is required")
    try:
        datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{field} must be an ISO date-time") from exc
    return text


def validate_reviewer(record: Mapping[str, Any]) -> None:
    validate_reviewer_id(record.get("id"))
    for field in ("name", "privacy_notice_version"):
        if not isinstance(record.get(field), str) or not str(record[field]).strip():
            raise ValueError(f"Reviewer requires {field}")
    for field in ("affiliation", "email"):
        if not isinstance(record.get(field), str):
            raise ValueError(f"Reviewer requires string field {field}")
    if "@" not in str(record["email"]):
        raise ValueError("Reviewer email is invalid")
    for field in (
        "domain_expertise", "kg_ontology_experience", "sparql_experience",
        "nlp_llm_experience",
    ):
        if record.get(field) not in EXPERIENCE_LEVELS:
            raise ValueError(f"Unsupported {field}")
    languages = record.get("language_expertise")
    if not isinstance(languages, Mapping) or not languages:
        raise ValueError("Reviewer requires language_expertise")
    if any(level not in LANGUAGE_LEVELS for level in languages.values()):
        raise ValueError("Unsupported language expertise level")
    _iso_datetime(record.get("privacy_notice_acknowledged_at"), "privacy_notice_acknowledged_at")


def validate_kg_familiarities(
    records: Sequence[Mapping[str, Any]], *, reviewer_ids: set[str] | None = None
) -> None:
    seen: set[tuple[str, str]] = set()
    for record in records:
        reviewer_id = validate_reviewer_id(record.get("reviewer_id"))
        kg_id = str(record.get("kg_id") or "")
        if not kg_id:
            raise ValueError("Reviewer KG familiarity requires kg_id")
        if record.get("familiarity") not in KG_FAMILIARITY_LEVELS:
            raise ValueError("Unsupported KG familiarity")
        if reviewer_ids is not None and reviewer_id not in reviewer_ids:
            raise ValueError(f"Unknown reviewer_id in KG familiarity: {reviewer_id}")
        key = (reviewer_id, kg_id)
        if key in seen:
            raise ValueError(f"Duplicate reviewer/KG familiarity: {reviewer_id}/{kg_id}")
        seen.add(key)


def validate_review_provenance(review: Mapping[str, Any]) -> None:
    if not isinstance(review.get("review_id"), str) or not str(review["review_id"]):
        raise ValueError("Review requires review_id")
    validate_reviewer_id(review.get("reviewer_id"))
    _iso_datetime(review.get("reviewed_at"), "reviewed_at")
    for field in ("prior_review_ids", "authored_formulation_ids", "approved_formulation_ids"):
        values = review.get(field)
        if not isinstance(values, list) or not all(isinstance(item, str) and item for item in values):
            raise ValueError(f"Review requires {field} as a list of IDs")
        if len(values) != len(set(values)):
            raise ValueError(f"Review {field} must not contain duplicates")


def formulation_id(review_id: str, role: str) -> str:
    if not review_id or role not in {"candidate", "preferred", "literal"}:
        raise ValueError("Formulation IDs require a review ID and supported role")
    return f"{review_id}::formulation::{role}"
