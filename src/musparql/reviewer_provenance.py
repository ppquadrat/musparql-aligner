"""Validation helpers for confidential reviewer data and public-safe IDs."""
from __future__ import annotations

from datetime import datetime
import re
from typing import Any, Mapping, Sequence


REVIEWER_ID_RE = re.compile(r"^reviewer-[0-9]{4,}$")
RFC3339_DATETIME_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$"
)
EXPERIENCE_LEVELS = frozenset({"none", "occasional", "regular", "expert"})
LANGUAGE_LEVELS = frozenset({"basic", "advanced", "fluent", "native"})
KG_FAMILIARITY_LEVELS = frozenset({"none", "inspected", "queried", "regular_user", "creator"})
PUBLIC_REVIEWER_ID_FIELDS = frozenset({"reviewer_id", "authored_by_reviewer_id"})
PUBLIC_REVIEWER_ID_LIST_FIELDS = frozenset({"approval_reviewer_ids"})
REVIEWER_PROFILE_FIELDS = frozenset({
    "id", "name", "affiliation", "email", "domain_expertise",
    "kg_ontology_experience", "sparql_experience", "nlp_llm_experience",
    "language_expertise", "privacy_notice_version",
    "privacy_notice_acknowledged_at",
})
KG_FAMILIARITY_FIELDS = frozenset({"reviewer_id", "kg_id", "familiarity"})
LANGUAGE_TAG_RE = re.compile(r"^[a-z]{2,3}(?:-[A-Z]{2})?$")
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+$")
REVIEW_EVENT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]*::(reviewer-[0-9]{4,})$")
FORMULATION_RE = re.compile(
    r"^(.+::reviewer-[0-9]{4,})::formulation::(candidate|preferred|literal|sparql)$"
)


def validate_reviewer_id(value: Any) -> str:
    reviewer_id = str(value or "")
    if not REVIEWER_ID_RE.fullmatch(reviewer_id):
        raise ValueError("reviewer_id must use the pseudonymous form reviewer-NNNN")
    return reviewer_id


def _iso_datetime(value: Any, field: str) -> str:
    text = str(value or "")
    if not text:
        raise ValueError(f"{field} is required")
    if not RFC3339_DATETIME_RE.fullmatch(text):
        raise ValueError(f"{field} must be an RFC 3339 date-time with a timezone")
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{field} must be an RFC 3339 date-time with a timezone") from exc
    if parsed.utcoffset() is None:
        raise ValueError(f"{field} must be an RFC 3339 date-time with a timezone")
    return text


def validate_reviewer(record: Mapping[str, Any]) -> None:
    unknown = set(record) - REVIEWER_PROFILE_FIELDS
    if unknown:
        raise ValueError(f"Reviewer has unsupported fields: {sorted(unknown)}")
    validate_reviewer_id(record.get("id"))
    for field in ("name", "privacy_notice_version"):
        if not isinstance(record.get(field), str) or not str(record[field]).strip():
            raise ValueError(f"Reviewer requires {field}")
    for field in ("affiliation", "email"):
        if not isinstance(record.get(field), str):
            raise ValueError(f"Reviewer requires string field {field}")
    if not EMAIL_RE.fullmatch(str(record["email"])):
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
    if any(not isinstance(tag, str) or not LANGUAGE_TAG_RE.fullmatch(tag) for tag in languages):
        raise ValueError("Reviewer language_expertise keys must be language tags")
    if any(level not in LANGUAGE_LEVELS for level in languages.values()):
        raise ValueError("Unsupported language expertise level")
    _iso_datetime(record.get("privacy_notice_acknowledged_at"), "privacy_notice_acknowledged_at")


def validate_kg_familiarities(
    records: Sequence[Mapping[str, Any]], *, reviewer_ids: set[str] | None = None
) -> None:
    seen: set[tuple[str, str]] = set()
    for record in records:
        unknown = set(record) - KG_FAMILIARITY_FIELDS
        if unknown:
            raise ValueError(f"Reviewer KG familiarity has unsupported fields: {sorted(unknown)}")
        reviewer_id = validate_reviewer_id(record.get("reviewer_id"))
        kg_id = record.get("kg_id")
        if not isinstance(kg_id, str) or not kg_id:
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
    reviewer_id = validate_reviewer_id(review.get("reviewer_id"))
    review_id = str(review["review_id"])
    if not review_id.endswith(f"::{reviewer_id}"):
        raise ValueError("Review review_id must end with its reviewer_id")
    _iso_datetime(review.get("reviewed_at"), "reviewed_at")
    for field in ("prior_review_ids", "authored_formulation_ids", "approved_formulation_ids"):
        values = review.get(field)
        if not isinstance(values, list) or not all(isinstance(item, str) and item for item in values):
            raise ValueError(f"Review requires {field} as a list of IDs")
        if len(values) != len(set(values)):
            raise ValueError(f"Review {field} must not contain duplicates")
    formulation_prefix = f"{review_id}::formulation::"
    if any(not value.startswith(formulation_prefix) for value in review["authored_formulation_ids"]):
        raise ValueError("Authored formulation IDs must be rooted at the current review_id")


def formulation_id(review_id: str, role: str) -> str:
    if not review_id or role not in {"candidate", "preferred", "literal"}:
        raise ValueError("Formulation IDs require a review ID and supported role")
    return f"{review_id}::formulation::{role}"


def validate_public_reviewer_ids(value: Any, location: str = "root") -> None:
    """Reject non-pseudonymous reviewer identities anywhere in a public artifact."""
    if isinstance(value, Mapping):
        reviewer_id = value.get("reviewer_id")
        approval_review_id = value.get("approval_review_id")
        formulation_id_value = value.get("formulation_id")
        authored_by = value.get("authored_by_reviewer_id")
        if approval_review_id is not None:
            event_reviewer = _review_event_reviewer(approval_review_id, f"{location}.approval_review_id")
            if reviewer_id is None or event_reviewer != reviewer_id:
                raise ValueError(f"Public approval review/reviewer mismatch at {location}")
        if formulation_id_value is not None:
            formulation_event, _ = _formulation_parts(formulation_id_value, f"{location}.formulation_id")
            if approval_review_id is not None and formulation_event != approval_review_id:
                raise ValueError(f"Public formulation/approval review mismatch at {location}")
            if authored_by is not None and _review_event_reviewer(formulation_event, location) != authored_by:
                raise ValueError(f"Public formulation/authorship mismatch at {location}")
        approval_review_ids = value.get("approval_review_ids")
        approval_reviewer_ids = value.get("approval_reviewer_ids")
        if approval_review_ids is not None or approval_reviewer_ids is not None:
            if not isinstance(approval_review_ids, list) or not isinstance(approval_reviewer_ids, list):
                raise ValueError(f"Public approval links must be paired lists at {location}")
            if len(approval_review_ids) != len(set(approval_review_ids)):
                raise ValueError(f"Public approval review IDs must be unique at {location}")
            if len(approval_reviewer_ids) != len(set(approval_reviewer_ids)):
                raise ValueError(f"Public approval reviewer IDs must be unique at {location}")
            event_reviewers = {
                _review_event_reviewer(
                    event_id, f"{location}.approval_review_ids[{index}]"
                )
                for index, event_id in enumerate(approval_review_ids)
            }
            if event_reviewers != set(approval_reviewer_ids):
                raise ValueError(f"Public approval review/reviewer mismatch at {location}")
        for key, item in value.items():
            child = f"{location}.{key}"
            if key in PUBLIC_REVIEWER_ID_FIELDS and item is not None:
                try:
                    validate_reviewer_id(item)
                except ValueError as exc:
                    raise ValueError(f"Invalid public reviewer ID at {child}") from exc
            if key in PUBLIC_REVIEWER_ID_LIST_FIELDS:
                if not isinstance(item, list):
                    raise ValueError(f"Public reviewer IDs must be a list at {child}")
                for index, reviewer_id in enumerate(item):
                    try:
                        validate_reviewer_id(reviewer_id)
                    except ValueError as exc:
                        raise ValueError(
                            f"Invalid public reviewer ID at {child}[{index}]"
                        ) from exc
            validate_public_reviewer_ids(item, child)
    elif isinstance(value, list):
        for index, item in enumerate(value):
            validate_public_reviewer_ids(item, f"{location}[{index}]")


def _review_event_reviewer(value: Any, location: str) -> str:
    match = REVIEW_EVENT_RE.fullmatch(str(value or ""))
    if not match:
        raise ValueError(f"Invalid public review event ID at {location}")
    return validate_reviewer_id(match.group(1))


def _formulation_parts(value: Any, location: str) -> tuple[str, str]:
    match = FORMULATION_RE.fullmatch(str(value or ""))
    if not match:
        raise ValueError(f"Invalid public formulation ID at {location}")
    _review_event_reviewer(match.group(1), location)
    return match.group(1), match.group(2)
