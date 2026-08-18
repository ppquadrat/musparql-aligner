"""Validation helpers for confidential reviewer data and public-safe IDs."""
from __future__ import annotations

from datetime import datetime
import re
from typing import Any, Mapping, Sequence

from .source_catalog import validate_kg_seed_snapshots


REVIEWER_ID_RE = re.compile(r"^reviewer-[0-9]{4,}$")
RFC3339_DATETIME_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$"
)
EXPERIENCE_LEVELS = frozenset({"none", "occasional", "regular", "expert"})
LANGUAGE_LEVELS = frozenset({"basic", "advanced", "fluent", "native"})
KG_FAMILIARITY_LEVELS = frozenset({"none", "inspected", "queried", "regular_user", "creator"})
SUBJECT_EXPERTISE_LEVELS = frozenset({"none", "basic", "working", "advanced", "expert"})
RESOURCE_FAMILIARITY_LEVELS = frozenset(
    {"none", "inspected", "worked", "regular_user", "creator"}
)
PUBLIC_REVIEWER_ID_FIELDS = frozenset({"reviewer_id", "authored_by_reviewer_id"})
PUBLIC_REVIEWER_ID_LIST_FIELDS = frozenset({"approval_reviewer_ids"})
REVIEWER_PROFILE_FIELDS = frozenset({
    "id", "name", "affiliation", "email", "domain_expertise",
    "kg_ontology_experience", "sparql_experience", "nlp_llm_experience",
    "language_expertise", "privacy_notice_version",
    "privacy_notice_acknowledged_at",
})
KG_FAMILIARITY_FIELDS = frozenset({"reviewer_id", "kg_id", "familiarity"})
REVIEWER_PROFILE_V2_FIELDS = frozenset({
    "schema", "id", "name", "affiliation", "email", "domain_expertise",
    "kg_ontology_experience", "sparql_experience", "nlp_llm_experience",
    "language_expertise", "privacy_notice_version", "privacy_notice_acknowledged_at",
})
DOMAIN_EXPERTISE_FIELDS = frozenset({
    "domain_id", "assertion_id", "entered_label", "normalized_label", "vocabulary_name",
    "vocabulary_concept_uri", "vocabulary_version", "expertise_level",
    "first_asserted_at", "updated_at",
})
REVIEWER_DOMAIN_EXPERTISE_ASSERTION_FIELDS = frozenset({
    "schema", "id", "reviewer_id", "domain_id", "entered_label", "normalized_label",
    "vocabulary_name", "vocabulary_concept_uri", "vocabulary_version",
    "expertise_level", "asserted_at", "supersedes_id",
})
KG_DOMAIN_ASSESSMENT_FIELDS = frozenset({
    "schema", "id", "reviewer_id", "kg_id", "review_domain_id", "review_domain_label",
    "subject_expertise_level", "assessed_at", "context", "assignment_id", "seed_version",
    "previous_assessment_id",
})
RESOURCE_FAMILIARITY_ASSESSMENT_FIELDS = frozenset({
    "schema", "id", "reviewer_id", "kg_id", "familiarity_scope_id",
    "familiarity_scope_label", "familiarity_level", "assessed_at", "context",
    "assignment_id", "seed_version", "previous_assessment_id",
})
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


def _parsed_datetime(value: Any, field: str) -> datetime:
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
    return parsed


def _iso_datetime(value: Any, field: str) -> str:
    text = str(value or "")
    _parsed_datetime(value, field)
    return text


def validate_reviewer(record: Mapping[str, Any]) -> None:
    """Validate the legacy confidential registry contract pending Phase 2 migration."""
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


def _required_text(record: Mapping[str, Any], field: str, subject: str) -> str:
    value = record.get(field)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{subject} requires {field}")
    return value


def validate_reviewer_profile_v2(record: Mapping[str, Any]) -> None:
    missing = REVIEWER_PROFILE_V2_FIELDS - set(record)
    if missing:
        raise ValueError(f"Reviewer profile is missing fields: {sorted(missing)}")
    unknown = set(record) - REVIEWER_PROFILE_V2_FIELDS
    if unknown:
        raise ValueError(f"Reviewer profile has unsupported fields: {sorted(unknown)}")
    if record.get("schema") != "musparql.reviewer-profile.v2":
        raise ValueError("Reviewer profile requires schema musparql.reviewer-profile.v2")
    validate_reviewer_id(record.get("id"))
    for field in ("name", "privacy_notice_version"):
        _required_text(record, field, "Reviewer profile")
    for field in ("affiliation", "email"):
        if not isinstance(record.get(field), str):
            raise ValueError(f"Reviewer profile requires string field {field}")
    if not EMAIL_RE.fullmatch(str(record["email"])):
        raise ValueError("Reviewer profile email is invalid")
    for field in ("kg_ontology_experience", "sparql_experience", "nlp_llm_experience"):
        if record.get(field) not in EXPERIENCE_LEVELS:
            raise ValueError(f"Unsupported {field}")
    languages = record.get("language_expertise")
    if not isinstance(languages, Mapping) or not languages:
        raise ValueError("Reviewer profile requires language_expertise")
    if any(not isinstance(tag, str) or not LANGUAGE_TAG_RE.fullmatch(tag) for tag in languages):
        raise ValueError("Reviewer profile language_expertise keys must be language tags")
    if any(level not in LANGUAGE_LEVELS for level in languages.values()):
        raise ValueError("Unsupported language expertise level")
    domains = record.get("domain_expertise")
    if not isinstance(domains, list) or not domains:
        raise ValueError("Reviewer profile requires domain_expertise projections")
    seen_domains: set[str] = set()
    seen_domain_ids: set[str] = set()
    seen_assertion_ids: set[str] = set()
    for index, domain in enumerate(domains):
        if not isinstance(domain, Mapping):
            raise ValueError(f"domain_expertise[{index}] must be an object")
        missing_domain = DOMAIN_EXPERTISE_FIELDS - set(domain)
        if missing_domain:
            raise ValueError(f"Domain expertise is missing fields: {sorted(missing_domain)}")
        unknown_domain = set(domain) - DOMAIN_EXPERTISE_FIELDS
        if unknown_domain:
            raise ValueError(f"Domain expertise has unsupported fields: {sorted(unknown_domain)}")
        domain_id = _required_text(domain, "domain_id", "Domain expertise")
        assertion_id = _required_text(domain, "assertion_id", "Domain expertise")
        if domain_id in seen_domain_ids:
            raise ValueError(f"Duplicate domain expertise ID: {domain_id}")
        if assertion_id in seen_assertion_ids:
            raise ValueError(f"Duplicate domain expertise assertion ID: {assertion_id}")
        seen_domain_ids.add(domain_id)
        seen_assertion_ids.add(assertion_id)
        _required_text(domain, "entered_label", "Domain expertise")
        normalized = _required_text(domain, "normalized_label", "Domain expertise")
        if normalized in seen_domains:
            raise ValueError(f"Duplicate normalized domain expertise: {normalized}")
        seen_domains.add(normalized)
        if domain.get("expertise_level") not in SUBJECT_EXPERTISE_LEVELS:
            raise ValueError("Unsupported domain expertise level")
        vocabulary_values = [
            domain.get("vocabulary_name"), domain.get("vocabulary_concept_uri"),
            domain.get("vocabulary_version"),
        ]
        if any(value is not None and not _nonempty_string(value) for value in vocabulary_values):
            raise ValueError("Domain vocabulary fields must be non-empty strings or null")
        if domain.get("vocabulary_concept_uri") is not None and not re.match(
            r"^https?://", str(domain["vocabulary_concept_uri"])
        ):
            raise ValueError("Domain vocabulary_concept_uri must use http or https")
        if domain.get("vocabulary_name") is None and any(value is not None for value in vocabulary_values[1:]):
            raise ValueError("Domain vocabulary URI/version requires vocabulary_name")
        if domain.get("vocabulary_name") is not None and any(
            value is None for value in vocabulary_values[1:]
        ):
            raise ValueError("Domain vocabulary selection requires URI and version")
        _iso_datetime(domain.get("first_asserted_at"), "first_asserted_at")
        _iso_datetime(domain.get("updated_at"), "updated_at")
    _iso_datetime(record.get("privacy_notice_acknowledged_at"), "privacy_notice_acknowledged_at")


def _nonempty_string(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _validate_predecessor_chains(
    records: Sequence[Mapping[str, Any]],
    *,
    id_field: str,
    predecessor_field: str,
    subject_fields: tuple[str, ...],
    timestamp_field: str,
    subject: str,
) -> None:
    by_id = {str(record[id_field]): record for record in records}
    successors: dict[str, str] = {}
    groups: dict[tuple[Any, ...], list[Mapping[str, Any]]] = {}
    for record in records:
        record_id = str(record[id_field])
        group_key = tuple(record.get(field) for field in subject_fields)
        groups.setdefault(group_key, []).append(record)
        predecessor_id = record.get(predecessor_field)
        if predecessor_id is None:
            continue
        predecessor = by_id.get(str(predecessor_id))
        if predecessor is None:
            raise ValueError(f"Dangling {predecessor_field}: {predecessor_id}")
        if any(predecessor.get(field) != record.get(field) for field in subject_fields):
            raise ValueError(f"{subject} predecessor must describe the same subject")
        predecessor_time = _parsed_datetime(predecessor.get(timestamp_field), timestamp_field)
        current_time = _parsed_datetime(record.get(timestamp_field), timestamp_field)
        if predecessor_time >= current_time:
            raise ValueError(f"{subject} predecessor must be earlier than its successor")
        if str(predecessor_id) in successors:
            raise ValueError(f"{subject} predecessor chain branches at {predecessor_id}")
        successors[str(predecessor_id)] = record_id

    for group_records in groups.values():
        roots = [record for record in group_records if record.get(predecessor_field) is None]
        if len(roots) != 1:
            raise ValueError(f"{subject} history must have exactly one root per subject")
        visited: set[str] = set()
        current = str(roots[0][id_field])
        while current in successors:
            if current in visited:
                raise ValueError(f"{subject} predecessor chain contains a cycle")
            visited.add(current)
            current = successors[current]
        visited.add(current)
        expected = {str(record[id_field]) for record in group_records}
        if visited != expected:
            raise ValueError(f"{subject} predecessor history is disconnected")


def validate_reviewer_domain_expertise_assertions(
    records: Sequence[Mapping[str, Any]], *, reviewer_ids: set[str] | None = None
) -> None:
    """Validate the complete append-only history behind profile projections."""
    seen_ids: set[str] = set()
    for record in records:
        missing = REVIEWER_DOMAIN_EXPERTISE_ASSERTION_FIELDS - set(record)
        if missing:
            raise ValueError(f"Domain expertise assertion is missing fields: {sorted(missing)}")
        unknown = set(record) - REVIEWER_DOMAIN_EXPERTISE_ASSERTION_FIELDS
        if unknown:
            raise ValueError(f"Domain expertise assertion has unsupported fields: {sorted(unknown)}")
        if record.get("schema") != "musparql.reviewer-domain-expertise-assertion.v1":
            raise ValueError(
                "Domain expertise assertion requires schema "
                "musparql.reviewer-domain-expertise-assertion.v1"
            )
        assertion_id = _required_text(record, "id", "Domain expertise assertion")
        if assertion_id in seen_ids:
            raise ValueError(f"Duplicate domain expertise assertion id: {assertion_id}")
        seen_ids.add(assertion_id)
        reviewer_id = validate_reviewer_id(record.get("reviewer_id"))
        if reviewer_ids is not None and reviewer_id not in reviewer_ids:
            raise ValueError(f"Unknown reviewer_id in domain expertise assertion: {reviewer_id}")
        for field in ("domain_id", "entered_label", "normalized_label"):
            _required_text(record, field, "Domain expertise assertion")
        if record.get("expertise_level") not in SUBJECT_EXPERTISE_LEVELS:
            raise ValueError("Unsupported domain expertise assertion level")
        vocabulary_values = [
            record.get("vocabulary_name"), record.get("vocabulary_concept_uri"),
            record.get("vocabulary_version"),
        ]
        if any(value is not None and not _nonempty_string(value) for value in vocabulary_values):
            raise ValueError("Domain expertise vocabulary fields must be non-empty strings or null")
        if record.get("vocabulary_concept_uri") is not None and not re.match(
            r"^https?://", str(record["vocabulary_concept_uri"])
        ):
            raise ValueError("Domain expertise vocabulary URI must use http or https")
        if record.get("vocabulary_name") is None and any(
            value is not None for value in vocabulary_values[1:]
        ):
            raise ValueError("Domain expertise vocabulary URI/version requires vocabulary_name")
        if record.get("vocabulary_name") is not None and any(
            value is None for value in vocabulary_values[1:]
        ):
            raise ValueError("Domain expertise vocabulary selection requires URI and version")
        supersedes_id = record.get("supersedes_id")
        if supersedes_id is not None and not _nonempty_string(supersedes_id):
            raise ValueError("supersedes_id must be a non-empty string or null")
        if supersedes_id == assertion_id:
            raise ValueError("supersedes_id cannot reference the current assertion")
        _iso_datetime(record.get("asserted_at"), "asserted_at")

    _validate_predecessor_chains(
        records,
        id_field="id",
        predecessor_field="supersedes_id",
        subject_fields=("reviewer_id", "domain_id"),
        timestamp_field="asserted_at",
        subject="Domain expertise assertion",
    )


def validate_reviewer_profile_projection(
    profile: Mapping[str, Any], assertions: Sequence[Mapping[str, Any]]
) -> None:
    """Require a profile's projected domains to equal its assertion-chain heads."""
    validate_reviewer_profile_v2(profile)
    reviewer_id = str(profile["id"])
    relevant = [record for record in assertions if record.get("reviewer_id") == reviewer_id]
    if not relevant:
        raise ValueError("Reviewer profile has no domain expertise assertion history")
    validate_reviewer_domain_expertise_assertions(relevant, reviewer_ids={reviewer_id})

    by_id = {str(record["id"]): record for record in relevant}
    superseded_ids = {
        str(record["supersedes_id"])
        for record in relevant
        if record.get("supersedes_id") is not None
    }
    heads = {
        str(record["domain_id"]): record
        for record in relevant
        if str(record["id"]) not in superseded_ids
    }
    projections = {
        str(domain["domain_id"]): domain
        for domain in profile["domain_expertise"]
    }
    if set(projections) != set(heads):
        raise ValueError("Reviewer profile domains do not match assertion history heads")

    projected_fields = (
        "entered_label", "normalized_label", "vocabulary_name",
        "vocabulary_concept_uri", "vocabulary_version", "expertise_level",
    )
    for domain_id, projection in projections.items():
        head = heads[domain_id]
        if projection.get("assertion_id") != head.get("id"):
            raise ValueError("Reviewer profile assertion_id is not the current history head")
        if any(projection.get(field) != head.get(field) for field in projected_fields):
            raise ValueError("Reviewer profile domain values do not match the current assertion")
        root = head
        while root.get("supersedes_id") is not None:
            root = by_id[str(root["supersedes_id"])]
        if _parsed_datetime(projection.get("first_asserted_at"), "first_asserted_at") != (
            _parsed_datetime(root.get("asserted_at"), "asserted_at")
        ):
            raise ValueError("Reviewer profile first_asserted_at does not match history root")
        if _parsed_datetime(projection.get("updated_at"), "updated_at") != _parsed_datetime(
            head.get("asserted_at"), "asserted_at"
        ):
            raise ValueError("Reviewer profile updated_at does not match history head")


def _validate_longitudinal_assessments(
    records: Sequence[Mapping[str, Any]], *, domain: bool, reviewer_ids: set[str] | None
) -> None:
    expected_schema = (
        "musparql.reviewer-kg-domain-assessment.v1"
        if domain else "musparql.reviewer-resource-familiarity-assessment.v1"
    )
    allowed_fields = KG_DOMAIN_ASSESSMENT_FIELDS if domain else RESOURCE_FAMILIARITY_ASSESSMENT_FIELDS
    subject = "KG domain assessment" if domain else "Resource familiarity assessment"
    seen_ids: set[str] = set()
    for record in records:
        missing = allowed_fields - set(record)
        if missing:
            raise ValueError(f"{subject} is missing fields: {sorted(missing)}")
        unknown = set(record) - allowed_fields
        if unknown:
            raise ValueError(f"{subject} has unsupported fields: {sorted(unknown)}")
        if record.get("schema") != expected_schema:
            raise ValueError(f"{subject} requires schema {expected_schema}")
        assessment_id = _required_text(record, "id", subject)
        if assessment_id in seen_ids:
            raise ValueError(f"Duplicate assessment id: {assessment_id}")
        seen_ids.add(assessment_id)
        reviewer_id = validate_reviewer_id(record.get("reviewer_id"))
        if reviewer_ids is not None and reviewer_id not in reviewer_ids:
            raise ValueError(f"Unknown reviewer_id in {subject.lower()}: {reviewer_id}")
        for field in ("kg_id", "seed_version"):
            _required_text(record, field, subject)
        id_field = "review_domain_id" if domain else "familiarity_scope_id"
        label_field = "review_domain_label" if domain else "familiarity_scope_label"
        _required_text(record, id_field, subject)
        _required_text(record, label_field, subject)
        level_field = "subject_expertise_level" if domain else "familiarity_level"
        levels = SUBJECT_EXPERTISE_LEVELS if domain else RESOURCE_FAMILIARITY_LEVELS
        if record.get(level_field) not in levels:
            raise ValueError(f"Unsupported {level_field}")
        context = record.get("context")
        assignment_id = record.get("assignment_id")
        if context not in {"pre_review", "profile"}:
            raise ValueError(f"Unsupported {subject.lower()} context")
        if context == "pre_review" and not _nonempty_string(assignment_id):
            raise ValueError(f"{subject} pre_review context requires assignment_id")
        if context == "profile" and assignment_id is not None:
            raise ValueError(f"{subject} profile context requires null assignment_id")
        previous = record.get("previous_assessment_id")
        if previous is not None and not _nonempty_string(previous):
            raise ValueError("previous_assessment_id must be a non-empty string or null")
        if previous == assessment_id:
            raise ValueError("previous_assessment_id cannot reference the current assessment")
        _iso_datetime(record.get("assessed_at"), "assessed_at")

    _validate_predecessor_chains(
        records,
        id_field="id",
        predecessor_field="previous_assessment_id",
        subject_fields=(
            ("reviewer_id", "kg_id", "review_domain_id")
            if domain
            else ("reviewer_id", "kg_id", "familiarity_scope_id")
        ),
        timestamp_field="assessed_at",
        subject=subject,
    )


def validate_kg_domain_assessments(
    records: Sequence[Mapping[str, Any]], *, reviewer_ids: set[str] | None = None
) -> None:
    _validate_longitudinal_assessments(records, domain=True, reviewer_ids=reviewer_ids)


def validate_resource_familiarity_assessments(
    records: Sequence[Mapping[str, Any]], *, reviewer_ids: set[str] | None = None
) -> None:
    _validate_longitudinal_assessments(records, domain=False, reviewer_ids=reviewer_ids)


def validate_assessments_against_kg_seed_snapshots(
    domain_records: Sequence[Mapping[str, Any]],
    familiarity_records: Sequence[Mapping[str, Any]],
    archive_payload: Mapping[str, Any],
) -> None:
    """Resolve every assessment to the immutable seed prompt it snapshots."""
    snapshots = validate_kg_seed_snapshots(archive_payload)
    seeds = {
        (str(snapshot.get("kg_id")), str(snapshot.get("seed_version"))): snapshot.get("seed")
        for snapshot in snapshots
    }
    for record in domain_records:
        key = (str(record.get("kg_id")), str(record.get("seed_version")))
        seed = seeds.get(key)
        if not isinstance(seed, Mapping):
            raise ValueError(f"KG domain assessment references an unknown seed snapshot: {key}")
        domains = seed.get("review_domains")
        match = next(
            (
                item for item in domains
                if isinstance(item, Mapping)
                and item.get("domain_id") == record.get("review_domain_id")
            ),
            None,
        ) if isinstance(domains, list) else None
        if match is None:
            raise ValueError("KG domain assessment references an unknown review_domain_id")
        if match.get("label") != record.get("review_domain_label"):
            raise ValueError("KG domain assessment label does not match its seed snapshot")

    for record in familiarity_records:
        key = (str(record.get("kg_id")), str(record.get("seed_version")))
        seed = seeds.get(key)
        if not isinstance(seed, Mapping):
            raise ValueError(
                f"Resource familiarity assessment references an unknown seed snapshot: {key}"
            )
        scopes = seed.get("familiarity_scopes")
        match = next(
            (
                item for item in scopes
                if isinstance(item, Mapping)
                and item.get("scope_id") == record.get("familiarity_scope_id")
            ),
            None,
        ) if isinstance(scopes, list) else None
        if match is None:
            raise ValueError(
                "Resource familiarity assessment references an unknown familiarity_scope_id"
            )
        if match.get("label") != record.get("familiarity_scope_label"):
            raise ValueError("Resource familiarity label does not match its seed snapshot")


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
