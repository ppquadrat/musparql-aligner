"""Load and validate stable provenance for repository, web, and local sources."""
from __future__ import annotations

from datetime import date
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
from typing import Any, Dict, List, Mapping
from urllib.parse import urlparse

import yaml


SOURCE_TYPES = {"repository", "web_document", "publication", "local_document", "derivative"}
QUERY_ROLES = {"canonical", "edit_source", "none"}
KG_SEEDS_SCHEMA = "musparql.kg-seeds.v2"
KG_SEED_SNAPSHOTS_SCHEMA = "musparql.kg-seed-snapshots.v1"
FAMILIARITY_SCOPE_KINDS = {"resource", "knowledge_graph", "federation"}
EXPERTISE_SUGGESTIONS_SCHEMA = "musparql.expertise-domain-suggestions.v1"
KG_SEED_FIELDS = frozenset({
    "kg_id", "seed_version", "name", "project", "description_hint", "sparql",
    "dataset", "source_ids", "priority", "notes", "review_domains",
    "familiarity_scopes",
})
KG_SEED_REQUIRED_FIELDS = KG_SEED_FIELDS - {"sparql", "dataset"}
SPARQL_TARGET_FIELDS = frozenset({
    "endpoint", "auth", "graph", "expected_namespaces", "fallbacks",
})
SPARQL_FALLBACK_FIELDS = frozenset({"endpoint", "auth", "graph"})
DATASET_FIELDS = frozenset({"dump_url", "local_path", "format"})
REVIEW_DOMAIN_FIELDS = frozenset({
    "domain_id", "label", "description", "vocabulary_mappings",
})
VOCABULARY_MAPPING_FIELDS = frozenset({
    "vocabulary", "concept_uri", "owner_verified",
})
FAMILIARITY_SCOPE_FIELDS = frozenset({"scope_id", "label", "kind"})
EXPERTISE_SUGGESTION_FIELDS = frozenset({
    "suggestion_id", "preferred_label", "alternative_labels", "language",
    "broader_suggestion_ids", "source_id", "vocabulary_concept_uri",
    "vocabulary_version",
})
EXPERTISE_SUGGESTION_REQUIRED_FIELDS = EXPERTISE_SUGGESTION_FIELDS - {
    "vocabulary_concept_uri", "vocabulary_version",
}
EXPERTISE_SOURCE_FIELDS = frozenset({
    "source_id", "kind", "title", "url", "source_version", "usage", "note",
})
EXPERTISE_SOURCE_REQUIRED_FIELDS = frozenset({
    "source_id", "kind", "title", "source_version", "usage",
})
LANGUAGE_TAG_RE = re.compile(r"^[a-z]{2,3}(?:-[A-Z]{2})?$")
SHA256_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
KG_SEED_SNAPSHOT_FIELDS = frozenset({
    "kg_id", "seed_version", "seed_digest", "previous_seed_digest", "seed",
})


def _nonempty(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _validate_url(value: Any, location: str) -> None:
    if not _nonempty(value):
        raise ValueError(f"{location} must be a non-empty URL")
    parsed = urlparse(str(value))
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError(f"{location} must use http or https")


def _validate_local_path(value: Any, location: str) -> None:
    if not _nonempty(value):
        raise ValueError(f"{location} must be a non-empty repository-relative path")
    path = PurePosixPath(str(value))
    if path.is_absolute() or ".." in path.parts:
        raise ValueError(f"{location} must be repository-relative and cannot contain '..'")


def _validate_fields(
    record: Dict[str, Any], *, required: frozenset[str], allowed: frozenset[str], location: str
) -> None:
    missing = required - set(record)
    if missing:
        raise ValueError(f"{location} is missing fields: {sorted(missing)}")
    unknown = set(record) - allowed
    if unknown:
        raise ValueError(f"{location} has unsupported fields: {sorted(unknown)}")


def _validate_nonempty_fields(record: Dict[str, Any], fields: tuple[str, ...], location: str) -> None:
    for field in fields:
        if not _nonempty(record.get(field)):
            raise ValueError(f"{location}.{field} must be non-empty")


def _validate_date(value: Any, location: str) -> None:
    if not _nonempty(value):
        raise ValueError(f"{location} must be an ISO 8601 date")
    try:
        parsed = date.fromisoformat(str(value))
    except ValueError as exc:
        raise ValueError(f"{location} must be an ISO 8601 date") from exc
    if parsed.isoformat() != value:
        raise ValueError(f"{location} must use YYYY-MM-DD format")


def load_source_catalog(path: Path) -> Dict[str, Dict[str, Any]]:
    if not path.exists():
        raise FileNotFoundError(f"Missing source catalogue: {path}")
    payload = yaml.safe_load(path.read_text(encoding="utf-8-sig"))
    if not isinstance(payload, dict) or not isinstance(payload.get("sources"), list):
        raise ValueError("sources.yaml must contain a top-level 'sources' list")

    catalogue: Dict[str, Dict[str, Any]] = {}
    for index, raw in enumerate(payload["sources"]):
        location = f"sources[{index}]"
        if not isinstance(raw, dict):
            raise ValueError(f"{location} must be a mapping")
        source_id = raw.get("source_id")
        source_type = raw.get("type")
        title = raw.get("title")
        if not _nonempty(source_id):
            raise ValueError(f"{location}.source_id must be non-empty")
        if source_id in catalogue:
            raise ValueError(f"Duplicate source_id: {source_id}")
        if source_type not in SOURCE_TYPES:
            raise ValueError(f"{source_id}: unsupported source type {source_type!r}")
        query_role = raw.get("query_role")
        if query_role is not None and query_role not in QUERY_ROLES:
            raise ValueError(f"{source_id}: unsupported query_role {query_role!r}")
        exclude_test_fixtures = raw.get("exclude_test_fixtures")
        if exclude_test_fixtures is not None and not isinstance(exclude_test_fixtures, bool):
            raise ValueError(f"{source_id}: exclude_test_fixtures must be boolean")
        if exclude_test_fixtures and source_type != "repository":
            raise ValueError(
                f"{source_id}: exclude_test_fixtures is only valid for repository sources"
            )
        if not _nonempty(title):
            raise ValueError(f"{source_id}: title must be non-empty")
        if raw.get("url") is not None:
            _validate_url(raw.get("url"), f"{source_id}.url")
        if raw.get("local_path") is not None:
            _validate_local_path(raw.get("local_path"), f"{source_id}.local_path")
        derived_from = raw.get("derived_from") or []
        if not isinstance(derived_from, list) or not all(_nonempty(item) for item in derived_from):
            raise ValueError(f"{source_id}.derived_from must be a list of source IDs")
        has_external = _nonempty(raw.get("url"))
        has_derivation = bool(derived_from)
        has_justification = _nonempty(raw.get("description"))
        if not (has_external or has_derivation or has_justification):
            raise ValueError(
                f"{source_id}: source must have an external URL, derived_from reference, or description"
            )
        if source_type == "repository" and not has_external:
            raise ValueError(f"{source_id}: repository sources require a URL")
        if source_type == "derivative" and not has_derivation:
            raise ValueError(f"{source_id}: derivative sources require derived_from")
        if not has_external and not _nonempty(raw.get("local_path")):
            raise ValueError(f"{source_id}: non-downloadable sources require local_path")
        catalogue[str(source_id)] = dict(raw)

    for source_id, source in catalogue.items():
        for parent in source.get("derived_from") or []:
            if parent not in catalogue:
                raise ValueError(f"{source_id}: unknown derived_from source {parent!r}")
            if parent == source_id:
                raise ValueError(f"{source_id}: source cannot derive from itself")
    return catalogue


def validate_kg_seeds(payload: Any, *, location: str = "seeds.yaml") -> List[Dict[str, Any]]:
    """Validate the complete versioned KG seed contract used by runtime loaders."""
    if not isinstance(payload, dict) or payload.get("schema") != KG_SEEDS_SCHEMA:
        raise ValueError(f"{location} must declare schema {KG_SEEDS_SCHEMA!r}")
    unknown_top_level = set(payload) - {"schema", "kgs"}
    if unknown_top_level:
        raise ValueError(f"{location} has unsupported fields: {sorted(unknown_top_level)}")
    kgs = payload.get("kgs")
    if not isinstance(kgs, list) or not kgs:
        raise ValueError(f"{location} must contain a non-empty top-level 'kgs' list")

    seen_kgs: set[str] = set()
    for index, raw in enumerate(kgs):
        item_location = f"{location}: kgs[{index}]"
        if not isinstance(raw, dict):
            raise ValueError(f"{item_location} must be a mapping")
        _validate_fields(
            raw,
            required=KG_SEED_REQUIRED_FIELDS,
            allowed=KG_SEED_FIELDS,
            location=item_location,
        )
        _validate_nonempty_fields(
            raw,
            ("kg_id", "seed_version", "name", "project", "description_hint", "notes"),
            item_location,
        )
        kg_id = raw.get("kg_id")
        if kg_id in seen_kgs:
            raise ValueError(f"Duplicate kg_id: {kg_id}")
        seen_kgs.add(str(kg_id))
        if raw.get("priority") not in {"high", "medium", "low"}:
            raise ValueError(f"{kg_id}: priority is unsupported")
        source_ids = raw.get("source_ids")
        if not isinstance(source_ids, list) or not source_ids or not all(
            _nonempty(source_id) for source_id in source_ids
        ):
            raise ValueError(f"{kg_id}: source_ids must be a non-empty list of IDs")
        if len(source_ids) != len(set(source_ids)):
            raise ValueError(f"{kg_id}: source_ids must not contain duplicates")

        sparql = raw.get("sparql")
        dataset = raw.get("dataset")
        if "sparql" in raw and not isinstance(sparql, dict):
            raise ValueError(f"{kg_id}.sparql must be a mapping")
        if "dataset" in raw and not isinstance(dataset, dict):
            raise ValueError(f"{kg_id}.dataset must be a mapping")
        if not isinstance(sparql, dict) and not isinstance(dataset, dict):
            raise ValueError(f"{kg_id}: either sparql or dataset is required")
        if isinstance(sparql, dict):
            _validate_fields(
                sparql,
                required=frozenset({"endpoint", "auth"}),
                allowed=SPARQL_TARGET_FIELDS,
                location=f"{kg_id}.sparql",
            )
            _validate_url(sparql.get("endpoint"), f"{kg_id}.sparql.endpoint")
            if not _nonempty(sparql.get("auth")):
                raise ValueError(f"{kg_id}.sparql.auth must be non-empty")
            if "graph" in sparql:
                _validate_url(sparql.get("graph"), f"{kg_id}.sparql.graph")
            namespaces = sparql.get("expected_namespaces", [])
            if not isinstance(namespaces, list) or not all(_nonempty(value) for value in namespaces):
                raise ValueError(f"{kg_id}.sparql.expected_namespaces must be a list of URLs")
            for namespace_index, namespace in enumerate(namespaces):
                _validate_url(
                    namespace,
                    f"{kg_id}.sparql.expected_namespaces[{namespace_index}]",
                )
            fallbacks = sparql.get("fallbacks", [])
            if not isinstance(fallbacks, list):
                raise ValueError(f"{kg_id}.sparql.fallbacks must be a list")
            for fallback_index, fallback in enumerate(fallbacks):
                fallback_location = f"{kg_id}.sparql.fallbacks[{fallback_index}]"
                if not isinstance(fallback, dict):
                    raise ValueError(f"{fallback_location} must be a mapping")
                _validate_fields(
                    fallback,
                    required=frozenset({"endpoint", "auth"}),
                    allowed=SPARQL_FALLBACK_FIELDS,
                    location=fallback_location,
                )
                _validate_url(fallback.get("endpoint"), f"{fallback_location}.endpoint")
                if not _nonempty(fallback.get("auth")):
                    raise ValueError(f"{fallback_location}.auth must be non-empty")
                if "graph" in fallback:
                    _validate_url(fallback.get("graph"), f"{fallback_location}.graph")

        if isinstance(dataset, dict):
            _validate_fields(
                dataset,
                required=DATASET_FIELDS,
                allowed=DATASET_FIELDS,
                location=f"{kg_id}.dataset",
            )
            _validate_url(dataset.get("dump_url"), f"{kg_id}.dataset.dump_url")
            _validate_local_path(dataset.get("local_path"), f"{kg_id}.dataset.local_path")
            if not _nonempty(dataset.get("format")):
                raise ValueError(f"{kg_id}.dataset.format must be non-empty")

        review_domains = raw.get("review_domains")
        if not isinstance(review_domains, list) or not review_domains:
            raise ValueError(f"{kg_id}: review_domains must be a non-empty list")
        seen_domains: set[str] = set()
        for domain_index, domain in enumerate(review_domains):
            domain_location = f"{kg_id}.review_domains[{domain_index}]"
            if not isinstance(domain, dict):
                raise ValueError(f"{domain_location} must be a mapping")
            _validate_fields(
                domain,
                required=frozenset({"domain_id", "label", "description"}),
                allowed=REVIEW_DOMAIN_FIELDS,
                location=domain_location,
            )
            domain_id = domain.get("domain_id")
            if not _nonempty(domain_id) or not _nonempty(domain.get("label")) or not _nonempty(
                domain.get("description")
            ):
                raise ValueError(f"{domain_location} requires domain_id, label, and description")
            if domain_id in seen_domains:
                raise ValueError(f"{kg_id}: duplicate review domain {domain_id!r}")
            seen_domains.add(str(domain_id))
            mappings = domain.get("vocabulary_mappings", [])
            if not isinstance(mappings, list):
                raise ValueError(f"{domain_location}.vocabulary_mappings must be a list")
            for mapping_index, mapping in enumerate(mappings):
                mapping_location = f"{domain_location}.vocabulary_mappings[{mapping_index}]"
                if not isinstance(mapping, dict):
                    raise ValueError(f"{mapping_location} must be a mapping")
                _validate_fields(
                    mapping,
                    required=VOCABULARY_MAPPING_FIELDS,
                    allowed=VOCABULARY_MAPPING_FIELDS,
                    location=mapping_location,
                )
                if not _nonempty(mapping.get("vocabulary")):
                    raise ValueError(f"{mapping_location}.vocabulary must be non-empty")
                _validate_url(mapping.get("concept_uri"), f"{mapping_location}.concept_uri")
                if mapping.get("owner_verified") is not True:
                    raise ValueError(f"{mapping_location} must set owner_verified to true")

        scopes = raw.get("familiarity_scopes")
        if not isinstance(scopes, list) or not scopes:
            raise ValueError(f"{kg_id}: familiarity_scopes must be a non-empty list")
        seen_scopes: set[str] = set()
        for scope_index, scope in enumerate(scopes):
            scope_location = f"{kg_id}.familiarity_scopes[{scope_index}]"
            if not isinstance(scope, dict):
                raise ValueError(f"{scope_location} must be a mapping")
            _validate_fields(
                scope,
                required=FAMILIARITY_SCOPE_FIELDS,
                allowed=FAMILIARITY_SCOPE_FIELDS,
                location=scope_location,
            )
            scope_id = scope.get("scope_id")
            if not _nonempty(scope_id) or not _nonempty(scope.get("label")):
                raise ValueError(f"{scope_location} requires scope_id and label")
            if scope.get("kind") not in FAMILIARITY_SCOPE_KINDS:
                raise ValueError(f"{scope_location}.kind is unsupported")
            if scope_id in seen_scopes:
                raise ValueError(f"{kg_id}: duplicate familiarity scope {scope_id!r}")
            seen_scopes.add(str(scope_id))
    return kgs


def kg_seed_digest(seed: Mapping[str, Any]) -> str:
    """Return the stable digest used to address an immutable KG seed snapshot."""
    canonical = json.dumps(seed, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return f"sha256:{hashlib.sha256(canonical.encode('utf-8')).hexdigest()}"


def validate_kg_seed_snapshots(
    payload: Any, *, location: str = "kg_seed_snapshots.yaml"
) -> List[Dict[str, Any]]:
    """Validate the append-only archive that makes every seed version resolvable."""
    if not isinstance(payload, dict) or payload.get("schema") != KG_SEED_SNAPSHOTS_SCHEMA:
        raise ValueError(f"{location} must declare schema {KG_SEED_SNAPSHOTS_SCHEMA!r}")
    unknown_top_level = set(payload) - {"schema", "snapshots"}
    if unknown_top_level:
        raise ValueError(f"{location} has unsupported fields: {sorted(unknown_top_level)}")
    snapshots = payload.get("snapshots")
    if not isinstance(snapshots, list) or not snapshots:
        raise ValueError(f"{location} must contain a non-empty snapshots list")

    by_key: Dict[tuple[str, str], Dict[str, Any]] = {}
    by_digest: Dict[str, Dict[str, Any]] = {}
    for index, snapshot in enumerate(snapshots):
        snapshot_location = f"{location}: snapshots[{index}]"
        if not isinstance(snapshot, dict):
            raise ValueError(f"{snapshot_location} must be a mapping")
        _validate_fields(
            snapshot,
            required=KG_SEED_SNAPSHOT_FIELDS,
            allowed=KG_SEED_SNAPSHOT_FIELDS,
            location=snapshot_location,
        )
        _validate_nonempty_fields(snapshot, ("kg_id", "seed_version"), snapshot_location)
        digest = snapshot.get("seed_digest")
        previous_digest = snapshot.get("previous_seed_digest")
        if not isinstance(digest, str) or not SHA256_DIGEST_RE.fullmatch(digest):
            raise ValueError(f"{snapshot_location}.seed_digest must be a sha256 digest")
        if previous_digest is not None and (
            not isinstance(previous_digest, str)
            or not SHA256_DIGEST_RE.fullmatch(previous_digest)
        ):
            raise ValueError(
                f"{snapshot_location}.previous_seed_digest must be a sha256 digest or null"
            )
        seed = snapshot.get("seed")
        if not isinstance(seed, dict):
            raise ValueError(f"{snapshot_location}.seed must be a mapping")
        validate_kg_seeds(
            {"schema": KG_SEEDS_SCHEMA, "kgs": [seed]},
            location=f"{snapshot_location}.seed",
        )
        if snapshot["kg_id"] != seed.get("kg_id"):
            raise ValueError(f"{snapshot_location}.kg_id does not match its seed")
        if snapshot["seed_version"] != seed.get("seed_version"):
            raise ValueError(f"{snapshot_location}.seed_version does not match its seed")
        expected_digest = kg_seed_digest(seed)
        if digest != expected_digest:
            raise ValueError(
                f"{snapshot_location}.seed_digest does not match its canonical seed content"
            )
        key = (str(snapshot["kg_id"]), str(snapshot["seed_version"]))
        if key in by_key:
            raise ValueError(f"Duplicate KG seed snapshot version: {key[0]}/{key[1]}")
        if digest in by_digest:
            raise ValueError(f"Duplicate KG seed snapshot digest: {digest}")
        by_key[key] = snapshot
        by_digest[digest] = snapshot

    successors: Dict[str, str] = {}
    roots_by_kg: Dict[str, List[str]] = {}
    for digest, snapshot in by_digest.items():
        kg_id = str(snapshot["kg_id"])
        previous_digest = snapshot.get("previous_seed_digest")
        if previous_digest is None:
            roots_by_kg.setdefault(kg_id, []).append(digest)
            continue
        predecessor = by_digest.get(str(previous_digest))
        if predecessor is None:
            raise ValueError(f"Dangling previous_seed_digest: {previous_digest}")
        if predecessor.get("kg_id") != kg_id:
            raise ValueError("KG seed snapshot predecessor must belong to the same kg_id")
        if previous_digest in successors:
            raise ValueError(f"KG seed snapshot history branches at {previous_digest}")
        successors[str(previous_digest)] = digest

    kg_ids = {str(snapshot["kg_id"]) for snapshot in snapshots}
    for kg_id in kg_ids:
        if len(roots_by_kg.get(kg_id, [])) != 1:
            raise ValueError(f"KG seed snapshot history for {kg_id} must have exactly one root")
        heads = [
            digest for digest, snapshot in by_digest.items()
            if snapshot.get("kg_id") == kg_id and digest not in successors
        ]
        if len(heads) != 1:
            raise ValueError(f"KG seed snapshot history for {kg_id} must have exactly one head")
        visited: set[str] = set()
        current = roots_by_kg[kg_id][0]
        while current in successors:
            if current in visited:
                raise ValueError(f"KG seed snapshot history for {kg_id} contains a cycle")
            visited.add(current)
            current = successors[current]
        visited.add(current)
        expected = {
            digest for digest, snapshot in by_digest.items()
            if snapshot.get("kg_id") == kg_id
        }
        if visited != expected:
            raise ValueError(f"KG seed snapshot history for {kg_id} is disconnected")
    return snapshots


def validate_current_kg_seed_snapshots(
    seeds: List[Dict[str, Any]], snapshots: List[Dict[str, Any]]
) -> None:
    """Require every current seed to equal the unique head of its archived history."""
    archived = {
        (str(snapshot["kg_id"]), str(snapshot["seed_version"])): snapshot
        for snapshot in snapshots
    }
    predecessor_digests = {
        str(snapshot["previous_seed_digest"])
        for snapshot in snapshots
        if snapshot.get("previous_seed_digest") is not None
    }
    head_by_kg = {
        str(snapshot["kg_id"]): str(snapshot["seed_digest"])
        for snapshot in snapshots
        if snapshot["seed_digest"] not in predecessor_digests
    }
    current_kg_ids = {str(seed.get("kg_id")) for seed in seeds}
    missing_heads = current_kg_ids - set(head_by_kg)
    if missing_heads:
        raise ValueError(f"Current KG seeds have no archived history head: {sorted(missing_heads)}")
    for seed in seeds:
        kg_id = str(seed["kg_id"])
        seed_version = str(seed["seed_version"])
        snapshot = archived.get((kg_id, seed_version))
        if snapshot is None:
            raise ValueError(f"Current KG seed has no immutable snapshot: {kg_id}/{seed_version}")
        digest = kg_seed_digest(seed)
        if snapshot.get("seed_digest") != digest:
            raise ValueError(f"KG seed version was reused with changed content: {kg_id}/{seed_version}")
        if head_by_kg.get(kg_id) != digest:
            raise ValueError(f"Current KG seed is not the archived history head: {kg_id}/{seed_version}")


def load_kg_seed_snapshots(path: Path) -> List[Dict[str, Any]]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8-sig"))
    return validate_kg_seed_snapshots(payload, location=str(path))


def load_expertise_domain_suggestions(path: Path) -> Dict[str, Any]:
    """Load the reproducible local suggestion set without contacting a live vocabulary."""
    payload = yaml.safe_load(path.read_text(encoding="utf-8-sig"))
    if not isinstance(payload, dict) or payload.get("schema") != EXPERTISE_SUGGESTIONS_SCHEMA:
        raise ValueError(f"{path} must declare schema {EXPERTISE_SUGGESTIONS_SCHEMA!r}")
    _validate_fields(
        payload,
        required=frozenset({"schema", "snapshot_id", "created_on", "sources", "suggestions"}),
        allowed=frozenset({"schema", "snapshot_id", "created_on", "sources", "suggestions"}),
        location=str(path),
    )
    if not _nonempty(payload.get("snapshot_id")):
        raise ValueError(f"{path}.snapshot_id must be non-empty")
    _validate_date(payload.get("created_on"), f"{path}.created_on")
    sources = payload.get("sources")
    suggestions = payload.get("suggestions")
    if not isinstance(sources, list) or not sources or not isinstance(suggestions, list):
        raise ValueError(f"{path} requires non-empty sources and a suggestions list")
    source_ids: set[str] = set()
    source_usage: Dict[str, str] = {}
    for index, source in enumerate(sources):
        source_location = f"{path}: sources[{index}]"
        if not isinstance(source, dict):
            raise ValueError(f"{source_location} must be a mapping")
        _validate_fields(
            source,
            required=EXPERTISE_SOURCE_REQUIRED_FIELDS,
            allowed=EXPERTISE_SOURCE_FIELDS,
            location=source_location,
        )
        _validate_nonempty_fields(source, ("source_id", "title"), source_location)
        source_id = str(source["source_id"])
        if source_id in source_ids:
            raise ValueError(f"Duplicate expertise suggestion source_id: {source_id}")
        source_ids.add(source_id)
        if source.get("kind") not in {"owner_reviewed_project_terms", "external_vocabulary"}:
            raise ValueError(f"{source_id}: unsupported expertise suggestion source kind")
        if source.get("usage") not in {"suggestion_entries", "reference_only"}:
            raise ValueError(f"{source_id}: unsupported expertise suggestion source usage")
        source_usage[source_id] = str(source.get("usage"))
        source_version = source.get("source_version")
        if source_version is not None and not _nonempty(source_version):
            raise ValueError(f"{source_id}.source_version must be non-empty or null")
        if source.get("url") is not None:
            _validate_url(source.get("url"), f"{source_id}.url")
        if source.get("note") is not None and not _nonempty(source.get("note")):
            raise ValueError(f"{source_id}.note must be non-empty when present")
    suggestion_ids: set[str] = set()
    for index, suggestion in enumerate(suggestions):
        location = f"{path}: suggestions[{index}]"
        if not isinstance(suggestion, dict):
            raise ValueError(f"{location} must be a mapping")
        _validate_fields(
            suggestion,
            required=EXPERTISE_SUGGESTION_REQUIRED_FIELDS,
            allowed=EXPERTISE_SUGGESTION_FIELDS,
            location=location,
        )
        suggestion_id = suggestion.get("suggestion_id")
        if not _nonempty(suggestion_id) or not _nonempty(suggestion.get("preferred_label")):
            raise ValueError(f"{location} requires suggestion_id and preferred_label")
        if suggestion_id in suggestion_ids:
            raise ValueError(f"Duplicate expertise suggestion_id: {suggestion_id}")
        suggestion_ids.add(str(suggestion_id))
        if suggestion.get("source_id") not in source_ids:
            raise ValueError(f"{location} references an unknown source_id")
        if source_usage[str(suggestion.get("source_id"))] != "suggestion_entries":
            raise ValueError(f"{location} cannot use a reference-only source")
        language = suggestion.get("language")
        if not isinstance(language, str) or not LANGUAGE_TAG_RE.fullmatch(language):
            raise ValueError(f"{location}.language must be a language tag")
        broader = suggestion.get("broader_suggestion_ids")
        alternatives = suggestion.get("alternative_labels")
        if not isinstance(broader, list) or not all(_nonempty(value) for value in broader):
            raise ValueError(f"{location}.broader_suggestion_ids must be a list of IDs")
        if not isinstance(alternatives, list) or not all(_nonempty(value) for value in alternatives):
            raise ValueError(f"{location}.alternative_labels must be a list of labels")
        concept_uri = suggestion.get("vocabulary_concept_uri")
        vocabulary_version = suggestion.get("vocabulary_version")
        if concept_uri is not None:
            _validate_url(concept_uri, f"{location}.vocabulary_concept_uri")
        if vocabulary_version is not None and not _nonempty(vocabulary_version):
            raise ValueError(f"{location}.vocabulary_version must be non-empty when present")
        if (concept_uri is None) != (vocabulary_version is None):
            raise ValueError(f"{location} vocabulary entries require URI and version together")
    unknown_broader = {
        broader_id
        for suggestion in suggestions
        for broader_id in suggestion.get("broader_suggestion_ids", [])
        if broader_id not in suggestion_ids
    }
    if unknown_broader:
        raise ValueError(f"Unknown broader expertise suggestion IDs: {sorted(unknown_broader)}")
    return payload


def load_current_kg_seeds(
    seeds_path: Path, snapshots_path: Path | None = None
) -> List[Dict[str, Any]]:
    """Load current seeds only after resolving them to immutable archive heads."""
    snapshots_path = snapshots_path or seeds_path.with_name("kg_seed_snapshots.yaml")
    payload = yaml.safe_load(seeds_path.read_text(encoding="utf-8-sig"))
    seeds = validate_kg_seeds(payload, location=str(seeds_path))
    if not snapshots_path.exists():
        raise FileNotFoundError(f"Missing KG seed snapshot archive: {snapshots_path}")
    snapshots = load_kg_seed_snapshots(snapshots_path)
    validate_current_kg_seed_snapshots(seeds, snapshots)
    return seeds


def load_hydrated_seeds(
    seeds_path: Path,
    sources_path: Path | None = None,
    snapshots_path: Path | None = None,
) -> List[Dict[str, Any]]:
    sources_path = sources_path or seeds_path.with_name("sources.yaml")
    catalogue = load_source_catalog(sources_path)
    raw_kgs = load_current_kg_seeds(seeds_path, snapshots_path)

    hydrated: List[Dict[str, Any]] = []
    for index, raw in enumerate(raw_kgs):
        if not isinstance(raw, dict):
            raise ValueError(f"seeds.yaml: kgs[{index}] must be a mapping")
        source_ids = raw.get("source_ids") or []
        if not isinstance(source_ids, list) or not all(_nonempty(item) for item in source_ids):
            raise ValueError(f"seeds.yaml: kgs[{index}].source_ids must be a list of IDs")
        source_records = []
        repos: List[str] = []
        docs: List[str] = []
        for source_id in source_ids:
            if source_id not in catalogue:
                raise ValueError(f"seeds.yaml: unknown source_id {source_id!r}")
            source = dict(catalogue[source_id])
            source_records.append(source)
            locator = source.get("local_path") or source.get("url")
            if source.get("type") == "repository":
                repos.append(str(source["url"]))
            elif locator:
                docs.append(str(locator))
        item = dict(raw)
        item["repos"] = repos
        item["docs"] = docs
        item["source_records"] = source_records
        hydrated.append(item)
    return hydrated


def source_for_locator(
    source_path: Any,
    source_url: Any,
    catalogue: Dict[str, Dict[str, Any]],
) -> Dict[str, Any] | None:
    path_text = str(source_path or "")
    url_text = str(source_url or "")
    for source in catalogue.values():
        if path_text and path_text == str(source.get("local_path") or ""):
            return source
        url = str(source.get("url") or "")
        if url and url_text and (
            url_text == url
            or url_text.startswith(url.rstrip("/") + "/")
            or (
                "github.com/" in url
                and url.split("github.com/", 1)[1].split("/blob/", 1)[0] in url_text
            )
        ):
            return source
    return None


def validate_catalogued_local_files(catalogue: Dict[str, Dict[str, Any]], roots: List[Path]) -> List[str]:
    # Catalog roots are repository_root/catalog/<kind>; local_path values stay
    # repository-relative so provenance is stable across machines.
    repository_root = roots[0].parents[1] if roots else Path.cwd()
    catalogued = {
        str((repository_root / str(source.get("local_path"))).resolve())
        for source in catalogue.values()
        if source.get("local_path")
    }
    missing: List[str] = []
    for root in roots:
        if not root.exists():
            continue
        for path in sorted(item for item in root.iterdir() if item.is_file() and not item.name.startswith(".")):
            if str(path.resolve()) not in catalogued:
                missing.append(str(path))
    return missing
