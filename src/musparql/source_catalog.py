"""Load and validate stable provenance for repository, web, and local sources."""
from __future__ import annotations

from pathlib import Path, PurePosixPath
from typing import Any, Dict, List
from urllib.parse import urlparse

import yaml


SOURCE_TYPES = {"repository", "web_document", "publication", "local_document", "derivative"}
QUERY_ROLES = {"canonical", "edit_source", "none"}
KG_SEEDS_SCHEMA = "musparql.kg-seeds.v2"
FAMILIARITY_SCOPE_KINDS = {"resource", "knowledge_graph", "federation"}
EXPERTISE_SUGGESTIONS_SCHEMA = "musparql.expertise-domain-suggestions.v1"


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
    """Validate the versioned review-domain portion of the KG seed contract."""
    if not isinstance(payload, dict) or payload.get("schema") != KG_SEEDS_SCHEMA:
        raise ValueError(f"{location} must declare schema {KG_SEEDS_SCHEMA!r}")
    kgs = payload.get("kgs")
    if not isinstance(kgs, list) or not kgs:
        raise ValueError(f"{location} must contain a non-empty top-level 'kgs' list")

    seen_kgs: set[str] = set()
    for index, raw in enumerate(kgs):
        item_location = f"{location}: kgs[{index}]"
        if not isinstance(raw, dict):
            raise ValueError(f"{item_location} must be a mapping")
        kg_id = raw.get("kg_id")
        if not _nonempty(kg_id):
            raise ValueError(f"{item_location}.kg_id must be non-empty")
        if kg_id in seen_kgs:
            raise ValueError(f"Duplicate kg_id: {kg_id}")
        seen_kgs.add(str(kg_id))
        if not _nonempty(raw.get("seed_version")):
            raise ValueError(f"{kg_id}: seed_version must be non-empty")

        review_domains = raw.get("review_domains")
        if not isinstance(review_domains, list) or not review_domains:
            raise ValueError(f"{kg_id}: review_domains must be a non-empty list")
        seen_domains: set[str] = set()
        for domain_index, domain in enumerate(review_domains):
            domain_location = f"{kg_id}.review_domains[{domain_index}]"
            if not isinstance(domain, dict):
                raise ValueError(f"{domain_location} must be a mapping")
            domain_id = domain.get("domain_id")
            if not _nonempty(domain_id) or not _nonempty(domain.get("label")) or not _nonempty(
                domain.get("description")
            ):
                raise ValueError(f"{domain_location} requires domain_id, label, and description")
            if domain_id in seen_domains:
                raise ValueError(f"{kg_id}: duplicate review domain {domain_id!r}")
            seen_domains.add(str(domain_id))
            mappings = domain.get("vocabulary_mappings") or []
            if not isinstance(mappings, list):
                raise ValueError(f"{domain_location}.vocabulary_mappings must be a list")
            for mapping_index, mapping in enumerate(mappings):
                mapping_location = f"{domain_location}.vocabulary_mappings[{mapping_index}]"
                if not isinstance(mapping, dict) or not _nonempty(mapping.get("vocabulary")):
                    raise ValueError(f"{mapping_location} requires vocabulary")
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
            scope_id = scope.get("scope_id")
            if not _nonempty(scope_id) or not _nonempty(scope.get("label")):
                raise ValueError(f"{scope_location} requires scope_id and label")
            if scope.get("kind") not in FAMILIARITY_SCOPE_KINDS:
                raise ValueError(f"{scope_location}.kind is unsupported")
            if scope_id in seen_scopes:
                raise ValueError(f"{kg_id}: duplicate familiarity scope {scope_id!r}")
            seen_scopes.add(str(scope_id))
    return kgs


def load_expertise_domain_suggestions(path: Path) -> Dict[str, Any]:
    """Load the reproducible local suggestion set without contacting a live vocabulary."""
    payload = yaml.safe_load(path.read_text(encoding="utf-8-sig"))
    if not isinstance(payload, dict) or payload.get("schema") != EXPERTISE_SUGGESTIONS_SCHEMA:
        raise ValueError(f"{path} must declare schema {EXPERTISE_SUGGESTIONS_SCHEMA!r}")
    if not _nonempty(payload.get("snapshot_id")) or not _nonempty(payload.get("created_on")):
        raise ValueError(f"{path} requires snapshot_id and created_on")
    sources = payload.get("sources")
    suggestions = payload.get("suggestions")
    if not isinstance(sources, list) or not sources or not isinstance(suggestions, list):
        raise ValueError(f"{path} requires non-empty sources and a suggestions list")
    source_ids: set[str] = set()
    source_usage: Dict[str, str] = {}
    for index, source in enumerate(sources):
        if not isinstance(source, dict) or not _nonempty(source.get("source_id")):
            raise ValueError(f"{path}: sources[{index}] requires source_id")
        source_id = str(source["source_id"])
        if source_id in source_ids:
            raise ValueError(f"Duplicate expertise suggestion source_id: {source_id}")
        source_ids.add(source_id)
        if source.get("kind") not in {"owner_reviewed_project_terms", "external_vocabulary"}:
            raise ValueError(f"{source_id}: unsupported expertise suggestion source kind")
        if source.get("usage") not in {"suggestion_entries", "reference_only"}:
            raise ValueError(f"{source_id}: unsupported expertise suggestion source usage")
        source_usage[source_id] = str(source.get("usage"))
        if source.get("url") is not None:
            _validate_url(source.get("url"), f"{source_id}.url")
    suggestion_ids: set[str] = set()
    for index, suggestion in enumerate(suggestions):
        location = f"{path}: suggestions[{index}]"
        if not isinstance(suggestion, dict):
            raise ValueError(f"{location} must be a mapping")
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
        broader = suggestion.get("broader_suggestion_ids")
        alternatives = suggestion.get("alternative_labels")
        if not isinstance(broader, list) or not all(_nonempty(value) for value in broader):
            raise ValueError(f"{location}.broader_suggestion_ids must be a list of IDs")
        if not isinstance(alternatives, list) or not all(_nonempty(value) for value in alternatives):
            raise ValueError(f"{location}.alternative_labels must be a list of labels")
        if suggestion.get("vocabulary_concept_uri") is not None:
            _validate_url(suggestion.get("vocabulary_concept_uri"), f"{location}.vocabulary_concept_uri")
            if not _nonempty(suggestion.get("vocabulary_version")):
                raise ValueError(f"{location} vocabulary entries require vocabulary_version")
    unknown_broader = {
        broader_id
        for suggestion in suggestions
        for broader_id in suggestion.get("broader_suggestion_ids", [])
        if broader_id not in suggestion_ids
    }
    if unknown_broader:
        raise ValueError(f"Unknown broader expertise suggestion IDs: {sorted(unknown_broader)}")
    return payload


def load_hydrated_seeds(seeds_path: Path, sources_path: Path | None = None) -> List[Dict[str, Any]]:
    sources_path = sources_path or seeds_path.with_name("sources.yaml")
    catalogue = load_source_catalog(sources_path)
    payload = yaml.safe_load(seeds_path.read_text(encoding="utf-8-sig"))
    raw_kgs = validate_kg_seeds(payload, location=str(seeds_path))

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
