"""Load and validate stable provenance for repository, web, and local sources."""
from __future__ import annotations

from pathlib import Path, PurePosixPath
from typing import Any, Dict, List
from urllib.parse import urlparse

import yaml


SOURCE_TYPES = {"repository", "web_document", "publication", "local_document", "derivative"}
QUERY_ROLES = {"canonical", "edit_source", "none"}


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


def load_hydrated_seeds(seeds_path: Path, sources_path: Path | None = None) -> List[Dict[str, Any]]:
    sources_path = sources_path or seeds_path.with_name("sources.yaml")
    catalogue = load_source_catalog(sources_path)
    payload = yaml.safe_load(seeds_path.read_text(encoding="utf-8-sig"))
    if not isinstance(payload, dict) or not isinstance(payload.get("kgs"), list):
        raise ValueError("seeds.yaml must contain a top-level 'kgs' list")

    hydrated: List[Dict[str, Any]] = []
    for index, raw in enumerate(payload["kgs"]):
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
    repository_root = roots[0].parent if roots else Path.cwd()
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
