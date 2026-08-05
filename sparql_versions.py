"""Resolve immutable source SPARQL and append-only edited versions."""
from __future__ import annotations

import hashlib
from typing import Any, Dict, List, Mapping, Union


VersionSelector = Union[int, str]


class SparqlVersionError(ValueError):
    """Raised when a query record has invalid or unresolvable SPARQL versions."""


def sparql_hash(sparql: str) -> str:
    if not isinstance(sparql, str) or not sparql.strip():
        raise SparqlVersionError("SPARQL text must be a non-empty string")
    return "sha256:" + hashlib.sha256(sparql.encode("utf-8")).hexdigest()


def available_sparql_versions(record: Mapping[str, Any]) -> List[Dict[str, Any]]:
    """Return every retained version, with version 0 representing the source query."""
    original = record.get("sparql_clean")
    if not isinstance(original, str) or not original.strip():
        raise SparqlVersionError("Query record is missing non-empty sparql_clean")

    original_hash = sparql_hash(original)
    stored_original_hash = record.get("sparql_hash")
    if stored_original_hash is not None and stored_original_hash != original_hash:
        raise SparqlVersionError(
            f"Version 0 hash mismatch: stored {stored_original_hash!r}, computed {original_hash!r}"
        )

    versions: List[Dict[str, Any]] = [
        {
            "sparql_version": 0,
            "sparql": original,
            "sparql_hash": original_hash,
            "note": None,
        }
    ]
    edits = record.get("sparql_edits") or []
    if not isinstance(edits, list):
        raise SparqlVersionError("sparql_edits must be a list")

    for expected_version, edit in enumerate(edits, start=1):
        if not isinstance(edit, Mapping):
            raise SparqlVersionError(f"SPARQL edit {expected_version} must be an object")
        version = edit.get("version")
        if version != expected_version:
            raise SparqlVersionError(
                f"SPARQL edit versions must be contiguous and append-only; expected {expected_version}, found {version!r}"
            )
        text = edit.get("sparql")
        if not isinstance(text, str) or not text.strip():
            raise SparqlVersionError(f"SPARQL edit version {version} has no query text")
        note = edit.get("note")
        if not isinstance(note, str) or not note.strip():
            raise SparqlVersionError(f"SPARQL edit version {version} requires a non-empty note")
        source_id = edit.get("source_id")
        if source_id is not None and (not isinstance(source_id, str) or not source_id.strip()):
            raise SparqlVersionError(
                f"SPARQL edit version {version} has an invalid source_id"
            )
        evidence_ids = edit.get("evidence_ids")
        if evidence_ids is not None and (
            not isinstance(evidence_ids, list)
            or not all(isinstance(item, str) and item.strip() for item in evidence_ids)
        ):
            raise SparqlVersionError(
                f"SPARQL edit version {version} has invalid evidence_ids"
            )
        provenance = edit.get("provenance")
        if provenance is not None and not isinstance(provenance, Mapping):
            raise SparqlVersionError(
                f"SPARQL edit version {version} has invalid provenance"
            )
        resolved_edit = {
                "sparql_version": version,
                "sparql": text,
                "sparql_hash": sparql_hash(text),
                "note": note,
                "source_id": source_id,
                "edit_type": edit.get("edit_type"),
                "evidence_ids": list(evidence_ids or []),
                "provenance": dict(provenance or {}),
            }
        versions.append(resolved_edit)
    return versions


def _selector_version(selector: VersionSelector, latest: int) -> int:
    if isinstance(selector, int):
        return selector
    normalized = str(selector).strip().lower()
    if normalized in {"original", "source"}:
        return 0
    if normalized == "latest":
        return latest
    if normalized.isdigit():
        return int(normalized)
    raise SparqlVersionError(f"Unsupported SPARQL version selector: {selector!r}")


def resolve_sparql_version(
    record: Mapping[str, Any], selector: VersionSelector = "latest"
) -> Dict[str, Any]:
    versions = available_sparql_versions(record)
    selected_version = _selector_version(selector, versions[-1]["sparql_version"])
    for version in versions:
        if version["sparql_version"] == selected_version:
            return dict(version)
    raise SparqlVersionError(
        f"SPARQL version {selected_version} is unavailable; retained versions are "
        + ", ".join(str(item["sparql_version"]) for item in versions)
    )


def select_sparql_versions(
    record: Mapping[str, Any], selector: VersionSelector = "latest"
) -> List[Dict[str, Any]]:
    if isinstance(selector, str) and selector.strip().lower() == "all":
        return available_sparql_versions(record)
    return [resolve_sparql_version(record, selector)]


def add_execution_version(
    execution: Mapping[str, Any], resolved: Mapping[str, Any]
) -> Dict[str, Any]:
    """Return an execution entry linked to one retained SPARQL version."""
    result = dict(execution)
    result["sparql_version"] = resolved["sparql_version"]
    result["sparql_hash"] = resolved["sparql_hash"]
    return result


def backfill_legacy_execution_versions(record: Dict[str, Any]) -> None:
    """Annotate legacy versionless execution entries as source-version executions."""
    versions = available_sparql_versions(record)
    by_version = {item["sparql_version"]: item for item in versions}
    by_hash: Dict[str, List[Dict[str, Any]]] = {}
    for item in versions:
        by_hash.setdefault(item["sparql_hash"], []).append(item)

    def backfill(execution: Dict[str, Any]) -> None:
        version = execution.get("sparql_version")
        digest = execution.get("sparql_hash")
        if version is None and digest is None:
            execution["sparql_version"] = 0
            execution["sparql_hash"] = by_version[0]["sparql_hash"]
            return
        if version is not None and not isinstance(version, int):
            raise SparqlVersionError(f"Execution has invalid sparql_version {version!r}")
        if digest is not None and not isinstance(digest, str):
            raise SparqlVersionError(f"Execution has invalid sparql_hash {digest!r}")
        if isinstance(version, int) and digest is None:
            if version not in by_version:
                raise SparqlVersionError(f"Execution refers to unavailable SPARQL version {version}")
            execution["sparql_hash"] = by_version[version]["sparql_hash"]
            return
        if version is None and isinstance(digest, str):
            matches = by_hash.get(digest, [])
            if len(matches) != 1:
                raise SparqlVersionError(
                    f"Execution hash {digest!r} does not identify exactly one retained SPARQL version"
                )
            execution["sparql_version"] = matches[0]["sparql_version"]
            return
        if by_version.get(version, {}).get("sparql_hash") != digest:
            raise SparqlVersionError(
                f"Execution SPARQL version/hash do not resolve: version={version!r}, hash={digest!r}"
            )

    seen: set[int] = set()
    for field in (
        "latest_execution",
        "latest_successful_execution",
        "latest_run",
        "latest_successful_run",
    ):
        value = record.get(field)
        if not isinstance(value, dict) or id(value) in seen:
            continue
        seen.add(id(value))
        backfill(value)
    for field in ("execution_history", "run_history"):
        history = record.get(field)
        if not isinstance(history, list) or id(history) in seen:
            continue
        seen.add(id(history))
        for execution in history:
            if not isinstance(execution, dict):
                continue
            backfill(execution)


def validate_execution_versions(record: Mapping[str, Any]) -> None:
    """Raise when any retained execution fails to resolve to its version text."""
    for field in (
        "latest_execution",
        "latest_successful_execution",
        "latest_run",
        "latest_successful_run",
    ):
        execution = record.get(field)
        if isinstance(execution, Mapping) and not execution_resolves(record, execution):
            raise SparqlVersionError(f"{field} does not resolve to retained SPARQL")
    for field in ("execution_history", "run_history"):
        history = record.get(field)
        if not isinstance(history, list):
            continue
        for index, execution in enumerate(history):
            if isinstance(execution, Mapping) and not execution_resolves(record, execution):
                raise SparqlVersionError(
                    f"{field}[{index}] does not resolve to retained SPARQL"
                )


def execution_resolves(record: Mapping[str, Any], execution: Mapping[str, Any]) -> bool:
    """Return whether an execution points to the retained text for its version."""
    version = execution.get("sparql_version")
    digest = execution.get("sparql_hash")
    if not isinstance(version, int) or not isinstance(digest, str):
        return False
    try:
        resolved = resolve_sparql_version(record, version)
    except SparqlVersionError:
        return False
    return resolved["sparql_hash"] == digest
