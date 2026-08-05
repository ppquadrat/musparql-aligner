"""Durable, public-safe archive for human-approved SPARQL versions."""
from __future__ import annotations

from copy import deepcopy
from typing import Any, Mapping, Sequence

from musparql.sparql_versions import resolve_sparql_version, sparql_hash


APPROVED_EDIT_SCHEMA = "musparql.approved-sparql-edit.v1"
ALLOWED_FIELDS = {
    "schema",
    "kg_id",
    "query_id",
    "query_label",
    "base_sparql_hash",
    "version",
    "sparql",
    "sparql_hash",
    "note",
    "source_id",
    "edit_type",
    "evidence_ids",
    "provenance",
}


def _validated_archive_edit(row: Mapping[str, Any]) -> dict[str, Any]:
    if row.get("schema") != APPROVED_EDIT_SCHEMA:
        raise ValueError("Unsupported approved SPARQL edit schema")
    unknown = sorted(set(row) - ALLOWED_FIELDS)
    if unknown:
        raise ValueError(f"Unknown approved SPARQL edit fields: {unknown}")
    for field in ("kg_id", "query_id", "base_sparql_hash", "sparql", "sparql_hash", "note"):
        if not isinstance(row.get(field), str) or not str(row[field]).strip():
            raise ValueError(f"Approved SPARQL edit requires {field}")
    version = row.get("version")
    if isinstance(version, bool) or not isinstance(version, int) or version < 1:
        raise ValueError("Approved SPARQL edit requires a positive integer version")
    if sparql_hash(str(row["sparql"])) != row["sparql_hash"]:
        raise ValueError(f"Approved SPARQL edit hash mismatch for {row['kg_id']}/{row['query_id']} v{version}")
    evidence_ids = row.get("evidence_ids") or []
    if not isinstance(evidence_ids, list) or not all(
        isinstance(item, str) and item.strip() for item in evidence_ids
    ):
        raise ValueError("Approved SPARQL edit evidence_ids must be strings")
    provenance = row.get("provenance") or {}
    if not isinstance(provenance, Mapping):
        raise ValueError("Approved SPARQL edit provenance must be an object")
    return {
        "version": version,
        "sparql": str(row["sparql"]),
        "note": str(row["note"]),
        "source_id": row.get("source_id"),
        "edit_type": row.get("edit_type"),
        "evidence_ids": list(evidence_ids),
        "provenance": deepcopy(dict(provenance)),
    }


def restore_approved_edits(
    records: list[dict[str, Any]], archive_rows: Sequence[Mapping[str, Any]]
) -> int:
    """Restore missing archived edits while preserving richer matching local state."""
    by_key = {
        (str(record.get("kg_id") or ""), str(record.get("query_id") or "")): record
        for record in records
    }
    seen: set[tuple[str, str, int]] = set()
    restored = 0
    ordered = sorted(
        archive_rows,
        key=lambda row: (
            str(row.get("kg_id") or ""),
            str(row.get("query_id") or ""),
            int(row.get("version") or 0),
        ),
    )
    for row in ordered:
        edit = _validated_archive_edit(row)
        kg_id, query_id, version = str(row["kg_id"]), str(row["query_id"]), int(row["version"])
        identity = (kg_id, query_id, version)
        if identity in seen:
            raise ValueError(f"Duplicate approved SPARQL edit: {kg_id}/{query_id} v{version}")
        seen.add(identity)
        record = by_key.get((kg_id, query_id))
        if record is None:
            raise ValueError(f"Approved SPARQL edit does not resolve: {kg_id}/{query_id}")
        if row.get("query_label") not in (None, record.get("query_label")):
            raise ValueError(f"Approved SPARQL edit label mismatch for {kg_id}/{query_id}")
        base = resolve_sparql_version(record, version - 1)
        if base["sparql_hash"] != row["base_sparql_hash"]:
            raise ValueError(f"Approved SPARQL edit base hash mismatch for {kg_id}/{query_id} v{version}")
        edits = record.setdefault("sparql_edits", [])
        if not isinstance(edits, list):
            raise ValueError("sparql_edits must be a list")
        if len(edits) >= version:
            existing = resolve_sparql_version(record, version)
            if existing["sparql_hash"] != row["sparql_hash"]:
                raise ValueError(f"Archived and local SPARQL edits diverge for {kg_id}/{query_id} v{version}")
            continue
        if len(edits) != version - 1:
            raise ValueError(f"Approved SPARQL edits are not contiguous for {kg_id}/{query_id}")
        edits.append(edit)
        restored += 1
    return restored


def archive_rows_from_records(records: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Project retained edits into a stable archive without raw review metadata."""
    rows: list[dict[str, Any]] = []
    for record in records:
        previous = resolve_sparql_version(record, 0)
        for edit in record.get("sparql_edits") or []:
            version = int(edit["version"])
            resolved = resolve_sparql_version(record, version)
            provenance = edit.get("provenance") if isinstance(edit.get("provenance"), Mapping) else {}
            safe_provenance = {
                key: deepcopy(provenance[key])
                for key in ("approval_source", "benchmark_version", "reconstructed_from")
                if provenance.get(key) is not None
            }
            if not safe_provenance:
                safe_provenance = {"approval_source": "human_sparql_correction_review"}
            rows.append(
                {
                    "schema": APPROVED_EDIT_SCHEMA,
                    "kg_id": record["kg_id"],
                    "query_id": record["query_id"],
                    "query_label": record.get("query_label"),
                    "base_sparql_hash": previous["sparql_hash"],
                    "version": version,
                    "sparql": resolved["sparql"],
                    "sparql_hash": resolved["sparql_hash"],
                    "note": str(edit["note"]),
                    "source_id": edit.get("source_id"),
                    "edit_type": edit.get("edit_type"),
                    "evidence_ids": list(edit.get("evidence_ids") or []),
                    "provenance": safe_provenance,
                }
            )
            previous = resolved
    return sorted(rows, key=lambda row: (row["kg_id"], row["query_id"], row["version"]))
