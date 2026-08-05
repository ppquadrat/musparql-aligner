from __future__ import annotations

from copy import deepcopy

import pytest

from musparql.approved_sparql_edits import (
    APPROVED_EDIT_SCHEMA,
    archive_rows_from_records,
    restore_approved_edits,
)
from musparql.sparql_versions import resolve_sparql_version, sparql_hash


ORIGINAL = "SELECT * WHERE { ?s ?p ?o }"
EDITED = "SELECT ?s WHERE { ?s ?p ?o }"


def query() -> dict:
    return {
        "kg_id": "synthetic-kg",
        "query_id": "synthetic-query",
        "query_label": "synthetic-0001",
        "sparql_clean": ORIGINAL,
        "sparql_hash": sparql_hash(ORIGINAL),
        "sparql_edits": [],
    }


def archive_row() -> dict:
    return {
        "schema": APPROVED_EDIT_SCHEMA,
        "kg_id": "synthetic-kg",
        "query_id": "synthetic-query",
        "query_label": "synthetic-0001",
        "base_sparql_hash": sparql_hash(ORIGINAL),
        "version": 1,
        "sparql": EDITED,
        "sparql_hash": sparql_hash(EDITED),
        "note": "Synthetic approved edit.",
        "source_id": "synthetic-source",
        "edit_type": "syntax_correction",
        "evidence_ids": [],
        "provenance": {"approval_source": "synthetic_human_review"},
    }


def test_archive_restores_edit_into_fresh_extraction() -> None:
    record = query()
    assert restore_approved_edits([record], [archive_row()]) == 1
    assert resolve_sparql_version(record, "latest")["sparql"] == EDITED
    assert restore_approved_edits([record], [archive_row()]) == 0


def test_archive_preserves_richer_matching_local_provenance() -> None:
    record = query()
    record["sparql_edits"] = [
        {
            "version": 1,
            "sparql": EDITED,
            "note": "Original detailed note.",
            "provenance": {"candidate_id": "synthetic-candidate"},
        }
    ]
    assert restore_approved_edits([record], [archive_row()]) == 0
    assert record["sparql_edits"][0]["note"] == "Original detailed note."


def test_archive_rejects_stale_or_divergent_edits() -> None:
    stale = archive_row()
    stale["base_sparql_hash"] = sparql_hash("SELECT * WHERE { ?x ?y ?z }")
    with pytest.raises(ValueError, match="base hash mismatch"):
        restore_approved_edits([query()], [stale])

    record = query()
    record["sparql_edits"] = [
        {"version": 1, "sparql": "SELECT ?o WHERE { ?s ?p ?o }", "note": "Different."}
    ]
    with pytest.raises(ValueError, match="diverge"):
        restore_approved_edits([record], [archive_row()])


def test_archive_projection_excludes_raw_review_provenance() -> None:
    record = query()
    restore_approved_edits([record], [archive_row()])
    record["sparql_edits"][0]["provenance"].update(
        {"review_export": "/private/local/review.json", "reviewer_note": "internal"}
    )
    projected = archive_rows_from_records([record])
    assert projected == [archive_row()]
    assert "review_export" not in projected[0]["provenance"]
