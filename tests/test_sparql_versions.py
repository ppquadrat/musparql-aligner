from __future__ import annotations

import pytest

from build_llm_inputs import build_prompt_input, dismissed_record_matches, load_exclusion_policy
from sparql_versions import (
    SparqlVersionError,
    add_execution_version,
    available_sparql_versions,
    backfill_legacy_execution_versions,
    execution_resolves,
    resolve_sparql_version,
    select_sparql_versions,
    sparql_hash,
    validate_execution_versions,
)


ORIGINAL = "SELECT * WHERE { ?s ?p ?o }"
EDIT_1 = "SELECT DISTINCT ?s WHERE { ?s ?p ?o }"
EDIT_2 = "SELECT DISTINCT ?s WHERE { GRAPH ?g { ?s ?p ?o } }"


def record_with_edits():
    return {
        "sparql_clean": ORIGINAL,
        "sparql_hash": sparql_hash(ORIGINAL),
        "sparql_edits": [
            {"version": 1, "sparql": EDIT_1, "note": "Select only subjects."},
            {"version": 2, "sparql": EDIT_2, "note": "Search named graphs."},
        ],
    }


def test_original_is_implicit_version_zero():
    resolved = resolve_sparql_version(record_with_edits(), "original")
    assert resolved == {
        "sparql_version": 0,
        "sparql": ORIGINAL,
        "sparql_hash": sparql_hash(ORIGINAL),
        "note": None,
    }


def test_latest_and_exact_versions_resolve():
    record = record_with_edits()
    assert resolve_sparql_version(record)["sparql_version"] == 2
    assert resolve_sparql_version(record, "1")["sparql"] == EDIT_1
    assert [item["sparql_version"] for item in select_sparql_versions(record, "all")] == [0, 1, 2]


def test_edits_must_be_contiguous_and_documented():
    record = record_with_edits()
    record["sparql_edits"][0]["version"] = 2
    with pytest.raises(SparqlVersionError, match="contiguous"):
        available_sparql_versions(record)

    record = record_with_edits()
    record["sparql_edits"][0]["note"] = ""
    with pytest.raises(SparqlVersionError, match="requires a non-empty note"):
        available_sparql_versions(record)

    record = record_with_edits()
    record["sparql_edits"][0]["source_id"] = ""
    with pytest.raises(SparqlVersionError, match="invalid source_id"):
        available_sparql_versions(record)


def test_hash_mismatch_is_rejected():
    record = record_with_edits()
    record["sparql_hash"] = "sha256:wrong"
    with pytest.raises(SparqlVersionError, match="hash mismatch"):
        resolve_sparql_version(record)


def test_execution_is_linked_to_retained_version_and_hash():
    record = record_with_edits()
    resolved = resolve_sparql_version(record, 1)
    execution = add_execution_version({"status": "ok"}, resolved)
    assert execution == {
        "status": "ok",
        "sparql_version": 1,
        "sparql_hash": sparql_hash(EDIT_1),
    }
    assert execution_resolves(record, execution)


def test_legacy_executions_are_backfilled_as_version_zero():
    shared = {"status": "ok"}
    history = [shared]
    record = {
        "sparql_clean": ORIGINAL,
        "sparql_hash": sparql_hash(ORIGINAL),
        "latest_execution": shared,
        "latest_run": shared,
        "execution_history": history,
        "run_history": history,
    }
    backfill_legacy_execution_versions(record)
    assert shared["sparql_version"] == 0
    assert shared["sparql_hash"] == sparql_hash(ORIGINAL)
    assert execution_resolves(record, shared)


def test_prompt_input_records_selected_version_and_hash():
    record = record_with_edits()
    record.update({"query_id": "q1", "query_label": "kg-0001", "kg_id": "kg", "evidence": []})
    payload = build_prompt_input(record, include_raw=False, include_sparql_blocks=False)
    assert payload["sparql_clean"] == EDIT_2
    assert payload["sparql_version"] == 2
    assert payload["sparql_hash"] == sparql_hash(EDIT_2)


def test_dismissal_only_applies_to_matching_sparql_version():
    record = record_with_edits()
    record.update({"query_id": "q1", "query_label": "kg-0001", "kg_id": "kg", "evidence": []})
    original = build_prompt_input(record, False, False, "original")
    latest = build_prompt_input(record, False, False, "latest")
    dismissed = {
        "query_id": "q1",
        "sparql_version": original["sparql_version"],
        "sparql_hash": original["sparql_hash"],
    }
    assert dismissed_record_matches(original, dismissed)
    assert not dismissed_record_matches(latest, dismissed)


def test_standalone_holdout_file_is_always_id_scoped(tmp_path):
    path = tmp_path / "holdout.jsonl"
    path.write_text('{"query_id":"q1","sparql_version":0}\n', encoding="utf-8")
    dismissed, holdout_ids = load_exclusion_policy(path)
    assert dismissed == []
    assert holdout_ids == {"q1"}


def test_partial_legacy_execution_uses_its_declared_version():
    record = record_with_edits()
    record["execution_history"] = [{"status": "ok", "sparql_version": 1}]
    backfill_legacy_execution_versions(record)
    assert record["execution_history"][0]["sparql_hash"] == sparql_hash(EDIT_1)
    validate_execution_versions(record)


def test_mismatched_execution_link_is_rejected():
    record = record_with_edits()
    record["execution_history"] = [
        {"status": "ok", "sparql_version": 1, "sparql_hash": sparql_hash(ORIGINAL)}
    ]
    with pytest.raises(SparqlVersionError, match="do not resolve"):
        backfill_legacy_execution_versions(record)
