from __future__ import annotations

import argparse
import pytest

from scripts.build_llm_inputs import build_prompt_input, dismissed_record_matches, load_holdout_selectors
from musparql.holdout_selectors import add_holdout_filter_arguments, holdout_input_policy
from musparql.sparql_versions import (
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
        "execution_history": [
            {"status": "ok", "ran_at": "2026-08-05T12:00:00+00:00", "sparql_version": 2, "sparql_hash": sparql_hash(EDIT_2)}
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
    assert payload["sparql_provenance"]["retained_edit_count"] == 2
    assert payload["sparql_provenance"]["selected_version"] == 2
    assert payload["sparql_provenance"]["selected_hash"] == sparql_hash(EDIT_2)
    assert payload["sparql_provenance"]["execution_observation"]["status"] == "ok"
    assert payload["sparql_provenance"]["history_digest"].startswith("sha256:")


def test_original_prompt_selection_still_reports_retained_edit_history():
    record = record_with_edits()
    record.update({"query_id": "q1", "query_label": "kg-0001", "kg_id": "kg", "evidence": []})
    payload = build_prompt_input(record, False, False, "original")
    assert payload["sparql_version"] == 0
    assert payload["sparql_provenance"]["retained_edit_count"] == 2


def test_dismissal_only_applies_to_matching_sparql_version():
    record = record_with_edits()
    record.update({"query_id": "q1", "query_label": "kg-0001", "kg_id": "kg", "evidence": []})
    original = build_prompt_input(record, False, False, "original")
    latest = build_prompt_input(record, False, False, "latest")
    dismissed = {
        "kg_id": "kg",
        "query_id": "q1",
        "sparql_version": original["sparql_version"],
        "sparql_hash": original["sparql_hash"],
    }
    assert dismissed_record_matches(original, dismissed)
    assert not dismissed_record_matches(latest, dismissed)


def test_holdout_selector_file_is_identity_scoped(tmp_path):
    path = tmp_path / "selectors.jsonl"
    path.write_text('{"kg_id":"kg","query_id":"q1"}\n', encoding="utf-8")
    assert load_holdout_selectors(path) == {("kg", "q1")}


def test_holdout_handling_choice_is_required_and_exclusive():
    parser = argparse.ArgumentParser()
    add_holdout_filter_arguments(parser)
    with pytest.raises(SystemExit):
        parser.parse_args([])
    with pytest.raises(SystemExit):
        parser.parse_args(["--no-holdout", "--holdout-filtered-upstream"])
    with pytest.raises(SystemExit):
        parser.parse_args(["--holdout-selectors", ""])
    assert holdout_input_policy(parser.parse_args(["--no-holdout"])) == "no_holdout"
    assert holdout_input_policy(parser.parse_args(["--holdout-filtered-upstream"])) == "identity_private_filtered_upstream"
    assert holdout_input_policy(parser.parse_args(["--holdout-selectors", "selectors.jsonl"])) == "identity_visible_selectors"


def test_holdout_selector_rejects_reviewer_fields(tmp_path):
    path = tmp_path / "selectors.jsonl"
    path.write_text('{"kg_id":"kg","query_id":"q1","internal_comment":"SYNTHETIC_CANARY"}\n', encoding="utf-8")
    with pytest.raises(ValueError, match="identity/version fields only"):
        load_holdout_selectors(path)


def test_holdout_selector_rejects_malformed_version_pin(tmp_path):
    path = tmp_path / "selectors.jsonl"
    path.write_text(
        '{"kg_id":"kg","query_id":"q1","sparql_version":1,'
        '"sparql_hash":"SYNTHETIC_ANNOTATION_NOT_A_DIGEST"}\n',
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="SHA-256"):
        load_holdout_selectors(path)


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
