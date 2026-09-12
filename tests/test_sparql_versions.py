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


def test_prompt_input_retains_structured_authored_question_as_evidence():
    record = record_with_edits()
    record.update({
        "query_id": "q1",
        "query_label": "kg-0001",
        "kg_id": "kg",
        "nl_question": {
            "text": "Which subjects occur in the graph?",
            "source": "synthetic-curated-source",
            "generated_at": None,
            "generator": None,
        },
        "evidence": [{
            "evidence_id": "e1",
            "type": "curated_query",
            "snippet": ORIGINAL,
            "source_id": "synthetic-curated-source",
            "source_path": "synthetic/queries.jsonl",
            "source_url": "https://example.invalid/queries",
        }],
    })

    payload = build_prompt_input(record, False, False)

    assert payload["evidence"] == [{
        "evidence_id": "e2",
        "type": "curated_nl_question",
        "snippet": "Which subjects occur in the graph?",
        "source_id": "synthetic-curated-source",
        "source_path": "synthetic/queries.jsonl",
        "source_url": "https://example.invalid/queries",
    }]


def test_prompt_input_extracts_question_comment_without_structured_nl():
    query = "# Which subjects occur in the graph?\nSELECT ?s WHERE { ?s ?p ?o }"
    record = {
        "query_id": "q1",
        "query_label": "kg-0001",
        "kg_id": "kg",
        "sparql_clean": query,
        "sparql_hash": sparql_hash(query),
        "evidence": [{
            "evidence_id": "e1",
            "type": "repo_file",
            "snippet": query,
            "source_id": "synthetic-repository",
            "source_path": "queries/example.rq",
            "source_url": "https://example.invalid/repository",
        }],
    }

    payload = build_prompt_input(record, False, False)

    assert payload["evidence"] == [{
        "evidence_id": "e2",
        "type": "query_comment",
        "snippet": "Which subjects occur in the graph?",
        "source_id": "synthetic-repository",
        "source_path": "queries/example.rq",
        "source_url": "https://example.invalid/repository",
    }]


def test_prompt_input_deduplicates_matching_structured_nl_and_query_comment():
    query = "# Which subjects occur in the graph?\nSELECT ?s WHERE { ?s ?p ?o }"
    record = {
        "query_id": "q1", "query_label": "kg-0001", "kg_id": "kg",
        "sparql_clean": query, "sparql_hash": sparql_hash(query),
        "nl_question": {
            "text": "Which subjects occur in the graph?", "source": "synthetic-source",
            "generated_at": None, "generator": None,
        },
        "evidence": [{
            "evidence_id": "e1", "type": "curated_query", "snippet": query,
            "source_id": "synthetic-source",
        }],
    }

    payload = build_prompt_input(record, False, False)

    assert [item["type"] for item in payload["evidence"]] == ["curated_nl_question"]


def test_prompt_input_deduplicates_question_contained_in_existing_comment_evidence():
    query = "# Which subjects occur in the graph?\nSELECT ?s WHERE { ?s ?p ?o }"
    record = {
        "query_id": "q1", "query_label": "kg-0001", "kg_id": "kg",
        "sparql_clean": query, "sparql_hash": sparql_hash(query),
        "evidence": [{
            "evidence_id": "e1", "type": "query_comment",
            "snippet": "Example query\nWhich subjects occur in the graph?\nReturns every subject.",
            "source_id": "synthetic-source",
        }],
    }

    payload = build_prompt_input(record, False, False)

    assert [item["evidence_id"] for item in payload["evidence"]] == ["e1"]


@pytest.mark.parametrize("nl_question", [
    {"text": "Legacy generated question?"},
    {
        "text": "Question with an unknown source?", "source": "missing-source",
        "generated_at": None, "generator": None,
    },
])
def test_prompt_input_does_not_infer_authorship_from_incomplete_provenance(nl_question):
    record = {
        "query_id": "q1", "query_label": "kg-0001", "kg_id": "kg",
        "sparql_clean": ORIGINAL, "sparql_hash": sparql_hash(ORIGINAL),
        "nl_question": nl_question,
        "evidence": [],
    }

    payload = build_prompt_input(record, False, False)

    assert payload["evidence"] == []


def test_prompt_input_does_not_mix_unmatched_source_id_with_fallback_metadata():
    edited = "# Which edited subjects occur?\nSELECT DISTINCT ?s WHERE { ?s ?p ?o }"
    record = {
        "query_id": "q1", "query_label": "kg-0001", "kg_id": "kg",
        "sparql_clean": ORIGINAL, "sparql_hash": sparql_hash(ORIGINAL),
        "sparql_edits": [{
            "version": 1, "sparql": edited, "note": "Synthetic sourced edit.",
            "source_id": "missing-source",
        }],
        "evidence": [{
            "evidence_id": "e1", "type": "curated_query", "snippet": ORIGINAL,
            "source_id": "different-source", "source_path": "different/query.rq",
            "source_url": "https://example.invalid/different",
        }],
    }

    payload = build_prompt_input(record, False, False)

    assert payload["evidence"] == [{
        "evidence_id": "e2", "type": "query_comment",
        "snippet": "Which edited subjects occur?", "source_id": "missing-source",
        "source_path": "", "source_url": "",
    }]


def test_prompt_comment_uses_selected_edit_source_metadata():
    edited = "# Which edited subjects occur?\nSELECT DISTINCT ?s WHERE { ?s ?p ?o }"
    record = {
        "query_id": "q1", "query_label": "kg-0001", "kg_id": "kg",
        "sparql_clean": ORIGINAL, "sparql_hash": sparql_hash(ORIGINAL),
        "sparql_edits": [{
            "version": 1, "sparql": edited, "note": "Synthetic sourced edit.",
            "source_id": "edit-source",
        }],
        "evidence": [
            {
                "evidence_id": "e1", "type": "curated_query", "snippet": ORIGINAL,
                "source_id": "original-source", "source_path": "original/query.rq",
            },
            {
                "evidence_id": "e2", "type": "repo_file", "snippet": edited,
                "source_id": "edit-source", "source_path": "edits/query.rq",
            },
        ],
    }

    payload = build_prompt_input(record, False, False)

    assert payload["evidence"] == [{
        "evidence_id": "e3", "type": "query_comment",
        "snippet": "Which edited subjects occur?", "source_id": "edit-source",
        "source_path": "edits/query.rq", "source_url": "",
    }]


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
