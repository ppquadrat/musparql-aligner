from __future__ import annotations

import pytest
from rdflib.plugins.sparql.parser import parseQuery

from run_queries import (
    apply_graph,
    build_query_jobs,
    clean_query,
    failure_matches_job,
    is_remote_executable,
    non_executable_reason,
    record_matches_sources,
    record_query_execution,
)
from sparql_versions import sparql_hash


def versioned_record():
    original = "SELECT * WHERE { ?s ?p ?o }"
    edit = "SELECT DISTINCT ?s WHERE { ?s ?p ?o }"
    return {
        "query_id": "q1",
        "sparql_clean": original,
        "sparql_hash": sparql_hash(original),
        "sparql_edits": [{"version": 1, "sparql": edit, "note": "Select subjects."}],
        "execution_history": [],
    }


def test_build_query_jobs_selects_all_retained_versions():
    jobs = build_query_jobs([versioned_record()], "all")
    assert [(resolved["sparql_version"], resolved["sparql_hash"]) for _, resolved in jobs] == [
        (0, sparql_hash("SELECT * WHERE { ?s ?p ?o }")),
        (1, sparql_hash("SELECT DISTINCT ?s WHERE { ?s ?p ?o }")),
    ]


def test_record_execution_requires_version_link():
    with pytest.raises(ValueError, match="must identify"):
        record_query_execution(versioned_record(), {"status": "ok"})


def test_record_execution_preserves_version_link_in_aliases():
    record = versioned_record()
    execution = {
        "status": "ok",
        "sparql_version": 1,
        "sparql_hash": sparql_hash("SELECT DISTINCT ?s WHERE { ?s ?p ?o }"),
    }
    record_query_execution(record, execution)
    assert record["latest_execution"] is execution
    assert record["latest_run"] is execution
    assert record["execution_history"][-1]["sparql_version"] == 1
    assert record["run_history"] is record["execution_history"]


def test_skipped_observation_does_not_replace_latest_success():
    record = versioned_record()
    success = {
        "status": "ok",
        "sparql_version": 1,
        "sparql_hash": sparql_hash("SELECT DISTINCT ?s WHERE { ?s ?p ?o }"),
    }
    record["latest_successful_execution"] = success
    skipped = {**success, "status": "skipped_endpoint_unavailable"}
    record_query_execution(record, skipped)
    assert record["latest_execution"] is skipped
    assert record["latest_successful_execution"] is success


def test_source_filter_matches_edit_provenance():
    record = versioned_record()
    record["sparql_edits"][0]["source_id"] = "corrected-source"
    assert record_matches_sources(record, {"corrected-source"})


def test_versionless_failure_is_source_version_zero():
    legacy = {"kg_id": "kg", "query_id": "q1", "status": "query_error"}
    assert failure_matches_job(legacy, {("kg", "q1", 0)})
    assert not failure_matches_job(legacy, {("kg", "q1", 1)})


def test_unused_facade_x_prefix_does_not_make_query_local_only():
    query = """PREFIX fx: <http://sparql.xyz/facade-x/ns/>
SELECT * WHERE { ?s ?p ?o }"""
    assert is_remote_executable(query)
    assert not is_remote_executable(
        query.replace("?s ?p ?o", "SERVICE <x-sparql-anything:> { fx:properties fx:location 'x.csv' }")
    )


def test_printf_query_templates_are_not_executed_as_literal_sparql():
    assert non_executable_reason('SELECT * WHERE { ?s rdfs:label "%s" }') == "parameterized_template"
    assert non_executable_reason("SELECT ?s\n## WHERE { <%s> ?p ?o }") == "parameterized_template"
    assert non_executable_reason("SELECT * WHERE { wd:{qid} ?p ?o }") == "parameterized_template"


def test_effective_query_can_be_audited_separately_from_retained_text():
    retained = "# comment\nSELECT * WHERE { ?s ?p ?o }\n"
    effective = apply_graph(clean_query(retained), "https://example.org/graph")
    assert effective != retained
    assert sparql_hash(effective) != sparql_hash(retained)
    assert "SELECT * FROM <https://example.org/graph>\nWHERE" in effective
    parseQuery(effective)
