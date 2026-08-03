import json
from pathlib import Path

from build_review_bundle import previous_review_matches
from evals.evaluate_runs import resolve_run_query_id
from sparql_versions import sparql_hash
from sparql_versions import resolve_sparql_version


def test_previous_review_requires_same_versioned_sparql():
    old = "SELECT * WHERE { ?s ?p ?o }"
    new = "SELECT DISTINCT ?s WHERE { ?s ?p ?o }"
    previous = {"sparql_version": 0, "sparql_hash": sparql_hash(old), "sparql": old}
    current = {"sparql_version": 1, "sparql_hash": sparql_hash(new), "sparql_clean": new}
    assert not previous_review_matches(previous, current)


def test_historical_review_without_version_metadata_matches_by_text():
    query = "SELECT * WHERE { ?s ?p ?o }"
    assert previous_review_matches({"sparql": query}, {"sparql_clean": query})


def test_legacy_benchmark_id_resolves_by_unique_sparql():
    query = "SELECT * WHERE { ?s ?p ?o }"
    run = {"inputs": {"new-id": {"kg_id": "kg", "sparql_clean": query}}}
    assert resolve_run_query_id(run, {"query_id": "old-id", "kg_id": "kg", "sparql": query}) == ("new-id", True)


def test_legacy_benchmark_id_does_not_resolve_ambiguous_sparql():
    query = "SELECT * WHERE { ?s ?p ?o }"
    run = {"inputs": {"a": {"kg_id": "kg", "sparql_clean": query}, "b": {"kg_id": "kg", "sparql_clean": query}}}
    assert resolve_run_query_id(run, {"query_id": "old-id", "kg_id": "kg", "sparql": query}) == ("old-id", False)


def test_legacy_alias_never_crosses_kg_boundary():
    query = "SELECT * WHERE { ?s ?p ?o }"
    run = {"inputs": {"other": {"kg_id": "other-kg", "sparql_clean": query}}}
    assert resolve_run_query_id(run, {"query_id": "old", "kg_id": "kg", "sparql": query}) == ("old", False)


def test_all_linkedmusic_v7_ids_resolve_to_current_version_one():
    root = Path(__file__).resolve().parents[1]
    current = [json.loads(line) for line in (root / "kg_queries.jsonl").read_text().splitlines()]
    inputs = {
        record["query_id"]: {
            "kg_id": "linkedmusic",
            "sparql_clean": resolve_sparql_version(record, 1)["sparql"],
        }
        for record in current
        if record.get("kg_id") == "linkedmusic" and record.get("sparql_edits")
    }
    legacy = [
        json.loads(line)
        for line in (root / "benchmark/v7/benchmark.jsonl").read_text().splitlines()
        if '"kg_id": "linkedmusic"' in line
    ]
    assert len(legacy) == 20
    assert all(resolve_run_query_id({"inputs": inputs}, item)[1] for item in legacy)
