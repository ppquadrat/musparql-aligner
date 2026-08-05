from __future__ import annotations

from scripts.migrations.migrate_benchmark_v8 import CORRECTIONS, one_value_query, update_snapshot
from musparql.sparql_versions import resolve_sparql_version


def test_one_value_query_replaces_the_complete_values_body() -> None:
    source = "SELECT * WHERE { VALUES ?organ_query_iri { organs:A organs:B organs:C } }"
    result = one_value_query(source, "organs:B")
    assert "organs:A" not in result
    assert "organs:C" not in result
    assert result.count("organs:B") == 1


def test_snapshot_update_names_each_selected_organ() -> None:
    included = []
    queries = []
    for label, correction in CORRECTIONS.items():
        source = "SELECT * WHERE { VALUES ?organ_query_iri { organs:A } }"
        record = {
            "query_id": f"organs__{label}",
            "query_label": label,
            "sparql_clean": source,
            "sparql_hash": None,
            "sparql_edits": [],
        }
        if correction["value"] is not None:
            edited = one_value_query(source, str(correction["value"]))
            record["sparql_edits"] = [
                {"version": 1, "sparql": edited, "note": "Resolved value."}
            ]
        queries.append(record)
        included.append({"query_label": label, "review": {}})

    update_snapshot(included, queries, "fixed")

    for row in included:
        expected_version = 0 if row["query_label"] == "organs-0006" else 1
        assert row["sparql_version"] == expected_version
        assert row["gold_question"] == CORRECTIONS[row["query_label"]]["question"]
        assert "given organ" not in row["gold_question"].lower()
        assert row["sparql"] == resolve_sparql_version(
            next(item for item in queries if item["query_label"] == row["query_label"]),
            expected_version,
        )["sparql"]
