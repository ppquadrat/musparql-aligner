from __future__ import annotations

import unittest
from pathlib import Path

from scripts import extract_queries


class ExtractQueriesTests(unittest.TestCase):
    def test_repository_test_fixtures_are_identified_by_path(self) -> None:
        repo = Path("/tmp/example-repo")
        self.assertTrue(
            extract_queries.is_test_fixture_path(repo / "tests/test_validate.py", repo)
        )
        self.assertTrue(
            extract_queries.is_test_fixture_path(repo / "src/query_test.py", repo)
        )
        self.assertFalse(
            extract_queries.is_test_fixture_path(repo / "src/examples.py", repo)
        )

    def test_test_fixture_exclusion_is_configured_per_repository(self) -> None:
        seed = extract_queries.parse_kg_seed(
            {
                "kg_id": "synthetic",
                "repos": ["https://example.org/kept", "https://example.org/excluded"],
                "source_records": [
                    {
                        "type": "repository",
                        "url": "https://example.org/kept",
                        "source_id": "kept",
                    },
                    {
                        "type": "repository",
                        "url": "https://example.org/excluded",
                        "source_id": "excluded",
                        "exclude_test_fixtures": True,
                    },
                ],
            }
        )
        self.assertFalse(seed.repo_exclude_test_fixtures["https://example.org/kept"])
        self.assertTrue(seed.repo_exclude_test_fixtures["https://example.org/excluded"])

    def test_extracts_markdown_fenced_sparql(self) -> None:
        text = """
Example query:

```sparql
SELECT ?s WHERE {
  ?s a ?type .
}
```
"""
        blocks = extract_queries.extract_queries_from_md(text)
        self.assertEqual(len(blocks), 1)
        normalized = extract_queries.normalize_query(blocks[0])
        self.assertTrue(extract_queries.is_select_query(normalized))
        self.assertTrue(extract_queries.is_well_formed_query(normalized))

    def test_extracts_html_pre_sparql_with_entities(self) -> None:
        text = """
<p>Example</p>
<pre>
SELECT ?s WHERE {
  ?s &lt;http://example.org/p&gt; ?o .
}
</pre>
"""
        blocks = extract_queries.extract_queries_from_pre(text)
        self.assertEqual(len(blocks), 1)
        self.assertIn("<http://example.org/p>", blocks[0])
        self.assertTrue(extract_queries.is_well_formed_query(extract_queries.normalize_query(blocks[0])))

    def test_splits_multiple_queries_with_prefixes(self) -> None:
        text = """
PREFIX ex: <http://example.org/>

SELECT ?s WHERE {
  ?s ex:p ?o .
}

PREFIX ex: <http://example.org/>

SELECT ?o WHERE {
  ?s ex:p ?o .
}
"""
        queries = extract_queries.split_queries(text)
        self.assertEqual(len(queries), 2)
        self.assertTrue(all(extract_queries.is_select_query(q) for q in queries))

    def test_normalize_injects_common_prefix(self) -> None:
        normalized = extract_queries.normalize_query("SELECT ?s WHERE { ?s rdf:type ?type . }")
        self.assertTrue(normalized.startswith("PREFIX rdf:"))
        self.assertIn("rdf:type", normalized)

    def test_normalize_can_preserve_missing_source_prefixes(self) -> None:
        normalized = extract_queries.normalize_query(
            "SELECT ?s WHERE { GRAPH dtl: { ?s a dtl:Solo . } }",
            inject_missing_prefixes=False,
        )
        self.assertTrue(normalized.startswith("SELECT"))
        self.assertNotIn("PREFIX dtl:", normalized)

    def test_rejects_non_select_and_malformed_queries(self) -> None:
        self.assertFalse(extract_queries.is_select_query("ASK WHERE { ?s ?p ?o }"))
        malformed = "SELECT ?s WHERE { ?s ?p ?o ."
        self.assertFalse(extract_queries.is_well_formed_query(malformed))

    def test_extracts_pdf_like_query_with_broken_prefix_line(self) -> None:
        text = """
Figure 1. Example query.
PREFIX ex:
<http://example.org/>
SELECT ?s
WHERE {
  ?s ex:p ?o .
}
"""
        queries = extract_queries.extract_queries_from_pdf_text(text)
        normalized = [extract_queries.normalize_query(q) for q in queries]
        self.assertEqual(len(normalized), 1)
        self.assertIn("PREFIX ex: <http://example.org/>", normalized[0])
        self.assertTrue(extract_queries.is_well_formed_query(normalized[0]))

    def test_loads_curated_query_jsonl(self) -> None:
        from pathlib import Path
        from tempfile import TemporaryDirectory

        with TemporaryDirectory() as directory:
            path = Path(directory) / "queries.jsonl"
            path.write_text(
                '{"prompt":"Question?","sparql":"SELECT ?s WHERE { ?s ?p ?o . }"}\n',
                encoding="utf-8",
            )
            records = extract_queries.load_curated_query_records(path)
        self.assertEqual(records[0]["prompt"], "Question?")

    def test_curated_non_select_remains_an_error(self) -> None:
        from pathlib import Path
        from tempfile import TemporaryDirectory

        with TemporaryDirectory() as directory:
            path = Path(directory) / "queries.jsonl"
            path.write_text('{"sparql":"ASK { ?s ?p ?o }"}\n', encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "must be a nonempty SELECT"):
                extract_queries.prepare_curated_select_records(path)

    def test_retains_malformed_curated_select_without_stopping(self) -> None:
        from contextlib import redirect_stderr
        from io import StringIO
        from pathlib import Path
        from tempfile import TemporaryDirectory

        with TemporaryDirectory() as directory:
            path = Path(directory) / "queries.jsonl"
            path.write_text(
                '{"challenge":3,"cq":1,"sparql":"SELECT ?s WHERE { OPTIONAL { ?s ?p ?o . }"}\n'
                '{"challenge":3,"cq":2,"sparql":"SELECT ?s WHERE { ?s ?p ?o . }"}\n',
                encoding="utf-8",
            )
            stderr = StringIO()
            with redirect_stderr(stderr):
                prepared = extract_queries.prepare_curated_select_records(path)

        self.assertEqual(len(prepared), 2)
        self.assertEqual(prepared[0][0]["cq"], 1)
        self.assertFalse(extract_queries.is_well_formed_query(prepared[0][2]))
        self.assertEqual(prepared[0][3], "unbalanced_braces")
        self.assertEqual(prepared[1][0]["cq"], 2)
        self.assertTrue(extract_queries.is_well_formed_query(prepared[1][2]))
        self.assertIsNone(prepared[1][3])
        self.assertRegex(
            stderr.getvalue(),
            r"Retaining malformed curated SELECT query .* record 1 \(challenge=3, cq=1\)",
        )

        record = extract_queries.build_query_record(
            "kg",
            "kg-0001",
            "select",
            prepared[0][1],
            prepared[0][2],
            extract_queries.sha256_hash(prepared[0][1]),
            extract_queries.sha256_hash(prepared[0][2]),
        )
        extract_queries.add_extraction_diagnostic(
            record, prepared[0][3], record["sparql_hash"]
        )
        self.assertEqual(record["sparql_diagnostics"][0]["sparql_version"], 0)
        self.assertEqual(
            record["sparql_diagnostics"][0]["sparql_hash"], record["sparql_hash"]
        )

    def test_reextraction_preserves_version_state(self) -> None:
        original = "SELECT * WHERE { ?s ?p ?o }"
        digest = extract_queries.sha256_hash(original)
        record = extract_queries.build_query_record(
            "kg", "kg-0001", "select", original, original, digest, digest
        )
        previous = {
            "sparql_edits": [
                {"version": 1, "sparql": "SELECT ?s WHERE { ?s ?p ?o }", "note": "Edit."}
            ],
            "execution_history": [
                {"status": "ok", "sparql_version": 1, "sparql_hash": "sha256:test"}
            ],
        }
        extract_queries.preserve_version_state(record, previous)
        self.assertEqual(record["sparql_edits"], previous["sparql_edits"])
        self.assertEqual(record["execution_history"], previous["execution_history"])
        self.assertIsNot(record["execution_history"], previous["execution_history"])

    def test_stable_query_labels_survive_new_earlier_sources(self) -> None:
        old_hash = "sha256:old"
        existing = {
            f"kg__{old_hash}": {
                "kg_id": "kg",
                "query_id": f"kg__{old_hash}",
                "query_label": "kg-0007",
            }
        }
        counters = extract_queries.initialize_label_counters(existing)

        self.assertEqual(
            extract_queries.stable_query_label("kg", old_hash, existing, counters),
            "kg-0007",
        )
        self.assertEqual(
            extract_queries.stable_query_label("kg", "sha256:new", existing, counters),
            "kg-0008",
        )


if __name__ == "__main__":
    unittest.main()
