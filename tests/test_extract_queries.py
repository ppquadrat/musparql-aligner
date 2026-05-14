from __future__ import annotations

import unittest

import extract_queries


class ExtractQueriesTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
