from __future__ import annotations

import unittest

from scripts import enrich_evidence


class EnrichEvidenceTests(unittest.TestCase):
    def test_extracts_preceding_line_comments(self) -> None:
        lines = [
            "# Find all tracks for an album",
            "# ordered by track number",
            "SELECT ?track WHERE {",
            "  ?album ?p ?track .",
            "}",
        ]
        self.assertEqual(
            enrich_evidence.extract_preceding_comments(lines, 2),
            "Find all tracks for an album ordered by track number",
        )

    def test_markdown_block_keeps_nearest_description(self) -> None:
        text = """
This query finds the labels for the given organs.

```sparql
SELECT ?label WHERE {
  ?organ rdfs:label ?label .
}
```
"""
        blocks = enrich_evidence.extract_md_blocks_with_desc(text)
        self.assertEqual(len(blocks), 1)
        self.assertEqual(blocks[0]["desc"], "This query finds the labels for the given organs.")

    def test_html_pre_block_uses_nearest_bullet_description(self) -> None:
        text = """
<ul><li>Find the current location for an organ.</li></ul>
<pre>SELECT ?place WHERE { ?organ ?p ?place . }</pre>
"""
        blocks = enrich_evidence.extract_pre_blocks_with_desc(text)
        self.assertEqual(len(blocks), 1)
        self.assertEqual(blocks[0]["desc"], "Find the current location for an organ.")

    def test_extracts_competency_questions_from_table(self) -> None:
        text = """
| ID | Competency question |
| --- | --- |
| CQ1 | Which bands played a given tune? |
| CQ2 | Which instruments did a performer play? |
"""
        items = enrich_evidence.extract_cq_items_from_text(text)
        self.assertEqual(
            items,
            [
                "CQ1 | Which bands played a given tune?",
                "CQ2 | Which instruments did a performer play?",
            ],
        )

    def test_extracts_competency_questions_from_bullets(self) -> None:
        text = """
- Which places did a musician visit?
- Which people met in Paris?
"""
        self.assertEqual(
            enrich_evidence.extract_cq_items_from_text(text),
            ["Which places did a musician visit?", "Which people met in Paris?"],
        )

    def test_add_evidence_deduplicates_same_snippet(self) -> None:
        record: dict[str, object] = {"evidence": []}
        for _ in range(2):
            enrich_evidence.add_evidence(
                record,
                "cq_item",
                "source",
                "path.md",
                "",
                "CQ1 Which bands played a given tune?",
                "2026-01-01T00:00:00+00:00",
            )
        self.assertEqual(len(record["evidence"]), 1)

    def test_clean_desc_strips_sparql_lines(self) -> None:
        desc = """
Find labels for organs.
SELECT ?label WHERE {
?organ rdfs:label ?label .
}
```
"""
        self.assertEqual(enrich_evidence.clean_desc(desc), "Find labels for organs.")


if __name__ == "__main__":
    unittest.main()
