from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import enrich_evidence


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

    def test_extract_query_terms_resolves_curie_roles(self) -> None:
        query = """
PREFIX mtp: <http://w3id.org/polifonia/ontology/meetups-ontology#>
SELECT ?participant WHERE {
  ?meetup a mtp:Meetup ;
    mtp:hasParticipant ?participant .
}
"""
        terms = enrich_evidence.extract_query_terms(query)
        self.assertIn("http://w3id.org/polifonia/ontology/meetups-ontology#Meetup", terms)
        self.assertIn("http://w3id.org/polifonia/ontology/meetups-ontology#hasParticipant", terms)
        self.assertIn(
            "class",
            terms["http://w3id.org/polifonia/ontology/meetups-ontology#Meetup"]["roles"],
        )
        self.assertIn(
            "predicate",
            terms["http://w3id.org/polifonia/ontology/meetups-ontology#hasParticipant"]["roles"],
        )

    def test_build_ontology_context_snippet_uses_labels_comments_domain_range(self) -> None:
        try:
            from rdflib import Graph, Namespace, RDF, RDFS, OWL, Literal
        except Exception as exc:  # pragma: no cover - environment guard
            self.skipTest(f"rdflib unavailable: {exc}")
        mtp = Namespace("http://w3id.org/polifonia/ontology/meetups-ontology#")
        graph = Graph()
        graph.add((mtp.Meetup, RDF.type, OWL.Class))
        graph.add((mtp.Meetup, RDFS.label, Literal("Meetup")))
        graph.add((mtp.Meetup, RDFS.comment, Literal("A meeting or musical encounter.")))
        graph.add((mtp.hasParticipant, RDF.type, OWL.ObjectProperty))
        graph.add((mtp.hasParticipant, RDFS.label, Literal("has participant")))
        graph.add((mtp.hasParticipant, RDFS.domain, mtp.Meetup))
        graph.add((mtp.hasParticipant, RDFS.range, mtp.Participant))
        query = """
PREFIX mtp: <http://w3id.org/polifonia/ontology/meetups-ontology#>
SELECT ?participant WHERE { ?meetup a mtp:Meetup ; mtp:hasParticipant ?participant . }
"""
        snippet = enrich_evidence.build_ontology_context_snippet(query, graph, ["local ontology"])
        self.assertIn("mtp:Meetup", snippet)
        self.assertIn("A meeting or musical encounter", snippet)
        self.assertIn("domain: Meetup", snippet)
        self.assertIn("range: mtp:Participant", snippet)

    def test_build_graph_shape_context_snippet_summarizes_local_graph(self) -> None:
        try:
            from rdflib import Graph, Namespace, RDF, RDFS, Literal
        except Exception as exc:  # pragma: no cover - environment guard
            self.skipTest(f"rdflib unavailable: {exc}")
        mtp = Namespace("http://w3id.org/polifonia/ontology/meetups-ontology#")
        ex = Namespace("http://example.org/")
        graph = Graph()
        graph.add((ex.m1, RDF.type, mtp.Meetup))
        graph.add((ex.m1, mtp.hasParticipant, ex.p1))
        graph.add((ex.m1, RDFS.label, Literal("A meetup")))
        graph.add((ex.p1, RDF.type, mtp.Participant))
        query = """
PREFIX mtp: <http://w3id.org/polifonia/ontology/meetups-ontology#>
SELECT ?participant WHERE { ?meetup a mtp:Meetup ; mtp:hasParticipant ?participant . }
"""
        snippet = enrich_evidence.build_graph_shape_context_snippet(query, graph)
        self.assertIn("mtp:Meetup", snippet)
        self.assertIn("common outgoing predicates: mtp:hasParticipant", snippet)
        self.assertIn("object shapes: mtp:Participant", snippet)

    def test_load_seed_ontology_sources_allows_multiple_sources(self) -> None:
        with TemporaryDirectory() as tmp:
            seed_path = Path(tmp) / "seeds.yaml"
            seed_path.write_text(
                """
kgs:
  - kg_id: meetups
    ontology_sources:
      - url: https://w3id.org/polifonia/ontology/meetups-ontology
        format: turtle
      - local_path: ontologies/core.ttl
        format: turtle
""",
                encoding="utf-8",
            )
            sources = enrich_evidence.load_seed_ontology_sources(seed_path)
        self.assertEqual(len(sources["meetups"]), 2)
        self.assertEqual(sources["meetups"][0]["format"], "turtle")

    def test_parse_rdf_file_falls_back_when_declared_format_is_wrong(self) -> None:
        try:
            from rdflib import Graph
        except Exception as exc:  # pragma: no cover - environment guard
            self.skipTest(f"rdflib unavailable: {exc}")
        with TemporaryDirectory() as tmp:
            ontology_path = Path(tmp) / "ontology.owl"
            ontology_path.write_text(
                """
@prefix ex: <http://example.org/> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
ex:Thing a owl:Class .
""",
                encoding="utf-8",
            )
            graph = Graph()
            self.assertTrue(enrich_evidence.parse_rdf_file(graph, ontology_path, "xml"))
            self.assertGreater(len(graph), 0)


if __name__ == "__main__":
    unittest.main()
