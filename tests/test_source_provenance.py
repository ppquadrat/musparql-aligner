from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from musparql.source_catalog import (
    load_hydrated_seeds,
    load_source_catalog,
    validate_catalogued_local_files,
)
from scripts.normalize_source_provenance import normalize_kgs, normalize_query_catalog


class SourceProvenanceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.repository = Path(__file__).resolve().parents[1]
        cls.catalogue = load_source_catalog(cls.repository / "catalog/sources.yaml")

    def test_all_manually_stored_sources_are_catalogued(self) -> None:
        missing = validate_catalogued_local_files(
            self.catalogue,
            [self.repository / "catalog/pdfs", self.repository / "catalog/curated"],
        )
        self.assertEqual(missing, [])

    def test_seed_sources_resolve_to_normalized_catalogue(self) -> None:
        seeds = load_hydrated_seeds(
            self.repository / "catalog/seeds.yaml", self.repository / "catalog/sources.yaml"
        )
        self.assertTrue(seeds)
        for seed in seeds:
            self.assertTrue(seed["source_ids"])
            self.assertEqual(len(seed["source_ids"]), len(seed["source_records"]))
            for source_id in seed["source_ids"]:
                self.assertIn(source_id, self.catalogue)

    def test_generated_kg_metadata_preserves_catalogue_provenance(self) -> None:
        records = [
            json.loads(line)
            for line in (self.repository / "catalog/kgs.jsonl").read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        for record in records:
            self.assertEqual(
                record.get("source_ids"),
                [detail.get("source_id") for detail in record.get("source_details", [])],
            )
            for detail in record.get("source_details", []):
                source_id = detail.get("source_id")
                self.assertIn(source_id, self.catalogue)
                self.assertEqual(
                    detail.get("catalog_provenance", {}).get("title"),
                    self.catalogue[source_id]["title"],
                )

    def test_local_query_evidence_retains_source_identifier(self) -> None:
        path = self.repository / "var/queries/kg_queries.jsonl"
        if not path.exists():
            self.skipTest("local generated kg_queries.jsonl is intentionally absent from a clean checkout")
        for line in path.read_text(encoding="utf-8").splitlines():
            record = json.loads(line)
            for evidence in record.get("evidence", []):
                path = str(evidence.get("source_path") or "")
                if path.startswith("catalog/pdfs/") or path.startswith("catalog/curated/"):
                    self.assertIn(evidence.get("source_id"), self.catalogue)

    def test_normalization_matches_reordered_details_by_identity(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "catalog").mkdir()
            sources = root / "catalog/sources.yaml"
            seeds = root / "catalog/seeds.yaml"
            kgs = root / "catalog/kgs.jsonl"
            sources.write_text(
                "sources:\n"
                "  - source_id: first\n    type: web_document\n    title: First\n    url: https://example.com/first\n"
                "  - source_id: second\n    type: web_document\n    title: Second\n    url: https://example.com/second\n",
                encoding="utf-8",
            )
            seeds.write_text(
                "kgs:\n  - kg_id: test\n    source_ids: [first, second]\n",
                encoding="utf-8",
            )
            kgs.write_text(
                json.dumps(
                    {
                        "kg_id": "test",
                        "source_details": [
                            {"source_url": "https://example.com/second", "marker": "second"},
                            {"source_url": "https://example.com/first", "marker": "first"},
                        ],
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            normalize_kgs(kgs, seeds, sources)
            [record] = [json.loads(line) for line in kgs.read_text(encoding="utf-8").splitlines()]
            self.assertEqual(
                [(detail["source_id"], detail["marker"]) for detail in record["source_details"]],
                [("first", "first"), ("second", "second")],
            )

    def test_query_normalization_preserves_captured_url(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            snapshot = root / "source.txt"
            pinned = "https://raw.githubusercontent.com/org/repo/abc123/README.md"
            snapshot.write_text(f"SOURCE: {pinned}\n\nbody", encoding="utf-8")
            query_catalog = root / "queries.jsonl"
            query_catalog.write_text(
                json.dumps(
                    {
                        "evidence": [
                            {
                                "source_path": str(snapshot),
                                "source_url": "https://github.com/org/repo/blob/main/README.md",
                            }
                        ]
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            catalogue = {
                "readme": {
                    "source_id": "readme",
                    "type": "web_document",
                    "title": "README",
                    "url": "https://github.com/org/repo/blob/main/README.md",
                }
            }
            normalize_query_catalog(query_catalog, catalogue)
            [record] = [
                json.loads(line) for line in query_catalog.read_text(encoding="utf-8").splitlines()
            ]
            evidence = record["evidence"][0]
            self.assertEqual(evidence["source_url"], pinned)
            self.assertEqual(evidence["source_catalog_url"], catalogue["readme"]["url"])


if __name__ == "__main__":
    unittest.main()
