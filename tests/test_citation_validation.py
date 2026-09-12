from __future__ import annotations

import json
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

from scripts import run_llm_generation


class CitationValidationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.schema = json.loads(
            (Path(__file__).resolve().parents[1] / "schemas" / "llm_output.schema.json").read_text(
                encoding="utf-8"
            )
        )

    def test_generation_defaults_persist_only_explicit_model_and_api_method(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "generation_config.json"
            run_llm_generation.save_generation_defaults(path, "MiniMax-M2.5", "chat.completions.create")
            self.assertEqual(
                run_llm_generation.load_generation_defaults(path),
                {"model": "MiniMax-M2.5", "api_method": "chat.completions.create"},
            )
            self.assertEqual(json.loads(path.read_text(encoding="utf-8"))["schema"], "musparql.llm-generation-defaults.v1")

    def test_invalid_model_error_is_fatal_configuration_error(self) -> None:
        error = RuntimeError("Invalid model name passed in model=gpt-5")
        self.assertTrue(run_llm_generation.is_fatal_configuration_error(error))
        self.assertFalse(run_llm_generation.is_fatal_configuration_error(RuntimeError("Schema validation failed")))

    def test_repairs_minimax_cq_number_evidence_id_drift(self) -> None:
        payload = {
            "evidence": [
                {"evidence_id": "e2", "type": "cq_item", "snippet": "CQ1 Where and when were the tracks on a CD recorded?"},
                {"evidence_id": "e3", "type": "cq_item", "snippet": "CQ2 What is the band line-up for the given performance?"},
                {"evidence_id": "e4", "type": "cq_item", "snippet": "CQ3 Which bands played/recorded a given tune?"},
                {"evidence_id": "e5", "type": "cq_item", "snippet": "CQ4 Which instruments has a given performer played?"},
                {
                    "evidence_id": "e6",
                    "type": "cq_item",
                    "snippet": "CQ5 Find performances whose recordings resulted in the same audio (to identify duplications or metadata ir- regularities)",
                },
            ]
        }
        output = {
            "ranked_evidence_phrases": [
                {
                    "text": "Find performances whose recordings resulted in the same audio (to identify duplications or metadata irregularities)",
                    "evidence_id": "e5",
                    "source_type": "cq_item",
                    "rank": 1,
                    "verbatim": True,
                }
            ],
            "nl_question_origin": {"mode": "paraphrased", "evidence_ids": ["e5"], "primary_evidence_id": "e5"},
        }

        report = run_llm_generation.validate_and_repair_citations(output, payload)

        self.assertEqual(output["ranked_evidence_phrases"][0]["evidence_id"], "e6")
        self.assertEqual(output["nl_question_origin"]["evidence_ids"], ["e6"])
        self.assertEqual(output["nl_question_origin"]["primary_evidence_id"], "e6")
        self.assertEqual(report["repair_count"], 1)
        self.assertEqual(report["repairs"][0]["type"], "evidence_id_repaired")

    def test_repairs_nonverbatim_when_clear_best_snippet_supports_phrase(self) -> None:
        payload = {
            "evidence": [
                {"evidence_id": "e7", "type": "cq_item", "snippet": "CQ6 Find all solos and their timestamps in a performance"},
                {"evidence_id": "e8", "type": "cq_item", "snippet": "CQ7 Find all performances which have solos with at- tributed musicians"},
            ]
        }
        output = {
            "ranked_evidence_phrases": [
                {
                    "text": "Find all performances which have solos with attributed musicians",
                    "evidence_id": "e7",
                    "source_type": "cq_item",
                    "rank": 2,
                    "verbatim": False,
                }
            ],
            "nl_question_origin": {"mode": "paraphrased", "evidence_ids": ["e7"], "primary_evidence_id": "e7"},
        }

        report = run_llm_generation.validate_and_repair_citations(output, payload)

        self.assertEqual(output["ranked_evidence_phrases"][0]["evidence_id"], "e8")
        self.assertEqual(output["nl_question_origin"]["evidence_ids"], ["e8"])
        self.assertEqual(report["repair_count"], 1)

    def test_does_not_repair_when_match_is_ambiguous(self) -> None:
        payload = {
            "evidence": [
                {"evidence_id": "e1", "type": "cq_item", "snippet": "Find solos by a musician"},
                {"evidence_id": "e2", "type": "cq_item", "snippet": "Find solos by a musician"},
            ]
        }
        output = {
            "ranked_evidence_phrases": [
                {
                    "text": "Find solos by a musician",
                    "evidence_id": "e1",
                    "source_type": "cq_item",
                    "rank": 1,
                    "verbatim": True,
                }
            ],
            "nl_question_origin": {"mode": "paraphrased", "evidence_ids": ["e1"], "primary_evidence_id": "e1"},
        }

        report = run_llm_generation.validate_and_repair_citations(output, payload)

        self.assertEqual(output["ranked_evidence_phrases"][0]["evidence_id"], "e1")
        self.assertEqual(report["repair_count"], 0)
        self.assertEqual(report["warning_count"], 0)

    def test_generated_origin_can_retain_partial_evidence(self) -> None:
        output = {
            "ranked_evidence_phrases": [{
                "text": "Synthetic partial context", "evidence_id": "e1",
                "source_type": "web_query_desc", "rank": 1, "verbatim": False,
            }],
            "nl_question": "Which synthetic records match the query?",
            "nl_question_origin": {
                "mode": "generated", "evidence_ids": ["e1"], "primary_evidence_id": None,
            },
            "confidence": 70, "confidence_rationale": "Synthetic test rationale.",
            "needs_review": True,
        }
        valid, error = run_llm_generation.validate_output(
            output, self.schema, {"evidence": [{"evidence_id": "e1", "type": "web_query_desc"}]}
        )
        self.assertTrue(valid, error)

    def test_generated_origin_rejects_primary_evidence(self) -> None:
        output = {
            "ranked_evidence_phrases": [{
                "text": "Synthetic source question?", "evidence_id": "e1",
                "source_type": "query_comment", "rank": 1, "verbatim": True,
            }],
            "nl_question": "A different generated question?",
            "nl_question_origin": {
                "mode": "generated", "evidence_ids": ["e1"], "primary_evidence_id": "e1",
            },
            "confidence": 70, "confidence_rationale": "Synthetic test rationale.",
            "needs_review": True,
        }
        valid, error = run_llm_generation.validate_output(output, self.schema)
        self.assertFalse(valid)
        self.assertIn("primary_evidence_id", str(error))

    def test_exact_authored_question_cannot_be_labelled_generated(self) -> None:
        output = {
            "ranked_evidence_phrases": [],
            "nl_question": "Which synthetic records match the query?",
            "nl_question_origin": {
                "mode": "generated", "evidence_ids": [], "primary_evidence_id": None,
            },
            "confidence": 70, "confidence_rationale": "Synthetic test rationale.",
            "needs_review": True,
        }
        payload = {"evidence": [{
            "evidence_id": "e1", "type": "curated_nl_question",
            "snippet": "Which synthetic records match the query?",
        }]}
        valid, error = run_llm_generation.validate_output(output, self.schema, payload)
        self.assertFalse(valid)
        self.assertIn("exactly matches source-authored", str(error))

    @unittest.skipUnless(shutil.which("jq"), "jq is required to exercise the Quagga filter")
    def test_quagga_filter_preserves_citations_for_generated_nl(self) -> None:
        input_record = {
            "kg_id": "synthetic", "query_id": "synthetic-q", "query_label": "synthetic-0001",
            "sparql_clean": "SELECT * WHERE { ?s ?p ?o }", "sparql_hash": "sha256:synthetic",
            "evidence": [{
                "evidence_id": "e1", "type": "web_query_desc", "source_id": "synthetic-source",
                "snippet": "Synthetic partial context", "source_url": "https://example.invalid/source",
                "source_path": "synthetic/source.txt",
            }],
        }
        output_record = {
            "query_id": "synthetic-q", "model": "synthetic-model",
            "llm_output": {
                "nl_question": "Which synthetic records match the query?", "needs_review": True,
                "nl_question_origin": {
                    "mode": "generated", "evidence_ids": ["e1"], "primary_evidence_id": None,
                },
                "ranked_evidence_phrases": [{
                    "text": "Synthetic partial context", "evidence_id": "e1",
                    "source_type": "web_query_desc", "rank": 1, "verbatim": True,
                }],
            },
        }
        ledger_record = {"query_id": "synthetic-q", "evidence": input_record["evidence"]}
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            paths = []
            for name, record in (
                ("inputs.jsonl", input_record), ("outputs.jsonl", output_record),
                ("ledger.jsonl", ledger_record),
            ):
                path = tmp_path / name
                path.write_text(json.dumps(record) + "\n", encoding="utf-8")
                paths.append(path)
            script = Path(__file__).resolve().parents[1] / "scripts" / "build_quagga_filter_candidates.jq"
            completed = subprocess.run(
                [
                    "jq", "-n", "--slurpfile", "inputs", str(paths[0]),
                    "--slurpfile", "outputs", str(paths[1]),
                    "--slurpfile", "ledger", str(paths[2]), "-f", str(script),
                ],
                check=True, capture_output=True, text=True,
            )
        source = json.loads(completed.stdout)["graphs"][0]["records"][0]["nl"]["sources"][0]
        self.assertEqual(source["evidence_id"], "e1")
        self.assertEqual(source["source_id"], "synthetic-source")


if __name__ == "__main__":
    unittest.main()
