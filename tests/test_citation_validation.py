from __future__ import annotations

import unittest

from scripts import run_llm_generation


class CitationValidationTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
