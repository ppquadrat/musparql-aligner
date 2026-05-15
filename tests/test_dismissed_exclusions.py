from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import build_llm_inputs
import build_review_diff_bundle


class DismissedExclusionTests(unittest.TestCase):
    def test_load_dismissed_query_ids_from_benchmark_dir(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            benchmark_dir = Path(tmp)
            dismissed_path = benchmark_dir / "dismissed.jsonl"
            dismissed_path.write_text(
                json.dumps({"query_id": "q1"}) + "\n"
                + json.dumps({"query_id": "q2"}) + "\n",
                encoding="utf-8",
            )

            self.assertEqual(
                build_llm_inputs.load_dismissed_query_ids(benchmark_dir),
                {"q1", "q2"},
            )

    def test_previous_review_status_by_pair_maps_dismissed_records(self) -> None:
        output = {
            "kg_id": "kg",
            "query_id": "q1",
            "query_label": "label",
            "llm_output": {"nl_question": "Question?"},
        }
        previous_outputs = {("kg", "q1"): (1, output)}
        review_id = build_review_diff_bundle.review_id_for(output, 1)

        statuses = build_review_diff_bundle.previous_review_status_by_pair(
            previous_outputs,
            {review_id: {"status": "dismiss"}},
        )

        self.assertEqual(statuses, {("kg", "q1"): "dismiss"})

    def test_rationale_only_change_is_not_review_worthy(self) -> None:
        self.assertFalse(
            build_review_diff_bundle.has_review_worthy_change(
                "changed",
                ["rationale_changed"],
            )
        )

    def test_question_change_is_review_worthy(self) -> None:
        self.assertTrue(
            build_review_diff_bundle.has_review_worthy_change(
                "changed",
                ["rationale_changed", "question_changed"],
            )
        )


if __name__ == "__main__":
    unittest.main()
