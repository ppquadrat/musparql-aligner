from __future__ import annotations

import json
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path

import build_llm_inputs
import build_review_diff_bundle

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "benchmark"))
import build_benchmark  # noqa: E402


class DismissedExclusionTests(unittest.TestCase):
    def test_load_excluded_query_ids_from_benchmark_dir(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            benchmark_dir = Path(tmp)
            dismissed_path = benchmark_dir / "dismissed.jsonl"
            holdout_path = benchmark_dir / "holdout.jsonl"
            dismissed_path.write_text(
                json.dumps({"query_id": "q1"}) + "\n"
                + json.dumps({"query_id": "q2"}) + "\n",
                encoding="utf-8",
            )
            holdout_path.write_text(json.dumps({"query_id": "q3"}) + "\n", encoding="utf-8")

            self.assertEqual(
                build_llm_inputs.load_excluded_query_ids(benchmark_dir),
                {"q1", "q2", "q3"},
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

    def test_previous_review_split_by_pair_maps_holdout_records(self) -> None:
        output = {
            "kg_id": "kg",
            "query_id": "q1",
            "query_label": "label",
            "llm_output": {"nl_question": "Question?"},
        }
        previous_outputs = {("kg", "q1"): (1, output)}
        review_id = build_review_diff_bundle.review_id_for(output, 1)

        splits = build_review_diff_bundle.previous_review_split_by_pair(
            previous_outputs,
            {review_id: {"status": "approve", "split": "private_holdout"}},
        )

        self.assertEqual(splits, {("kg", "q1"): "private_holdout"})

    def test_build_benchmark_routes_private_holdout_out_of_public_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            bundle_path = tmp_path / "review_data.js"
            reviews_path = tmp_path / "reviews.json"
            outdir = tmp_path / "benchmark"
            bundle = {
                "dataset_id": "ds",
                "run_ids": ["run"],
                "records": [
                    {
                        "review_id": "kg::q1::a",
                        "kg_id": "kg",
                        "query_id": "q1",
                        "query_label": "q1",
                        "run_id": "run",
                        "input": {"sparql_clean": "SELECT * WHERE {}", "evidence": []},
                        "output": {"nl_question": "Public question?"},
                        "output_meta": {"model": "test-model"},
                    },
                    {
                        "review_id": "kg::q2::b",
                        "kg_id": "kg",
                        "query_id": "q2",
                        "query_label": "q2",
                        "run_id": "run",
                        "input": {"sparql_clean": "SELECT * WHERE {}", "evidence": []},
                        "output": {"nl_question": "Holdout question?"},
                        "output_meta": {"model": "test-model"},
                    },
                ],
            }
            reviews = {
                "dataset_id": "ds",
                "run_id": "run",
                "reviews": {
                    "kg::q1::a": {"status": "approve", "preferred_question": "", "note": ""},
                    "kg::q2::b": {
                        "status": "approve",
                        "preferred_question": "Hidden preferred?",
                        "note": "",
                        "split": "private_holdout",
                    },
                },
            }
            bundle_path.write_text("window.REVIEW_DATA = " + json.dumps(bundle) + ";\n", encoding="utf-8")
            reviews_path.write_text(json.dumps(reviews), encoding="utf-8")

            original_argv = sys.argv
            try:
                sys.argv = [
                    "build_benchmark.py",
                    "--bundle",
                    str(bundle_path),
                    "--reviews",
                    str(reviews_path),
                    "--outdir",
                    str(outdir),
                ]
                with redirect_stdout(StringIO()):
                    build_benchmark.main()
            finally:
                sys.argv = original_argv

            public_records = [
                json.loads(line)
                for line in (outdir / "benchmark.jsonl").read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
            holdout_records = [
                json.loads(line)
                for line in (outdir / "holdout.jsonl").read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]

            self.assertEqual([rec["query_id"] for rec in public_records], ["q1"])
            self.assertEqual([rec["query_id"] for rec in holdout_records], ["q2"])

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
