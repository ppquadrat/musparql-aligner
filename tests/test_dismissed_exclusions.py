from __future__ import annotations

import json
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from unittest.mock import patch

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

    def test_benchmark_only_compare_uses_carried_forward_benchmark_statuses(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            previous_outputs = tmp_path / "previous_outputs.jsonl"
            current_outputs = tmp_path / "current_outputs.jsonl"
            previous_inputs = tmp_path / "previous_inputs.jsonl"
            current_inputs = tmp_path / "current_inputs.jsonl"
            benchmark_dir = tmp_path / "benchmark"
            out_path = tmp_path / "review_data.js"
            benchmark_dir.mkdir()

            def output(query_id: str, label: str, question: str) -> dict:
                return {
                    "kg_id": "kg",
                    "query_id": query_id,
                    "query_label": label,
                    "llm_output": {
                        "nl_question": question,
                        "confidence": 80,
                        "confidence_rationale": question,
                        "nl_question_origin": {"mode": "generated", "evidence_ids": [], "primary_evidence_id": None},
                        "ranked_evidence_phrases": [],
                        "needs_review": False,
                    },
                    "model": "model",
                }

            previous_rows = [
                output("q1", "label-1", "Old approved?"),
                output("q2", "label-2", "Old prompt fix?"),
                output("q3", "label-3", "Old non benchmark?"),
            ]
            current_rows = [
                output("q1", "label-1", "New approved?"),
                output("q2", "label-2", "New prompt fix?"),
                output("q3", "label-3", "New non benchmark?"),
            ]
            input_rows = [
                {"kg_id": "kg", "query_id": f"q{i}", "query_label": f"label-{i}", "sparql_clean": f"SELECT * WHERE {{ ?s ?p ?o }} # {i}", "evidence": []}
                for i in range(1, 4)
            ]
            for path, rows in (
                (previous_outputs, previous_rows),
                (current_outputs, current_rows),
                (previous_inputs, input_rows),
                (current_inputs, input_rows),
            ):
                path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")

            approved = {
                "benchmark_id": "kg::label-1::old",
                "kg_id": "kg",
                "query_id": "q1",
                "query_label": "label-1",
                "sparql": "SELECT * WHERE { ?s ?p ?o } # 1",
                "gold_question": "Old approved?",
                "gold_question_source": "approved_model_output",
                "review_status": "approve",
                "review": {"note": "approved note"},
            }
            pending = {
                "benchmark_id": "kg::label-2::old",
                "kg_id": "kg",
                "query_id": "q2",
                "query_label": "label-2",
                "sparql": "SELECT * WHERE { ?s ?p ?o } # 2",
                "gold_question": "Corrected prompt fix?",
                "gold_question_source": "reviewer_rewrite",
                "review_status": "needs_prompt_fix",
                "review": {"note": "prompt note"},
            }
            (benchmark_dir / "approved.jsonl").write_text(json.dumps(approved) + "\n", encoding="utf-8")
            (benchmark_dir / "pending.jsonl").write_text(json.dumps(pending) + "\n", encoding="utf-8")
            (benchmark_dir / "dismissed.jsonl").write_text("", encoding="utf-8")

            with patch.object(
                sys,
                "argv",
                [
                    "build_review_diff_bundle.py",
                    "--previous-outputs",
                    str(previous_outputs),
                    "--current-outputs",
                    str(current_outputs),
                    "--previous-inputs",
                    str(previous_inputs),
                    "--current-inputs",
                    str(current_inputs),
                    "--previous-benchmark",
                    str(benchmark_dir),
                    "--benchmark-only",
                    "--out",
                    str(out_path),
                ],
            ):
                with redirect_stdout(StringIO()):
                    build_review_diff_bundle.main()

            text = out_path.read_text(encoding="utf-8")
            data = json.loads(text[len("window.REVIEW_DATA = ") :].rstrip().rstrip(";"))
            statuses = {
                record["query_id"]: record["previous"]["review"]["status"]
                for record in data["records"]
            }

            self.assertEqual(data["record_count"], 2)
            self.assertEqual(data["summary"]["benchmark_approved"], 1)
            self.assertEqual(data["summary"]["benchmark_pending"], 1)
            self.assertEqual(data["summary"]["non_benchmark_excluded"], 1)
            self.assertEqual(statuses, {"q1": "approve", "q2": "needs_prompt_fix"})
            self.assertEqual(data["records"][1]["previous"]["review"]["preferred_question"], "Corrected prompt fix?")

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
