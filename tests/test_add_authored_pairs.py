from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from musparql.sparql_versions import sparql_hash

from scripts.benchmark import add_authored_pairs, audit_snapshot


def write_jsonl(path: Path, rows: list[dict]) -> None:
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")


def write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")


BASE_SPARQL = "SELECT ?s WHERE { ?s ?p ?o }"
EDITED_SPARQL = "SELECT DISTINCT ?s WHERE { ?s ?p ?o }"


def query_record(label: str, *, edited: bool = False, kg_id: str = "synthkg") -> dict:
    record: dict = {
        "kg_id": kg_id,
        "query_label": label,
        "query_id": f"{kg_id}__{sparql_hash(BASE_SPARQL)}",
        "sparql_clean": BASE_SPARQL,
        "sparql_hash": sparql_hash(BASE_SPARQL),
        "evidence": [
            {
                "evidence_id": "e1",
                "type": "curated_query",
                "source_id": "synthetic-source",
            }
        ],
    }
    if edited:
        record["sparql_edits"] = [
            {
                "version": 1,
                "sparql": EDITED_SPARQL,
                "sparql_hash": sparql_hash(EDITED_SPARQL),
                "note": "Synthetic correction.",
            }
        ]
    return record


def decision(label: str, *, canonical: str, alternatives: list[str] | None = None, kg_id: str = "synthkg") -> dict:
    return {
        "round": "synthetic-round",
        "dataset_id": "synthetic-dataset",
        "kg_id": kg_id,
        "query_label": label,
        "pair_review_id": f"{kg_id}::{label}::synthetic-run",
        "benchmark_disposition": "included",
        "pipeline_assessment": "input_data_improvement_recommended",
        "canonical_question": canonical,
        "alternative_questions": alternatives or [],
        "sparql_correction_required": True,
        "rationale": "Synthetic rationale.",
        "superseded_review_count": 1,
        "decided_at": "2026-11-09",
    }


def previous_snapshot(root: Path, *, included: list[dict] | None = None) -> Path:
    snapshot = root / "v10"
    snapshot.mkdir(parents=True)
    write_json(
        snapshot / "manifest.json",
        {
            "benchmark_version": "v10",
            "gold_question_policy": {"a_policy_asserted_when_content_was_admitted": True},
            "counts": {},
        },
    )
    write_jsonl(snapshot / "included.jsonl", included or [])
    write_jsonl(snapshot / "dismissed.jsonl", [])
    write_jsonl(snapshot / "alternatives.jsonl", [])
    write_jsonl(snapshot / "linguistic_annotations.jsonl", [])
    return snapshot


def run(argv: list[str]) -> int:
    with patch("sys.argv", ["add_authored_pairs", *argv]):
        return add_authored_pairs.main()


class AuthoredPairAdmissionTests(unittest.TestCase):
    def test_meta_review_pair_is_admitted_without_generation_and_audits(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            previous = previous_snapshot(root)
            queries = root / "kg_queries.jsonl"
            write_jsonl(queries, [query_record("synth-0001", edited=True)])
            decisions = root / "decisions.jsonl"
            write_jsonl(
                decisions,
                [decision("synth-0001", canonical="Which things are there?", alternatives=["What things exist?"])],
            )
            pairs = root / "pairs.jsonl"
            write_jsonl(pairs, [{"kg_id": "synthkg", "query_label": "synth-0001", "origin": "meta_review_decision"}])
            outdir = root / "v11"

            self.assertEqual(
                run([
                    "--previous-benchmark", str(previous),
                    "--pairs", str(pairs),
                    "--outdir", str(outdir),
                    "--kg-queries", str(queries),
                    "--decisions", str(decisions),
                    "--meta-reviewer-id", "reviewer-0001",
                    "--no-holdout",
                ]),
                0,
            )

            benchmark = [json.loads(l) for l in (outdir / "benchmark.jsonl").read_text().splitlines() if l.strip()]
            self.assertEqual(len(benchmark), 1)
            row = benchmark[0]
            self.assertEqual(row["gold_question"], "Which things are there?")
            self.assertEqual(row["gold_question_source"], "meta_review_decision")
            self.assertEqual(row["sparql"], EDITED_SPARQL)
            self.assertEqual(row["sparql_version"], 1)
            self.assertEqual(row["provenance"]["source_type"], "meta_review_decision")
            # No reviewer identity reaches a public record, in any field or
            # embedded in any identifier.
            for field in ("reviewer_id", "approval_review_id", "formulation_id",
                          "authored_by_reviewer_id"):
                self.assertNotIn(field, row["provenance"])
            self.assertNotIn("reviewer-", json.dumps(row))
            # The superseded team review is the one independent judgement.
            self.assertEqual(row["provenance"]["review_submissions"], 1)
            self.assertTrue(row["provenance"]["adjudicated"])

            included = [json.loads(l) for l in (outdir / "included.jsonl").read_text().splitlines() if l.strip()]
            self.assertIsNone(included[0]["model_output"]["nl_question"])
            self.assertIsNone(included[0]["run"]["generation_run_id"])
            self.assertEqual(included[0]["pipeline_assessment"], "not_applicable")
            self.assertEqual(
                included[0]["source"]["meta_review_pipeline_assessment"],
                "input_data_improvement_recommended",
            )
            self.assertEqual(included[0]["source"]["confirmation"], "single_meta_reviewer")
            self.assertNotIn("sparql_provenance", included[0])

            manifest = json.loads((outdir / "manifest.json").read_text())
            self.assertEqual(manifest["update_type"], "authored_pair_admission")
            self.assertIsNone(manifest["source_review_export"])
            # An assertion the previous snapshot made keeps travelling.
            self.assertTrue(
                manifest["gold_question_policy"]["a_policy_asserted_when_content_was_admitted"]
            )
            self.assertTrue(
                manifest["gold_question_policy"]["authored_pairs_admitted_without_generation_or_review"]
            )

            alternatives = [
                json.loads(l) for l in (outdir / "alternatives.jsonl").read_text().splitlines() if l.strip()
            ]
            self.assertEqual(len(alternatives), 1)
            alt = alternatives[0]["accepted_alternatives"][0]
            self.assertEqual(alt["text"], "What things exist?")
            self.assertEqual(alt["acceptance"], "human_accepted")
            self.assertTrue(alt["alternative_id"].startswith("alt-"))
            self.assertNotIn("reviewer-", json.dumps(alternatives[0]))

            with patch.object(audit_snapshot, "REPO_ROOT", root), patch.object(
                audit_snapshot, "read_jsonl", audit_snapshot.read_jsonl
            ):
                # audit_version_pins resolves pins against REPO_ROOT/var/queries.
                (root / "var" / "queries").mkdir(parents=True)
                (root / "var" / "queries" / "kg_queries.jsonl").write_text(
                    queries.read_text(), encoding="utf-8"
                )
                self.assertEqual(audit_snapshot.audit_snapshot(outdir), [])

    def test_source_prompt_pair_needs_a_question_and_a_source(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            previous = previous_snapshot(root)
            queries = root / "kg_queries.jsonl"
            write_jsonl(queries, [query_record("synth-0001")])
            pairs = root / "pairs.jsonl"
            outdir = root / "v11"
            argv = [
                "--previous-benchmark", str(previous),
                "--pairs", str(pairs),
                "--outdir", str(outdir),
                "--kg-queries", str(queries),
                "--no-holdout",
            ]

            write_jsonl(pairs, [{"kg_id": "synthkg", "query_label": "synth-0001", "origin": "source_prompt"}])
            with self.assertRaisesRegex(ValueError, "must carry gold_question"):
                run(argv)

            write_jsonl(
                pairs,
                [{
                    "kg_id": "synthkg", "query_label": "synth-0001", "origin": "source_prompt",
                    "gold_question": "Which things are there?",
                }],
            )
            with self.assertRaisesRegex(ValueError, "source block with source_id"):
                run(argv)

            write_jsonl(
                pairs,
                [{
                    "kg_id": "synthkg", "query_label": "synth-0001", "origin": "source_prompt",
                    "gold_question": "Which things are there?",
                    "source": {"source_id": "synthetic-source", "source_type": "curated_example"},
                }],
            )
            self.assertEqual(run(argv), 0)
            included = [json.loads(l) for l in (outdir / "included.jsonl").read_text().splitlines() if l.strip()]
            self.assertNotIn("reviewer_id", included[0]["review"])
            self.assertEqual(included[0]["source"]["confirmation"], "source_authored_pairing")

    def test_source_prompt_pair_cannot_carry_alternatives(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            previous = previous_snapshot(root)
            queries = root / "kg_queries.jsonl"
            write_jsonl(queries, [query_record("synth-0001")])
            pairs = root / "pairs.jsonl"
            write_jsonl(
                pairs,
                [{
                    "kg_id": "synthkg", "query_label": "synth-0001", "origin": "source_prompt",
                    "gold_question": "Which things are there?",
                    "source": {"source_id": "synthetic-source"},
                    "alternative_questions": ["What things exist?"],
                }],
            )
            with self.assertRaisesRegex(ValueError, "cannot carry alternative wordings"):
                run([
                    "--previous-benchmark", str(previous),
                    "--pairs", str(pairs),
                    "--outdir", str(root / "v11"),
                    "--kg-queries", str(queries),
                    "--no-holdout",
                ])

    def test_blank_canonical_question_is_refused(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            previous = previous_snapshot(root)
            queries = root / "kg_queries.jsonl"
            write_jsonl(queries, [query_record("synth-0001")])
            decisions = root / "decisions.jsonl"
            write_jsonl(decisions, [decision("synth-0001", canonical="")])
            pairs = root / "pairs.jsonl"
            write_jsonl(pairs, [{"kg_id": "synthkg", "query_label": "synth-0001", "origin": "meta_review_decision"}])
            with self.assertRaisesRegex(ValueError, "records no canonical question"):
                run([
                    "--previous-benchmark", str(previous),
                    "--pairs", str(pairs),
                    "--outdir", str(root / "v11"),
                    "--kg-queries", str(queries),
                    "--decisions", str(decisions),
                    "--meta-reviewer-id", "reviewer-0001",
                    "--no-holdout",
                ])

    def test_existing_pair_is_never_replaced_implicitly(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            query = query_record("synth-0001", edited=True)
            previous = previous_snapshot(
                root,
                included=[{
                    "benchmark_id": "synthkg::synth-0001::aaaaaaaaaaaa",
                    "kg_id": "synthkg",
                    "query_id": query["query_id"],
                    "query_label": "synth-0001",
                    "sparql": BASE_SPARQL,
                    "sparql_version": 0,
                    "sparql_hash": sparql_hash(BASE_SPARQL),
                    "gold_question": "A reviewed question?",
                    "gold_question_source": "reviewer_rewrite",
                    "benchmark_disposition": "included",
                    "split": "public",
                    "review": {
                        "review_id": "synthkg::synth-0001::synthetic-run::reviewer-9343",
                        "reviewer_id": "reviewer-9343",
                    },
                }],
            )
            queries = root / "kg_queries.jsonl"
            write_jsonl(queries, [query])
            decisions = root / "decisions.jsonl"
            write_jsonl(decisions, [decision("synth-0001", canonical="Which things are there?")])
            pairs = root / "pairs.jsonl"
            write_jsonl(pairs, [{"kg_id": "synthkg", "query_label": "synth-0001", "origin": "meta_review_decision"}])
            argv = [
                "--previous-benchmark", str(previous),
                "--pairs", str(pairs),
                "--outdir", str(root / "v11"),
                "--kg-queries", str(queries),
                "--decisions", str(decisions),
                "--meta-reviewer-id", "reviewer-0001",
                "--no-holdout",
            ]
            with self.assertRaisesRegex(ValueError, "--replace-existing"):
                run(argv)
            self.assertEqual(run([*argv, "--replace-existing"]), 0)

    def test_repin_keeps_the_question_and_the_review(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            query = query_record("synth-0001", edited=True)
            previous = previous_snapshot(
                root,
                included=[{
                    "benchmark_id": "synthkg::synth-0001::aaaaaaaaaaaa",
                    "kg_id": "synthkg",
                    "query_id": query["query_id"],
                    "query_label": "synth-0001",
                    "sparql": BASE_SPARQL,
                    "sparql_version": 0,
                    "sparql_hash": sparql_hash(BASE_SPARQL),
                    "sparql_provenance": {"retained_edit_count": 0},
                    "gold_question": "A reviewed question?",
                    "gold_question_source": "reviewer_rewrite",
                    "benchmark_disposition": "included",
                    "pipeline_assessment": "accepted",
                    "split": "public",
                    "review": {
                        "review_id": "synthkg::synth-0001::synthetic-run::reviewer-9343",
                        "reviewer_id": "reviewer-9343",
                    },
                }],
            )
            queries = root / "kg_queries.jsonl"
            write_jsonl(queries, [query])
            pairs = root / "pairs.jsonl"
            write_jsonl(
                pairs, [{"kg_id": "synthkg", "query_label": "synth-0001", "origin": "retained_sparql_repin"}]
            )
            outdir = root / "v11"
            self.assertEqual(
                run([
                    "--previous-benchmark", str(previous),
                    "--pairs", str(pairs),
                    "--outdir", str(outdir),
                    "--kg-queries", str(queries),
                    "--no-holdout",
                ]),
                0,
            )
            included = [json.loads(l) for l in (outdir / "included.jsonl").read_text().splitlines() if l.strip()]
            self.assertEqual(included[0]["gold_question"], "A reviewed question?")
            self.assertEqual(included[0]["gold_question_source"], "reviewer_rewrite")
            self.assertEqual(included[0]["review"]["reviewer_id"], "reviewer-9343")
            self.assertEqual(included[0]["benchmark_id"], "synthkg::synth-0001::aaaaaaaaaaaa")
            self.assertEqual(included[0]["sparql_version"], 1)
            self.assertEqual(included[0]["source"]["repinned_from_sparql_version"], 0)
            # A re-pin must not downgrade human_review provenance to a
            # description of the bookkeeping step.
            self.assertNotIn("source_type", included[0]["source"])
            benchmark = [
                json.loads(l) for l in (outdir / "benchmark.jsonl").read_text().splitlines() if l.strip()
            ]
            self.assertEqual(benchmark[0]["provenance"]["source_type"], "human_review")
            self.assertEqual(benchmark[0]["provenance"]["question_source"], "reviewer_rewrite")
            self.assertNotIn("sparql_provenance", included[0])

    def test_repin_refuses_a_pair_that_is_not_in_the_previous_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            previous = previous_snapshot(root)
            queries = root / "kg_queries.jsonl"
            write_jsonl(queries, [query_record("synth-0001", edited=True)])
            pairs = root / "pairs.jsonl"
            write_jsonl(
                pairs, [{"kg_id": "synthkg", "query_label": "synth-0001", "origin": "retained_sparql_repin"}]
            )
            with self.assertRaisesRegex(ValueError, "no pin to move"):
                run([
                    "--previous-benchmark", str(previous),
                    "--pairs", str(pairs),
                    "--outdir", str(root / "v11"),
                    "--kg-queries", str(queries),
                    "--no-holdout",
                ])

    def test_holdout_selected_pair_is_refused(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            previous = previous_snapshot(root)
            query = query_record("synth-0001")
            queries = root / "kg_queries.jsonl"
            write_jsonl(queries, [query])
            selectors = root / "selectors.jsonl"
            write_jsonl(selectors, [{"kg_id": "synthkg", "query_id": query["query_id"]}])
            decisions = root / "decisions.jsonl"
            write_jsonl(decisions, [decision("synth-0001", canonical="Which things are there?")])
            pairs = root / "pairs.jsonl"
            write_jsonl(pairs, [{"kg_id": "synthkg", "query_label": "synth-0001", "origin": "meta_review_decision"}])
            with self.assertRaisesRegex(ValueError, "holdout-selected"):
                run([
                    "--previous-benchmark", str(previous),
                    "--pairs", str(pairs),
                    "--outdir", str(root / "v11"),
                    "--kg-queries", str(queries),
                    "--decisions", str(decisions),
                    "--meta-reviewer-id", "reviewer-0001",
                    "--holdout-selectors", str(selectors),
                ])

    def test_unknown_origin_is_refused(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            previous = previous_snapshot(root)
            queries = root / "kg_queries.jsonl"
            write_jsonl(queries, [query_record("synth-0001")])
            pairs = root / "pairs.jsonl"
            write_jsonl(pairs, [{"kg_id": "synthkg", "query_label": "synth-0001", "origin": "invented"}])
            with self.assertRaisesRegex(ValueError, "origin must be one of"):
                run([
                    "--previous-benchmark", str(previous),
                    "--pairs", str(pairs),
                    "--outdir", str(root / "v11"),
                    "--kg-queries", str(queries),
                    "--no-holdout",
                ])


class IndexedAlternativeFormulationTests(unittest.TestCase):
    def test_indexed_alternative_formulation_ids_are_public_valid(self) -> None:
        from musparql.reviewer_provenance import validate_public_reviewer_ids

        review_id = "synthkg::synth-0001::synthetic-run::reviewer-0001"
        rows = [
            {
                "formulation_id": f"{review_id}::formulation::alternative::{index}",
                "authored_by_reviewer_id": "reviewer-0001",
                "approval_review_ids": [review_id],
                "approval_reviewer_ids": ["reviewer-0001"],
            }
            for index in (1, 2)
        ]
        validate_public_reviewer_ids(rows, "alternatives.jsonl")

    def test_an_unknown_formulation_role_is_still_refused(self) -> None:
        from musparql.reviewer_provenance import validate_public_reviewer_ids

        review_id = "synthkg::synth-0001::synthetic-run::reviewer-0001"
        with self.assertRaisesRegex(ValueError, "Invalid public formulation ID"):
            validate_public_reviewer_ids(
                [{"formulation_id": f"{review_id}::formulation::invented"}], "alternatives.jsonl"
            )


if __name__ == "__main__":
    unittest.main()
