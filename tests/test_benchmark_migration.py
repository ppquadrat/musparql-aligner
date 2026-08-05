from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


from scripts.benchmark import audit_snapshot  # noqa: E402
from scripts.benchmark import audit_eval_reports  # noqa: E402
from scripts.benchmark import build_benchmark  # noqa: E402
from scripts.benchmark import build_public_release  # noqa: E402
from scripts.benchmark import update_from_initial_review  # noqa: E402

from scripts import build_review_diff_bundle
from musparql.sparql_versions import sparql_hash


def write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")


def write_jsonl(path: Path, rows: list[dict]) -> None:
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")


class DecisionSchemaTests(unittest.TestCase):
    def test_unknown_and_conflicting_values_fail_closed(self) -> None:
        with self.assertRaisesRegex(ValueError, "Legacy review"):
            build_benchmark.normalized_review_decision({"status": "obsolete"})
        with self.assertRaisesRegex(ValueError, "Unknown benchmark"):
            build_benchmark.normalized_review_decision({"benchmark_disposition": "exluded"})
        with self.assertRaisesRegex(ValueError, "requires a pipeline assessment"):
            build_benchmark.normalized_review_decision({"benchmark_disposition": "included"})
        with self.assertRaisesRegex(ValueError, "cannot carry"):
            build_benchmark.normalized_review_decision(
                {"benchmark_disposition": "excluded", "pipeline_assessment": "accepted"}
            )

    def test_missing_canonical_question_fails_instead_of_disappearing(self) -> None:
        with self.assertRaisesRegex(ValueError, "no canonical question"):
            build_benchmark.benchmark_gold_records(
                included=[{"benchmark_id": "b1", "gold_question": ""}],
                benchmark_version="v-test",
                built_at="fixed",
            )

    def test_public_sparql_provenance_excludes_working_correction_artifacts(self) -> None:
        projected = build_benchmark.public_sparql_provenance({
            "retained_edit_count": 1,
            "selected_version": 1,
            "selected_hash": "sha256:selected",
            "history_digest": "sha256:history",
            "execution_observation": {
                "status": "ok",
                "attempted": True,
                "observed_at": "2026-08-05T18:12:01+00:00",
                "result_count": 1,
                "execution_digest": "SYNTHETIC_PRIVATE_CANARY",
                "duration_ms": 42,
            },
            "selected_edit": {
                "decision": "approve_edit",
                "edit_type": "parameter_instantiation",
                "rationale": "Synthetic public rationale.",
                "proposal_origin": "human",
                "reviewed_at": "2026-08-05T18:20:46+00:00",
                "approved_sparql_version": 1,
                "approved_sparql_hash": "sha256:selected",
                "candidate_id": "SYNTHETIC_PRIVATE_CANARY",
                "candidate_digest": "SYNTHETIC_PRIVATE_CANARY",
                "review_export_hash": "SYNTHETIC_PRIVATE_CANARY",
                "ui_execution_observations": [{"attempt_id": "SYNTHETIC_PRIVATE_CANARY"}],
            },
        })
        self.assertEqual(projected, {
            "retained_edit_count": 1,
            "selected_version": 1,
            "selected_hash": "sha256:selected",
            "history_digest": "sha256:history",
            "execution_observation": {
                "status": "ok",
                "attempted": True,
                "observed_at": "2026-08-05T18:12:01+00:00",
                "result_count": 1,
            },
            "selected_edit": {
                "decision": "approve_edit",
                "edit_type": "parameter_instantiation",
                "rationale": "Synthetic public rationale.",
                "proposal_origin": "human",
                "reviewed_at": "2026-08-05T18:20:46+00:00",
                "approved_sparql_version": 1,
                "approved_sparql_hash": "sha256:selected",
            },
        })

    def test_public_scoring_serializer_uses_record_provenance(self) -> None:
        rows = build_benchmark.benchmark_gold_records(
            included=[
                {
                    "benchmark_id": "b1",
                    "kg_id": "kg1",
                    "query_id": "q1",
                    "query_label": "one",
                    "sparql": "SELECT * WHERE {}",
                    "gold_question": "Question one?",
                    "gold_question_source": "reviewer_rewrite",
                    "review": {"note": "private"},
                },
                {
                    "benchmark_id": "b2",
                    "kg_id": "kg2",
                    "query_id": "q2",
                    "query_label": "two",
                    "sparql": "ASK {}",
                    "gold_question": "Question two?",
                    "gold_question_source": "source_prompt",
                    "source": {
                        "source_type": "curated_example",
                        "challenge": "Challenge 1",
                    },
                },
            ],
            benchmark_version="v-test",
            built_at="fixed",
        )
        self.assertEqual(rows[0]["provenance"]["source_type"], "human_review")
        self.assertEqual(rows[1]["provenance"]["source_type"], "curated_example")
        self.assertEqual(rows[1]["provenance"]["challenge"], "Challenge 1")
        self.assertNotIn("reviewer_comment", rows[0]["provenance"])
        self.assertNotIn("review", rows[0])
        self.assertNotIn("run", rows[0])
        self.assertNotIn("sparql_provenance", rows[0])


class UpdaterAndReleaseTests(unittest.TestCase):
    def test_public_only_snapshot_audits_without_working_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            snapshot = Path(tmp) / "v1"
            snapshot.mkdir()
            write_json(snapshot / "manifest.json", {
                "benchmark_version": "v1",
                "counts": {"benchmark": 1, "alternatives": 0},
                "files": {"benchmark": "benchmark.jsonl", "alternatives": "alternatives.jsonl"},
            })
            write_jsonl(snapshot / "benchmark.jsonl", [{
                "benchmark_id": "synthetic::one", "kg_id": "synthetic",
                "query_id": "synthetic-q1", "sparql": "ASK {}", "gold_question": "Synthetic?",
            }])
            write_jsonl(snapshot / "alternatives.jsonl", [])
            self.assertEqual(audit_snapshot.audit_snapshot(snapshot), [])

    def test_public_only_snapshot_rejects_unaccepted_or_interpretive_alternatives(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            snapshot = Path(tmp) / "v1"
            snapshot.mkdir()
            write_json(snapshot / "manifest.json", {
                "benchmark_version": "v1",
                "counts": {"benchmark": 1, "alternatives": 1},
                "files": {"benchmark": "benchmark.jsonl", "alternatives": "alternatives.jsonl"},
            })
            write_jsonl(snapshot / "benchmark.jsonl", [{
                "benchmark_id": "synthetic::one", "kg_id": "synthetic",
                "query_id": "synthetic-q1", "sparql": "ASK {}", "gold_question": "Synthetic?",
            }])
            write_jsonl(snapshot / "alternatives.jsonl", [{
                "benchmark_id": "synthetic::one", "kg_id": "synthetic", "query_id": "synthetic-q1",
                "accepted_alternatives": [{"text": "Unverified wording"}],
                "interpretive": {"naturalness": 50},
            }])
            errors = audit_snapshot.audit_snapshot(snapshot)
            self.assertTrue(any("acceptance provenance" in error for error in errors))
            self.assertTrue(any("linguistic ratings" in error for error in errors))

    def test_snapshot_rejects_non_public_sparql_provenance(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            snapshot = Path(tmp) / "v1"
            snapshot.mkdir()
            write_json(snapshot / "manifest.json", {
                "benchmark_version": "v1",
                "counts": {"benchmark": 1, "alternatives": 0},
            })
            write_jsonl(snapshot / "benchmark.jsonl", [{
                "benchmark_id": "synthetic::one",
                "kg_id": "synthetic",
                "query_id": "synthetic-q1",
                "sparql": "ASK {}",
                "gold_question": "Synthetic?",
                "sparql_provenance": {
                    "selected_edit": {"candidate_id": "SYNTHETIC_PRIVATE_CANARY"},
                },
            }])
            write_jsonl(snapshot / "alternatives.jsonl", [])
            errors = audit_snapshot.audit_snapshot(snapshot)
            self.assertTrue(any("non-public SPARQL provenance" in error for error in errors))

    def test_updater_refuses_compact_snapshot_as_provenance_source(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            snapshot = Path(tmp)
            write_jsonl(snapshot / "benchmark.jsonl", [])
            with self.assertRaisesRegex(FileNotFoundError, "cannot reconstruct review provenance"):
                update_from_initial_review.included_records_path(snapshot)

    def test_repository_evaluation_reports_reference_their_benchmarks(self) -> None:
        repository = Path(__file__).resolve().parents[1]
        reports = repository / "evals" / "reports"
        errors = []
        for manifest in reports.glob("*/manifest.json"):
            errors.extend(audit_eval_reports.audit_report(manifest.parent, repository))
        self.assertEqual(errors, [])

    def test_ui_legacy_normalization_and_compare_identity_checks(self) -> None:
        script = r'''
const fs = require("fs");
const vm = require("vm");
const sandbox = {
  window: { REVIEW_DATA: null, alert: () => {} },
  document: { getElementById: () => null, querySelectorAll: () => [] },
};
vm.runInNewContext(fs.readFileSync("review/app.js", "utf8"), sandbox);
const schema = sandbox.window.MUSPARQL_REVIEW_SCHEMA;
const internal = schema.internalReviews({x: {
  benchmark_disposition: "included",
  pipeline_assessment: "accepted",
  status: "",
  note: "Literal: Exact wording?\nkept",
  literal_wording: "Exact wording?",
}}).x;
if ("benchmark_disposition" in internal || "pipeline_assessment" in internal) process.exit(2);
if (internal.public_comment !== "" || internal.internal_comment !== "kept") process.exit(8);
const exported = schema.exportableReview({ status: "excluded" });
if (exported.benchmark_disposition !== "excluded" || exported.pipeline_assessment !== null) process.exit(3);
const partitioned = schema.partitionReviewMap({
  public: {status:"accepted", public_comment:"safe"},
  private: {status:"accepted", split:"private_holdout", internal_comment:"SYNTHETIC_PRIVATE_CANARY"},
});
if (!("public" in partitioned.publicReviews) || ("private" in partitioned.publicReviews)) process.exit(9);
if (!("private" in partitioned.privateReviews) || ("public" in partitioned.privateReviews)) process.exit(10);
let privateImportRejected = false;
try { schema.rejectPrivateImport({}, {x:{split:"private_holdout"}}); } catch (_) { privateImportRejected = true; }
if (!privateImportRejected) process.exit(11);
const matched = schema.matchPrivateRecords({x:{}}, [{review_id:"x"}], (record) => record.review_id, "synthetic dataset");
if (!matched || matched.length !== 1) process.exit(12);
const unmatched = schema.matchPrivateRecords({missing:{}}, [{review_id:"x"}], (record) => record.review_id, "synthetic dataset");
if (unmatched !== null) process.exit(13);
let legacyRejected = false;
try { schema.validateImportedReviews({x:{status:"obsolete"}}); } catch (_) { legacyRejected = true; }
if (!legacyRejected) process.exit(4);
const current = {dataset_id:"ds", previous_run:{run_id:"old"}, current_run:{run_id:"new"}};
schema.validateCompareImportPayload({mode:"compare", dataset_id:"ds", previous_run:{run_id:"old"}, current_run:{run_id:"new"}}, current);
let rejected = false;
try { schema.validateCompareImportPayload({mode:"compare", dataset_id:"other"}, current); } catch (_) { rejected = true; }
if (!rejected) process.exit(5);
let unidentifiedRejected = false;
try { schema.validateCompareImportPayload({reviews:{}}, current); } catch (_) { unidentifiedRejected = true; }
if (!unidentifiedRejected) process.exit(6);
let conflictRejected = false;
try { schema.validateImportedReviews({x:{benchmark_disposition:"excluded", pipeline_assessment:"accepted"}}); } catch (_) { conflictRejected = true; }
if (!conflictRejected) process.exit(7);
if (schema.hasReviewerDecision({})) process.exit(14);
if (schema.hasReviewerDecision({interpretive:{naturalness:null, requires_graph_context_knowledge:false}})) process.exit(15);
if (!schema.hasReviewerDecision({status:"accepted"})) process.exit(16);
if (!schema.hasReviewerDecision({public_comment:"reviewed earlier"})) process.exit(17);
if (!schema.hasReviewerDecision({reviewed:true})) process.exit(18);
const cleanRecord = {review_scope:"new", input:{sparql_provenance:{retained_edit_count:0}}};
const editedRecord = {review_scope:"new", input:{sparql_provenance:{retained_edit_count:1}}};
if (schema.initialHoldoutEligibility(cleanRecord).eligible) process.exit(19);
if (!schema.initialHoldoutEligibility(cleanRecord, true).eligible) process.exit(20);
if (schema.initialHoldoutEligibility(editedRecord, true).eligible) process.exit(28);
if (schema.initialHoldoutEligibility({review_scope:"new", has_prior_pair_review:true, input:{sparql_provenance:{retained_edit_count:0}}}, true).eligible) process.exit(24);
if (schema.initialHoldoutEligibility({review_scope:"previously_reviewed", previous_review:{reviewed:true}, input:{sparql_provenance:{retained_edit_count:0}}}, true).eligible) process.exit(25);
const unreviewedComparison = {previous:{record:{}, review:{}}, current:{record:cleanRecord}};
if (schema.compareHoldoutEligibility(unreviewedComparison).eligible) process.exit(21);
if (!schema.compareHoldoutEligibility(unreviewedComparison, true).eligible) process.exit(26);
const editedComparison = {previous:{record:{}, review:{}}, current:{record:editedRecord}};
if (schema.compareHoldoutEligibility(editedComparison, true).eligible) process.exit(29);
const reviewedComparison = {previous:{record:{}, review:{literal_wording:"Earlier wording"}}, current:{record:cleanRecord}};
if (schema.compareHoldoutEligibility(reviewedComparison, true).eligible) process.exit(22);
if (schema.compareHoldoutEligibility({previous:{review:{}}, current:{record:null}}, true).eligible) process.exit(23);
const reusedPrivate = schema.reusedPreviousReview({status:"accepted"}, {split:"private_holdout"}, "old-review");
if (reusedPrivate.split !== "private_holdout" || reusedPrivate.copied_from_review_id !== "old-review") process.exit(27);
'''
        subprocess.run(["node", "-e", script], check=True, cwd=Path(__file__).resolve().parents[1])

    def test_normal_review_updater_executes_end_to_end(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            previous = root / "v1"
            outdir = root / "v2"
            previous.mkdir()
            existing = {
                "benchmark_id": "kg::old",
                "kg_id": "kg",
                "query_id": "old",
                "query_label": "old",
                "sparql": "ASK {}",
                "sparql_version": 1,
                "sparql_hash": "sha256:abc",
                "gold_question": "Old question?",
                "gold_question_source": "approved_model_output",
                "benchmark_disposition": "included",
                "pipeline_assessment": "accepted",
                "review": {},
                "run": {},
            }
            write_json(previous / "manifest.json", {"benchmark_version": "v1", "counts": {}})
            write_jsonl(previous / "included.jsonl", [existing])
            write_jsonl(previous / "dismissed.jsonl", [])
            write_jsonl(previous / "holdout.jsonl", [])
            write_jsonl(previous / "alternatives.jsonl", [])
            write_jsonl(previous / "linguistic_annotations.jsonl", [])

            bundle = {
                "dataset_id": "dataset",
                "records": [
                    {
                        "review_id": "kg::new",
                        "kg_id": "kg",
                        "query_id": "new",
                        "query_label": "new",
                        "run_id": "run",
                        "input": {"sparql_clean": "SELECT * WHERE {}", "evidence": []},
                        "output": {"nl_question": "Generated question?"},
                        "output_meta": {"model": "test-model"},
                    }
                ],
                "runs": [{"run_id": "run"}],
            }
            bundle_path = root / "bundle.js"
            bundle_path.write_text("window.REVIEW_DATA = " + json.dumps(bundle) + ";\n", encoding="utf-8")
            reviews_path = root / "reviews.json"
            write_json(
                reviews_path,
                {
                    "kind": "non_holdout_review_export",
                    "dataset_id": "dataset",
                    "reviews": {
                        "kg::new": {
                            "benchmark_disposition": "included",
                            "pipeline_assessment": "prompt_improvement_recommended",
                            "preferred_question": "Human question?",
                        }
                    },
                },
            )
            with patch.object(
                sys,
                "argv",
                [
                    "update_from_initial_review.py",
                    "--previous-benchmark",
                    str(previous),
                    "--bundle",
                    str(bundle_path),
                    "--reviews",
                    str(reviews_path),
                    "--outdir",
                    str(outdir),
                ],
            ):
                update_from_initial_review.main()
            rows = build_benchmark.read_jsonl(outdir / "included.jsonl")
            self.assertEqual(len(rows), 2)
            new = next(row for row in rows if row["query_id"] == "new")
            self.assertEqual(new["gold_question"], "Human question?")
            self.assertEqual(new["pipeline_assessment"], "prompt_improvement_recommended")
            self.assertEqual(audit_snapshot.audit_snapshot(outdir), [])

    def test_normal_review_updater_replaces_re_reviewed_sparql_revision(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            previous = root / "v8"
            outdir = root / "v9"
            previous.mkdir()
            original_sparql = "ASK {}"
            replacement_sparql = "SELECT * WHERE {}"
            existing = {
                "benchmark_id": "kg::old-review",
                "kg_id": "kg",
                "query_id": "same-identity",
                "query_label": "example-0001",
                "sparql": original_sparql,
                "sparql_version": 0,
                "sparql_hash": sparql_hash(original_sparql),
                "gold_question": "Old question?",
                "gold_question_source": "approved_model_output",
                "benchmark_disposition": "included",
                "pipeline_assessment": "accepted",
                "review": {},
                "run": {},
            }
            write_json(previous / "manifest.json", {"benchmark_version": "v8", "counts": {}})
            write_jsonl(previous / "included.jsonl", [existing])
            write_jsonl(previous / "dismissed.jsonl", [])
            write_jsonl(previous / "holdout.jsonl", [])
            write_jsonl(previous / "alternatives.jsonl", [])
            write_jsonl(previous / "linguistic_annotations.jsonl", [])

            bundle = {
                "dataset_id": "dataset",
                "records": [{
                    "review_id": "kg::new-review",
                    "kg_id": "kg",
                    "query_id": "same-identity",
                    "query_label": "example-0001",
                    "review_scope": "new",
                    "has_prior_pair_review": True,
                    "run_id": "run",
                    "input": {
                        "sparql_clean": replacement_sparql,
                        "sparql_version": 1,
                        "sparql_hash": sparql_hash(replacement_sparql),
                        "evidence": [],
                    },
                    "output": {"nl_question": "Question for the corrected query?"},
                    "output_meta": {"model": "test-model"},
                }],
                "runs": [{"run_id": "run"}],
            }
            bundle_path = root / "bundle.js"
            bundle_path.write_text("window.REVIEW_DATA = " + json.dumps(bundle) + ";\n", encoding="utf-8")
            reviews_path = root / "reviews.json"
            write_json(reviews_path, {
                "kind": "non_holdout_review_export",
                "dataset_id": "dataset",
                "reviews": {
                    "kg::new-review": {
                        "benchmark_disposition": "included",
                        "pipeline_assessment": "accepted",
                    }
                },
            })

            query_path = root / "var" / "queries" / "kg_queries.jsonl"
            query_path.parent.mkdir(parents=True)
            write_jsonl(query_path, [{
                "kg_id": "kg",
                "query_id": "same-identity",
                "sparql_clean": original_sparql,
                "sparql_hash": sparql_hash(original_sparql),
                "sparql_edits": [{
                    "version": 1,
                    "sparql": replacement_sparql,
                    "note": "Synthetic reviewed correction.",
                }],
                "execution_history": [],
            }])

            with patch.object(sys, "argv", [
                "update_from_initial_review.py",
                "--previous-benchmark", str(previous),
                "--bundle", str(bundle_path),
                "--reviews", str(reviews_path),
                "--outdir", str(outdir),
                "--kg-queries", str(query_path),
            ]):
                update_from_initial_review.main()

            rows = build_benchmark.read_jsonl(outdir / "included.jsonl")
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["benchmark_id"], "kg::new-review")
            self.assertEqual(rows[0]["sparql"], replacement_sparql)
            self.assertEqual(rows[0]["sparql_version"], 1)
            manifest = build_benchmark.read_json(outdir / "manifest.json")
            self.assertEqual(manifest["counts"]["replaced_sparql_revisions"], 1)
            self.assertEqual(manifest["sparql_version_policy"], "latest_retained")
            self.assertEqual(manifest["execution_snapshot"]["status_counts"], {"not_attempted": 1})

            with patch.object(audit_snapshot, "REPO_ROOT", root):
                self.assertEqual(audit_snapshot.audit_snapshot(outdir), [])

    def test_public_release_is_allowlisted_and_deterministic(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            snapshot = root / "v7"
            snapshot.mkdir()
            internal_record = {
                "benchmark_id": "b1",
                "kg_id": "kg",
                "query_id": "q1",
                "query_label": "one",
                "sparql": "ASK {}",
                "gold_question": "Question?",
                "gold_question_source": "reviewer_rewrite",
                "benchmark_disposition": "included",
                "pipeline_assessment": "accepted",
                "review": {
                    "public_comment": "Public rationale",
                    "internal_comment": "private",
                    "review_export": "/Users/name/review.json",
                },
                "run": {"request_config": {"api_key_env": "SECRET"}},
            }
            write_json(
                snapshot / "manifest.json",
                {
                    "benchmark_version": "v7",
                    "private": "/Users/name",
                    "counts": {
                        "benchmark": 1,
                        "included": 1,
                        "dismissed": 0,
                        "holdout": 0,
                        "alternatives": 1,
                        "linguistic_annotations": 0,
                        "pipeline_assessment_counts": {"accepted": 1},
                        "benchmark_disposition_counts": {"included": 1},
                    },
                },
            )
            write_jsonl(snapshot / "included.jsonl", [internal_record])
            write_jsonl(snapshot / "dismissed.jsonl", [])
            write_jsonl(snapshot / "holdout.jsonl", [])
            write_jsonl(snapshot / "linguistic_annotations.jsonl", [])
            write_jsonl(
                snapshot / "benchmark.jsonl",
                [
                    {
                        "benchmark_version": "v7",
                        "benchmark_id": "b1",
                        "kg_id": "kg",
                        "query_id": "q1",
                        "query_label": "one",
                        "sparql": "ASK {}",
                        "sparql_version": 1,
                        "sparql_hash": "sha256:abc",
                        "gold_question": "Question?",
                        "gold_question_source": "reviewer_rewrite",
                        "review": {"note": "private", "review_export": "/Users/name/review.json"},
                        "run": {"request_config": {"api_key_env": "SECRET"}},
                    }
                ],
            )
            write_jsonl(
                snapshot / "alternatives.jsonl",
                [
                    {
                        "benchmark_version": "v7",
                        "benchmark_id": "b1",
                        "kg_id": "kg",
                        "query_id": "q1",
                        "sparql_version": 1,
                        "sparql_hash": "sha256:abc",
                        "accepted_alternatives": [
                            {
                                "text": "Alternative?",
                                "source_type": "model_output",
                                "acceptance": "human_accepted",
                                "model": "model",
                                "run": {"response_metadata": {"id": "secret"}},
                            }
                        ],
                    }
                ],
            )
            releases = []
            for name in ("release-a", "release-b"):
                outdir = root / name
                with patch.object(
                    sys,
                    "argv",
                    ["build_public_release.py", "--snapshot", str(snapshot), "--outdir", str(outdir)],
                ):
                    build_public_release.main()
                releases.append(outdir)
            for filename in build_benchmark.PUBLIC_RELEASE_FILES:
                self.assertEqual(
                    (releases[0] / filename).read_bytes(),
                    (releases[1] / filename).read_bytes(),
                )
            public_text = "".join(path.read_text(encoding="utf-8") for path in releases[0].iterdir())
            self.assertNotIn("/Users/", public_text)
            self.assertNotIn("review_export", public_text)
            self.assertNotIn("request_config", public_text)
            self.assertNotIn("response_metadata", public_text)
            public_benchmark = build_benchmark.read_jsonl(releases[0] / "benchmark.jsonl")[0]
            public_alternatives = build_benchmark.read_jsonl(releases[0] / "alternatives.jsonl")[0]
            public_manifest = json.loads((releases[0] / "manifest.json").read_text())
            self.assertEqual(public_benchmark["sparql_version"], 1)
            self.assertEqual(public_benchmark["sparql_hash"], "sha256:abc")
            self.assertEqual(public_alternatives["sparql_version"], 1)
            self.assertEqual(public_alternatives["sparql_hash"], "sha256:abc")
            self.assertEqual(public_manifest["release_schema_version"], "1.2")
            self.assertEqual(
                set(path.name for path in releases[0].iterdir()),
                set(build_benchmark.PUBLIC_RELEASE_FILES),
            )
            self.assertEqual(public_manifest["licensing"]["benchmark_spdx"], "CC-BY-4.0")
            self.assertIn(
                "Creative Commons Attribution 4.0",
                (releases[0] / "LICENSE").read_text(),
            )
            self.assertIn(
                "Distributed Digital Music Archives",
                (releases[0] / "THIRD_PARTY_NOTICES.md").read_text(),
            )
            for filename, details in public_manifest["files"].items():
                self.assertEqual(
                    details["sha256"],
                    build_public_release.sha256(releases[0] / filename),
                )

    def test_public_nested_provenance_is_allowlisted_and_private_paths_fail(self) -> None:
        record = {
            "benchmark_id": "b1",
            "gold_question": "Question?",
            "sparql_version": 1,
            "sparql_hash": "sha256:abc",
            "sparql_provenance": {
                "retained_edit_count": 1,
                "selected_version": 1,
                "selected_hash": "sha256:abc",
                "selected_edit": {
                    "decision": "approve_edit",
                    "candidate_id": "SYNTHETIC_PRIVATE_CANARY",
                    "review_export_hash": "SYNTHETIC_PRIVATE_CANARY",
                    "ui_execution_observations": [{"attempt_id": "SYNTHETIC_PRIVATE_CANARY"}],
                },
            },
            "provenance": {
                "question_source": "reviewer_rewrite",
                "source_type": "human_review",
                "api_key_env": "SECRET_TOKEN",
                "base_url": "https://internal.invalid",
            },
        }
        public = build_public_release.public_benchmark_record(record)
        self.assertEqual(public["sparql_version"], 1)
        self.assertEqual(public["sparql_hash"], "sha256:abc")
        self.assertEqual(public["sparql_provenance"], {
            "retained_edit_count": 1,
            "selected_version": 1,
            "selected_hash": "sha256:abc",
            "selected_edit": {"decision": "approve_edit"},
        })
        self.assertEqual(
            public["provenance"],
            {"question_source": "reviewer_rewrite", "source_type": "human_review"},
        )
        with self.assertRaisesRegex(ValueError, "Private filesystem path"):
            build_public_release.assert_public_safe(
                {"provenance": {"prompt_source": "/home/alice/private-review.json"}}
            )

    def test_review_comments_remove_only_matching_literal_duplication(self) -> None:
        review = {
            "note": "Semantic explanation.\nLiteral:\u00a0Which labels are returned?",
            "literal_wording": "Which labels are returned?",
        }
        self.assertEqual(build_benchmark.review_comments(review), ("", "Semantic explanation."))
        mismatch = {
            "note": "Literal: A different formulation?",
            "literal_wording": "Canonical literal formulation?",
        }
        self.assertEqual(
            build_benchmark.review_comments(mismatch)[1],
            "Literal: A different formulation?",
        )

    def test_compare_bundle_keeps_legacy_notes_private_and_extracts_literal(self) -> None:
        review = build_review_diff_bundle.benchmark_review_for_record(
            {
                "gold_question_source": "reviewer_rewrite",
                "gold_question": "Preferred?",
                "pipeline_assessment": "accepted",
                "review": {"note": "Literal: Exact wording?\nWorking note"},
            },
            "included",
            Path("benchmark/v1"),
        )
        self.assertEqual(review["literal_wording"], "Exact wording?")
        self.assertEqual(review["public_comment"], "")
        self.assertEqual(review["internal_comment"], "Working note")

        explicitly_cleared = build_review_diff_bundle.benchmark_review_for_record(
            {"review": {"note": "stale", "public_comment": "", "internal_comment": "private"}},
            "excluded",
            Path("benchmark/v1"),
        )
        self.assertEqual(explicitly_cleared["public_comment"], "")
        self.assertEqual(explicitly_cleared["internal_comment"], "private")

    def test_current_snapshot_has_normalized_public_comments(self) -> None:
        repository = Path(__file__).resolve().parents[1]
        included = build_benchmark.read_jsonl(repository / "benchmark/v7/included.jsonl")
        for record in included:
            review = record.get("review", {})
            self.assertNotIn("note", review)
            self.assertIn("public_comment", review)
            self.assertIn("internal_comment", review)
            literal = build_benchmark.normalize_rephrasing_text(review.get("literal_wording"))
            if literal:
                for line in str(review.get("public_comment") or "").splitlines():
                    if line.strip().lower().startswith("literal:"):
                        self.assertNotEqual(
                            build_benchmark.normalize_rephrasing_text(line.split(":", 1)[1]),
                            literal,
                        )


if __name__ == "__main__":
    unittest.main()
