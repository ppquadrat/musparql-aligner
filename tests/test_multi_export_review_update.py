from __future__ import annotations

import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts.benchmark import build_benchmark, update_from_initial_review


def write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")


def write_jsonl(path: Path, rows: list[dict]) -> None:
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")


def previous_snapshot(root: Path) -> Path:
    previous = root / "v1"
    previous.mkdir()
    write_json(previous / "manifest.json", {"benchmark_version": "v1", "counts": {}})
    for name in ("included.jsonl", "dismissed.jsonl", "alternatives.jsonl", "linguistic_annotations.jsonl"):
        write_jsonl(previous / name, [])
    return previous


def bundle_file(path: Path, *, dataset_id: str, labels: list[str]) -> None:
    bundle = {
        "dataset_id": dataset_id,
        "records": [
            {
                "review_id": f"kg::{label}",
                "kg_id": "kg",
                "query_id": label,
                "query_label": label,
                "run_id": f"run-{dataset_id}",
                "input": {"sparql_clean": "SELECT * WHERE {}", "evidence": []},
                "output": {"nl_question": f"Generated question for {label}?"},
                "output_meta": {"model": "test-model"},
            }
            for label in labels
        ],
        "runs": [{"run_id": f"run-{dataset_id}"}],
    }
    path.write_text("window.REVIEW_DATA = " + json.dumps(bundle) + ";\n", encoding="utf-8")


def export_file(path: Path, *, dataset_id: str, labels: list[str]) -> None:
    write_json(
        path,
        {
            "kind": "non_holdout_review_export",
            "dataset_id": dataset_id,
            "reviews": {
                f"kg::{label}": {
                    "benchmark_disposition": "included",
                    "pipeline_assessment": "accepted",
                    "preferred_question": f"Human question for {label}?",
                }
                for label in labels
            },
        },
    )


class MultiExportReviewUpdateTests(unittest.TestCase):
    def test_a_round_exported_per_team_is_one_update_that_names_every_export(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            previous = previous_snapshot(root)
            outdir = root / "v2"

            bundle_a, bundle_b = root / "a.js", root / "b.js"
            export_a1, export_a2, export_b = root / "a1.json", root / "a2.json", root / "b.json"
            bundle_file(bundle_a, dataset_id="dataset-a", labels=["a-one", "a-two"])
            bundle_file(bundle_b, dataset_id="dataset-b", labels=["b-one"])
            # Two teams reviewed dataset A, each owning different pairs.
            export_file(export_a1, dataset_id="dataset-a", labels=["a-one"])
            export_file(export_a2, dataset_id="dataset-a", labels=["a-two"])
            export_file(export_b, dataset_id="dataset-b", labels=["b-one"])

            with patch.object(
                sys,
                "argv",
                [
                    "update_from_initial_review.py",
                    "--previous-benchmark", str(previous),
                    "--bundle", str(bundle_a), "--reviews", str(export_a1),
                    "--bundle", str(bundle_a), "--reviews", str(export_a2),
                    "--bundle", str(bundle_b), "--reviews", str(export_b),
                    "--outdir", str(outdir),
                ],
            ):
                update_from_initial_review.main()

            rows = build_benchmark.read_jsonl(outdir / "included.jsonl")
            self.assertEqual({row["query_label"] for row in rows}, {"a-one", "a-two", "b-one"})

            manifest = json.loads((outdir / "manifest.json").read_text())
            self.assertEqual(manifest["update_type"], "multi_export_review_update")
            self.assertEqual(manifest["dataset_id"], ["dataset-a", "dataset-b"])
            # A merged export's filename carries its reviewing group's ID, so
            # the manifest names each export by content digest instead.
            digests = [stage["source_review_export_digest"] for stage in manifest["review_stages"]]
            self.assertEqual(
                digests,
                [
                    "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()
                    for path in (export_a1, export_a2, export_b)
                ],
            )
            self.assertNotIn("source_review_export", manifest)
            self.assertEqual(
                [stage["applied_reviews"] for stage in manifest["review_stages"]], [1, 1, 1]
            )
            self.assertEqual(manifest["counts"]["applied_normal_reviews"], 3)
            # previous_benchmark still names the real snapshot, not a scratch
            # directory from a chain of throwaway builds.
            self.assertEqual(manifest["previous_benchmark"], str(previous))

    def test_a_single_export_keeps_the_scalar_manifest_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            previous = previous_snapshot(root)
            outdir = root / "v2"
            bundle, export = root / "a.js", root / "a.json"
            bundle_file(bundle, dataset_id="dataset-a", labels=["a-one"])
            export_file(export, dataset_id="dataset-a", labels=["a-one"])

            with patch.object(
                sys,
                "argv",
                [
                    "update_from_initial_review.py",
                    "--previous-benchmark", str(previous),
                    "--bundle", str(bundle),
                    "--reviews", str(export),
                    "--outdir", str(outdir),
                ],
            ):
                update_from_initial_review.main()

            manifest = json.loads((outdir / "manifest.json").read_text())
            self.assertEqual(manifest["update_type"], "normal_review_update")
            self.assertEqual(manifest["source_bundle"], str(bundle))
            self.assertEqual(
                manifest["source_review_export_digest"],
                "sha256:" + hashlib.sha256(export.read_bytes()).hexdigest(),
            )
            self.assertEqual(manifest["dataset_id"], "dataset-a")

    def test_a_pair_applied_by_an_earlier_stage_is_still_an_overlap(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            previous = previous_snapshot(root)
            bundle, export = root / "a.js", root / "a.json"
            bundle_file(bundle, dataset_id="dataset-a", labels=["a-one"])
            export_file(export, dataset_id="dataset-a", labels=["a-one"])

            with patch.object(
                sys,
                "argv",
                [
                    "update_from_initial_review.py",
                    "--previous-benchmark", str(previous),
                    "--bundle", str(bundle), "--reviews", str(export),
                    "--bundle", str(bundle), "--reviews", str(export),
                    "--outdir", str(root / "v2"),
                ],
            ):
                with self.assertRaisesRegex(ValueError, "already-reviewed pairs"):
                    update_from_initial_review.main()

    def test_unpaired_bundles_and_exports_are_refused(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            previous = previous_snapshot(root)
            bundle, export = root / "a.js", root / "a.json"
            bundle_file(bundle, dataset_id="dataset-a", labels=["a-one"])
            export_file(export, dataset_id="dataset-a", labels=["a-one"])

            with patch.object(
                sys,
                "argv",
                [
                    "update_from_initial_review.py",
                    "--previous-benchmark", str(previous),
                    "--bundle", str(bundle), "--bundle", str(bundle),
                    "--reviews", str(export),
                    "--outdir", str(root / "v2"),
                ],
            ):
                with self.assertRaisesRegex(ValueError, "must be paired in order"):
                    update_from_initial_review.main()


if __name__ == "__main__":
    unittest.main()
