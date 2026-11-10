from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from musparql.sparql_corrections import build_candidate, candidate_id
from musparql.sparql_versions import sparql_hash

from scripts import retriage_sparql_corrections as retriage

SPARQL = "SELECT ?s WHERE { ?s ?p ?o }"


def write_jsonl(path: Path, rows: list[dict]) -> None:
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")


def read_jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def query_record(label: str, observation: dict | None, *, kg_id: str = "synthkg") -> dict:
    record: dict = {
        "kg_id": kg_id,
        "query_label": label,
        "query_id": f"{kg_id}__{label}",
        "sparql_clean": SPARQL,
        "sparql_hash": sparql_hash(SPARQL),
        "evidence": [],
    }
    if observation is not None:
        record["execution_history"] = [
            {
                "sparql_version": 0,
                "sparql_hash": sparql_hash(SPARQL),
                "ran_at": "2026-11-01T00:00:00+00:00",
                **observation,
            }
        ]
    return record


def candidate_for(record: dict) -> dict:
    observation = (record.get("execution_history") or [{}])[-1]
    failure = {
        **observation,
        "kg_id": record["kg_id"],
        "query_id": record["query_id"],
        "query_label": record["query_label"],
        "sparql_version": 0,
        "sparql_hash": sparql_hash(SPARQL),
        "observed_at": observation.get("ran_at"),
    }
    return build_candidate(failure, record)


def run(argv: list[str]) -> int:
    with patch.object(sys, "argv", ["retriage_sparql_corrections.py", *argv]):
        return retriage.main()


class RetriageTests(unittest.TestCase):
    def test_a_gateway_block_is_reclassified_without_touching_the_observation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            queries = root / "kg_queries.jsonl"
            candidates = root / "candidates.jsonl"
            record = query_record(
                "synth-0001",
                {"status": "query_error", "http_status": 403, "error": "Just a moment..."},
            )
            write_jsonl(queries, [record])
            stale = candidate_for(record)
            # The reading the old classifier produced for this observation,
            # with the candidate id it would have carried.
            stale_triage = {
                "reason_code": "endpoint_rejected_query",
                "category": "likely_correction",
                "priority": "high",
                "summary": "stale",
            }
            stale["triage"] = stale_triage
            stale["candidate_id"] = candidate_id(
                {
                    "kg_id": record["kg_id"],
                    "query_id": record["query_id"],
                    "sparql_version": 0,
                    "sparql_hash": sparql_hash(SPARQL),
                },
                stale_triage,
            )
            write_jsonl(candidates, [stale])

            self.assertEqual(
                run(["--queries", str(queries), "--candidates", str(candidates), "--no-holdout"]), 0
            )
            rows = read_jsonl(candidates)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["triage"]["reason_code"], "gateway_blocked_request")
            self.assertEqual(rows[0]["triage"]["category"], "investigate")
            # The evidence is untouched: same status, same http status, and the
            # observation's own timestamp is kept as captured_at.
            self.assertEqual(rows[0]["execution"]["status"], "query_error")
            self.assertEqual(rows[0]["execution"]["http_status"], 403)
            self.assertEqual(rows[0]["captured_at"], "2026-11-01T00:00:00+00:00")
            self.assertEqual(rows[0]["capture_provenance"]["producer"], retriage.PRODUCER)
            # Reason code is part of candidate identity, so the id moves with it.
            self.assertNotEqual(rows[0]["candidate_id"], stale["candidate_id"])

    def test_unobserved_pairs_are_left_out_unless_asked_for(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            queries = root / "kg_queries.jsonl"
            candidates = root / "candidates.jsonl"
            observed = query_record("synth-0001", {"status": "ok", "result_count": 3})
            never_run = query_record("synth-0002", None)
            write_jsonl(queries, [observed, never_run])
            write_jsonl(candidates, [candidate_for(observed)])
            argv = ["--queries", str(queries), "--candidates", str(candidates), "--no-holdout"]

            self.assertEqual(run(argv), 0)
            self.assertEqual([row["query_label"] for row in read_jsonl(candidates)], ["synth-0001"])

            self.assertEqual(run([*argv, "--include-unobserved"]), 0)
            rows = read_jsonl(candidates)
            self.assertEqual([row["query_label"] for row in rows], ["synth-0001", "synth-0002"])
            added = next(row for row in rows if row["query_label"] == "synth-0002")
            self.assertEqual(added["triage"]["reason_code"], "not_attempted")
            self.assertEqual(added["triage"]["category"], "needs_observation")

    def test_a_dry_run_writes_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            queries = root / "kg_queries.jsonl"
            candidates = root / "candidates.jsonl"
            record = query_record("synth-0001", {"status": "request_error", "error": "ReadTimeout"})
            write_jsonl(queries, [record])
            stale = candidate_for(record)
            stale["triage"] = {
                "reason_code": "execution_failure",
                "category": "investigate",
                "priority": "medium",
                "summary": "stale",
            }
            write_jsonl(candidates, [stale])
            before = candidates.read_text(encoding="utf-8")

            self.assertEqual(
                run([
                    "--queries", str(queries), "--candidates", str(candidates),
                    "--no-holdout", "--dry-run",
                ]),
                0,
            )
            self.assertEqual(candidates.read_text(encoding="utf-8"), before)

    def test_a_holdout_selected_pair_is_never_written(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            queries = root / "kg_queries.jsonl"
            candidates = root / "candidates.jsonl"
            selectors = root / "selectors.jsonl"
            held = query_record("synth-0002", {"status": "ok", "result_count": 1})
            open_pair = query_record("synth-0001", {"status": "ok", "result_count": 1})
            write_jsonl(queries, [open_pair, held])
            write_jsonl(candidates, [candidate_for(open_pair), candidate_for(held)])
            write_jsonl(selectors, [{"kg_id": "synthkg", "query_id": held["query_id"]}])

            self.assertEqual(
                run([
                    "--queries", str(queries), "--candidates", str(candidates),
                    "--holdout-selectors", str(selectors),
                ]),
                0,
            )
            self.assertEqual([row["query_label"] for row in read_jsonl(candidates)], ["synth-0001"])


if __name__ == "__main__":
    unittest.main()
