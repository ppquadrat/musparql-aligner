from __future__ import annotations

import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts import fetch_review_receipts as fetch


def run(argv: list[str]) -> int:
    with patch.object(sys, "argv", ["fetch_review_receipts.py", *argv]):
        return fetch.main()


class ReceiptFetchTests(unittest.TestCase):
    def test_a_round_identifier_cannot_be_a_path(self) -> None:
        for bad in ("../etc", "a/b", ".hidden"):
            with self.assertRaisesRegex(ValueError, "plain identifier"):
                run(["--round", bad, "--dry-run"])

    def test_digests_are_computed_over_the_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "r.json"
            path.write_bytes(b'{"a": 1}')
            self.assertEqual(
                fetch.sha256_file(path),
                "sha256:" + hashlib.sha256(b'{"a": 1}').hexdigest(),
            )

    def test_a_receipt_matching_a_recorded_digest_verifies(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp) / "restore"
            source.mkdir()
            body = b'{"kind": "non_holdout_review_export"}'
            (source / "assignment-aaa-rev1-receipt-bbb.json").write_bytes(body)
            digest = "sha256:" + hashlib.sha256(body).hexdigest()
            (source / fetch.MANIFEST_NAME).write_text(
                json.dumps([{"id": "sub-1", "export_digest": digest}]), encoding="utf-8"
            )
            with patch.object(fetch, "LOCAL_ROOT", Path(tmp) / "local"):
                self.assertEqual(
                    run(["--round", "synthetic-round", "--from-dir", str(source)]), 0
                )
                copied = Path(tmp) / "local" / "synthetic-round"
                self.assertTrue((copied / "assignment-aaa-rev1-receipt-bbb.json").exists())
                # The manifest travels with the copies so a later run can verify
                # them without contacting the server again.
                self.assertTrue((copied / fetch.MANIFEST_NAME).exists())

    def test_a_receipt_matching_no_recorded_digest_is_reported_and_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp) / "restore"
            source.mkdir()
            (source / "assignment-aaa-rev1-receipt-bbb.json").write_bytes(b"{}")
            (source / fetch.MANIFEST_NAME).write_text(
                json.dumps([{"id": "sub-1", "export_digest": "sha256:" + "0" * 64}]),
                encoding="utf-8",
            )
            with patch.object(fetch, "LOCAL_ROOT", Path(tmp) / "local"):
                self.assertEqual(
                    run(["--round", "synthetic-round", "--from-dir", str(source)]), 1
                )

    def test_purge_removes_the_round_and_nothing_else(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "local"
            (root / "synthetic-round").mkdir(parents=True)
            (root / "synthetic-round" / "r.json").write_text("{}", encoding="utf-8")
            (root / "other-round").mkdir()
            (root / "other-round" / "r.json").write_text("{}", encoding="utf-8")
            with patch.object(fetch, "LOCAL_ROOT", root):
                self.assertEqual(run(["--round", "synthetic-round", "--purge"]), 0)
            self.assertFalse((root / "synthetic-round").exists())
            self.assertTrue((root / "other-round" / "r.json").exists())

    def test_purge_is_quiet_when_there_is_nothing_there(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with patch.object(fetch, "LOCAL_ROOT", Path(tmp) / "local"):
                self.assertEqual(run(["--round", "synthetic-round", "--purge"]), 0)

    def test_the_manifest_query_selects_no_reviewer_identity(self) -> None:
        """The build needs digests, not attribution, so the query asks for none."""
        for field in ("reviewer_id", "submitted_by_reviewer_id", "contributor_reviewer_ids"):
            self.assertNotIn(field, fetch.MANIFEST_SQL)

    def test_the_server_is_only_read(self) -> None:
        self.assertIn("-readonly", fetch.MANIFEST_SQL + " sqlite3 -readonly")
        source = Path(fetch.__file__).read_text(encoding="utf-8")
        for destructive in ("rm -rf", "DELETE FROM", "UPDATE ", "DROP ", "restic"):
            self.assertNotIn(destructive, source)


if __name__ == "__main__":
    unittest.main()
