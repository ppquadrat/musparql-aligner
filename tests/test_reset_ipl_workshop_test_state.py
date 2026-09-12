from __future__ import annotations

from pathlib import Path
import sqlite3

from musparql.database.migrations import upgrade_database
from scripts.reset_ipl_workshop_test_state import build_clean_database


def test_clean_database_preserves_workshop_and_removes_participant_state(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source.sqlite3"
    upgrade_database(source)
    connection = sqlite3.connect(source)
    try:
        connection.execute("PRAGMA foreign_keys = ON")
        connection.executescript(
            """
            INSERT INTO reviewers (
              id, name, title, first_name, last_name, affiliation,
              email_display, email_normalized, status, disabled_from_status,
              created_at, updated_at, privacy_notice_version,
              privacy_notice_acknowledged_at, registration_method,
              email_verified_at, consent_statement_version, consented_at
            ) VALUES
              ('reviewer-0001', 'Owner', '', 'Test', 'Owner', '',
               'owner@example.invalid', 'owner@example.invalid', 'active', NULL,
               '2026-09-12T00:00:00Z', '2026-09-12T00:00:00Z', NULL, NULL,
               'email_invitation', '2026-09-12T00:00:00Z', NULL, NULL),
              ('reviewer-1234', 'Participant', '', 'Test', 'Participant', '',
               'participant@example.invalid', 'participant@example.invalid',
               'active', NULL, '2026-09-12T00:00:00Z',
               '2026-09-12T00:00:00Z', NULL, NULL, 'workshop_code', NULL,
               NULL, NULL);
            INSERT INTO workshop_rounds VALUES (
              'ipl-2026', 'IPL', 'open', '2026-09-12T00:00:00Z',
              '2026-09-19T00:00:00Z', 30, 0, '2026-09-12T00:00:00Z'
            );
            INSERT INTO workshop_entry_codes VALUES (
              'entry-1', 'ipl-2026', 'digest', '2026-09-19T00:00:00Z',
              30, 1, NULL, '2026-09-12T00:00:00Z'
            );
            INSERT INTO workshop_entry_redemptions VALUES (
              'redemption-1', 'entry-1', 'reviewer-1234',
              '2026-09-12T00:00:00Z'
            );
            """
        )
        connection.commit()
    finally:
        connection.close()

    output = tmp_path / "clean.sqlite3"
    build_clean_database(source, output, owner_reviewer_id="reviewer-0001")

    connection = sqlite3.connect(output)
    try:
        assert connection.execute("SELECT id FROM reviewers").fetchall() == [
            ("reviewer-0001",)
        ]
        assert connection.execute(
            "SELECT redemption_count FROM workshop_entry_codes"
        ).fetchone() == (0,)
        assert connection.execute(
            "SELECT COUNT(*) FROM workshop_entry_redemptions"
        ).fetchone() == (0,)
        assert connection.execute("PRAGMA integrity_check").fetchone() == ("ok",)
    finally:
        connection.close()
