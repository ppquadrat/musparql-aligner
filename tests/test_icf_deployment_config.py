from __future__ import annotations

from pathlib import Path

import pytest

from musparql.database import create_database_engine, session_factory
from musparql.database.migrations import upgrade_database
from musparql.database.models import Reviewer
from musparql.web import create_app
from musparql.web.auth import timestamp, utc_now
from musparql.web.email import SyntheticEmailSender


SECRET = "synthetic-deployment-secret-at-least-32-bytes"
OWNER_ID = "reviewer-0001"


def _database(path: Path) -> None:
    upgrade_database(path)
    engine = create_database_engine(path)
    sessions = session_factory(engine)
    now = timestamp(utc_now())
    with sessions.begin() as session:
        session.add(
            Reviewer(
                id=OWNER_ID,
                name="Synthetic Owner",
                affiliation="Synthetic Institute",
                email_display="owner@example.invalid",
                email_normalized="owner@example.invalid",
                status="active",
                created_at=now,
                updated_at=now,
                privacy_notice_version=None,
                privacy_notice_acknowledged_at=None,
                registration_method="email_invitation",
                email_verified_at=now,
                consent_statement_version=None,
                consented_at=None,
            )
        )
    engine.dispose()


def _config(tmp_path: Path) -> dict[str, object]:
    database_path = tmp_path / "deployment.sqlite3"
    _database(database_path)
    return {
        "TESTING": True,
        "DATABASE_PATH": database_path,
        "APP_SECRET": SECRET,
        "OWNER_REVIEWER_ID": OWNER_ID,
        "COOKIE_SECURE": False,
        "EMAIL_SENDER": SyntheticEmailSender(),
    }


def test_proxy_requires_an_explicit_trusted_host(tmp_path: Path) -> None:
    config = _config(tmp_path)
    config.update({"BEHIND_SINGLE_PROXY": True, "TRUSTED_HOSTS": None})
    with pytest.raises(RuntimeError, match="TRUSTED_HOSTS is required"):
        create_app(config)


def test_trusted_host_rejects_an_unexpected_host(tmp_path: Path) -> None:
    config = _config(tmp_path)
    config.update(
        {
            "BEHIND_SINGLE_PROXY": True,
            "TRUSTED_HOSTS": ["musparql.industrycommons.net"],
        }
    )
    app = create_app(config)
    try:
        assert app.test_client().get("/", headers={"Host": "example.invalid"}).status_code == 400
        assert (
            app.test_client()
            .get("/", headers={"Host": "musparql.industrycommons.net"})
            .status_code
            == 200
        )
    finally:
        app.extensions["musparql_email_dispatcher"].shutdown()
        app.extensions["musparql_engine"].dispose()


def test_privacy_notice_can_be_loaded_from_a_restricted_file(tmp_path: Path) -> None:
    config = _config(tmp_path)
    notice = tmp_path / "participant-notice.txt"
    notice.write_text("Synthetic participant notice from a file.", encoding="utf-8")
    config.update(
        {
            "PRIVACY_NOTICE_VERSION": "test-notice-v1",
            "PRIVACY_NOTICE_BODY": None,
            "PRIVACY_NOTICE_PATH": notice,
        }
    )
    app = create_app(config)
    try:
        assert app.config["PRIVACY_NOTICE_BODY"] == notice.read_text(encoding="utf-8")
    finally:
        app.extensions["musparql_email_dispatcher"].shutdown()
        app.extensions["musparql_engine"].dispose()


def test_consent_copy_can_be_loaded_from_restricted_files(tmp_path: Path) -> None:
    config = _config(tmp_path)
    summary = tmp_path / "consent-summary.txt"
    statement = tmp_path / "consent-statement.txt"
    summary.write_text("Synthetic summary from a file.\n", encoding="utf-8")
    statement.write_text("Synthetic affirmative statement.\n", encoding="utf-8")
    config.update(
        {
            "CONSENT_STATEMENT_VERSION": "synthetic-consent-v1",
            "CONSENT_SUMMARY_BODY": None,
            "CONSENT_SUMMARY_PATH": summary,
            "CONSENT_STATEMENT_BODY": None,
            "CONSENT_STATEMENT_PATH": statement,
        }
    )
    app = create_app(config)
    try:
        assert app.config["CONSENT_SUMMARY_BODY"] == "Synthetic summary from a file."
        assert app.config["CONSENT_STATEMENT_BODY"] == "Synthetic affirmative statement."
    finally:
        app.extensions["musparql_email_dispatcher"].shutdown()
        app.extensions["musparql_engine"].dispose()


def test_application_secret_can_be_loaded_from_a_restricted_file(tmp_path: Path) -> None:
    config = _config(tmp_path)
    secret = tmp_path / "app-secret"
    secret.write_text(SECRET + "\n", encoding="utf-8")
    config.update({"APP_SECRET": None, "APP_SECRET_PATH": secret})
    app = create_app(config)
    try:
        assert app.config["APP_SECRET"] == SECRET
    finally:
        app.extensions["musparql_email_dispatcher"].shutdown()
        app.extensions["musparql_engine"].dispose()
