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


def _production_config(tmp_path: Path) -> dict[str, object]:
    config = _config(tmp_path)
    notice = tmp_path / "approved-notice.txt"
    summary = tmp_path / "approved-summary.txt"
    statement = tmp_path / "approved-statement.txt"
    notice.write_text("Approved participant notice.", encoding="utf-8")
    summary.write_text("Approved consent summary.", encoding="utf-8")
    statement.write_text("Approved consent statement.", encoding="utf-8")
    config.update(
        {
            "TESTING": False,
            "PRIVACY_NOTICE_VERSION": "approved-notice-v1",
            "PRIVACY_NOTICE_PATH": notice,
            "CONSENT_STATEMENT_VERSION": "approved-statement-v1",
            "CONSENT_SUMMARY_PATH": summary,
            "CONSENT_STATEMENT_PATH": statement,
        }
    )
    return config


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


def test_non_testing_startup_rejects_the_synthetic_override(tmp_path: Path) -> None:
    config = _production_config(tmp_path)
    config["ALLOW_SYNTHETIC_PRIVACY_NOTICE"] = True
    with pytest.raises(RuntimeError, match="restricted to the test environment"):
        create_app(config)


def test_non_testing_startup_rejects_missing_consent_configuration(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    config["TESTING"] = False
    with pytest.raises(RuntimeError, match="controller-approved privacy notice"):
        create_app(config)


def test_non_testing_startup_accepts_complete_file_backed_copy(tmp_path: Path) -> None:
    app = create_app(_production_config(tmp_path))
    try:
        assert app.config["PRIVACY_NOTICE_BODY"] == "Approved participant notice."
        assert app.config["CONSENT_SUMMARY_BODY"] == "Approved consent summary."
        assert app.config["CONSENT_STATEMENT_BODY"] == "Approved consent statement."
        page = app.test_client().get("/participant-notice")
        assert b"Approved participant notice." in page.data
    finally:
        app.extensions["musparql_email_dispatcher"].shutdown()
        app.extensions["musparql_engine"].dispose()


@pytest.mark.parametrize(
    ("version_key", "version"),
    (
        ("PRIVACY_NOTICE_VERSION", "synthetic-notice-v1"),
        ("CONSENT_STATEMENT_VERSION", "synthetic-statement-v1"),
    ),
)
def test_non_testing_startup_rejects_synthetic_versions(
    tmp_path: Path, version_key: str, version: str
) -> None:
    config = _production_config(tmp_path)
    config[version_key] = version
    with pytest.raises(RuntimeError, match="synthetic .* is not allowed"):
        create_app(config)


def test_non_testing_startup_requires_file_backed_consent_copy(tmp_path: Path) -> None:
    config = _production_config(tmp_path)
    config["CONSENT_SUMMARY_PATH"] = None
    config["CONSENT_SUMMARY_BODY"] = "Inline copy is not sufficient."
    with pytest.raises(RuntimeError, match="must be loaded from restricted files"):
        create_app(config)


@pytest.mark.parametrize(
    ("body_key", "path_key", "label"),
    (
        ("PRIVACY_NOTICE_BODY", "PRIVACY_NOTICE_PATH", "privacy notice"),
        ("CONSENT_SUMMARY_BODY", "CONSENT_SUMMARY_PATH", "consent summary"),
        ("CONSENT_STATEMENT_BODY", "CONSENT_STATEMENT_PATH", "consent statement"),
    ),
)
def test_unreadable_copy_file_fails_startup(
    tmp_path: Path, body_key: str, path_key: str, label: str
) -> None:
    config = _config(tmp_path)
    config.update(
        {
            "PRIVACY_NOTICE_VERSION": "test-notice-v1",
            "CONSENT_STATEMENT_VERSION": "test-statement-v1",
            body_key: None,
            path_key: tmp_path / "missing.txt",
        }
    )
    with pytest.raises(RuntimeError, match=rf"configured {label} cannot be read"):
        create_app(config)


@pytest.mark.parametrize(
    ("body_key", "path_key"),
    (
        ("PRIVACY_NOTICE_BODY", "PRIVACY_NOTICE_PATH"),
        ("CONSENT_SUMMARY_BODY", "CONSENT_SUMMARY_PATH"),
        ("CONSENT_STATEMENT_BODY", "CONSENT_STATEMENT_PATH"),
    ),
)
def test_empty_copy_file_fails_startup(
    tmp_path: Path, body_key: str, path_key: str
) -> None:
    config = _production_config(tmp_path)
    empty = tmp_path / f"empty-{path_key}.txt"
    empty.write_text("  \n", encoding="utf-8")
    config.update({body_key: None, path_key: empty})
    with pytest.raises(RuntimeError, match="privacy notice|configured together"):
        create_app(config)


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
