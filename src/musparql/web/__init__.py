"""Flask application factory for the invitation-only Musparql portal."""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from flask import Flask

from musparql.database import create_database_engine, session_factory
from musparql.database.migrations import upgrade_database
from musparql.database.models import Reviewer

from .auth import AuthService, DigestRateLimiter
from .email import SyntheticEmailSender
from .security import install_security


def create_app(test_config: dict[str, Any] | None = None) -> Flask:
    app = Flask(__name__, template_folder="templates")
    app.config.from_mapping(
        DATABASE_PATH=os.environ.get("MUSPARQL_DATABASE_PATH"),
        APP_SECRET=os.environ.get("MUSPARQL_APP_SECRET"),
        OWNER_REVIEWER_ID=os.environ.get("MUSPARQL_OWNER_REVIEWER_ID"),
        AUTH_COOKIE_NAME="musparql_session",
        LOGIN_CHALLENGE_COOKIE_NAME="musparql_login_challenge",
        CSRF_COOKIE_NAME="musparql_csrf",
        COOKIE_SECURE=True,
        LOGIN_CODE_TTL_SECONDS=15 * 60,
        LOGIN_CODE_MAX_ATTEMPTS=5,
        LOGIN_REQUEST_WINDOW_SECONDS=15 * 60,
        LOGIN_REQUESTS_PER_ADDRESS=3,
        LOGIN_REQUESTS_PER_CONTEXT=10,
        REVIEWER_IDLE_SECONDS=2 * 60 * 60,
        REVIEWER_ABSOLUTE_SECONDS=24 * 60 * 60,
        REMEMBERED_IDLE_SECONDS=7 * 24 * 60 * 60,
        REMEMBERED_ABSOLUTE_SECONDS=30 * 24 * 60 * 60,
        OWNER_IDLE_SECONDS=2 * 60 * 60,
        OWNER_ABSOLUTE_SECONDS=12 * 60 * 60,
        OWNER_RECENT_AUTH_SECONDS=15 * 60,
        MAX_CONTENT_LENGTH=64 * 1024,
        AUTO_UPGRADE_DATABASE=False,
        ALLOW_SYNTHETIC_EMAIL=os.environ.get("MUSPARQL_ALLOW_SYNTHETIC_EMAIL") == "1",
    )
    if test_config:
        app.config.update(test_config)

    _validate_config(app)
    database_path = Path(app.config["DATABASE_PATH"]).expanduser().resolve()
    if app.config["AUTO_UPGRADE_DATABASE"]:
        upgrade_database(database_path)
    if not database_path.is_file():
        raise RuntimeError("The configured database does not exist; run musparql-db upgrade first")

    engine = create_database_engine(database_path)
    sessions = session_factory(engine)
    with sessions() as session:
        owner = session.get(Reviewer, app.config["OWNER_REVIEWER_ID"])
        if owner is None or owner.status != "active":
            engine.dispose()
            raise RuntimeError("The configured owner account does not exist or is not active")
    sender = app.config.get("EMAIL_SENDER")
    if sender is None:
        if not app.config["ALLOW_SYNTHETIC_EMAIL"]:
            engine.dispose()
            raise RuntimeError(
                "No email sender is configured; synthetic email must be explicitly enabled"
            )
        sender = SyntheticEmailSender()
    limiter = DigestRateLimiter(
        secret=app.config["APP_SECRET"].encode("utf-8"),
        window_seconds=app.config["LOGIN_REQUEST_WINDOW_SECONDS"],
        address_limit=app.config["LOGIN_REQUESTS_PER_ADDRESS"],
        context_limit=app.config["LOGIN_REQUESTS_PER_CONTEXT"],
    )
    app.extensions["musparql_engine"] = engine
    app.extensions["musparql_sessions"] = sessions
    app.extensions["musparql_email_sender"] = sender
    app.extensions["musparql_auth"] = AuthService(
        sessions=sessions,
        sender=sender,
        limiter=limiter,
        config=app.config,
    )

    install_security(app)
    from .routes import portal

    app.register_blueprint(portal)
    return app


def _validate_config(app: Flask) -> None:
    missing = [
        name
        for name in ("DATABASE_PATH", "APP_SECRET", "OWNER_REVIEWER_ID")
        if not app.config.get(name)
    ]
    if missing:
        raise RuntimeError("Missing required application configuration: " + ", ".join(missing))
    if len(app.config["APP_SECRET"].encode("utf-8")) < 32:
        raise RuntimeError("APP_SECRET must contain at least 32 UTF-8 bytes")
    owner_id = app.config["OWNER_REVIEWER_ID"]
    if not isinstance(owner_id, str) or not owner_id.startswith("reviewer-"):
        raise RuntimeError("OWNER_REVIEWER_ID must be a pseudonymous reviewer ID")


__all__ = ["create_app"]
