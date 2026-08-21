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
from .email import AsyncEmailDispatcher, SyntheticEmailSender
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
        LOGIN_LIMITER_MAX_ADDRESS_KEYS=4096,
        LOGIN_LIMITER_MAX_CONTEXT_KEYS=4096,
        LOGIN_DELIVERY_MAX_PENDING=256,
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
        EXPERTISE_SUGGESTIONS_PATH=os.environ.get(
            "MUSPARQL_EXPERTISE_SUGGESTIONS_PATH",
            "catalog/expertise_domain_suggestions.yaml",
        ),
        LANGUAGE_OPTIONS_PATH=os.environ.get(
            "MUSPARQL_LANGUAGE_OPTIONS_PATH", "catalog/language_options.json"
        ),
        ASSIGNMENT_BUNDLE_ROOT=os.environ.get(
            "MUSPARQL_ASSIGNMENT_BUNDLE_ROOT", "var/review/bundles"
        ),
        REVIEW_WORKBENCH_ROOT=os.environ.get(
            "MUSPARQL_REVIEW_WORKBENCH_ROOT", "review"
        ),
        LINGUISTIC_WORKBENCH_ROOT=os.environ.get(
            "MUSPARQL_LINGUISTIC_WORKBENCH_ROOT", "review/linguistic"
        ),
        SUBMISSION_ROOT=os.environ.get(
            "MUSPARQL_SUBMISSION_ROOT", "var/review/submissions"
        ),
        CANDIDATE_ROOT=os.environ.get(
            "MUSPARQL_CANDIDATE_ROOT", "var/review/candidates"
        ),
        REVIEW_EXPORT_SCHEMA_PATH=os.environ.get(
            "MUSPARQL_REVIEW_EXPORT_SCHEMA_PATH", "schemas/review_export.schema.json"
        ),
        LINGUISTIC_EXPORT_SCHEMA_PATH=os.environ.get(
            "MUSPARQL_LINGUISTIC_EXPORT_SCHEMA_PATH", "schemas/linguistic_annotation_export.schema.json"
        ),
        PRIVACY_NOTICE_VERSION=os.environ.get("MUSPARQL_PRIVACY_NOTICE_VERSION"),
        PRIVACY_NOTICE_BODY=os.environ.get("MUSPARQL_PRIVACY_NOTICE_BODY"),
        ALLOW_SYNTHETIC_PRIVACY_NOTICE=(
            os.environ.get("MUSPARQL_ALLOW_SYNTHETIC_PRIVACY_NOTICE") == "1"
        ),
    )
    if test_config:
        app.config.update(test_config)

    if app.config["TESTING"] or app.config["ALLOW_SYNTHETIC_PRIVACY_NOTICE"]:
        app.config["PRIVACY_NOTICE_VERSION"] = app.config.get(
            "PRIVACY_NOTICE_VERSION"
        ) or "synthetic-development-v1"
        app.config["PRIVACY_NOTICE_BODY"] = app.config.get("PRIVACY_NOTICE_BODY") or (
            "Synthetic development notice. Do not enter real personal data. "
            "This notice is only for testing the Musparql onboarding workflow."
        )

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
        max_address_keys=app.config["LOGIN_LIMITER_MAX_ADDRESS_KEYS"],
        max_context_keys=app.config["LOGIN_LIMITER_MAX_CONTEXT_KEYS"],
    )
    dispatcher = AsyncEmailDispatcher(max_pending=app.config["LOGIN_DELIVERY_MAX_PENDING"])
    app.extensions["musparql_engine"] = engine
    app.extensions["musparql_sessions"] = sessions
    app.extensions["musparql_email_sender"] = sender
    app.extensions["musparql_email_dispatcher"] = dispatcher
    app.extensions["musparql_auth"] = AuthService(
        sessions=sessions,
        sender=sender,
        dispatcher=dispatcher,
        limiter=limiter,
        config=app.config,
    )
    from .profile import ProfileService
    from .assignments import AssignmentService
    from .submissions import ProcessingService, SubmissionService

    app.extensions["musparql_profiles"] = ProfileService(
        sessions=sessions,
        notice_version=app.config["PRIVACY_NOTICE_VERSION"],
        suggestions_path=Path(app.config["EXPERTISE_SUGGESTIONS_PATH"]).expanduser().resolve(),
        language_options_path=Path(app.config["LANGUAGE_OPTIONS_PATH"]).expanduser().resolve(),
    )
    app.extensions["musparql_assignments"] = AssignmentService(
        sessions=sessions,
        bundle_root=Path(app.config["ASSIGNMENT_BUNDLE_ROOT"]).expanduser().resolve(),
    )
    app.extensions["musparql_submissions"] = SubmissionService(
        sessions=sessions,
        assignments=app.extensions["musparql_assignments"],
        submission_root=Path(app.config["SUBMISSION_ROOT"]).expanduser().resolve(),
        review_schema_path=Path(app.config["REVIEW_EXPORT_SCHEMA_PATH"]).expanduser().resolve(),
        linguistic_schema_path=Path(app.config["LINGUISTIC_EXPORT_SCHEMA_PATH"]).expanduser().resolve(),
    )
    app.extensions["musparql_processing"] = ProcessingService(
        sessions=sessions,
        submission_root=Path(app.config["SUBMISSION_ROOT"]).expanduser().resolve(),
        candidate_root=Path(app.config["CANDIDATE_ROOT"]).expanduser().resolve(),
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
    if not app.config.get("PRIVACY_NOTICE_VERSION") or not app.config.get(
        "PRIVACY_NOTICE_BODY"
    ):
        raise RuntimeError(
            "A controller-approved privacy notice version and body must be configured"
        )
    if (
        str(app.config["PRIVACY_NOTICE_VERSION"]).startswith("synthetic-")
        and not (app.config["TESTING"] or app.config["ALLOW_SYNTHETIC_PRIVACY_NOTICE"])
    ):
        raise RuntimeError("A synthetic privacy notice is not allowed in this environment")
    suggestions_path = Path(app.config["EXPERTISE_SUGGESTIONS_PATH"]).expanduser().resolve()
    if not suggestions_path.is_file():
        raise RuntimeError("The configured expertise suggestion snapshot does not exist")
    language_options_path = Path(app.config["LANGUAGE_OPTIONS_PATH"]).expanduser().resolve()
    if not language_options_path.is_file():
        raise RuntimeError("The configured language option snapshot does not exist")
    workbench_root = Path(app.config["REVIEW_WORKBENCH_ROOT"]).expanduser().resolve()
    missing_workbench_files = [
        name for name in ("index.html", "styles.css", "app.js", "host_context.js")
        if not (workbench_root / name).is_file()
    ]
    if missing_workbench_files:
        raise RuntimeError(
            "The configured review workbench is incomplete: "
            + ", ".join(missing_workbench_files)
        )
    linguistic_root = Path(app.config["LINGUISTIC_WORKBENCH_ROOT"]).expanduser().resolve()
    missing_linguistic_files = [
        name for name in ("index.html", "styles.css", "app.js")
        if not (linguistic_root / name).is_file()
    ]
    if missing_linguistic_files:
        raise RuntimeError(
            "The configured linguistic workbench is incomplete: "
            + ", ".join(missing_linguistic_files)
        )
    owner_id = app.config["OWNER_REVIEWER_ID"]
    if not isinstance(owner_id, str) or not owner_id.startswith("reviewer-"):
        raise RuntimeError("OWNER_REVIEWER_ID must be a pseudonymous reviewer ID")
    for name in ("REVIEW_EXPORT_SCHEMA_PATH", "LINGUISTIC_EXPORT_SCHEMA_PATH"):
        if not Path(app.config[name]).expanduser().resolve().is_file():
            raise RuntimeError(f"The configured schema does not exist: {name}")


__all__ = ["create_app"]
