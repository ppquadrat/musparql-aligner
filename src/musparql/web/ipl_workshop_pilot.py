"""Run the frozen IPL workshop packages as a local synthetic participant pilot."""
from __future__ import annotations

import argparse
from datetime import timedelta
import json
from pathlib import Path
import shutil
import tempfile
from typing import Any, Callable

import yaml
from werkzeug.serving import BaseWSGIServer, make_server

from musparql.database import create_database_engine, session_factory
from musparql.database.migrations import upgrade_database
from musparql.database.models import Reviewer, WorkshopRound
from musparql.database.services import SeedSnapshotService
from musparql.workshop_packages import register_package_set

from . import create_app
from .auth import timestamp, utc_now
from .email import SyntheticEmailSender
from .local_hardening import _close_app


APP_SECRET = "synthetic-ipl-pilot-secret-with-at-least-32-bytes"
OWNER_ID = "reviewer-9000"
ROUND_ID = "ipl-2026"


def _mapping(path: Path, *, yaml_input: bool = False) -> dict[str, Any]:
    payload = (
        yaml.safe_load(path.read_text(encoding="utf-8-sig"))
        if yaml_input
        else json.loads(path.read_text(encoding="utf-8-sig"))
    )
    if not isinstance(payload, dict):
        raise ValueError(f"Expected an object in {path}")
    return payload


def _owner() -> Reviewer:
    now = timestamp(utc_now())
    return Reviewer(
        id=OWNER_ID,
        name="Synthetic IPL Pilot Owner",
        affiliation="",
        email_display="ipl-pilot-owner@example.invalid",
        email_normalized="ipl-pilot-owner@example.invalid",
        status="active",
        disabled_from_status=None,
        created_at=now,
        updated_at=now,
        privacy_notice_version=None,
        privacy_notice_acknowledged_at=None,
        registration_method="email_invitation",
        email_verified_at=now,
        consent_statement_version=None,
        consented_at=None,
    )


def prepare_workspace(
    root: Path,
    *,
    package_dir: Path,
    seed_snapshots: Path,
) -> Path:
    """Create an isolated open workshop and return its database path."""
    root = root.resolve()
    package_dir = package_dir.resolve()
    seed_snapshots = seed_snapshots.resolve()
    if root.exists() and (not root.is_dir() or any(root.iterdir())):
        raise ValueError("The IPL pilot workspace must be an empty directory")
    manifest_path = package_dir / "manifest.json"
    if not manifest_path.is_file():
        raise ValueError(f"IPL package manifest does not exist: {manifest_path}")

    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    bundle_root = root / "bundles"
    destination = bundle_root / package_dir.name
    bundle_root.mkdir(mode=0o700)
    shutil.copytree(package_dir, destination)

    database_path = root / "ipl-pilot.sqlite3"
    upgrade_database(database_path)
    engine = create_database_engine(database_path)
    sessions = session_factory(engine)
    now = utc_now()
    now_text = timestamp(now)
    manifest = _mapping(destination / "manifest.json")
    if manifest.get("workshop_round_id") != ROUND_ID:
        engine.dispose()
        raise ValueError(f"Expected workshop round {ROUND_ID}")
    try:
        with sessions.begin() as session:
            session.add(_owner())
            session.add(
                WorkshopRound(
                    id=ROUND_ID,
                    name="Synthetic rehearsal — IPL workshop 16 September 2026",
                    status="draft",
                    opens_at=timestamp(now - timedelta(minutes=5)),
                    closes_at=timestamp(now + timedelta(hours=6)),
                    max_participants=10,
                    allow_additional_assignments=False,
                    created_at=now_text,
                )
            )
            SeedSnapshotService.import_archive_in_session(
                session, _mapping(seed_snapshots, yaml_input=True)
            )
            register_package_set(manifest, session=session, bundle_root=bundle_root)
        with sessions.begin() as session:
            workshop_round = session.get(WorkshopRound, ROUND_ID)
            assert workshop_round is not None
            workshop_round.status = "open"
    finally:
        engine.dispose()
    return database_path


def _app_config(root: Path, database_path: Path) -> dict[str, Any]:
    project_root = Path(__file__).resolve().parents[3]
    return {
        "TESTING": True,
        "DATABASE_PATH": database_path,
        "APP_SECRET": APP_SECRET,
        "OWNER_REVIEWER_ID": OWNER_ID,
        "COOKIE_SECURE": False,
        "EMAIL_SENDER": SyntheticEmailSender(),
        "ASSIGNMENT_BUNDLE_ROOT": root / "bundles",
        "SUBMISSION_ROOT": root / "submissions",
        "CANDIDATE_ROOT": root / "candidates",
        "REVIEW_WORKBENCH_ROOT": project_root / "review",
        "LINGUISTIC_WORKBENCH_ROOT": project_root / "review/linguistic",
        "REVIEW_EXPORT_SCHEMA_PATH": project_root / "schemas/review_export.schema.json",
        "LINGUISTIC_EXPORT_SCHEMA_PATH": (
            project_root / "schemas/linguistic_annotation_export.schema.json"
        ),
        "EXPERTISE_SUGGESTIONS_PATH": (
            project_root / "catalog/expertise_domain_suggestions.yaml"
        ),
        "LANGUAGE_OPTIONS_PATH": project_root / "catalog/language_options.json",
        "PRIVACY_NOTICE_VERSION": "synthetic-ipl-pilot-notice-v1",
        "PRIVACY_NOTICE_BODY": (
            "Synthetic IPL workshop rehearsal notice. Do not enter your real name, "
            "email address, affiliation, expertise, or other personal data."
        ),
        "CONSENT_STATEMENT_VERSION": "synthetic-ipl-pilot-consent-v1",
    }


def run_interactive_pilot(
    root: Path,
    *,
    package_dir: Path,
    seed_snapshots: Path,
    port: int = 8766,
    emit: Callable[[str], None] = print,
    server_factory: Callable[..., BaseWSGIServer] = make_server,
) -> int:
    if not 0 <= port <= 65535:
        raise ValueError("Interactive pilot port must be between 0 and 65535")
    database_path = prepare_workspace(
        root, package_dir=package_dir, seed_snapshots=seed_snapshots
    )
    app = create_app(_app_config(root.resolve(), database_path))
    entry_code = app.extensions["musparql_workshop_admission"].issue(ROUND_ID)
    server = server_factory("127.0.0.1", port, app, threaded=True)
    actual_port = int(server.server_port)
    emit(
        "Musparql IPL interactive synthetic rehearsal\n"
        f"Open: http://127.0.0.1:{actual_port}/auth/login\n"
        f"Shared entry code: {entry_code}\n"
        "Use only fictional profile and annotation data. Nothing is sent to production.\n"
        "Press Ctrl-C here when the rehearsal is complete."
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        emit("\nSynthetic IPL rehearsal stopped. The workspace is disposable.")
    finally:
        server.server_close()
        _close_app(app)
    return 0


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument(
        "--packages",
        type=Path,
        default=Path("var/review/bundles/ipl-2026"),
        help="Directory containing the frozen four-KG manifest and bundles",
    )
    result.add_argument(
        "--seed-snapshots",
        type=Path,
        default=Path("catalog/kg_seed_snapshots.yaml"),
    )
    result.add_argument("--workspace", type=Path)
    result.add_argument("--port", type=int, default=8766)
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    if args.workspace is None:
        with tempfile.TemporaryDirectory(prefix="musparql-ipl-pilot-") as temporary:
            return run_interactive_pilot(
                Path(temporary),
                package_dir=args.packages,
                seed_snapshots=args.seed_snapshots,
                port=args.port,
            )
    return run_interactive_pilot(
        args.workspace,
        package_dir=args.packages,
        seed_snapshots=args.seed_snapshots,
        port=args.port,
    )


if __name__ == "__main__":
    raise SystemExit(main())
