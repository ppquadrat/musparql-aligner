"""Local, owner-operated setup commands for the Phase 3 web application."""
from __future__ import annotations

import argparse
from pathlib import Path
import re

from sqlalchemy import select

from musparql.database import create_database_engine, session_factory
from musparql.database.migrations import upgrade_database
from musparql.database.models import Reviewer
from musparql.database.services import normalize_email
from .auth import timestamp, utc_now


REVIEWER_ID = re.compile(r"reviewer-[0-9]{4,}")


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description="Manage the local Musparql web application")
    subparsers = result.add_subparsers(dest="command", required=True)
    bootstrap = subparsers.add_parser("bootstrap-owner")
    bootstrap.add_argument("--database", type=Path, required=True)
    bootstrap.add_argument("--reviewer-id", required=True)
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    if not REVIEWER_ID.fullmatch(args.reviewer_id):
        raise SystemExit("Owner ID must match reviewer-NNNN (with at least four digits).")
    name = input("Owner name: ").strip()
    email_display = input("Owner email: ").strip()
    if not name or len(name) > 200:
        raise SystemExit("Owner name must contain 1-200 characters.")
    try:
        email_normalized = normalize_email(email_display)
    except ValueError as exc:
        raise SystemExit("Owner email is invalid.") from exc

    upgrade_database(args.database)
    engine = create_database_engine(args.database)
    sessions = session_factory(engine)
    now = timestamp(utc_now())
    try:
        with sessions.begin() as session:
            if session.get(Reviewer, args.reviewer_id) is not None:
                raise SystemExit("That reviewer ID already exists.")
            if session.scalar(
                select(Reviewer.id).where(Reviewer.email_normalized == email_normalized)
            ):
                raise SystemExit("That email address already has an account.")
            session.add(
                Reviewer(
                    id=args.reviewer_id,
                    name=name,
                    affiliation="",
                    email_display=email_display,
                    email_normalized=email_normalized,
                    status="active",
                    created_at=now,
                    updated_at=now,
                    privacy_notice_version=None,
                    privacy_notice_acknowledged_at=None,
                )
            )
    finally:
        engine.dispose()
    print(f"Owner account {args.reviewer_id} created; configure it as MUSPARQL_OWNER_REVIEWER_ID.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
