"""Owner-facing schema migration commands with non-sensitive diagnostics."""
from __future__ import annotations

import argparse
from pathlib import Path

from sqlalchemy import func, select

from .engine import create_database_engine, session_factory
from .migrations import current_revision, upgrade_database
from .models import KgSeedSnapshot, Reviewer


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description="Manage the Musparql v2 database schema")
    subparsers = result.add_subparsers(dest="command", required=True)
    for name in ("upgrade", "revision", "check"):
        command = subparsers.add_parser(name)
        command.add_argument("--database", type=Path, required=True)
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    if args.command == "upgrade":
        upgrade_database(args.database)
        print(f"Database schema upgraded to {current_revision(args.database)}.")
    elif args.command == "revision":
        revision = current_revision(args.database)
        print(f"Database schema revision: {revision or 'unversioned'}.")
    else:
        revision = current_revision(args.database)
        if revision is None:
            print("Database schema revision: unversioned.")
            return 1
        engine = create_database_engine(args.database)
        sessions = session_factory(engine)
        try:
            with sessions() as session:
                reviewer_ids = list(session.scalars(select(Reviewer.id).order_by(Reviewer.id)))
                seed_count = session.scalar(select(func.count()).select_from(KgSeedSnapshot)) or 0
            print(f"Database schema revision: {revision}.")
            print(f"Reviewer records: {len(reviewer_ids)}.")
            if reviewer_ids:
                print("Reviewer IDs: " + ", ".join(reviewer_ids) + ".")
            print(f"Frozen seed snapshots: {seed_count}.")
        finally:
            engine.dispose()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
