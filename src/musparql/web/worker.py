"""Owner-operated persistent Phase 7 processing worker."""
from __future__ import annotations

import argparse
import os
from pathlib import Path
import time

from musparql.database import create_database_engine, session_factory
from .submissions import ProcessingService


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description="Process durable Musparql review jobs")
    result.add_argument("--database", type=Path, default=os.environ.get("MUSPARQL_DATABASE_PATH"))
    result.add_argument("--submission-root", type=Path, default=os.environ.get("MUSPARQL_SUBMISSION_ROOT", "var/review/submissions"))
    result.add_argument("--candidate-root", type=Path, default=os.environ.get("MUSPARQL_CANDIDATE_ROOT", "var/review/candidates"))
    result.add_argument("--once", action="store_true")
    result.add_argument("--poll-seconds", type=float, default=1.0)
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    if args.database is None or not args.database.is_file():
        raise SystemExit("A migrated database is required.")
    if args.poll_seconds < 0.1 or args.poll_seconds > 60:
        raise SystemExit("--poll-seconds must be between 0.1 and 60.")
    engine = create_database_engine(args.database)
    service = ProcessingService(
        session_factory(engine), args.submission_root, args.candidate_root
    )
    try:
        service.recover_interrupted()
        while True:
            processed = service.process_next()
            if args.once:
                return 0 if processed is not None else 2
            if processed is None:
                time.sleep(args.poll_seconds)
    finally:
        engine.dispose()


if __name__ == "__main__":
    raise SystemExit(main())
