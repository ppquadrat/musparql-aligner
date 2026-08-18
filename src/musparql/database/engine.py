"""SQLite engine configuration with the invariants required by Musparql v2."""
from __future__ import annotations

from pathlib import Path

from sqlalchemy import Engine, create_engine, event
from sqlalchemy.orm import Session, sessionmaker


def sqlite_url(database_path: Path | str) -> str:
    path = Path(database_path).expanduser().resolve()
    return f"sqlite:///{path}"


def create_database_engine(
    database_path: Path | str, *, timeout_seconds: float = 10.0
) -> Engine:
    """Create an engine that enables foreign keys, WAL and a bounded busy wait."""
    path = Path(database_path).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    engine = create_engine(
        sqlite_url(path),
        connect_args={"timeout": timeout_seconds},
        future=True,
    )

    @event.listens_for(engine, "connect")
    def _configure_sqlite(dbapi_connection, _connection_record) -> None:  # type: ignore[no-untyped-def]
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA synchronous=FULL")
        cursor.execute(f"PRAGMA busy_timeout={int(timeout_seconds * 1000)}")
        cursor.close()
        for candidate in (path, Path(f"{path}-wal"), Path(f"{path}-shm")):
            if candidate.exists():
                candidate.chmod(0o600)

    return engine


def session_factory(engine: Engine) -> sessionmaker[Session]:
    return sessionmaker(bind=engine, expire_on_commit=False, future=True)
