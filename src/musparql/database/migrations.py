"""Programmatic Alembic entry points with safe, explicit database paths."""
from __future__ import annotations

from pathlib import Path

from alembic import command
from alembic.config import Config

from .engine import sqlite_url


ROOT = Path(__file__).resolve().parents[3]


def alembic_config(database_path: Path | str) -> Config:
    config = Config(str(ROOT / "alembic.ini"))
    config.set_main_option("script_location", str(ROOT / "migrations"))
    config.set_main_option("sqlalchemy.url", sqlite_url(database_path).replace("%", "%%"))
    return config


def upgrade_database(database_path: Path | str, revision: str = "head") -> None:
    path = Path(database_path).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    command.upgrade(alembic_config(path), revision)
    path.chmod(0o600)


def current_revision(database_path: Path | str) -> str | None:
    from alembic.runtime.migration import MigrationContext
    from sqlalchemy import create_engine

    path = Path(database_path).expanduser().resolve()
    if not path.exists():
        return None
    engine = create_engine(sqlite_url(path))
    try:
        with engine.connect() as connection:
            return MigrationContext.configure(connection).get_current_revision()
    finally:
        engine.dispose()
