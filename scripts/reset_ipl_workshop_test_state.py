"""Build a clean IPL workshop database while preserving frozen configuration.

This command is intentionally non-destructive: it writes a new database and
refuses to replace either its source or an existing output. An operator can
inspect the result and install it during a stopped-service maintenance window.
"""
from __future__ import annotations

import argparse
from pathlib import Path
import sqlite3

from musparql.database.migrations import upgrade_database


REFERENCE_TABLES = (
    "kg_seed_snapshots",
    "kg_seed_review_domains",
    "kg_seed_familiarity_scopes",
    "workshop_rounds",
    "workshop_work_packages",
)

OWNER_TABLES = (
    "reviewer_experience",
    "reviewer_languages",
    "reviewer_domain_expertise",
    "auth_sessions",
)

EMPTY_OPERATIONAL_TABLES = (
    "workshop_entry_redemptions",
    "workshop_admission_nonces",
    "workshop_admission_attempts",
    "review_groups",
    "review_group_members",
    "workshop_assessment_deferrals",
    "review_assignments",
    "reviewer_workshop_batch_contexts",
    "assignment_kg_seeds",
    "reviewer_kg_domain_assessments",
    "reviewer_resource_familiarity_assessments",
    "review_submissions",
    "processing_jobs",
    "owner_processing_decisions",
    "workshop_session_resets",
)


def _columns(connection: sqlite3.Connection, schema: str, table: str) -> tuple[str, ...]:
    return tuple(
        str(row[1])
        for row in connection.execute(f'PRAGMA {schema}.table_info("{table}")')
    )


def _copy_all(connection: sqlite3.Connection, table: str) -> None:
    columns = _columns(connection, "main", table)
    if columns != _columns(connection, "source", table):
        raise ValueError(f"Source and destination schemas differ for {table}")
    names = ", ".join(f'"{column}"' for column in columns)
    connection.execute(
        f'INSERT INTO main."{table}" ({names}) '
        f'SELECT {names} FROM source."{table}"'
    )


def _copy_owner_rows(
    connection: sqlite3.Connection, table: str, owner_reviewer_id: str
) -> None:
    columns = _columns(connection, "main", table)
    if columns != _columns(connection, "source", table):
        raise ValueError(f"Source and destination schemas differ for {table}")
    names = ", ".join(f'"{column}"' for column in columns)
    connection.execute(
        f'INSERT INTO main."{table}" ({names}) '
        f'SELECT {names} FROM source."{table}" WHERE reviewer_id = ?',
        (owner_reviewer_id,),
    )


def build_clean_database(
    source_database: Path,
    output_database: Path,
    *,
    owner_reviewer_id: str,
) -> None:
    source_database = source_database.resolve()
    output_database = output_database.resolve()
    if not source_database.is_file():
        raise ValueError(f"Source database does not exist: {source_database}")
    if output_database.exists():
        raise ValueError(f"Output database already exists: {output_database}")
    if source_database == output_database:
        raise ValueError("Source and output database paths must differ")

    output_database.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    upgrade_database(output_database)
    connection = sqlite3.connect(output_database)
    try:
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("ATTACH DATABASE ? AS source", (str(source_database),))
        with connection:
            owner_count = connection.execute(
                "SELECT COUNT(*) FROM source.reviewers WHERE id = ?",
                (owner_reviewer_id,),
            ).fetchone()[0]
            if owner_count != 1:
                raise ValueError(
                    f"Expected exactly one owner row for {owner_reviewer_id}"
                )

            reviewer_columns = _columns(connection, "main", "reviewers")
            if reviewer_columns != _columns(connection, "source", "reviewers"):
                raise ValueError("Source and destination schemas differ for reviewers")
            reviewer_names = ", ".join(
                f'"{column}"' for column in reviewer_columns
            )
            connection.execute(
                f"INSERT INTO main.reviewers ({reviewer_names}) "
                f"SELECT {reviewer_names} FROM source.reviewers WHERE id = ?",
                (owner_reviewer_id,),
            )

            domain_columns = _columns(connection, "main", "expertise_domains")
            if domain_columns != _columns(connection, "source", "expertise_domains"):
                raise ValueError(
                    "Source and destination schemas differ for expertise_domains"
                )
            domain_names = ", ".join(f'"{column}"' for column in domain_columns)
            connection.execute(
                f"INSERT INTO main.expertise_domains ({domain_names}) "
                f"SELECT {domain_names} FROM source.expertise_domains "
                "WHERE id IN (SELECT domain_id FROM "
                "source.reviewer_domain_expertise WHERE reviewer_id = ?)",
                (owner_reviewer_id,),
            )

            for table in REFERENCE_TABLES:
                _copy_all(connection, table)
            for table in OWNER_TABLES:
                _copy_owner_rows(connection, table, owner_reviewer_id)

            entry_columns = _columns(connection, "main", "workshop_entry_codes")
            if entry_columns != _columns(
                connection, "source", "workshop_entry_codes"
            ):
                raise ValueError(
                    "Source and destination schemas differ for workshop_entry_codes"
                )
            entry_names = ", ".join(f'"{column}"' for column in entry_columns)
            entry_values = ", ".join(
                "0" if column == "redemption_count" else f'"{column}"'
                for column in entry_columns
            )
            connection.execute(
                f"INSERT INTO main.workshop_entry_codes ({entry_names}) "
                f"SELECT {entry_values} FROM source.workshop_entry_codes "
                "WHERE revoked_at IS NULL"
            )

            active_codes = connection.execute(
                "SELECT COUNT(*) FROM main.workshop_entry_codes "
                "WHERE revoked_at IS NULL"
            ).fetchone()[0]
            if active_codes != 1:
                raise ValueError(
                    f"Expected one active workshop entry code, found {active_codes}"
                )

        foreign_key_errors = list(connection.execute("PRAGMA foreign_key_check"))
        if foreign_key_errors:
            raise ValueError(f"Foreign-key validation failed: {foreign_key_errors}")
        integrity = connection.execute("PRAGMA integrity_check").fetchone()[0]
        if integrity != "ok":
            raise ValueError(f"SQLite integrity check failed: {integrity}")
        for table in EMPTY_OPERATIONAL_TABLES:
            count = connection.execute(
                f'SELECT COUNT(*) FROM main."{table}"'
            ).fetchone()[0]
            if count:
                raise ValueError(f"Expected {table} to be empty, found {count} rows")
        reviewer_count = connection.execute(
            "SELECT COUNT(*) FROM main.reviewers"
        ).fetchone()[0]
        if reviewer_count != 1:
            raise ValueError(f"Expected only the owner reviewer, found {reviewer_count}")
    except Exception:
        connection.close()
        output_database.unlink(missing_ok=True)
        raise
    else:
        connection.close()


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--source", required=True, type=Path)
    result.add_argument("--output", required=True, type=Path)
    result.add_argument("--owner-reviewer-id", required=True)
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    build_clean_database(
        args.source,
        args.output,
        owner_reviewer_id=args.owner_reviewer_id,
    )
    print(f"Clean workshop test database written to {args.output.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
