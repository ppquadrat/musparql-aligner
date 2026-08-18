# Musparql v2 database runbook

The SQLite database contains confidential reviewer information and operational
portal state. Keep it under ignored local storage, use restrictive filesystem
permissions, and never commit or attach it to issue reports. It contains no
private holdout annotations; those remain outside application and agent
workflows.

## Install and upgrade

Install the package and database dependencies in the project environment:

```bash
.venv/bin/pip install --no-build-isolation -e '.[test]'
```

Create or upgrade an explicitly selected database:

```bash
.venv/bin/python -m musparql.database.cli upgrade \
  --database var/musparql.sqlite3
```

The command is idempotent. It enables SQLite foreign keys, WAL journal mode,
full synchronous durability, and a bounded busy timeout. Alembic revisions are
tracked under `migrations/`; never replace a deployed database with
`Base.metadata.create_all()`.

## Safe checks

Print only the schema revision:

```bash
.venv/bin/python -m musparql.database.cli revision \
  --database var/musparql.sqlite3
```

Check the revision plus non-sensitive counts and pseudonymous reviewer IDs:

```bash
.venv/bin/python -m musparql.database.cli check \
  --database var/musparql.sqlite3
```

These commands deliberately do not print names, affiliations, email addresses,
expertise, familiarity, authentication values, paths, or submission contents.
Do not add raw SQL inspection commands to routine logs.

## Legacy registry decision

The documented JSONL registry was never populated. There is no legacy data
migration and no legacy-value table. If an unexpected legacy registry appears,
stop: its scalar domain and `queried` familiarity values must not be silently
mapped into v2 assertions.

## Backup boundary

Backup and restore are not part of this runbook or Phase 2. They are the
separate Phase 2b described in `docs/MUSPARQL_V2_PLAN.md` and
`docs/OPEN_ISSUES.md`. Real reviewer data must not be collected before that
phase supplies encrypted, verified recovery for both the database and valuable
Git-ignored review/provenance files.
