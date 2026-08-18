# Database migrations

Run schema upgrades through the owner-facing command so SQLite safety pragmas
and the explicit database path are applied:

```bash
.venv/bin/python -m musparql.database.cli upgrade --database var/musparql.sqlite3
```

The database is confidential local state and remains ignored by Git. Migration
output contains schema revision information only; it must never print profile
fields. Phase 2 starts from the v2 model because the documented legacy JSONL
registry was never populated.
