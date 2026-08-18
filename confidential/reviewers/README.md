# Confidential reviewer registry

This directory documents the historical JSONL registry. Reviewer-supplied
information cannot be reconstructed from pipeline inputs; Phase 2 stores new v2
records in the confidential SQLite database instead.

Historical, never-populated file locations were:

- `reviewers.jsonl`: one record conforming to
  `schemas/reviewer.schema.json` per line;
- `kg_familiarity.jsonl`: one record conforming to
  `schemas/reviewer_kg_familiarity.schema.json` per line.

These legacy contracts were not populated. Phase 2 starts directly with the v2
SQLite model; it creates no legacy-value tables and does not reinterpret scalar
domain or `queried` familiarity values as v2 measurements. If an unexpected
legacy file is found later, do not import it without a new owner-approved policy.

The v2 contracts persisted by the database are:

- `schemas/reviewer_profile_v2.schema.json` for profiles with repeatable general
  domain assertions;
- `schemas/reviewer_kg_domain_assessment.schema.json` for repeated KG-specific
  subject-expertise assertions; and
- `schemas/reviewer_resource_familiarity_assessment.schema.json` for repeated
  resource/data-model/KG familiarity assertions.

The database belongs under ignored local state such as `var/musparql.sqlite3`;
see `docs/DATABASE_RUNBOOK.md`. Backup and recovery are a separate Phase 2b and
must be completed before real reviewer data is collected.

Only pseudonymous IDs such as `reviewer-0001` leave this directory. Names,
affiliations, email addresses, expertise profiles, privacy-notice records, and
KG-familiarity values must not enter review bundles, submitted review exports,
benchmark snapshots, prompts, or logs. Only obviously synthetic examples may
appear in tests.

Reviewer administration data are not holdout data. An authorised agent may read
them only when a task actually requires reviewer administration. Use obviously
synthetic profiles in code and tests.
