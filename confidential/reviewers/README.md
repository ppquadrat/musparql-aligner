# Confidential reviewer registry

This directory is the durable local home for reviewer profiles. It is outside
`var/` because reviewer-supplied information cannot be reconstructed from
pipeline inputs. The data files are ignored by Git and must be backed up by the
human owner in an access-controlled location.

Expected files:

- `reviewers.jsonl`: one record conforming to
  `schemas/reviewer.schema.json` per line;
- `kg_familiarity.jsonl`: one record conforming to
  `schemas/reviewer_kg_familiarity.schema.json` per line.

Only pseudonymous IDs such as `reviewer-0001` leave this directory. Names,
affiliations, email addresses, expertise profiles, privacy-notice records, and
KG-familiarity values must not enter review bundles, exports, benchmark
snapshots, logs, or test fixtures.

This registry is not holdout data. An authorised agent may read it only when a
task actually requires reviewer administration. Use obviously synthetic
profiles in code and tests.
