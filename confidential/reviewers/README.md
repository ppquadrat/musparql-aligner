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

These are legacy contracts and remain unchanged until the explicit, owner-run
Phase 2 migration. Do not reinterpret their scalar domain or `queried`
familiarity values as v2 measurements.

The replacement contracts have been defined but are not yet migrated into this
directory:

- `schemas/reviewer_profile_v2.schema.json` for profiles with repeatable general
  domain assertions;
- `schemas/reviewer_kg_domain_assessment.schema.json` for repeated KG-specific
  subject-expertise assertions; and
- `schemas/reviewer_resource_familiarity_assessment.schema.json` for repeated
  resource/data-model/KG familiarity assertions.

Phase 2 will define their durable database tables and the owner-run migration.

Only pseudonymous IDs such as `reviewer-0001` leave this directory. Names,
affiliations, email addresses, expertise profiles, privacy-notice records, and
KG-familiarity values must not enter review bundles, submitted review exports,
benchmark snapshots, prompts, or logs. Only obviously synthetic examples may
appear in tests.

This registry is not holdout data. An authorised agent may read it only when a
task actually requires reviewer administration. Use obviously synthetic
profiles in code and tests.
