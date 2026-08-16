# Agent boundary

Real holdout annotations are human-only data. Agents must never read, search,
summarize, transform, audit, migrate, or run commands against any of these paths:

- `review/exports/`
- `review/private/`
- `benchmark/v*/holdout.jsonl`
- any file named `musparql-holdout-private-*`

Do not use recursive filesystem commands that bypass repository ignore rules to
discover those files. If work appears to require private holdout data, stop and
ask for a sanitized public export or a synthetic fixture. Agent-authored code and
tests may use obviously synthetic holdout examples only.

Browser-sanitized `non_holdout_review_export` files may be placed in ignored
`var/review/exports/`; agents may use only that directory for review input.

Selector-only files containing no reviewer fields may be used only when the
human owner has explicitly chosen the identity-visible holdout policy described
in `docs/HOLDOUT_SECURITY.md`.

Reviewer profiles under `confidential/reviewers/` are not holdout data, but
they contain personal information. Agents may read them only when the user's
task explicitly requires reviewer administration. Never copy profile or
familiarity fields into prompts, generated bundles, review exports, benchmark
artifacts, logs, or tests; only pseudonymous `reviewer-NNNN` IDs may cross that
boundary. Use synthetic reviewer records in code and tests.
