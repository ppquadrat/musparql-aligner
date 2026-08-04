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
`review/public_exports/`; agents may use only that directory for review input.

Selector-only files containing no reviewer fields may be used only when the
human owner has explicitly chosen the identity-visible holdout policy described
in `HOLDOUT_SECURITY.md`.
