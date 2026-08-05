# SPARQL editing policy

## Purpose

Source-authored SPARQL is evidence and remains immutable as version `0`.
Musparql may retain a corrected or instantiated query only as an append-only
version with explicit human approval and reproducible provenance.

## Holdout boundary

A query identity with any retained `sparql_edits` is permanently ineligible for
holdout inclusion, regardless of which SPARQL version is selected later. A
reversion or selection of version `0` does not restore eligibility. Correction
review is development exposure and must never receive a selected holdout pair.

All agent-facing correction commands require one explicit holdout policy:
`--holdout-selectors`, `--no-holdout`, or `--holdout-filtered-upstream`.
Identity-visible selectors are pair-wide and correction builders exclude them;
the apply command refuses to review or edit them. Because `run_queries.py` and
the apply command read the full canonical query file, they reject
`--holdout-filtered-upstream`; that assertion is valid only for an already
filtered bundle input.

## What counts as an edit

Approved versions must state one edit type:

- `syntax_correction`
- `endpoint_dialect_adaptation`
- `parameter_instantiation`
- `benchmark_specialization`
- `federation_rewrite`
- `performance_optimization`
- `other`

Parameter instantiation and benchmark specialization are retained versions for
traceability, but must not be described as fixes to malformed source SPARQL.

## Evidence and approval requirements

An approved edit requires the exact base version and hash; proposed SPARQL that
differs from the base; an edit type and rationale; aligned evidence identifiers;
an authoritative-ledger digest for the triggering execution observation;
proposal origin and model/tool identifier
when an agent proposed it; and review time plus the review export path and hash.

Execution success is necessary evidence for many corrections but is not proof
of semantic equivalence. Parse errors and HTTP 400/422 responses are strong
triage signals, but do not by themselves prove endpoint health. Empty results,
HTTP 500, timeouts, missing
endpoints, local-file dependencies, and specialised runtimes require diagnosis
and are not automatically corrections.

## Mutation rules

- Never replace `sparql_raw`, `sparql_clean`, the version-0 hash, or an existing edit.
- Append only version `N+1` to the current latest version.
- Reject stale proposals whose base version/hash is no longer latest.
- Record `no_edit` and `defer` in `sparql_correction_history` without a new version.
- Re-execute an approved version before using it in generation or a benchmark;
  builders fail closed until a matching version/hash has an `ok` or `empty` execution.
- Rebuild inputs and review bundles so edit provenance propagates downstream.

## Private-data prohibition

Correction reports, proposals, tests, and browser fixtures must never use raw or
private holdout annotations. Use only canonical public query records,
annotation-free selectors, browser-sanitized non-holdout inputs, or obviously
synthetic fixtures.
