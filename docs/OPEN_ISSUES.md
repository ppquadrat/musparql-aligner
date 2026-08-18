# Open issues

This is the maintained index of known implementation and governance work. It
contains only issues that remain open; completed code-review findings belong in
the implementation, tests, and durable runbooks rather than in temporary review
notes.

## Reviewer administration and privacy

### Reviewer profile administration UI

Reviewer profiles, repeatable general-domain expertise, KG-specific subject
expertise, and resource/data-model/KG familiarity have versioned Phase 1 schemas,
append-only history contracts, chain and seed-snapshot validation, synthetic
examples, a privacy notice, and a confidential storage boundary, but there is no
owner-only profile administration form or v2 database yet. Until Phase 2
migration and the later administration UI exist, the human owner must maintain
the legacy ignored registry outside agent-visible workflows. The legacy values
must not be silently reinterpreted as v2 levels.

Before implementing the form, decide and document:

- the data controller and contact route;
- the lawful basis for processing;
- retention and deletion periods;
- access, correction, and deletion procedures; and
- the owner-managed encrypted backup and recovery procedure.

The form must never place names, email addresses, affiliation, experience, or
KG-familiarity fields in review bundles, exports, benchmarks, logs, or tests.

### One canonical v2 review-export contract

The v2 review envelope is currently enforced across Python, browser code, and
the benchmark decision schema, but it does not yet have a single envelope JSON
Schema. Add `schemas/review_export.schema.json`, use it at browser-import and
Python-ingest boundaries, and add parity tests so required fields, RFC 3339
timestamps, reviewer/event identity rules, and unknown-property handling cannot
drift between implementations.

## SPARQL correction backlog

The detailed, prioritized correction-workbench backlog remains in
[`SPARQL_CORRECTION_FOLLOW_UP.md`](SPARQL_CORRECTION_FOLLOW_UP.md). Its largest
open items are durable non-approval decisions, explicit benchmark exclusion,
source/target execution modelling, contextual evidence, bounded parameter-value
discovery, clearer saved-decision feedback, and clean service shutdown.

The stale agent-metadata defect described there is fixed: changing an agent
proposal now clears the suggestion, edit type, rationale, and evidence IDs and
requires the human to enter fresh edit metadata.

## Dependency maintenance

The test suite currently emits deprecation warnings from `rdflib` using legacy
`pyparsing` APIs. They do not affect correctness today, but dependency upgrades
should remove or re-evaluate the warnings before they become runtime failures.
