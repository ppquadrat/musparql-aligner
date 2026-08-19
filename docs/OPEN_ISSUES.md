# Open issues

This is the maintained index of known implementation and governance work. It
contains only issues that remain open; completed code-review findings belong in
the implementation, tests, and durable runbooks rather than in temporary review
notes.

## Reviewer administration and privacy

### Reviewer privacy approval and real-data gate

Reviewer profiles, repeatable general-domain expertise, KG-specific subject
expertise, and resource/data-model/KG familiarity have versioned Phase 1 schemas,
append-only history contracts, chain and seed-snapshot validation, synthetic
examples, a privacy notice, and a confidential storage boundary. Phase 4 now
provides reviewer onboarding and correction, versioned notice acknowledgement,
the versioned local suggestion set with free-text fallback, and owner-visible
pseudonymous completion state. The documented legacy registry was never
populated, so Phase 2 creates the v2 database directly and deliberately does not
add legacy-value tables or infer v2 assertions from legacy scales.

The proposed decisions, working ICF-controller assessment, rights/incident
procedures, and ICF/ODOMA questions are now recorded in
[`REVIEWER_DATA_GOVERNANCE_DRAFT.md`](REVIEWER_DATA_GOVERNANCE_DRAFT.md).
Before collecting real reviewer data, ICF must confirm:

- the controller and contact route;
- the lawful basis and any required legitimate-interests, ethics, grant, or
  security review;
- the UK home-server, encrypted Google Drive, email, tunnelling, and monitoring
  arrangements; and
- the final privacy notice and allocation of rights/incident responsibilities.

Retention periods, access/correction/deletion procedures, and the proposed
consequences of withdrawal are decided for implementation but remain subject to
that controller approval.

The form must never place names, email addresses, affiliation, experience, or
KG-familiarity fields in review bundles, exports, benchmarks, logs, or tests.

### Durable backup and recovery

Backup and recovery are a separate implementation phase rather than part of the
SQLite-foundation phase. The design must protect more than the database: review
outcomes remain irreplaceable before benchmark publication, and substantial
provenance is intentionally Git-ignored.

The detailed Phase 2b plan is in
[`PHASE_2B_BACKUP_RECOVERY_PLAN.md`](PHASE_2B_BACKUP_RECOVERY_PLAN.md). The phase
is on hold pending end-to-end confirmation of the VocalLanes backup
healthcheck/dead-man design. Synthetic-only development in later phases may
continue, but no real reviewer data may be collected before Phase 2b passes its
backup, monitoring, and restore gates.

Define, implement, and test an encrypted, authenticated, versioned backup of:

- the confidential and operational SQLite database;
- server-received non-holdout review submissions and sanitized exports;
- the working query catalogue and its local execution/correction provenance;
- frozen generation runs and any model output not already frozen into a run; and
- separately, through a human-only process, any private or holdout-bearing
  review material that application and agent workflows must never access.

The owner chose Google Drive as the sole encrypted, versioned destination for
Phase 2b on 2026-08-18 and explicitly deferred a physically separate on-site
copy as future hardening. The phase must still define key custody and rotation,
retention, monitoring, restore isolation, and recovery objectives. A second
directory on the same disk must not be represented as a backup. Until hosted
durable submission exists, completed browser reviews must be exported promptly
because browser local storage is not a recovery mechanism.

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
