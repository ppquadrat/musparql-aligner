# Musparql v2 Phase 7 submission and processing runbook

Phase 7 removes the reviewer file-moving step without changing Phase 5
assignment authorization or any holdout boundary. Hosted workbenches submit
only non-holdout assignment-attributed JSON to the authenticated assignment
endpoint. Reviewers never supply a filesystem path, recipe, command, receipt,
revision, or job identifier.

## Contracts and durable state

- Initial and comparative reviews use
  `schemas/review_export.schema.json`; linguistic annotations use
  `schemas/linguistic_annotation_export.schema.json`.
- Both contracts reject undeclared properties and unknown enum values at the
  envelope and review/annotation levels.
- The server re-derives reviewer identity, assignment identity, dataset,
  bundle digest, mode, recipe, and permitted record/trial identities from the
  authenticated assignment.
- Accepted JSON is canonicalized, hashed, written through a same-directory
  temporary file, fsynced, atomically renamed, and registered with a receipt.
- An identical retry returns the existing receipt. Changed JSON is a numbered
  revision. An approved assignment accepts identical retries but no new
  revisions.
- Each accepted revision gets one persistent processing job. Processing writes
  only beneath its server-derived job directory and never mutates a benchmark.

Configure separate private operational roots:

```text
MUSPARQL_SUBMISSION_ROOT=/srv/musparql/var/review/submissions
MUSPARQL_CANDIDATE_ROOT=/srv/musparql/var/review/candidates
```

These paths are application configuration, not reviewer input. They contain
confidential operational data, must be mode `0700`, and belong in the encrypted
Phase 2b backup. They must not be placed in the public repository or any
holdout directory.

## Upgrade and worker

Upgrade the configured database before starting the Phase 7 application:

```bash
musparql-db upgrade --database "$MUSPARQL_DATABASE_PATH"
```

Run one long-lived worker against the same database and configured roots:

```bash
musparql-worker --database "$MUSPARQL_DATABASE_PATH"
```

For a controlled single-job diagnostic:

```bash
musparql-worker --database "$MUSPARQL_DATABASE_PATH" --once
```

Startup returns any interrupted `running` job to `queued`. Job claiming uses an
immediate SQLite transaction, so concurrent workers cannot claim the same job.
A processing failure leaves the immutable receipt intact, records a safe
failure summary, and does not change any prior benchmark.

## Owner workflow

Open `/owner/processing` with an authenticated owner session. The two gates are
separate:

1. Include, request revision, or reject the submission as a whole. Individual
   items can then be included, omitted, or sent for revision. Omission and
   revision require a reason.
2. Select one or more successfully processed, included revisions with the same
   mode, recipe, and immutable baseline. This queues a combined-candidate job;
   the worker writes one manifest containing that exact revision set without
   mutating a shared benchmark.
3. Approve or reject the combined candidate. Approval creates a separate,
   immutable promotion manifest containing every selected receipt revision and
   digest, the configured baseline reference, and the latest item overrides.

Every action appends an immutable `owner_processing_decisions` row. Projection
fields drive the current dashboard state; they do not erase earlier decisions.
Final Git operations, push, and publication remain manual.

## Verification

```bash
.venv/bin/python -m pytest -q tests/test_v2_phase7_submission.py
.venv/bin/python -m pytest -q
.venv/bin/pip check
```

The focused suite covers ten simultaneous synthetic reviewers, unique durable
receipts, idempotent retries, numbered revisions, strict-schema failures,
restart recovery, isolated processing, append-only owner gates, and failure
preservation. Phase 8 remains responsible for measured workshop latency and a
full restart/load exercise in the deployment-shaped environment.
