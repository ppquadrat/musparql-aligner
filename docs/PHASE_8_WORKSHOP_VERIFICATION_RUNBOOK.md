# Musparql v2 Phase 8 workshop concurrency verification

Phase 8 provides a repeatable, synthetic-only pre-workshop load and recovery
gate. It exercises the authenticated Flask submission endpoint, atomic receipt
storage, SQLite registrations, and the persistent worker queue without reading
real review exports, holdout annotations, or reviewer profiles.

## Workshop target

The first-release target remains at least ten distinct reviewers submitting in
one short interval. A passing run must establish all of these invariants:

- every accepted revision has one database registration, one processing job,
  and one durable server-derived file with the registered digest;
- the concurrent reviewers receive unique receipts;
- an identical retry returns the original receipt and a changed retry becomes
  revision 2;
- no atomic-write temporary file remains;
- the FIFO queue is observed in its declared `(created_at, id)` order;
- a claimed job is recovered after the application and worker are recreated;
- one deliberately corrupted synthetic input fails without removing its
  receipt or preventing later jobs from finishing; and
- HTTP acknowledgment latency is measured while a worker has already claimed
  a deliberately blocked job.

The plan intentionally does not declare a universal millisecond threshold.
The command reports minimum, median, nearest-rank p95, and maximum latency so
the owner can compare the deployment-shaped environment with earlier runs.
Any timeout, HTTP rejection, invariant failure, or material unexplained
regression blocks the workshop until investigated.

## Reference local result

On 2026-08-21, the completed implementation passed on macOS 26.5.2 arm64 with
Python 3.14.4 and SQLite 3.53.0. The 10-request burst completed with 8.837 ms
minimum, 21.679 ms median, and 55.588 ms p95/maximum acknowledgment latency
while the synthetic worker was busy. The run verified 12 durable accepted
revisions, recovered one interrupted job, isolated one deliberately failed job,
and completed all 10 jobs that followed that failure. This result establishes
the local Phase 8 baseline; it does not replace a fresh pre-workshop run on the
target deployment commit and machine.

## Run the gate

From a clean checkout with the test dependencies installed:

```bash
.venv/bin/python -m musparql.web.workshop_verify \
  --reviewers 10 \
  --output var/verification/phase8-workshop.json
```

The default verification workspace is a temporary mode-`0700` directory and is
removed after the result is printed. To retain its synthetic database, receipt
files, and candidate audits for diagnosis, pass a new or empty directory:

```bash
.venv/bin/python -m musparql.web.workshop_verify \
  --workspace var/verification/phase8-workspace \
  --output var/verification/phase8-workshop.json
```

Never point `--workspace` at an application, submission, review, holdout, or
benchmark directory. The command refuses a non-empty workspace and uses only
obviously synthetic reviewers, bundles, exports, and email addresses. It does
not connect to the home server or any external service.

The report conforms to
`schemas/workshop_verification.schema.json`. Retain the JSON report with the
workshop operations record, including the commit tested and whether the target
machine was otherwise under unusual load. Do not commit retained workspaces or
reports: platform details and timings are operational evidence, not fixtures.

## What the measurement includes

Each measured operation is an authenticated JSON `POST` to the real Flask
submission route using a separate synthetic reviewer session. It includes
session authentication, CSRF validation, assignment and JSON Schema checks,
canonicalization and hashing, atomic file persistence, the SQLite transaction,
queue insertion, and receipt serialization.

The local gate uses Flask's in-process test transport, so it deliberately does
not measure Gunicorn scheduling, TLS, Funnel, or reviewer network latency.
Those deployment layers remain part of the isolated deployment and external
synthetic-review checks in Phases 9 and 10. Phase 8 isolates the application and
storage behavior that must remain responsive while processing is busy.

## Verification tests

```bash
.venv/bin/python -m pytest -q tests/test_v2_phase8_workshop_verification.py
.venv/bin/python -m pytest -q
.venv/bin/pip check
```

The focused test validates the report schema and reruns the complete load,
retry, revision, restart, queue-order, failure-isolation, and file-integrity
scenario. It also verifies the lower reviewer bound and the refusal to reuse a
non-empty workspace.
