# SPARQL editing policy

## The rule in one sentence

Keep every query version, let a human approve changes, and treat execution as
useful evidence—not as permission to use the query.

## What stays permanent

The SPARQL found in the source is version 0. It is never replaced. Each approved
change is added as version 1, 2, and so on. Older versions remain available for
comparison and can be selected explicitly. When nobody chooses a version, the
latest approved version is used.

There is no silent fallback to an older query when the latest version fails.

## Who can enter correction review

Any non-holdout query can be reviewed. That includes a query that succeeded,
failed, has no endpoint, contains placeholders, needs a local file or specialist
tool, or has never been run. A failed query may be shown earlier in the queue,
but failure does not prove that the query is wrong. A successful query may still
ask the wrong question.

Holdout identities are removed before candidate details, evidence, execution
observations, or agent prompts are created. A query identity with any retained
SPARQL edit can never become a holdout, even if someone later selects version 0.

## Static extraction diagnostics

When a curated source introduces a malformed `SELECT`, extraction retains the
source text as version 0 and records a diagnostic pinned to that version and its
SPARQL hash. If the flagged version is still the latest retained version, the
next correction round classifies it as a high-priority likely correction even
when execution was not attempted or the endpoint is unavailable.

Static diagnostics are findings, not edits or execution observations. They do
not change the source text and cannot approve a correction. Once an approved
version 1 or later exists, a diagnostic on version 0 remains historical
provenance but no longer affects correction triage, including runs that inspect
original or all retained versions.

## What execution means

An execution observation says what happened for one exact SPARQL hash: success,
an empty result, endpoint error, unavailable infrastructure, unsupported
runtime, or not attempted. It may include endpoint, graph, duration, result
count, a small sample, and a safe error.

Execution is not approval, verification, or an eligibility decision. Missing or
failed execution never removes an approved version from NL generation, review,
benchmark construction, or public release. The execution status travels with
the query so later users can judge trust.

At present the workbench runner reads `SELECT` results. Other query forms are
shown as unsupported, which does not prevent review or approval.

Running a query in the workbench does not save or alter SPARQL. The workbench
records the attempt separately. Only an exported and explicitly applied human
decision can add a canonical version.

## Human approval

The reviewer compares the retained query, proposal, readable diff, evidence,
and execution observations before approval.

If the proposal came from the agent, the workbench fills in and retains the
model/request identity, prompt and schema hashes, suggestion, rationale, edit
type, evidence IDs, and proposal hash. The human does not retype those fields.
The agent can suggest “no edit”, and its suggestion is never approved
automatically.

For a manual edit, the human supplies:

- changed, complete SPARQL; and
- either an edit type or a short rationale.

Evidence IDs and reviewer notes are optional. No model/tool field is shown for a
human proposal. No-edit and defer need no explanation, although the human may
leave a note.

## Safe application

Browser decisions are local drafts until export and apply. The apply command
checks the exact `(kg_id, query_id)`, base version, hashes, authoritative
candidate digest, and service records. It rejects stale or changed proposals,
duplicate decisions, unknown evidence IDs, and selected holdouts.

Approved edits and correction history are append-only, and the canonical file
is replaced atomically only after the full export validates. This explicit
boundary protects against an accidental click changing the dataset.

## Data handling

Correction bundles, browser exports, and service logs are local working data and
must not enter the public repository tree. Put a downloaded, sanitized
non-holdout export under ignored `var/review/exports/`. Never place private
holdout annotations in a correction bundle, prompt, test, or service request.
Tests use clearly synthetic identities and evidence only.

Exact commands are in `SPARQL_CORRECTION_RUNBOOK.md`.
