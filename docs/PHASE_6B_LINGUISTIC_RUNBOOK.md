# Phase 6b linguistic-dimensions workbench runbook

Phase 6b is a separate annotation mode. The privacy, home-server, authenticated
assignment, frozen-KG assessment, and holdout boundaries in the v2 plan remain
in force. Durable hosted submission and processing are still Phase 7.

## Contracts and construction

- Stimulus bundles use `musparql.linguistic-stimulus-bundle.v1` and
  [`linguistic_stimulus_bundle.schema.json`](../schemas/linguistic_stimulus_bundle.schema.json).
- Normalized exports use `musparql.linguistic-annotation-export.v1` and
  [`linguistic_annotation_export.schema.json`](../schemas/linguistic_annotation_export.schema.json).
- Build a bundle from an ordinary, pre-filtered synthetic or authorized JSONL
  pool with `scripts/build_linguistic_bundle.py --input INPUT --output OUTPUT
  --dataset-id ID --seed RECORDED-SEED [--target-trials N]`.
- The builder rejects a missing validated literal, digest mismatch, two-way
  presentation, holdout-marked stimulus, or anything other than exactly two
  eligible non-literal candidates. It balances across `sampling_stratum` in a
  reproducible round-robin sample before shuffling the chosen queue.

The source pool must already exclude private holdout identities. Never use the
builder on a protected holdout path. The recorded seed is experiment metadata,
not a secret.

## Assignment and reviewer flow

Create an owner assignment with mode `linguistic`, recipe
`validate_linguistic_annotation`, a reviewer-neutral bundle, and the exact
frozen KG seed set represented by its trials. As in other hosted modes, the
reviewer completes the frozen pre-review assessment before the workbench opens.

The server verifies the assigned file digest and every frozen stimulus on each
read. It derives assignment and reviewer attribution from the authenticated
session. Candidate and literal-validation provenance remain in the controlled
server bundle but are removed before data reaches the browser.

The browser randomizes and records candidate A/B order on first presentation.
It stores one atomic assignment-scoped state object under dataset, reviewer,
and assignment IDs. A slider drawn at zero remains unanswered until moved.
Normal submission requires all six touched integer controls. Skip rotates an
item behind the remaining queue; cannot-assess completes a non-rating outcome;
literal-inaccurate discards all slider data and records only the optional
correction proposal/comment. Finish preserves the current draft and completed
trials for a later resume.

Use **Export normalized annotations** from the saved-progress screen to make a
Phase 6b JSON export. Until Phase 7 lands, this download is a transition
artifact: it is not a durable server submission or processing receipt.

## Verification

Run:

```text
.venv/bin/python -m pytest tests/test_v2_phase6b_linguistic.py tests/test_v2_phase5_assignments.py -q
.venv/bin/python -m pytest -q
```

The synthetic tests cover deterministic sampling, schema validation, frozen
digests, untouched-zero rejection, A/B order, non-rating score removal,
assignment-scoped drafts, authenticated routing, provenance blinding, and
cross-reviewer denial. No protected review data is needed.
