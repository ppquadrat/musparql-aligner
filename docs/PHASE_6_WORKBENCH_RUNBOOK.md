# Musparql v2 Phase 6 workbench runbook

This runbook covers synthetic verification of the authenticated initial and
comparative review workbenches. It does not authorise real reviewer data,
private holdout access, hosted submission, automated processing, or deployment.
The governance, recovery, email, and hosting gates in
[`MUSPARQL_V2_PLAN.md`](MUSPARQL_V2_PLAN.md) still apply.

## Implemented boundary

- Flask serves the existing `review/index.html`, `review/styles.css`, and
  `review/app.js` files. Hosted and local review therefore use the same decision,
  import, export, and comparison code rather than maintained copies.
- An authenticated assignment endpoint supplies `review_data.js`. The server
  reloads the reviewer-neutral bundle, verifies its frozen digest, and adds the
  reviewer and assignment IDs from the authorised session.
- A separate hosted-context endpoint supplies the signed-in pseudonymous ID,
  assignment/profile links, logout form data, and the assignment's false
  holdout capability. No email, profile answer, expertise assessment, or other
  personal field enters the workbench context.
- The workbench is unavailable until the complete pre-review assessment batch
  has activated the assignment. Owner, incomplete-profile, cross-reviewer, and
  bundle-integrity checks use the same fail-closed boundary as attributed bundle
  access.
- Hosted browser drafts use a key containing dataset ID, reviewer ID, and
  assignment ID. Hosted pages never fall back to local-workflow or owner legacy
  keys. Initial and comparative modes have separate namespaces.
- Hosted ordinary assignments hide private-holdout selection, filtering,
  private export, selector export, and private-state controls. Phase 5 already
  rejects holdout-marked records; the browser reflects that server-side
  capability boundary.
- Non-holdout JSON export and explicit JSON import remain available for
  transition compatibility. Durable authenticated submission remains Phase 7.

## Configuration

The default workbench directory is `review` relative to the trusted Musparql
working directory. Set an explicit absolute path when the service working
directory differs:

```bash
export MUSPARQL_REVIEW_WORKBENCH_ROOT="/absolute/path/to/Musparql/review"
```

Application startup rejects an incomplete workbench root. The required files
are `index.html`, `styles.css`, `app.js`, and `host_context.js`.

## Synthetic workflow

1. Create an initial or comparative reviewer-neutral bundle and a Phase 5
   assignment using only synthetic, non-holdout input.
2. Sign in as the assigned synthetic reviewer and complete every frozen
   pre-review assessment.
3. Open the assignment and select **Open review workbench**.
4. Confirm that the signed-in pseudonymous ID, assignment link, profile link,
   and sign-out control are visible.
5. Record a decision, reload the page, and confirm that the draft remains.
6. Open another synthetic assignment for the same reviewer and dataset and
   confirm that its draft begins empty.
7. Export the non-holdout review JSON and, if transition testing requires it,
   import that file explicitly. Import is an intentional user action and is not
   automatic draft inheritance.

The browser download is still a compatibility artifact, not a hosted durable
submission. Do not move it into `review/exports/` or inspect any quarantined or
private holdout path. Sanitized agent-readable review input belongs only in
ignored `var/review/exports/` under the existing policy.

## Verification

```bash
.venv/bin/python -m pytest -q tests/test_review_ui.py tests/test_v2_phase5_assignments.py
.venv/bin/python -m pytest -q
.venv/bin/pip check
```

The focused tests cover the shared-asset parity boundary, local-key
compatibility, assignment-scoped initial and comparative draft namespaces,
assessment gating, cross-reviewer isolation, authenticated context and data,
hidden hosted holdout controls, unknown assets, and digest-tamper failure.

## Phase 7 handoff

Phase 6 deliberately retains file export/import and browser-local drafts. Phase
7 must add the canonical review-export schema, authenticated submission,
server-derived atomic storage, idempotent/versioned receipts, and controlled
processing without changing the Phase 6 assignment authorisation or holdout
boundaries.
