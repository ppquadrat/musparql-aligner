# Musparql v2 Phase 5 assignment runbook

This runbook covers synthetic preparation and verification of assignments and
pre-review assessments. It does not authorise real reviewer data, private
holdout access, or deployment. The governance, backup/recovery, email, and
hosting gates in `MUSPARQL_V2_PLAN.md` still apply.

## Implemented boundary

- The owner creates a ready assignment from an active pseudonymous reviewer, a
  reviewer-neutral `musparql.review-bundle.v2` file, an allowlisted mode/recipe,
  and one or more archived KG seed snapshots.
- Bundle paths are relative to `MUSPARQL_ASSIGNMENT_BUNDLE_ROOT`. Absolute paths,
  traversal outside that root, malformed files, non-v2 bundles, bundles carrying
  `reviewer_id`, unsupported holdout policies, mode mismatches, and bundle/seed
  KG mismatches, embedded reviewer identities, and explicit holdout markers are
  rejected.
- The stored SHA-256 digest is checked again whenever bundle data is requested.
  A changed file produces an integrity failure rather than changed review data.
- A reviewer sees only ready or active assignments addressed to their own
  authenticated pseudonymous ID. Cross-reviewer assignment and bundle requests
  return not found.
- Every domain and familiarity prompt comes from the immutable KG seed version
  and digest frozen on the assignment. Previous current values are preselected,
  but the reviewer must explicitly confirm or update all answers.
- The complete prompt set is written in one database transaction. The bundle is
  unavailable until that batch succeeds. A later round appends new rows that
  point to the previous heads; historical assessments are not updated.
- Bundle JSON receives `reviewer_id`, `assignment_id`, and the authoritative
  `bundle_digest` only after authentication and assignment authorization. A
  browser-provided identity is never accepted.

Phase 5 deliberately stops at attributed bundle JSON. Serving and integrating
the existing initial and comparative workbench assets is Phase 6.

## Build a reviewer-neutral bundle

The initial and comparative builders now require exactly one identity mode.
Existing local review continues to use `--reviewer-id reviewer-NNNN`. Hosted
preparation uses `--reviewer-neutral`, for example with synthetic inputs:

```bash
.venv/bin/python scripts/build_review_bundle.py \
  --reviewer-neutral \
  --inputs var/synthetic/inputs.jsonl \
  --outputs var/synthetic/outputs.jsonl \
  --no-holdout \
  --out var/review/bundles/synthetic/initial.js
```

Choose exactly one of the existing holdout controls required by the builder:
`--no-holdout`, `--holdout-filtered-upstream`, or the explicitly approved
selector-only route documented in `HOLDOUT_SECURITY.md`. Never provide real
holdout annotations to the builder or portal.

Configure the portal root to the directory containing issued bundles:

```bash
export MUSPARQL_ASSIGNMENT_BUNDLE_ROOT="/absolute/musparql/assignments"
```

The owner enters only a path relative to that directory. Copying or staging
operational files is outside this HTTP workflow and must follow the approved
deployment and backup runbooks.

## Synthetic workflow

1. Import or verify the approved KG seed snapshot with `musparql-db` tooling.
2. Sign in as the owner and open `/owner/assignments`.
3. Select an active synthetic reviewer, the mode and matching fixed recipe, a
   relative neutral-bundle path, and every KG seed represented by its records.
4. Create the assignment. It begins in `ready` state.
5. Sign in as that synthetic reviewer. The assignment appears on the home page.
6. Open it, answer every frozen subject and familiarity prompt, confirm the
   current answers, and submit the assessment.
7. Confirm that the assignment is now `active` and its attributed bundle JSON is
   available. Do not treat that JSON endpoint as the Phase 6 workbench.

## Verification

```bash
.venv/bin/python -m pytest -q tests/test_v2_phase5_assignments.py
.venv/bin/python -m pytest -q
.venv/bin/pip check
```

The Phase 5 tests cover neutral-bundle validation, owner creation, frozen prompt
display, the assessment-before-bundle gate, authenticated attribution,
cross-reviewer isolation, path rejection, digest tamper detection, prior-value
confirmation, and append-only history across a second synthetic round.

## Real-data gate

Do not create an assignment for a real reviewer until the controller/ICF,
Phase 2b recovery, production email, and Phase 10 deployment gates listed in
`MUSPARQL_V2_PLAN.md` have all been cleared. Bundle preparation must also use an
approved holdout-exclusion route, and Phase 6 must complete before the existing
review workbench is offered remotely.
