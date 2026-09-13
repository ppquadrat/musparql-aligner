# IPL workshop UX implementation

**Status:** implemented on 12 September 2026; batch navigation refined on 13
September 2026

This change implements the participant-flow and workshop-workbench proposal in
[`IPL_WORKSHOP_UX_PROPOSAL.md`](IPL_WORKSHOP_UX_PROPOSAL.md) without changing
the established non-workshop review interfaces.

## Delivery plan

1. Gate workshop access on a complete profile, including separate required
   first- and last-name fields and a contact email for shared-code participants.
2. Replace the group-first dashboard with one review-batch catalogue and keep
   each team's membership scoped to its selected batch.
3. Route batch selection through the frozen KG form and directly into the
   workshop workbench.
4. Give active-team joiners a direct route to the review with optional setup.
5. Apply a workshop-only presentation mode to the existing initial-review
   workbench and preserve its review decisions, local draft key, attribution,
   and stable package ordering.
6. Verify the route, service, profile, and browser-static contracts with
   synthetic tests.

## Implemented behavior

- Starting a batch silently creates a one-person team for that batch. Changing
  batches starts with a new one-person team, because expertise and teamwork are
  KG-specific; collaborators add one another separately for each batch.
- The Workshop page shows each enabled batch exactly once and has no global
  team selector. Each batch card independently returns the reviewer to the team
  assignment they last opened for that batch. Starting a different batch
  creates a separate team automatically.
- A reviewer may belong to several teams, including several teams reviewing the
  same batch. Opening one of those assignments updates only that reviewer's
  remembered batch context; it does not change another member's batch card.
- A participant can add a consenting collaborator from the workbench by their
  pseudonymous reviewer ID. The server verifies that the reviewer exists and
  records batch-specific membership; entering an ID is not a login mechanism.
- Batch cards expose contextual states and actions: available/Start, setup
  needed/Complete setup, in progress/Continue, and submitted.
- Saving the frozen KG-specific questions opens the workbench directly.
  Joining an active team opens its setup page, which includes a visible
  **Skip for now and join review** route. Skipping creates no expertise value.
  It creates an append-only, pseudonymous deferral event with reviewer,
  assignment, and timestamp.
- Shared-code profiles collect optional title plus separate, non-prefilled,
  required first and last names, and a normalized, uniqueness-checked contact email.
  It remains unverified, is not a login credential, and is not added to review
  bundles or exports. The original synthetic placeholder no longer satisfies
  profile completion.
- Workshop workbenches show the authenticated reviewer, team code and joining
  instruction, profile/sign-out links, Submit current work, Back to workshop,
  Records and Reviewed. The pair view retains SPARQL, model question, origin,
  Previous/Next, and the existing decision controls. It displays only evidence
  explicitly cited by the question provenance or retained as a ranked phrase,
  including its evidence type and a web source link when available; unrelated
  input evidence remains hidden. General filters, record list,
  run/model/confidence details, holdout controls, import/export controls, and
  full evidence are hidden only in workshop mode.
- A zero-item submission produces an inline prompt and preserves the draft.
  Completion type is derived from reviewed counts by the existing client and
  server contract.

## Submission lifecycle

Workshop submission is non-terminal and has no confirmation dialog. It records
an immutable revision, displays an inline success banner without exposing a
participant-facing receipt link, and leaves the
workbench open. A team can continue reviewing and submit a later revision; the
latest accepted revision is selected for owner processing while earlier
receipts remain immutable. Non-workshop assignments retain their existing
terminal lifecycle.

Back to workshop submits the current reviewed work to the server first and
navigates only after acceptance. If validation or transmission fails, the
workbench remains open and reports the error. When no pair has a review
decision, there is nothing valid to submit and the link returns immediately.

A skipped joiner can enter the active workbench without an expertise answer.
If they later complete the form, the versioned assessment contract and database
migration store those answers with `post_review_followup` context rather than
misclassifying them as `pre_review`. Outstanding forms remain visible after
submission in the success banner and on the owing reviewer's Workshop page,
even when that reviewer is currently viewing another team. A link opened by the
owing reviewer shows the actual expertise and graph-familiarity form with their
name and reviewer ID. The same address opened in another member's signed-in
browser shows an identity-handoff page, preventing answers from being
attributed to the wrong person.

## Verification

The focused regression suite is:

```bash
.venv/bin/pytest -q \
  tests/test_ipl_workshop_groups.py \
  tests/test_review_ui.py \
  tests/test_v2_phase4_profiles.py
```

The tests cover batch-scoped team creation, per-reviewer last-opened assignment
memory, multiple teams for the same batch, direct collaborator addition,
explicit member identity, a non-duplicated batch catalogue, structured-name and
contact-email collection, cross-team outstanding-form recovery, identity-safe
handoff, direct setup/join routes, workshop-only presentation scoping, stable
attribution, submission behavior, and existing profile behavior.
