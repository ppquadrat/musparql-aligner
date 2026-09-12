# IPL workshop UX implementation

**Status:** implemented on 12 September 2026

This change implements the participant-flow and workshop-workbench proposal in
[`IPL_WORKSHOP_UX_PROPOSAL.md`](IPL_WORKSHOP_UX_PROPOSAL.md) without changing
the established non-workshop review interfaces.

## Delivery plan

1. Gate workshop access on a complete profile, including a contact email for
   shared-code participants.
2. Replace the group-first dashboard with one selected team strip and one
   review-batch catalogue.
3. Route batch selection through the frozen KG form and directly into the
   workshop workbench.
4. Give active-team joiners a direct route to the review with optional setup.
5. Apply a workshop-only presentation mode to the existing initial-review
   workbench and preserve its review decisions, local draft key, attribution,
   and stable package ordering.
6. Verify the route, service, profile, and browser-static contracts with
   synthetic tests.

## Implemented behavior

- The first Workshop-page visit silently creates a one-person team when the
  participant has no team in the open round.
- The Workshop page shows the current six-digit team code, member count,
  concise join/share guidance, one join field, and each enabled batch exactly
  once. Participant wording uses *team*, *review batch*, and *review*.
- Batch cards expose contextual states and actions: available/Start, setup
  needed/Complete setup, in progress/Continue, and submitted.
- Saving the frozen KG-specific questions opens the workbench directly.
  Joining an active team opens its setup page, which includes a visible
  **Skip for now and join review** route. Skipping creates no expertise value.
  It creates an append-only, pseudonymous deferral event with reviewer,
  assignment, and timestamp.
- Shared-code profiles collect a normalized, uniqueness-checked contact email.
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
an immutable revision, displays an inline success banner, and leaves the
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
submission in the success banner and through the attributable assessment path.

## Verification

The focused regression suite is:

```bash
.venv/bin/pytest -q \
  tests/test_ipl_workshop_groups.py \
  tests/test_review_ui.py \
  tests/test_v2_phase4_profiles.py
```

The tests cover silent team creation, a non-duplicated batch catalogue,
contact-email collection and unverified persistence, direct setup/join routes,
workbench team context, workshop-only presentation scoping, stable attribution,
submission behavior, and existing profile behavior.
