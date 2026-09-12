# IPL workshop participant-flow and workbench proposal

**Status:** implemented for the workshop flow on 12 September 2026; the
revision-after-submission lifecycle remains deliberately deferred as documented in
[`IPL_WORKSHOP_UX_IMPLEMENTATION.md`](IPL_WORKSHOP_UX_IMPLEMENTATION.md)

**Recorded:** 12 September 2026

This proposal covers only the IPL workshop journey and an additional
workshop-specific initial-review workbench. It does not replace the existing
general review, comparison, correction, or linguistic interfaces.

## 1. Problems observed in the rehearsal

The current implementation exposes its data model too directly:

- participants must create or choose a reviewing group before they can see the
  available KG work;
- a reviewer can see several visually similar group sections, each containing
  its own package choices;
- choosing a package opens a KG-specific pre-assignment form, but completing
  the form returns to the group-heavy workshop page instead of opening the
  workbench;
- “workshop”, “package”, “assignment”, “group”, and “team” appear as separate
  participant concepts even though most people only need to choose a KG and
  start reviewing; and
- the general workbench exposes dataset, filtering, record-list, provenance,
  and export controls that are unnecessary in the time-boxed workshop.

The desired mental model is simpler: **choose a review batch, answer the short
KG-specific questions, and review its pairs**. Team joining is an optional
collaboration feature, not the gateway to the batch list.

## 2. Participant terminology

Use these participant-facing terms consistently:

- **Review batch:** one KG's frozen set of pairs, for example Europeana or CKG.
  This is the participant name for an internal `workshop_work_package`.
- **Team:** one or more reviewers working on the same claimed batch. A person
  working alone still has an internal one-person `review_group`, but the UI
  does not require them to create or understand it.
- **Review:** the team's working instance of a batch. Avoid displaying the
  internal word “assignment” except where an operational identifier is needed.

Pair IDs remain visible because they are useful when discussing an item with a
facilitator.

## 3. Recommended journey

1. The participant signs in, consents, and completes their profile, including
   a required contact email address, before any batch can be started or joined.
   For shared-code admission the address is retained but remains unverified.
2. On first entering the workshop, the server silently creates a one-person
   team if the participant has no current team context.
3. The participant lands on a single **Workshop** page that immediately shows
   the four review batches, with Europeana and CKG first and Camera dei
   Deputati and CDEC marked as specialist batches.
4. A compact team strip on that page shows the current team code and a field
   for joining another team. It does not wrap the batch list or duplicate it
   once per team.
5. Choosing a batch opens its KG-specific expertise/familiarity form.
6. Saving that form opens the workshop workbench directly. It must not return
   to an intermediate team-choice page.
7. A teammate can enter the visible six-digit team code on their own Workshop
   page. If that team already has an active batch, the joiner is offered the
   short KG-specific form with a clear **Skip for now and join review** option.
   Saving or skipping then opens the same review directly.
8. Returning from the workbench goes to the Workshop page, where the active
   batch has a clear **Continue review** action.

The Workshop page is the only participant dashboard. Separate per-assignment
detail pages should be folded into the batch setup step or used only when a
specific outstanding KG form needs attention.

## 4. Workshop-page layout

Prefer one assignment-first page over equal “teams” and “assignments” columns.
Two equal columns would still make teams look like a required planning task and
would compress poorly on laptops and phones.

The page should contain:

1. a compact current-team strip:
   - `Team 123 456`;
   - member count, and optionally pseudonymous member IDs behind a disclosure;
   - **Join another team** with one six-digit field; and
   - no “create team” action in the ordinary path;
2. one list or grid of review batches, shown once:
   - batch/KG name;
   - short domain description;
   - core or specialist marker;
   - status: available, setup needed, in progress, or submitted; and
   - one contextual action: **Start**, **Complete setup**, **Continue**, or
     **View submission**.

If a scarce expert belongs to more than one team, show only one selected team
context at a time in the compact strip, with a small team switcher. Do not
render full duplicate team panels or duplicate the batch catalogue.

## 5. Joining and team identity

Keep self-service joining by six-digit team code. Do not add a reviewer to a
team merely because another participant enters their reviewer number:

- the joining reviewer should actively opt in while authenticated;
- reviewer-number entry is vulnerable to transcription errors;
- it can add someone who has not agreed to that collaboration; and
- a team code communicates the intended team without treating a person's
  pseudonymous identity as an invitation credential.

The workbench may label the code **Add a teammate — ask them to enter 123 456
on their Workshop page**. A copy button may be present, but the number must be
large and spaced so it can be spoken across the room.

Joining is not live co-editing. Browser-local drafts on two devices do not
automatically merge. The workshop instructions and team strip must say that a
team should nominate one device/person to submit, or the application must gain
server-side draft synchronisation before claiming collaborative editing.

### Optional KG form for a joining teammate

The KG-specific form is useful covariate data, but it must not interrupt or
prevent a scarce expert from joining an active review. Immediately after a
reviewer enters a valid team code:

1. show the form for that team's active KG, if the reviewer has not already
   completed the frozen questions;
2. explain concisely: **These questions ask about your knowledge and
   familiarity before reviewing this batch**;
3. make **Save and join review** the primary action;
4. provide **Skip for now and join review** as a visible secondary action; and
5. route both actions directly to the workshop workbench.

Skipping records an explicit deferred state with reviewer, assignment, and
timestamp. It is not an expertise value and must not be interpreted as “none.”
It never blocks access, review, or submission.

Timing provenance matters. An answer collected before the reviewer sees or
discusses the batch may retain `pre_review` context. If the reviewer skips and
answers after contributing, store it as a distinct `post_review_followup`
context (or equivalent explicit timing field), not as `pre_review`. The current
assessment schemas and database constraints allow only `pre_review` and
`profile`, so this requires a versioned schema and migration before
implementation. If that change cannot be completed safely before the workshop,
collect the late form as explicitly labelled follow-up data outside the
pre-review analysis rather than misclassifying it.

After an accepted submission, show a non-blocking success panel such as:

> Submitted successfully. The KG background form is still missing for
> reviewer-1234. Please complete it if you have time. **Fill in the form**

For several missing contributors, list each pseudonymous reviewer ID and link
only the signed-in reviewer to the form they are authorised to complete. The
submission remains valid whether or not any outstanding form is later filled.
The owner dashboard may show pseudonymous missing-form status for operational
follow-up.

Every participant profile includes a required contact email address before the
participant can see or join an assignment. Email-invited reviewers have already
verified that address through login. A shared-code participant enters it during
profile completion; it remains explicitly unverified, so later contact is
best-effort and may fail if mistyped, but the owner may use it to request an
outstanding form where the approved notice permits that follow-up.

The current shared-code implementation instead leaves a synthetic placeholder
address on the account and does not expose an email field in profile editing.
That is an implementation gap against this proposal. The eventual change must
normalize and uniqueness-check the supplied address, retain
`email_verified_at = NULL`, keep email out of review bundles and exports, and
continue to block every assignment route until the full profile is complete.
Collection and follow-up wording remain subject to the approved participant
notice and consent materials.

## 6. Workshop-specific workbench

Select this additional interface only for an authenticated IPL workshop review
batch. Leave every existing workbench available for its current use cases.

### Header

The header contains only:

- top right: **Signed in as reviewer-NNNN**, **My profile**, **Sign out**;
- **Submit current work**;
- **Back to workshop** (preferred wording because it returns to the review
  batch list; use “Back to review” only if it returns to a specific review
  detail page);
- the visible six-digit team code with brief joining instruction;
- `Records: X`; and
- `Reviewed: Y`.

Do not show dataset, visible-count, holdout, run, confidence, filters, import,
export, or correction-workbench controls in this mode.

### Pair frame

Use one frame rather than a record list plus multiple detail panels:

1. top row: pair ID on the left and **Previous / Next** on the right;
2. two-column content on ordinary screens:
   - left: SPARQL;
   - right: model question;
   - immediately below the question: origin type
     (`verbatim`, `paraphrased`, or `generated`); and
   - retained evidence phrases, when present;
3. below the pair content: retain the existing reviewer-decision frame and its
   controls; and
4. on a narrow screen, stack SPARQL, question/provenance, and decision in that
   order.

Do not show an example list, filters, model/run metadata, confidence, empty
evidence scaffolding, or other provenance fields. Absence of retained evidence
should be represented by absence, not a large empty panel.

The serving contract remains unchanged: deduplicated pairs first, randomised
within that pass, followed by remaining pairs randomised within their pass. The
order is stable for that review across reloads.

## 7. Submission and closure recommendation

For this workshop, manual assignment closure is unnecessary participant work.
Use one **Submit current work** action:

- it creates an immutable, attributable receipt for the currently reviewed
  pairs;
- zero reviewed pairs produces a direct inline prompt rather than a server
  error;
- repeated submission creates a revision or returns the existing idempotent
  receipt; and
- the participant does not choose between “complete” and “partial”. Counts
  determine that description automatically.

The preferred next lifecycle is for submission not to close the review. The
team may continue and submit a later revision during the workshop; the round
deadline stops new admission and claims but does not revoke an already claimed
review. At finalisation, the latest accepted revision is the candidate for
owner processing, while every earlier receipt remains immutable.

This lifecycle is now implemented for workshop assignments through coordinated
participant-status, contributor-snapshot, revision, and processing-selection
changes. Submission records a revision without closing the workbench and does
not require a confirmation dialog. Non-workshop assignments retain their
existing lifecycle.

There should be no ordinary participant-facing **Abandon**, **Finish**, or
**Close assignment** action. Exceptional cleanup can remain an owner-assisted
operation.

## 8. Acceptance criteria for a later implementation

- A newly profiled participant sees all four review batches without creating
  or selecting a team.
- Both email-login and shared-code participants must provide a profile contact
  email before seeing or joining a batch; shared-code email remains visibly
  unverified and never enters review artifacts.
- Each batch appears once, regardless of the participant's team memberships.
- Starting a batch, completing its KG form, and reaching its workbench requires
  no intermediate dashboard choice.
- A six-digit team code is visible on the Workshop page and in the workbench.
- Entering that code as another authenticated reviewer joins the intended team
  and offers the optional KG form before routing to its active batch.
- A joining reviewer can skip the KG form without blocking team access or
  submission, and the skip is not stored as an expertise answer.
- A late KG form is distinguishable from a genuine pre-review assessment.
- Submission success identifies outstanding forms by pseudonymous reviewer ID
  and provides an authorised link without weakening the accepted submission.
- The workshop workbench contains only the specified header, pair frame, and
  existing decision frame.
- Deduplicated-first pass ordering and assignment-stable randomisation remain
  intact.
- Back navigation never submits, discards, closes, or changes team membership.
- Submission never selects an invalid complete/partial endpoint.
- The workshop deadline cannot turn an existing review or submission into a
  404.
- Existing non-workshop workbenches are visually and behaviourally unchanged.
