# IPL workshop participant-flow and workbench proposal

**Status:** proposed from synthetic rehearsal feedback; not implemented

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

1. The participant signs in, consents, and completes their profile.
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
   page. If that team already has an active batch, the joiner completes any
   required KG-specific questions and then opens the same review.
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

This differs from the current terminal-submit implementation and requires a
deliberate backend change to participant status, contributor snapshots,
processing-job selection, and owner finalisation. It must not be achieved by a
label-only UI change. Until that change is made, the confirmation must state
that submitting closes the review.

There should be no ordinary participant-facing **Abandon**, **Finish**, or
**Close assignment** action. Exceptional cleanup can remain an owner-assisted
operation.

## 8. Acceptance criteria for a later implementation

- A newly profiled participant sees all four review batches without creating
  or selecting a team.
- Each batch appears once, regardless of the participant's team memberships.
- Starting a batch, completing its KG form, and reaching its workbench requires
  no intermediate dashboard choice.
- A six-digit team code is visible on the Workshop page and in the workbench.
- Entering that code as another authenticated reviewer joins the intended team
  and routes to its active batch.
- The workshop workbench contains only the specified header, pair frame, and
  existing decision frame.
- Deduplicated-first pass ordering and assignment-stable randomisation remain
  intact.
- Back navigation never submits, discards, closes, or changes team membership.
- Submission never selects an invalid complete/partial endpoint.
- The workshop deadline cannot turn an existing review or submission into a
  404.
- Existing non-workshop workbenches are visually and behaviourally unchanged.

