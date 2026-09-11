# IPL workshop: minimal release plan

**Status:** standalone, time-boxed delivery plan

**Prepared:** 10 September 2026

**Workshop:** Wednesday 16 September 2026, 09:00–10:45 CET

**Expected attendance:** 2–12 participants

**Admission cap:** 30 participants

## 1. Decision

This is a separate plan. Do not amend or try to finish the full v2 plan before
the workshop. After the workshop, rewrite the v2 plan against the current ICF
hosting setup and fold the successful workshop work into it.

The workshop release has one narrow purpose: allow individually registered and
consented participants to form reviewing groups of one or more people in the
room, select one of four identical-for-everyone KG packages, complete the work,
and return for another assignment.

This plan covers the data and pipeline changes needed to make that journey
honest and reliable. The IPL-specific redesign of the initial-review workbench
is a separate piece of work and will have its own plan. This release must expose
the group and assignment context that redesign will need, but it does not decide
the redesign's layout, wording, or interaction priorities.

Production remains the dedicated ICF Ubuntu VPS described in
[`ICF_HOSTING_BOUNDARY.md`](ICF_HOSTING_BOUNDARY.md). The legacy WSL deployment
is out of scope.

## 2. Minimal participant journey

1. A participant signs in through the normal email flow if SMTP is ready, or
   uses the single shared workshop entry code if it is not.
2. The system creates a distinct reviewer identity and session for that person.
   The shared code is never a shared account.
3. The participant sees the approved notice, gives affirmative consent, and
   completes their own profile.
4. In the room, the participant either creates a reviewing group or joins one
   using a short group-join code shown by another participant.
5. A group may contain one or more reviewers. A person working alone is simply
   a one-member reviewing group; there is no separate assignment path.
6. The group sees the same four frozen KG packages as every other group. The
   facilitator can advise which one to choose.
7. Each group member completes their own KG-specific pre-assignment questions.
   Existing profile and assessment answers provide expertise information; the
   group has no expertise-role fields. A participant added after review has
   started may defer these questions until the assignment ends.
8. The group claims a package and completes one joint assignment. Any signed-in
   member of that group may operate the browser and submit it.
9. While the assignment is active, another registered and consented participant
   may join the group and access its work if extra expertise is needed. They may
   already be contributing to another group. Missing KG-specific questions do
   not prevent them from joining or prevent the group from submitting.
10. Leaving an unfinished assignment preserves it for return. Finishing,
   submitting partial work, or abandoning it returns the group to the package
   page.
11. The group may choose another package if the facilitator enables further
    assignments.
12. Several groups may independently review the same frozen package. They
    remain distinct rater units for inter-rater analysis.

Teams do not need to be known or allocated in advance. Packages are
reviewer-neutral and reusable; no participant or group identifiers are baked
into them.

## 3. What already exists

The current v2 application already provides most of the individual-reviewer
foundation:

- owner-created reviewers and email login-code sessions;
- pseudonymous reviewer IDs and separation of profiles from review artifacts;
- profile onboarding and stored technical, language, and domain expertise;
- frozen KG seeds and individual KG expertise/familiarity assessments;
- reviewer-neutral initial-review bundles with recorded digests;
- assignments with assessment gating and reviewer isolation;
- a hosted initial-review workbench with browser-local draft recovery;
- authenticated submission, immutable export receipts, processing jobs, and
  owner decisions;
- synthetic end-to-end and local-hardening test paths; and
- a documented ICF production boundary.

These should be extended, not rebuilt.

## 4. What is missing for IPL

Only the following is workshop-critical:

1. **Fallback admission:** one time-limited, capped, revocable shared entry code
   that creates separate reviewer accounts and sessions.
2. **Affirmative consent:** an explicit consent gate before profile collection,
   using the final approved notice and statement.
3. **Reviewing groups:** a simple 1+ member group and self-join mechanism, with
   no roles and no membership-history workflow.
4. **Reusable packages:** four owner-prepared frozen KG packages visible to all
   eligible groups and independently claimable more than once.
5. **Group attribution:** assignments, submissions, and processing provenance
   must treat the reviewing group as one rater while retaining its contributor
   reviewer IDs.
6. **Simple lifecycle:** return, finish, partial submit, and abandon without
   trapping the participant in a dead assignment.
7. **ICF deployment:** install and rehearse the exact release on the current VPS,
   not WSL.
8. **Workshop operation:** a short run sheet, safe counts, code close/revoke,
   and post-session receipt check.

## 5. Minimal data model

Implement this in focused migrations while preserving existing individual
assignments and submissions.

### 5.1 Workshop admission

```text
workshop_rounds
  id
  name
  status                    draft, open, closed
  opens_at
  closes_at
  max_participants
  allow_additional_assignments
  created_at

workshop_entry_codes
  id
  workshop_round_id
  code_digest               never plaintext
  expires_at
  max_redemptions
  redemption_count
  revoked_at                nullable
  created_at

workshop_entry_redemptions
  id
  entry_code_id
  reviewer_id
  redeemed_at

workshop_admission_nonces
  nonce_digest              one-time, browser-bound; never plaintext
  reviewer_id
  created_at

workshop_admission_attempts
  id
  candidate_digest          never the presented code
  context_digest            trusted remote-address digest
  requested_at
```

Add only the reviewer fields needed to distinguish fallback registration and
record consent:

```text
registration_method         email_invitation or workshop_code
email_verified_at           nullable
consent_statement_version   nullable
consented_at                nullable
```

The code, reviewer allocation, redemption count, one-time nonce, and session
creation are one transaction. An already authenticated participant must log out
before registering another person. Durable attempt records enforce the same
remote-address and candidate-code limits across worker processes and application
restarts; client-controlled headers such as User-Agent are not bucket keys. Only
one shared entry code is needed for the workshop.

### 5.2 Reviewing groups

Use “reviewing group” in the data model because it covers both a single reviewer
and a team without special cases.

```text
review_groups
  id
  workshop_round_id
  join_code_digest
  created_at

review_group_members
  group_id
  reviewer_id
  joined_at
  primary key               group_id, reviewer_id
```

There are deliberately no expertise roles, invitations, acceptance states,
drivers, member history, or administrator pre-allocation.

A signed-in participant creates a group and receives a short join code. Other
signed-in, consented participants add themselves using that code. A participant
who will work alone creates a group and continues without inviting anyone.

Joining remains open while the group's assignment is active. This supports the
workshop case where a reviewer starts, discovers that more expertise is needed,
and asks another participant in the room to help. Joining is additive: there is
no member-removal or membership-history workflow during an assignment. A newly
joined member may access and contribute to the existing work immediately. If
their KG-specific questions are missing, the application records them as owed;
this never blocks access, submission, partial submission, or abandonment.

A reviewer may belong to more than one group in the round, so a scarce expert
can help another active assignment. Membership is unique only within a group.
Joining means being named as a contributor to that assignment; it is not an
informal observer role.

The active assignment page keeps the group's join code available through a
simple “add a participant” action. Joining the group while it has an active
assignment also joins that assignment; it does not create or restart one.

The contributor set freezes into the assignment when it reaches a terminal
outcome (complete, partial, or abandoned). A later change requires a new
assignment; it never changes the attribution of closed work.

After any terminal outcome, each contributor with an owed KG-specific form sees
a clear reminder and direct link to complete it. The assignment remains closed
whether or not that follow-up form has yet been completed.

### 5.3 Reusable KG packages

```text
workshop_work_packages
  id
  workshop_round_id
  kg_id
  seed_version
  seed_digest
  display_name
  short_description
  display_order
  bundle_path
  bundle_digest
  processing_recipe
  enabled
  created_at
```

Prepare four primary packages. A package is a frozen template, not an
assignment. Any number of groups may claim it and receive separate assignment
and submission IDs over identical item identities and digests.

The four workshop packages are fixed as follows:

| Display name | Canonical `kg_id` |
| --- | --- |
| Europeana Knowledge Graph | `europeana` |
| NFDI4Culture Culture Knowledge Graph (CKG) | `nfdi4culture` |
| Camera dei Deputati Knowledge Graph | `camera-dei-deputati` |
| CDEC Knowledge Graph | `cdec` |

Each package contains two passes: received deduplicated candidates first, then
the remaining eligible all-pairs candidates. The record set and pass membership
are frozen in the package digest. Presentation is randomised independently for
each assignment within each pass, so a group never sees a pair twice and
multiple groups naturally provide overlapping judgments for inter-rater data.

### 5.4 Group assignments and submissions

Extend `review_assignments` so an assignment is owned by either a legacy
individual reviewer or a reviewing group:

```text
reviewer_id                 nullable for group assignments
review_group_id             nullable for legacy assignments
work_package_id             nullable for legacy/manual assignments
participant_status          not_started, active, completed, partial, abandoned
claimed_at                  nullable
completed_at                nullable
completion_item_count       nullable
completion_total_count      nullable
closed_contributor_ids      nullable JSON; frozen on terminal outcome
```

For new rows, enforce exactly one of `reviewer_id` and `review_group_id`.

Keep the existing pre-assignment responses as individual reviewer data. The
initial members complete the selected KG's prompts before starting work. A
member added during review receives access immediately. If they have not
completed the selected KG's prompts for this assignment, derive an outstanding
form flag from the assignment membership and existing assessment records.
Application and database checks must ensure each response belongs to a member
of that assignment's group, but the outstanding flag must not be used as a
submission gate.

Do not create a general membership-history table. At submission, store the
small immutable authorship snapshot actually needed for provenance:

```text
review_submissions
  review_group_id           nullable for legacy submissions
  submitted_by_reviewer_id  authenticated member who pressed submit
  contributor_reviewer_ids  pseudonymous member IDs frozen at submission
```

The export contract must similarly record the group ID, contributor IDs, and
authenticated submitter. The pipeline counts the group submission as one
judgment. It must not duplicate that judgment once per member.

Current compatibility boundary: group exports may contain review events by any
frozen contributor, while the envelope reviewer remains the authenticated
submitter. The browser importer and benchmark builder validate that distinction
without expanding contributors into separate judgments. Content-equivalent
HTTP retries recover the original receipt despite a fresh browser export
timestamp or a different frozen member acting as submitter.

## 6. Authentication and consent

### 6.1 SMTP-first, shared-code fallback

Keep the existing email-code flow as the preferred route. Add the shared route
so the owner can enable it if the production SMTP rehearsal has not passed by
the chosen cutoff.

The shared workshop code must:

- be generated by the application and stored only as a digest;
- be valid only for the configured round and time window;
- have a participant cap and attempt throttling;
- reject retries from an active signed-in browser and make parallel duplicate
  form submissions consume at most one place;
- persist digest-only throttling by trusted remote address across application
  restarts, without using User-Agent or another client-controlled bucket key;
- be closable and revocable immediately;
- never appear in URLs, logs, analytics, exports, or persistent browser storage;
- create a distinct reviewer ID and secure session per redemption; and
- route directly to consent, never directly to a package or workbench.

The synthetic fallback address is excluded from normal login-code issuance and
verification, and the database prevents it from being marked verified. If SMTP
is unavailable, the practical MVP recovery procedure is facilitator-assisted:
keep the participant on the same browser where possible and provide one narrow,
audited owner reset operation for a genuinely lost session. Do not build a
general recovery system for next week.

### 6.2 Consent

Use the participant notice and consent statement already sent for approval.
The software work can proceed with versioned placeholder text, but real data
collection uses only the approved final wording.

The consent page must:

- appear before any profile form;
- show or link the approved notice;
- use an unticked affirmative checkbox;
- store the notice and statement versions with their acknowledgement
  timestamps;
- enforce consent server-side on profile, group, package, assignment, and
  submission routes; and
- provide the withdrawal route described by the approved notice.

Non-testing startup must reject missing, inline-only, empty, unreadable, or
synthetic notice/consent configuration. Approved production copy is loaded from
the three restricted files, while synthetic copy is confined to automated test
mode and the loopback-only synthetic pilot.

The notice should mention that participants may work alone or in a group and
that a joint submission is attributed to the group's pseudonymous members.
This is a clarification to the existing workshop description, not a request to
create expertise roles.

## 7. Package choice, overlap, and lifecycle

Package choice is intentionally simple:

- all eligible groups see the same four enabled packages, with Europeana and
  CKG labelled core and Camera dei Deputati and CDEC labelled specialist;
- there is no automatic matching or capacity allocation;
- claiming a package creates a fresh group assignment atomically;
- several groups may claim the same package;
- a group has at most one active assignment at a time; and
- package and seed digests are checked when claimed, opened, and submitted.

Assignment outcomes are:

- **Leave for now:** keep the assignment active and preserve the browser-local
  draft on that device;
- **Finish:** submit a complete durable result;
- **Submit partial:** submit the work completed so far and its item counts; or
- **Abandon:** close the assignment without a submission.

The list above is the Track A item 7 contract. The route and service
implementation now distinguishes complete and partial submissions, derives and
stores their item counts server-side, freezes the current contributor set, and
creates the same durable receipt and processing job for either submission type.
Abandonment atomically freezes the contributor set without creating a receipt.
Leaving is a non-mutating return to package choice, so the assignment and its
assignment-namespaced browser-local draft remain active.

Every terminal outcome returns to package choice. Additional claims are allowed
only when the round's `allow_additional_assignments` switch is on. That switch
is enough for spare capacity; no separate “old pairs” feature is required.
For each signed-in contributor, the outcome page also flags an outstanding
KG-specific form and links directly to it when one is owed. Completing that
form updates the flag but does not reopen or alter the assignment.

ALyrA, LinkedMusic, and the linguistic-dimensions study are outside this
workshop package set.

## 8. Pipeline boundary and separate UI work

This plan does not redesign the initial workbench. The pipeline work required
before the redesign is:

1. create an assignment from a frozen package for a reviewing group and permit
   authenticated late joins until the assignment closes;
2. pass assignment ID, group ID, contributor IDs, package digest, and safe
   return URL into the hosted workbench context;
3. namespace browser-local draft state by assignment ID so two groups using one
   computer profile cannot collide;
4. validate that the submitter is a current member and that the submitted
   attribution matches the server-owned assignment;
5. write an immutable group-attributed receipt before enqueueing processing;
6. preserve group attribution through processing and owner review; and
7. provide a pseudonymous overlap export keyed by group, package, assignment,
   item, bundle digest, completion type, and decision.

The later IPL workbench-redesign plan can change the sequence and presentation
inside the workbench without reopening group, package, or provenance design.

## 9. Deployment and data safety

Use the current ICF production boundary and add only the missing focused ICF
deployment artifacts: unprivileged web/worker service definitions, non-secret
environment template, HTTPS proxy configuration, migration/rollback steps, and
health/permission/log checks.

Do not change ICF's firewall, SSH, fail2ban, update, escrow, or backup controls.
All database, package, submission, and processing state stays under ICF-backed
paths. No reviewer personal data leaves the server; only the already permitted
pseudonymous research export may leave it.

The workshop does **not** require a new 15-minute off-site backup objective.
For this bounded release:

- verify that the database and referenced files are inside the existing daily
  encrypted backup set;
- perform one isolated restore test before the workshop;
- take an application-consistent local snapshot before deployment/migration;
- retain immutable submission receipts as they are created; and
- coordinate or trigger the next normal backup promptly after the workshop if
  the existing ICF procedure permits it.

Frequent local snapshots may be added as a low-risk operational convenience,
but are not a release blocker and are not a substitute for ICF's off-server
backup.

## 10. Governance: proportionate workshop position

The following distinctions prevent internal process from being mistaken for a
legal rule:

- **Shared-code entry:** there is no separate legal form called “shared-code
  approval.” Record the fallback and its controls in the workshop readiness
  note sent to ICF. If ICF objects or has a contrary policy, do not enable it;
  otherwise a second standalone approval exercise is not required.
- **Consent:** approved final notice/statement wording and an affirmative,
  demonstrable consent record are required before real participant data are
  collected. The material is already with ICF, so do not create a duplicate
  approval request.
- **Team working:** add one sentence to the notice/readiness note explaining
  pseudonymous joint attribution. No expertise-role or team-allocation approval
  is required unless ICF asks for a change.
- **DMP registration:** do not seek a separate pre-workshop confirmation. ICF
  has not identified it as an additional action needed for this workshop. Keep
  it as part of the wider project's governance work after the workshop; it is
  neither a software dependency nor, by itself, a GDPR precondition to this
  release.
- **Recovery time:** GDPR requires risk-appropriate measures and timely ability
  to restore access; it does not prescribe a universal 15-minute recovery
  point. The existing daily encrypted backup plus the checks above is the MVP
  position unless ICF identifies a stricter contractual requirement.

Because the checked-in hosting boundary currently lists SMTP and DMP among its
real-use gates, reconcile that document before deployment with this recorded
workshop decision. Do not leave two contradictory release rules in the
repository. This is a documentation/governance correction, not a request to
reconfigure ICF infrastructure.

## 11. Delivery order

Implementation status (11 September 2026): route and service implementation
through item 7 is complete. The database
foundation and participant-facing journey cover reviewing-group creation,
code-based self-join, reusable-package discovery, atomic package claims,
per-member assessment gating, and assessment-gated workbench access during the
open round. Browser-local group drafts are shared by assignment on the same
device. Any eligible group member can submit; the server freezes the current
contributor set, authenticated submitter, and group identity into the immutable
export and receipt, and preserves that attribution through isolated processing
and owner review. Browser recovery treats a fresh export timestamp—and a retry
by another frozen contributor—as the same submission when the review content is
unchanged. Canonical group exports round-trip through the importer and enter the
benchmark builder as one rater judgment. SMTP remains the preferred sign-in
path; the owner can issue or immediately revoke one digest-only shared entry
code. Durable digest-only throttling is keyed by trusted remote address rather
than User-Agent and survives application restart. Redemption rejects an active
signed-in browser, consumes a one-time admission nonce, and atomically creates a
distinct pseudonymous reviewer, cap record, and secure session before routing
to the consent boundary. Reissuing the code preserves round-wide redemption
counts and availability rather than resetting the displayed capacity.
For a genuinely lost fallback session, the owner can revoke the participant's
sessions and display a one-time recovery code; the reset is recorded in a
dedicated update- and delete-protected append-only audit table. Recovery-code
consumption and failed-attempt counting are serialized. Normal email login and
the database both prevent a fallback synthetic address from becoming verified.
All participant routing compares the stored privacy-notice and consent-statement
versions with both currently configured versions and requires both recorded
timestamps. Missing or obsolete consent therefore stays behind a dedicated
unticked affirmative-consent screen before profile collection, including after
a notice-only version change and application restart. One atomic acceptance
records both versions and timestamps. Non-testing startup rejects synthetic
configuration and requires all three approved copy files.
The complete notice is linked before login and throughout the site, together
with the approved email-based withdrawal route.
Group linguistic submission is kept unavailable as part of the explicit
linguistic-mode deferral. The item 6 package tooling now builds and validates
exactly the fixed four-KG set. Its v2 manifest embeds canonical annotation-free
selection pins; validation independently derives selection membership, record
order, the selection digest, and the package-set ID, and rejects noncanonical
bundle paths. Package files are replaced atomically without following existing
output symlinks, while seed import and draft-round package registration commit
in one transaction. Provisional packages remain disabled and unregistrable. The
received deduplicated subset and complete all-pairs membership; holdout pairs
are explicitly excluded from every IPL package.
Complete, partial, and abandoned outcomes now close group membership, return to
package choice, and expose another claim only when the round's
`allow_additional_assignments` switch permits it. Submission outcomes preserve
server-derived item counts and completion type in their immutable export and
processing audit. Pre-item-7 v2 group receipts without those additive fields
remain schema-valid; a retry derives and persists the missing assignment counts
before its queued job is processed. A frozen contributor who still owes the
KG-specific form sees a direct link after closure and can complete it without
changing the terminal assignment, including after the workshop round closes.
Submission, abandonment, retry, and late-join terminal races are serialized and
covered by regression tests. The owner-approved item 6 selection freeze, ICF
deployment, and rehearsal remain operational work.

### Track A — must work first

1. Freeze this scope for the recorded 16 September workshop and 30-person cap.
2. Add reviewing groups, membership, reusable packages, and group attribution.
3. Adapt assignment assessment checks and submission processing for groups.
4. Add SMTP-first/shared-code-fallback registration.
5. Add the versioned affirmative-consent gate.
6. Prepare and validate the four anonymous KG packages.
7. Implement finish, partial, abandon, return, and optional-next-assignment
   behaviour at the route/service level.
8. Deploy the exact passing commit to the ICF VPS.
9. Rehearse the whole journey with synthetic participants.

Tracks 2–6 can be developed together, but schema and provenance tests should
land before route/UI work so later redesign does not encode a false two-person
assumption.

### Track B — separate but parallel

- Obtain the final response on the participant notice/consent statement.
- Send ICF one concise readiness update covering self-formed 1+ person groups,
  joint pseudonymous attribution, and the shared-code fallback.
- Reconcile `ICF_HOSTING_BOUNDARY.md` with this time-boxed workshop decision;
  do not ask ICF for a separate DMP confirmation unless they raise it.
- Write the separate IPL initial-workbench redesign plan.

### Explicitly defer

- full v2-plan rewrite;
- role-labelled or automatically matched teams;
- membership invitations, acceptance, and history;
- live collaborative editing across browsers;
- automatic balancing of package claims;
- an inter-rater statistics dashboard;
- linguistic-dimensions mode; and
- a new 15-minute off-site backup system.

## 12. Verification and narrow go/no gate

Automated and synthetic rehearsal must prove:

- concurrent use of the shared code creates distinct reviewers and cannot
  exceed its cap;
- replay from an authenticated browser and parallel submission of the same
  admission form consume at most one place;
- workshop attempt limits survive application restart and cannot be bypassed by
  changing User-Agent;
- reissued codes report and enforce total round-wide redemptions;
- fallback addresses cannot enter normal email verification, recovery guesses
  and consumption are serialized, and recovery audit rows cannot be updated or
  deleted;
- no protected route works before current consent and profile completion;
- non-null consent for an obsolete statement or notice version still routes to
  the consent boundary, including after application restart;
- non-testing startup rejects missing, synthetic, empty, unreadable, or
  inline-only consent copy, while complete approved file-backed copy renders;
- email and shared-code admission both record the same current consent, while
  owner access remains independent of participant consent;
- a one-member group and a multi-member group can each complete the journey;
- a participant may join a second group without losing access to their first;
- a consented/profile-complete participant can join before or during active
  review and access the existing work immediately;
- a missing late-joiner KG form never blocks complete submission, partial
  submission, or abandonment;
- after any terminal outcome, that contributor sees an outstanding-form flag
  and direct link until the form is completed;
- no one can join after a terminal assignment outcome;
- two groups can claim the same package without sharing assignment or draft
  state;
- submission attribution is server-derived and freezes the member set present
  when the assignment closes;
- submission versus abandonment, retry versus abandonment, and late join versus
  either terminal operation serialize to one consistent outcome;
- legacy v2 group receipts validate, retry idempotently, backfill authoritative
  counts, and produce a processing audit with a non-null total;
- a group submission is processed as one judgment, not one per contributor;
- leave, finish, partial, and abandon all return to a usable package page;
- web and worker recover after restart, and durable receipts remain intact;
- external HTTPS/session/origin/CSRF/logging checks pass; and
- an isolated database-plus-file restore succeeds.

Real workshop collection is a **no-go** only if one of these essential
conditions remains red at the agreed cutoff:

1. the final participant notice and consent statement have not been approved;
2. the deployed application fails its security, consent, or data-isolation
   checks;
3. the four primary packages are not frozen and validated;
4. group attribution or durable submission fails in the synthetic rehearsal;
5. the production database/files are outside the existing backup set or the
   restore test fails; or
6. ICF explicitly instructs the project not to proceed.

SMTP failure alone is not a no-go if the shared-code fallback passes rehearsal.
DMP registration status is not part of this workshop's go/no gate. Absence of a
15-minute recovery point is not a no-go.

## 13. Workshop run sheet

Before participants arrive:

- verify HTTPS health, web/worker state, disk space, database access, and owner
  access;
- confirm the four package digests and run one synthetic claim/submission;
- confirm the last backup status and keep the pre-workshop snapshot;
- choose SMTP or enable the shared-code fallback; and
- open the workshop round shortly before entry.

During the workshop:

- display the one shared entry code only if needed;
- help participants create or join groups in the room;
- advise package choice without pre-assigning teams;
- monitor safe aggregate counts and service health, not participant answers;
- close shared entry after arrivals, reopening only for a late participant; and
- use the additional-assignment switch only if the core round is stable.

At the end:

- close the round;
- compare expected assignments with durable submission/abandonment records;
- record any incident or manual recovery action;
- confirm worker queue state; and
- arrange the next normal encrypted backup as soon as the ICF process allows.

## 14. Inputs still needed from the owner

These choices do not prevent schema/auth work from starting:

1. whether an abandoned or partial assignment permits another primary package;
2. the shared-code opening time (its redemption cap is fixed at 30); and
3. the cutoff at which failed SMTP causes the shared-code fallback to be
   enabled.

Everything else in this plan has a simple default: groups contain one or more
participants, people add themselves in the room, all groups see the same
packages, profiles carry expertise, and every group submission is one rater
judgment with all contributors recorded.
