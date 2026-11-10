# Open issues

This is the maintained index of known implementation and governance work. Open
items are listed first. A short resolved reference is retained where it explains
the provenance of the currently deployed workshop packages; other completed
findings belong in the implementation, tests, and durable runbooks.

## Workshop admission throttling and 16 September incident

The 16 September 2026 IPL workshop exposed a production-blocking conflict
between the shared-code admission design and an ordinary venue network. The
round allowed 30 participants, but workshop-code admission separately allowed
only ten attempts from one trusted remote address in a rolling 15-minute
window. Successful and unsuccessful attempts both consumed that allowance.
Multiple participant devices behind the venue's shared NAT therefore appeared
as one request context and exhausted the limit even though the round remained
well below capacity.

The production evidence is consistent with that failure mode. The first code
had 17 successful redemptions in total and its last successful redemption was
at 07:27:11 UTC. A replacement code was issued at 07:28:42 UTC, immediately
revoking the first code, but reissuing did not clear the independent
request-context throttle. At the time of diagnosis one digest-only context
bucket was at the configured limit of ten attempts. The replacement code's
first successful redemption was not until 07:46:11 UTC, after the 15-minute
window had aged out; it then received two successful redemptions. The new code
was valid throughout, so replacing it could not resolve the actual block.

This materially disrupted the collection. The final operational audit found
19 code redemptions, 16 active workshop accounts, 14 teams with valid
submissions, and 49 schema-valid, digest-intact receipts whose processing jobs
all succeeded. Four active accounts never joined a team or started an
assignment, and two active accounts had started assignments but no receipt.
These aggregates establish the durable state that landed; they do not show how
many physical devices were used because device, User-Agent, session, and IP
identifiers are deliberately not stored with submissions.

SMTP does not make this issue disappear. Normal email login currently applies
three requests per email address and ten requests per trusted remote address in
the same 15-minute window. A cohort requesting email codes from one venue Wi-Fi
could therefore reproduce the incident: later participants would receive the
membership-neutral response but no email would be sent. Existing fallback
workshop-code accounts also cannot automatically become email-login accounts,
because their synthetic addresses are intentionally unverified.

The verification gap was that concurrent shared-code tests supplied a distinct
synthetic IP address for each participant. Submission load tests exercised
concurrent reviewers but did not test admission or email-code delivery for many
independent browsers behind one forwarded address. The implementation therefore
proved the participant cap, per-context throttle, and submission concurrency in
isolation without proving their combined behaviour on a realistic venue NAT.

Do not reuse the current admission or email-login limits for another cohort
until all of the following are complete:

- redesign the shared-network limit so the expected cohort plus retry headroom
  can authenticate behind one NAT while retaining per-address, per-code, round-
  capacity, expiry, nonce, and failed-guess protections;
- ensure valid admissions and valid invited-address email requests cannot
  exhaust a small venue-wide failure budget merely through normal use;
- add shared-code and SMTP tests with at least 30 distinct browsers/accounts
  behind one trusted remote address, including retries and concurrent bursts;
- give the owner a pseudonymous, non-sensitive warning when a context throttle
  is blocking admission or email delivery, plus a narrow audited reset or
  recovery procedure that does not weaken the infrastructure boundary;
- preserve membership-neutral participant responses without silently treating
  the absence of an email as adequate operational feedback; and
- record a new production acceptance exercise using the deployed proxy path
  before admitting another real cohort.

The later `a61ba62` deployment fixed a separate submission-export defect in
which an undecided touched item was sent with
`benchmark_disposition: null`. It did not repair the admission throttle; the
replacement workshop code began working because the rolling window expired.

## Reviewer administration and privacy

### Reviewer privacy approval and real-data gate

Reviewer profiles, repeatable general-domain expertise, KG-specific subject
expertise, and resource/data-model/KG familiarity have versioned Phase 1 schemas,
append-only history contracts, chain and seed-snapshot validation, synthetic
examples, a privacy notice, and a confidential storage boundary. Phase 4 now
provides reviewer onboarding and correction, versioned notice acknowledgement,
the versioned local suggestion set with free-text fallback, and owner-visible
pseudonymous completion state. The documented legacy registry was never
populated, so Phase 2 creates the v2 database directly and deliberately does not
add legacy-value tables or infer v2 assertions from legacy scales.

The proposed decisions, working ICF-controller assessment, rights/incident
procedures, and ICF/ODOMA questions are now recorded in
[`REVIEWER_DATA_GOVERNANCE_DRAFT.md`](REVIEWER_DATA_GOVERNANCE_DRAFT.md).
ICF has confirmed the controller allocation, consent basis, participant contact
route, ICF-owned hosting arrangement, retention position, and allocation of
rights and incident responsibilities; these decisions are recorded in
[`ICF_HOSTING_BOUNDARY.md`](ICF_HOSTING_BOUNDARY.md). ICF approved the final
participant notice and affirmative-consent wording on 15 September 2026 as
`musparql-workshop-2026-09-16-v1`. The approved source and deployable copy are
checked in. The production service must be updated to that version before real
participants enter. The remaining ICF data-management-plan registration check
and applicable technical gates still need an explicit readiness record.

The profile now offers a separate, optional, unticked preference allowing email
invitations to future Musparql review rounds. ICF approval of that additional
purpose and participant-facing wording is still required before deploying the
preference for real participants; it is not silently treated as part of the
already approved workshop notice.

Retention periods, access/correction/deletion procedures, and the proposed
consequences of withdrawal are decided for implementation but remain subject to
that controller approval.

The form must never place names, email addresses, affiliation, experience, or
KG-familiarity fields in review bundles, exports, benchmarks, logs, or tests.

### Durable backup and recovery

ICF now supplies daily client-side-encrypted restic backup of `/etc`, `/home`,
`/opt`, and `/srv`, with 90-day retention and integrity checking. Application
state is kept under those backed-up paths. Backup design must still protect more
than the database: review outcomes remain irreplaceable before benchmark
publication, and substantial provenance is intentionally Git-ignored.

The detailed Phase 2b plan is in
[`PHASE_2B_BACKUP_RECOVERY_PLAN.md`](PHASE_2B_BACKUP_RECOVERY_PLAN.md), but its
home-server/Google Drive topology is historical. The current work is to prove a
coherent application restore using ICF's backup: WAL-safe database capture,
matching referenced files, isolated validation, deletion replay, owner-visible
failure alerts, and an accepted recovery-point objective. No real reviewer data
may be collected until those gates pass.

Define, implement, and test an encrypted, authenticated, versioned backup of:

- the confidential and operational SQLite database;
- server-received non-holdout review submissions and sanitized exports;
- the working query catalogue and its local execution/correction provenance;
- frozen generation runs and any model output not already frozen into a run; and
- separately, through a human-only process, any private or holdout-bearing
  review material that application and agent workflows must never access.

ICF controls the off-server repository, encryption material, retention, and
infrastructure-level integrity checks. The application must not access or
reconfigure those secrets. A second directory on the same disk must not be
represented as a backup. Hosted durable submission protects accepted reviews
from browser-local loss, but those receipts and submission files remain unique
state that must be recovered coherently with the database.

### Linguistic workbench introduction and correction policy

The Phase 6b rating screen and navigation have received an initial reviewer UI
pass, but the starting instruction/calibration page still needs a dedicated
content and usability review before a pilot or real collection. Revisit its
information hierarchy, amount of calibration text, examples, and transition
into the first trial; validate the result with representative reviewers rather
than treating the current development copy as final.

Completed linguistic observations are currently locked, which protects the
primary cognition-study design from hindsight and recalibration after later
stimuli. If reviewer operations require amendments, design an explicit
append-only correction mechanism that preserves the original observation,
revised value, reason, actor, and timestamp. Do not add an ordinary overwrite
or silently requeue completed observations.

Phase 7 may omit an unusable linguistic observation or request an objective,
append-only correction. A genuine re-rating after later stimuli must be a new
observation or separately designated round rather than a replacement. The
implementation and reviewer-facing wording remain open.

## Reviewer-ID exposure and the v12 release blocker

Found on 9 November 2026 while checking the docs against the new snapshots.
This supersedes an earlier framing in this file that called the commitment an
unimplemented draft action: it is a commitment in the notice participants
actually consented to.

### What participants were promised

The approved notice served at the 16 September 2026 workshop
(`docs/participant-notices/2026-09-16_Musparql_workshop_participant_notice_APPROVED.docx`,
version `musparql-workshop-2026-09-16-v1`) states, under *Pseudonymisation and
research outputs*:

> New releases using external reviewers will not publish their stable reviewer
> IDs, reviewer-level language, expertise or familiarity, or linked individual
> annotation histories.

The consent summary on the same page is blunter: "We publish benchmark
questions and ambiguity or alternative data, but not your name, contact
details, reviewer profile, or reviewer-linked annotations." The notice also
says individual-level reviewer research data "is not published or routinely
shared", and that the research dataset stays personal data after the identity
link is removed "if a reviewer could still reasonably be identified". Consent
under Article 6(1)(a) was given against that text.

### A live exposure, independent of any release

`catalog/curated/meta_review_decisions.jsonl` is tracked, and it was pushed to
`github.com/ppquadrat/musparql-aligner` on 8 November 2026. That repository is
**public**. The file's `superseded_review_ids` carry **nine distinct external
reviewer IDs across 44 pair-level links** — the two most active on 13 pairs
each, the next on 8, the rest on 1 to 3. Each link names the pair the reviewer
reviewed, so the file is a reviewer-linked annotation history in the sense the
notice rules out, even though it contains no names. The IDs are deliberately
not repeated here: this document is tracked, and quoting them would add a
second public copy.

Removing the file in a new commit does not undo this: the blobs remain in the
repository's history, and in any clone, fork or cache. Remediation is a
data-protection decision for the owner and ICF, not a tidy-up. The options are
history rewrite plus force-push, making the repository private, or accepting
and documenting the exposure, and whichever is chosen the question of whether
participants must be told is separate and prior.

### The release blocker

`benchmark/v11` and `benchmark/v12` are built and audit clean, and **neither may
be published as a public release.** A release built from v12 carries reviewer
identity on 77 of its 177 pairs, naming nine reviewers, eight of them external
workshop participants.

The identity is not confined to one field. In each public benchmark record it
appears three times — as `provenance.reviewer_id`, and embedded inside
`provenance.approval_review_id` and `provenance.formulation_id` — which is 251
mentions in `benchmark.jsonl` and 18 more in `alternatives.jsonl`. Dropping the
field alone would not de-identify anything, and `validate_public_reviewer_ids`
positively *requires* the embedding to be consistent: a review-event ID must end
in its reviewer's pseudonym and a formulation ID must contain that event. So the
projection change means minting release-local identifiers for review events and
formulations, not deleting a key.

These snapshots are also tracked, so v11 and v12 must not be pushed in their
current form either. Nothing from this work has been pushed: `origin/main` is at
`233f1a7` with 17 local commits ahead of it.

### What publishing the IDs would buy, and why it is not needed

Only one thing of substance: a reader could compute inter-reviewer agreement and
group one reviewer's judgements across pairs directly from the public files,
without asking for access. Two lesser things: per-rewrite credit, and a way to
tell that several distinct reviewers rather than one confirmed a pair.

The notice already allocates that work elsewhere. It names "understand where
reviewers agree or disagree" and "study how expertise relates to review
judgements" as purposes of the **private** research dataset, and commits that
"public reporting about reviewer characteristics will use appropriate aggregate
results and avoid singling out an identifiable reviewer". The agreement analysis
is a paper, not a release field.

The third point is worth keeping in some form, because it is a claim about the
benchmark rather than about a person: a reader has a legitimate interest in
knowing a pair was confirmed by three reviewers rather than one. A per-pair
review count and an adjudication flag carry that without identifying anyone, and
neither exists in the public files today — the IDs are currently the only signal
of it, which is part of why their removal needs a replacement rather than a
deletion.

Note that a per-release salted pseudonym does **not** solve this. It is still
stable within the release, which is precisely what makes cross-pair grouping
possible, so it falls under the same prohibition.

### How provenance survives without them

What the public files keep, per pair: `query_id` and `query_label`, the SPARQL
with its version and hash, the canonical question, `canonical_question_source`
(`reviewer_rewrite`, `approved_model_output`, `meta_review_decision`,
`source_prompt`), each accepted alternative with its text and `source_type`,
`acceptance: human_accepted`, and the generation run and model that proposed the
wording. None of that identifies a person.

So a decision stays traceable to its **kind of actor and its artefact**, not to
its author. A reader can tell that a wording was a reviewer's rewrite rather
than accepted model output, that a human accepted a given alternative, and which
model run proposed it. They cannot tell which reviewer, and cannot group
decisions by reviewer.

Across versions the trail is visible because `query_id` is stable: diffing two
published snapshots shows the canonical question changing and alternatives
appearing or disappearing, with `canonical_question_source` saying what kind of
decision changed it. `benchmark_id` is not a reliable cross-version key — it is
carried forward for an unchanged pair but is reminted from the review event when
a pair is re-reviewed. Removal is the weakest link: an alternative that
disappears between snapshots leaves no public record of why or by whom, and no
field records a removal.

### Where the identity link still lives, and for how long

| where | tracked? | what it holds | durability |
| --- | --- | --- | --- |
| ICF server database | no | profiles, the identity-to-reviewer link, submissions | deleted on withdrawal within 30 days; otherwise until 2 years after formal project closure, then deleted or genuinely anonymised |
| encrypted off-server backups | no | the above as of each snapshot | expire within 90 days |
| `benchmark/vN/included.jsonl`, `dismissed.jsonl` | no, git-ignored | full review provenance per pair, reviewer IDs included | **no stated retention** |
| `var/review/exports/ipl-2026-09-16/` | no, git-ignored | the team exports themselves | **no stated retention** |
| `confidential/reviewers/` | no, git-ignored | reviewer profiles | covered by the server lifecycle in principle, local copy not |
| `catalog/curated/meta_review_decisions.jsonl` | **yes, and pushed** | 44 reviewer-to-pair links | permanent until remediated |

So the durable, governed home for "who decided what" is the private research
dataset on the ICF server, and it has a hard end of life: deleted or anonymised
two years after project closure. Nothing in the public benchmark is meant to
outlive that link, which is consistent — the published artefact is the pair, and
the reviewer's role in it is evidence held privately for as long as the research
needs it.

The ignored local working files are the gap. `included.jsonl`, the IPL team
exports and the local reviewer profiles hold the same personal data as the
server, on a laptop, outside every retention schedule the notice describes, and
a deletion on withdrawal would not reach them unless someone remembers. They
need either a stated retention and a deletion procedure, or to stop being kept
locally at all.

### Release readiness, verified 10 November 2026

Four checks, all passing:

- every tracked file of `benchmark/v11` and `benchmark/v12`, as committed, holds
  no `reviewer-NNNN` and no `group-...`; nor does
  `catalog/curated/meta_review_decisions.jsonl`;
- the whole committed tree passes `check_public_tree.py`, which now enforces the
  rule rather than relying on anyone remembering it;
- a public release built from v12 carries no reviewer, group or assignment
  identifier in any of its five files; and
- the released provenance is `question_source`, `source_type`,
  `review_submissions`, `adjudicated` and the source fields — no identity in any
  form, field or embedded.

The `review_cohort` aggregate stays in the working manifest and does **not**
reach the public release, whose manifest carries only version, counts, files,
licensing and policy. So the round's group-size distribution is internal.

**What is still not done is not technical.** The notice promises "We check for
indirect identification before publishing outputs", and that check is a human
act that has not happened. The only reviewer-authored free text a release
carries is `provenance.reviewer_comment`, on 7 of the 177 pairs: cdec-0008,
europeana-0019, -0026, -0049, -0053, nfdi4culture-0001 and -0005, between 48 and
477 characters each. A screen for first-person and person-referring words finds
none, but a regex is not the check the notice promises.

Two notes on what publication means here. The repository is public, so `git
push` *is* publication for these files; there is no separate gate between the
tracked snapshot and the world. And no new ICF approval is needed to publish:
the notice already promised that releases would not carry stable reviewer IDs,
and the projection now meets that promise rather than changing it. Approval
would have been needed to weaken the commitment, not to honour it.

The unresolved ICF-decision-versus-notice difference concerns withdrawal, not
publication, and does not block a release.

### The forward fix

History is being kept, so the 44 links already on the public repository are
permanent. That settles what the goal is: **stop the set growing.** Every future
publication that reuses the same enduring pseudonyms adds pair-links to an
identifier that is already public, so the fix is to make the enduring
`reviewer-NNNN` never reach a public artefact again, enforced mechanically
rather than by discipline.

Four facts make this cheaper than it looks.

- **Omitting the reviewer fields needs no validator change.** A public record
  carrying none of `reviewer_id`, `approval_review_id`, `formulation_id`,
  `authored_by_reviewer_id`, `approval_review_ids` or `approval_reviewer_ids`
  passes `validate_public_reviewer_ids` as it stands. Only a *retained*
  identity-free `formulation_id` would need the grammar relaxed, and it does not
  need to be retained.
- **The working files are already outside the boundary.** `included.jsonl`,
  `dismissed.jsonl` and `linguistic_annotations.jsonl` are in
  `check_public_tree.py`'s `FORBIDDEN_PATHS`, so full reviewer provenance
  already has a home that cannot be committed. Nothing is lost internally.
- **`superseded_review_ids` has one consumer**, `merge_adjudicated_reviews`,
  and it reads the decisions file from a path that is itself ignored. The
  tracked decisions file does not need that field at all.
- **`check_public_tree.py` already has the right shape** for the rule: it
  scans staged blobs against path patterns and structured markers, and it runs
  on every commit in this project's workflow.

**Steps 1 to 4 were implemented on 9 November 2026.** Both snapshots rebuild
with no reviewer or group pseudonym in any tracked file, both pass
`audit_snapshot`, and a release built from v12 contains none either. What
remains is step 5, the local-file lifecycle and the purge, and the procedure for
re-downloading the receipts so the local copies become disposable.

What landed:

- `check_public_tree.py` refuses any `reviewer-NNNN` in a tracked blob outside
  `reviewer-0001` and the reserved `reviewer-9000`–`9999` fixture block, with
  binary blobs skipped so the approved-notice `.docx` does not trip it. The
  sweep onto the reserved block touched 17 tracked files; one of them built its
  IDs with `f"reviewer-{index:04d}"`, so the helper had to move into the block
  rather than its call sites.
- `benchmark_gold_records` emits no reviewer identity at all — not the field,
  not `approval_review_id`, not `formulation_id` — and carries
  `review_submissions` and `adjudicated` instead. An alternative is identified
  by `alternative_id`, derived from the query ID and the normalised wording, so
  it is stable across releases and survives a withdrawal.
- `merge_adjudicated_reviews` stamps the true per-pair submission count on every
  review it emits. This matters: a tie-broken pair keeps one team's review and
  drops the rest, and an adjudicated pair's `prior_review_ids` cover only the
  superseded set, so a builder counting for itself understated independence on
  roughly 16 pairs. The merge also writes `round-summary.json` beside its
  exports, which is where the contributor distribution belongs.
- `decisions_from_notes` writes `superseded_review_count` to the tracked
  decisions file and the IDs to an ignored sidecar; `merge_adjudicated_reviews`
  reads the sidecar and refuses to run if a decision records superseded reviews
  and the sidecar is missing.
- The manifests lost their identifiers too: v11's `meta_reviewer_id` became
  `meta_review_adjudicated`, and v12's `review_stages` name each export by
  content digest rather than by a filename containing its group ID.

v12 now reports 136 pairs with one independent submission, 15 with two, 4 with
three, 2 with four, and 20 with none (the source-authored LinkedMusic pairs),
with 13 adjudicated.

The plan as proposed, for the record:

1. **Enforce it.** Add a rule to `check_public_tree.py`: no `reviewer-NNNN` in a
   tracked data or documentation artefact at all, and in code and tests only
   from a reserved synthetic block (`reviewer-9000`–`reviewer-9999`). This one
   rule would have caught every instance in this entry, including the original
   push. Needs a sweep of existing fixtures onto the reserved block; two tracked
   files currently carry a real ID as a fixture or an illustration.
2. **Change the projection.** Drop the six reviewer fields from
   `benchmark_gold_records` and the alternatives sidecar, and add what the
   fields were really being used for: a per-pair `review_count` and an
   `adjudicated` flag, which carry the claim that several humans confirmed a
   pair without naming them. Give each alternative a content-derived
   `alternative_id` — `sha256(benchmark_id + normalised text)` — so an
   individual wording stays citable across releases, is stable because it
   depends on the text rather than on who approved it, and survives a
   reviewer's withdrawal.
3. **Split the decisions file.** Keep the wording, disposition and rationale
   tracked, and move `superseded_review_ids` to an ignored sidecar under
   `var/review/analysis/` that `merge_adjudicated_reviews` reads.
4. **Rebuild v11 and v12** so their tracked files carry no reviewer identity,
   and verify with a grep for `reviewer-` over the built snapshot and release.
5. **Give the local working files a withdrawal path.** Scoped on 10 November
   2026 after checking what is actually on the researcher's machine: the
   identity registry is not there, and no receipt carries a name, email,
   affiliation, language, expertise or familiarity field. What is there is
   pseudonymous annotation content. So this is not a daily
   fetch-build-delete discipline and the local copies need no backup of their
   own; what it needs is that a withdrawal reaches them. The owner's position,
   which matches the ICF decision: remove the withdrawn pseudonym so nothing
   about that person can be reconstructed from the pieces, and leave the
   annotations as pseudonymous data.

   The notice is worded more strictly. Verbatim, on withdrawal: "We delete your name, optional affiliation,
   account information, identity-to-reviewer link, profile, language,
   expertise, familiarity **and reviewer-linked contributions** from the live
   service without undue delay, and within 30 days at the latest." And: "We
   retain a remaining output only if it no longer relates to you as an
   identifiable person. **Removing a reviewer ID alone is not enough.** If your
   personal data cannot be separated safely, we delete the joint submission."
   What may remain is only "genuinely non-identifying benchmark questions,
   queries and alternative formulations, including those already published".

   Absent withdrawal, retention is as elsewhere: kept only while needed and "no
   later than two years after formal project closure", then deleted or made
   genuinely anonymous, with closure a date ICF records rather than the end of
   funding. Logs and backups are 90 days.

   The gap between the two is narrow in practice: the only place a reviewer
   could "still reasonably be identified" from what remains is the free text —
   17 internal comments and 5 public comments in this round, about 4.5 KB in
   all — and only if a comment names or characterises someone. Dropping the
   pseudonym handles the rest.

   So what is needed is a purge that takes a withdrawn reviewer ID and clears
   it from `benchmark/vN/included.jsonl`
   and `dismissed.jsonl`, the IPL team exports under
   `var/review/exports/ipl-2026-09-16/`, the superseded-review sidecar from
   step 3, `confidential/reviewers/`, and `var/queries/kg_queries.jsonl`'s
   correction histories. It should report what it would remove and surface the
   free-text fields attached to that pseudonym for a human to judge, since that
   is the only part a rule cannot settle.

Steps 1 to 4 leave the public artefacts compliant, which was the part that
mattered. Step 5 is the next withdrawal request: today one would reach the
server and not the laptop.

**Done for step 5 so far:** `scripts/fetch_review_receipts.py` fetches a
round's receipts from the server, verifies each against its recorded database
digest, and purges them again, so the local copies are a build input rather
than a holding. The procedure is in the
[review runbook](REVIEW_RUNBOOK.md). The purge covering the rest — the
benchmark working files, the local profiles and the correction histories — is
still to be written, and the contradiction above has to be settled first.

### Unresolved: the ICF decision and the participant notice disagree on withdrawal

Found on 9 November 2026 while reading the hosting boundary before writing the
receipt-fetch procedure. The two approved documents say different things about
what a withdrawal removes, and the difference decides what the purge must do.

**The ICF data-protection decision** of 29 August 2026, as summarised in
[`ICF_HOSTING_BOUNDARY.md`](ICF_HOSTING_BOUNDARY.md) §5, records "deletion of
identity/profile data on withdrawal or two years after project close,
**retention of pseudonymous annotations with the dataset**, and 90-day backup
retention".

**The approved participant notice** of 16 September 2026 says that on
withdrawal "we delete your name, optional affiliation, account information,
identity-to-reviewer link, profile, language, expertise, familiarity **and
reviewer-linked contributions**", that a remaining output is retained "only if
it no longer relates to you as an identifiable person", and that "removing a
reviewer ID alone is not enough".

So the decision says the annotations stay as pseudonymous data; the notice says
reviewer-linked contributions go, and that de-linking is not sufficient to keep
them. The notice is later, it is the document participants read and consented
to, and consent is the lawful basis, so the notice is the binding promise to
participants. But the discrepancy is real and needs resolving with ICF rather
than quietly resolved in code, because:

- a purge built to the notice deletes annotations the ICF decision expects the
  dataset to keep, which affects the benchmark and any analysis resting on it;
- a purge built to the decision leaves data a participant was told would be
  deleted; and
- the notice's "genuinely non-identifying ... can remain" carve-out requires a
  judgement per withdrawal, not a rule, so whoever handles the first request
  needs to know which document governs.

Until it is resolved, the purge should implement the notice, report what it
would delete and what it would leave, and require confirmation — which is what
the step 5 design already says, for a different reason.

### Where "who decided this wording" actually lives

Worth recording because it is easy to assume the database is the only link. It
holds two different links and only one of them is the identity.

- **pseudonym to person**: the operational contact registry, server-side, and
  `confidential/reviewers/` locally.
- **decision to pseudonym**: the team review exports under
  `var/review/exports/<round>/`. Each export carries `assignment_id`,
  `review_group_id`, `contributor_reviewer_ids` and `submitted_by_reviewer_id`,
  and a `reviews` map keyed by pair whose entries hold `reviewer_id`,
  `review_id`, `preferred_question`, `literal_wording`,
  `benchmark_disposition`, `pipeline_assessment`, comments and `reviewed_at`.
  The same link is duplicated in `benchmark/vN/included.jsonl`, in
  `meta_review_decisions.jsonl`'s `superseded_review_ids`, and — until step 2
  lands — in the public `formulation_id` and `approval_review_id`.

To find who replaced a wording: take the pair's row in the decisions file for
the canonical wording and the `superseded_review_ids` it overrode; look each up
in the round's exports to see which reviewer or group proposed what, and when;
then resolve the pseudonym through the registry. Traced on `europeana-0019`,
that returns four superseded team reviews, two of which proposed a rewrite and
two of which left the wording alone, with the canonical wording belonging to
neither — it is the meta-reviewer's own, from
`ipl-2026-09-16-meta-review-decisions.md`.

#### Durability of the repo copies

The link exists in **40 files in this working copy**, all but two of them
git-ignored. They are copies; the authoritative record is elsewhere, and the
two sides have opposite problems.

**Authoritative and backed up (server).** Submission receipts are written under
`MUSPARQL_SUBMISSION_ROOT=/srv/musparql/var/review/submissions`, and the
processing audits and owner manifests "retain the group, submitter, and
contributor IDs". `/srv` is inside ICF's daily, client-side-encrypted restic
backup of `/etc`, `/home`, `/opt` and `/srv`. So the decision-to-pseudonym link
is backed up daily — on the server.

**Repo copies (laptop), not backed up by any of that.** ICF's backup covers the
server, not `/Users/polina`. These files are also git-ignored, so they are not
in the repository history and not on GitHub either. They are as durable as the
laptop and whatever personal backup exists:

- `var/review/exports/ipl-2026-09-16/` — the 14 downloaded assignment receipts,
  136 KB. The round's primary evidence as held locally.
- `var/review/exports/ipl-2026-09-16-merged/` — 11 files, derived by
  `merge_adjudicated_reviews` and regenerable.
- `benchmark/v11/included.jsonl`, `benchmark/v12/included.jsonl`,
  `dismissed.jsonl`, `linguistic_annotations.jsonl` — 1.2 MB, derived and
  regenerable from the above.
- `var/queries/kg_queries.jsonl`, and the older 2026-08 review and correction
  exports.

**The exception that runs the wrong way.**
`catalog/curated/meta_review_decisions.jsonl` is the one copy that *is* tracked
and pushed, so the most exposed copy of the link is also the most durable one,
while the primary evidence is the least durable. Step 3 reverses that.

**A single point of failure worth fixing separately.**
`var/review/analysis/ipl-2026-09-16-meta-review-decisions.md` is hand-written —
its own header says no script writes it — and it is git-ignored, 20 KB, on one
laptop. It is the source of truth for every canonical wording: the tracked
JSONL is generated from it and cannot reconstruct it. Losing it would not lose
the decisions, which are in the tracked JSONL, but would lose their rationale
and the slot structure needed to revise them. It holds group IDs but no
reviewer IDs, so tracking it would not add a reviewer-ID exposure; that should
be checked against the group-ID question before doing so.

**What this means for recovery.** If the laptop were lost, the derived files
regenerate provided the receipts can be downloaded from the server again, which
is the thing to confirm rather than assume. The hand-written notes would not
regenerate at all.

#### Submissions or reviewers? Both, at different granularities

The two are different constructs and only one of them belongs on a record.

- **submissions** = independent judgements. Two submissions are two readings of
  the pair that did not see each other. This is the unit the adjudication
  worked on and the correct denominator for agreement.
- **contributors** = how many people were involved. Within a group they
  deliberated, so three contributors are not three independent ratings. It
  measures effort and deliberation, not independence.

The round's numbers, over its 85 reviewed pairs:

| submissions per pair | pairs | | contributors per pair | pairs |
| --- | --- | --- | --- | --- |
| 1 | 57 | | 1 | 53 |
| 2 | 16 | | 2 | 12 |
| 3 | 9 | | 3 | 8 |
| 4 | 3 | | 4 | 8 |
| | | | 5 | 3 |
| | | | 6 | 1 |

The two differ for **24 of 85 pairs**. Four pairs have one submission and two
contributors, which is exactly the case worth distinguishing: one judgement,
two pairs of eyes.

**Why the contributor count should not go on the record.** It invites the wrong
denominator — a reader seeing four reviewers on a pair that had three
submissions will compute agreement over four. And it leaks group size: the
round had only three multi-person groups, so a pair showing two contributors
against one submission narrows to those three, and because a group reviewed
many pairs the counts cluster pairs by group even with no group ID published.
That is a quasi-identifier reintroduced through a number.

**Recommendation, which is "both" but not in the same place.**

- **Per pair**, in `benchmark.jsonl`: `review_submissions` (integer) and
  `adjudicated` (boolean). Both are properties of the pair, carry the claim a
  reader needs, and identify nobody.
- **Per round**, in the snapshot manifest: total submissions, the distribution
  of contributors per submission, and the number of groups. This is where the
  deliberation point belongs — it tells a reader that some judgements were
  made by more than one person, without attaching a size to any pair. It also
  matches what the notice promises: "public reporting about reviewer
  characteristics will use appropriate aggregate results".

So a reader can tell that a pair had three independent judgements and that the
round's judgements averaged more than one person each, but cannot tell which
pairs were the group ones.

#### Durability: the governed copy already exists

Posed as a choice between relying on the laptop's backup and adding the link to
the database. Neither is needed, because the server already holds it twice and
both copies are in ICF's backup.

- `review_submissions` carries `reviewer_id`, `review_group_id`,
  `submitted_by_reviewer_id`, `contributor_reviewer_ids`, `revision`,
  `submitted_at`, and — importantly — `export_path` and `export_digest`. So the
  database holds attribution at submission granularity plus an
  integrity-checked pointer to the receipt.
- the receipt itself, under `/srv/musparql/var/review/submissions`, holds the
  per-pair detail: which wording each reviewer or group proposed, the
  disposition, the assessment and the comments.

There is no per-pair review table; the per-pair content lives only in the
receipt file. Both the database and the receipts are under `/srv`, inside the
daily client-side-encrypted restic backup, with the retention and
reapply-deletion machinery the notice describes.

So the laptop copies add no durability at all. They are a cache that happens to
be the only copy outside ICF's control, which is the worst of both: no backup
guarantee, and an exposure and retention problem that the governed copy does not
have.

**Recommendation: neither option. Make the local copies disposable.** Confirm
the receipts are still on the server and in a restorable backup, write down the
procedure for re-downloading them — the 14 files here appear to have arrived
manually, and no script in the repo fetches them — and then treat
`var/review/exports/<round>/` as a cache that may be deleted, with the step 5
purge reaching it. Backing up the laptop copies would instead create a new
storage location for reviewer personal data that the notice does not describe;
the notice places storage at Hetzner in Germany with ICF-controlled off-server
backups and describes the UK only as a place the researcher *accesses* from.
Adding a personal backup would be a new processing location, probably a new
processor, and a notice change.

#### Would wrapping individual IDs in team IDs solve it?

Not for this round's data, and the numbers are the reason. The September round
produced **14 groups, of which 11 had exactly one contributor and 3 had two.**
No group exceeded two.

So for 79% of submissions a team ID is a one-to-one relabelling of an
individual: a stable personal pseudonym with a different prefix. Withholding
the size does not help, because the mapping is one-to-one whether or not the
size is stated, and in a cohort this small participants know who worked with
whom. The notice's own standard is the one that bites: the data remain personal
"if a reviewer could still reasonably be identified from their contribution or
combination of characteristics". A two-person group drawn from a 16-person
workshop does not clear that.

A stable team ID also still permits exactly what the commitment forbids —
grouping one party's annotations across pairs — so it fails on the substance of
"linked individual annotation histories" rather than only on the letter of
"reviewer IDs".

Where team IDs are the right unit is **internally**, and they already are: the
submission is the unit of attribution, and `review_submissions` records the
group, the submitter and the contributor set. What is publishable is a
**count, not an identifier of any granularity** — "adjudicated across four
independent submissions" carries the scientific claim, identifies nobody, and
survives a withdrawal. The small-cohort arithmetic means no stable identifier,
individual or team, is non-identifying here.

Team IDs could be made publishable by design in a *future* round — a minimum
group size, contributor sets never published, nothing published that lets
group-to-member be inferred — but that is a design decision for the next round,
not a fix for this one.

**Note for step 2.** `benchmark/v12/manifest.json` currently carries nine group
IDs, because `review_stages` records the merged export paths and those
filenames contain the group ID. It is unpushed. No group ID appears in any
pushed file today, so the decision about publishing them is still open and
should be taken before v12's manifest goes anywhere.

**A limit worth knowing.** In all 14 exports of the IPL round the per-decision
`reviewer_id` is the submitter, and 3 of the 14 came from groups of more than
one contributor. The contributor set is recorded on the export, not on the
decision, so for those three a decision resolves to "this group, submitted by
this member" and not to the individual who made the call. That matches what the
notice promised — a joint submission is "attributed to the pseudonymous
reviewer identifiers of the members who contributed to it" — but it means
individual attribution is not available for group work, and a withdrawal by one
contributor engages the joint-submission clause rather than a simple deletion.

### What this needs, in order

1. A decision on the exposure in `meta_review_decisions.jsonl`: remediate the
   public history, or accept and document it; and whether participants must be
   informed. Owner and ICF, not a code change.
2. A decision on what external-reviewer provenance a public release should
   carry. The recommendation above is a per-pair review count and adjudication
   flag, with no per-pair reviewer identity.
3. The projection change in `scripts/benchmark/build_public_release.py`:
   release-local review-event and formulation identifiers, the reviewer fields
   dropped, `validate_public_reviewer_ids` adjusted to match, and a test that a
   release built from a multi-reviewer snapshot contains no `reviewer-NNNN`
   anywhere. A grep for that pattern over a built release is a sufficient
   regression test and would have caught this.
4. The same treatment for `meta_review_decisions.jsonl`, which is generated from
   the meta-review notes and need not carry `superseded_review_ids` in a tracked
   file at all.
5. ICF approval of the notice version that records whatever is decided.

Until 1 to 4 are settled, v11 and v12 are internal-only, nothing is pushed, and
`build_public_release` must not be run against v12 for publication. Nothing in
the code enforces that, which is itself part of step 3.

## SPARQL correction queue re-triage, 9 November 2026

The queue was re-triaged against what this round's work established. The
classifier was wrong in four ways, and one tooling bug had been producing
verdicts about queries from evidence that said nothing about them.

### What was wrong

- **A gateway refusal was read as an endpoint rejection.** `classify_failure`
  matched `status in {"parse_error", "query_error"}` before it looked at the
  HTTP status, so a Cloudflare 403 became `endpoint_rejected_query` /
  `likely_correction` / **high**. Across the whole execution history there are
  exactly four `query_error` observations and all four are Cloudflare 403s on
  europeana-0008, so that branch has never once fired on a real endpoint
  verdict. `run_queries` caused it upstream by treating any non-200 with an
  HTML body as the endpoint explaining a bad query; it now does that only for
  400 and 422, the statuses an endpoint uses to reject a query it has read.
- **The read-only detector hid 21 plain SELECTs.** `read_only_query_reason`
  strips `PREFIX` and `BASE` in a loop, but `clean_query` only drops leading
  and trailing comment-only lines, so a comment between two declarations
  stopped the scan and the text no longer began with a keyword. All 21
  `unsupported` candidates were false: 10 LinkedMusic, 9 meetups, 2
  NFDI4Culture. Re-running them found **three genuine 400s**, high-priority
  correction candidates that the bug had been masking
  (linkedmusic-0015, -0041, -0042), and **two queries that simply work**
  (nfdi4culture-0004 with 7 rows, -0006 with 16).
- **Gateway errors were investigations.** A 502 fell through to the catch-all
  as `execution_failure` / `investigate`, putting europeana-0024 and -0032 in
  the same bucket as query defects.
- **Timeouts had no reading of their own.** Sixteen `request_error`
  observations were also catch-all `execution_failure`, which said nothing
  about the one thing a timeout does suggest: cost is a query property as much
  as an endpoint one.

### The classifier now

`gateway_blocked_request` (401/403/407/429, checked **before** the
query-error branch) is `investigate` / medium, and its summary says what the
observation is worth: re-run from the pipeline first, because a block that
appears only in the workbench is about the requester, while one that persists
from the pipeline is about the query text. `endpoint_unavailable` (502/503/504)
is infrastructure / informational. `unsupported_query_form` is
runtime_environment / informational. `request_error` becomes
`execution_timeout` / investigate. Static diagnostics still outrank every
execution observation.

### Re-triaging what the queue already held

`merge_candidates` replaces only the rows in the current scoped run, which is
right for a run against endpoints but means a corrected classifier never
reaches candidates whose queries were not re-executed.
`scripts/retriage_sparql_corrections.py` recomputes every candidate from the
observation already retained for its latest version. It contacts no endpoint
and records no observation: the evidence is unchanged and only the reading of
it is recomputed, with each candidate keeping its observation's own timestamp.
A reclassified candidate takes a new `candidate_id`, since the reason code is
part of candidate identity.

Twenty candidates changed reading. The queue is unchanged in size at 346:

| priority | count | reason codes |
| --- | --- | --- |
| high | 11 | `endpoint_rejected_query` 8, `static_sparql_validation` 3 |
| medium | 37 | `execution_timeout` 16, `parameterized_template` 13, `endpoint_or_query_failure` 6, `gateway_blocked_request` 1, `not_attempted` 1 |
| normal | 293 | `execution_ok` 207, `execution_empty` 86 |
| informational | 5 | `requires_sparql_anything` 3, `endpoint_unavailable` 2 |

The high-priority list is the same length as before but no longer the same
list. europeana-0008 left it, because a gateway refusal is not evidence about
the query. linkedmusic-0015, -0041 and -0042 entered it, because their real
400s had been hidden behind a false `unsupported`.

### A queue that was never complete, and now is

178 pairs had no observation for their latest version and so had never entered
the ledger at all: `merge_candidates` only ever admitted pairs from a scoped
run, and `classify_failure`'s `not_attempted` / `needs_observation` reading had
never been carried by a single pair. They were added with
`--include-unobserved`, taking the queue to **524 — every non-holdout pair**,
which is what execution being evidence rather than an eligibility gate implies.

Every one of the 178 is a LinkedMusic pair, and they share one cause: 178 of
them carry their evidence from `sesemmi-linkedmusic-repository`, a source that
was extracted and never executed. LinkedMusic now shows 247 candidates of which
179 are `not_attempted` — 72% of the knowledge graph has no execution evidence
at all, and one unrun source accounts for nearly all of it.

The fix needs no new tooling, because `run_queries` already filters by source:

```bash
.venv/bin/python -m scripts.run_queries \
  --source-id sesemmi-linkedmusic-repository \
  --sparql-version latest \
  --holdout-selectors var/holdout/selectors.jsonl
```

That is 178 queries against a third-party endpoint, so it is worth running
deliberately rather than as a side effect of something else. Until it runs,
`not_attempted` is the honest reading and the queue says so instead of staying
silent about the pairs.

### Still open

- europeana-0037 and -0038 remain high on static diagnostics, and their
  Cloudflare 403s are genuine content blocks: they use `concat(`, `substr(` or
  `replace(`, which the WAF rule refuses. They need rewriting to run at all.
- The 9 meetups pairs re-run here all return **empty**, which is the wrong
  configured endpoint rather than a query defect. That whole-KG problem is
  unresolved.
- `europeana-0048`'s corrected form still times out at 50 seconds.

## SPARQL correction backlog

The detailed, prioritized correction-workbench backlog remains in
[`SPARQL_CORRECTION_FOLLOW_UP.md`](SPARQL_CORRECTION_FOLLOW_UP.md). Its largest
open items are durable non-approval decisions, explicit benchmark exclusion,
source/target execution modelling, contextual evidence, bounded parameter-value
discovery, clearer saved-decision feedback, and clean service shutdown.

The stale agent-metadata defect described there is fixed: changing an agent
proposal now clears the suggestion, edit type, rationale, and evidence IDs and
requires the human to enter fresh edit metadata.

## Benchmark admission without generation or review

Traced on 9 November 2026, prompted by the five corrected pairs whose gold
wording comes from the meta-review rather than from a model.

### The gap

`build_review_bundle` iterates over the generation *outputs* and decorates each
one from the inputs index, so a pair absent from the outputs file yields no
bundle record; `--query-label` only filters that loop, and
`build_next_review_round` behaves the same way through `--current-run`. A
benchmark record is then made from a bundle record plus a review export. So a
pair whose question is already settled — authored by a source, or decided in
meta-review — can only enter the benchmark by generating a wording that will be
discarded and reviewing a pairing that has already been judged.

### The data model does not need changing

The included-record shape already has a generation-free, review-free variant,
and it has shipped: the 20 LinkedMusic pairs admitted in v8 and carried forward
through v10. Their shape is the template.

- `gold_question_source: "source_prompt"`, with a `source` block carrying
  `source_type`, `source_id`, `source_file`, `source_url`, `prompt_source` and
  the source's own `challenge` / `cq` / `dataset` / `reported_result_rows`.
- `model_output`: `nl_question: null`, `origin_mode: "source_prompt"`,
  `needs_review: false`.
- `run`: ids, model and `run_signature` all null, `run_label: "curated_source"`.
- `review`: no `reviewer_id`, `review_export: null`, `run_id: null`,
  `generation_run_id: null`, and an `internal_comment` recording how the pair
  was constituted.
- `pipeline_assessment: "not_applicable"`.
- no `sparql_provenance`, which is why these records pass the audit at SPARQL
  version 1: `audit_version_pins` checks edit provenance only
  `if provenance is not None`.

Nothing downstream objects. `benchmark_gold_records` reads whatever
`gold_question_source` the record carries, takes `source.source_type` or
defaults to `"human_review"`, and adds reviewer attribution and formulation ids
only when `review.reviewer_id` is present. `audit_snapshot` never requires a
review block. `build_public_release` does not either. No schema constrains
`gold_question_source`; there is no benchmark-record schema at all.

### What is actually missing is a writer

The v8 admission was a one-off migration — `update_type:
versioned_sparql_and_nl_alignment`, no `source_bundle`, no
`source_review_export` — and the script is no longer in the tree. Searching
`scripts/`, `src/`, `docs/` and the schemas for `source_prompt` returns one hit,
a fixture in `tests/test_benchmark_migration.py`. So the capability exists in
the record shape and in every consumer, but there is no supported command.

It also left the snapshots asserting less than v8 did. v8's manifest declared
`source_prompt_used_for_curated_linkedmusic: true` and
`versioned_linkedmusic_sparql_used: true` in `gold_question_policy`. v9 and v10
dropped both keys while carrying the 20 records forward, so a fifth of the
current snapshot's content rests on a policy statement the snapshot no longer
makes.

### Resolved: implemented as scripts/benchmark/add_authored_pairs.py

Done on 9 November 2026, with the shape below and a third origin,
`retained_sparql_repin`, that the meetups-0008 case called for. See the
[benchmark runbook](BENCHMARK_RUNBOOK.md) for the command. Verified against the
real catalogue: a probe snapshot built from v10 admits the five meta-review
pairs, re-pins meetups-0008, and passes both `audit_snapshot` and
`build_public_release`.

Re-pinning meetups-0008 also clears the failure v10 now has against the live
catalogue, described at the end of this section.

### Proposal, as implemented

One script that takes a previous snapshot and a JSONL of authored pairs and
writes `vN+1`, with an `update_type` of its own and a `gold_question_policy` key
that states the origin — so the assertion travels with the snapshot instead of
being dropped on the next build. Two origins:

- `source_prompt`, for a pair the source itself paired, as LinkedMusic was; and
- `meta_review_decision`, for a wording decided in meta-review: gold question
  from `canonical_question`, `source` block naming
  `catalog/curated/meta_review_decisions.jsonl` and the decision's
  `pair_review_id`, and `review.reviewer_id` set to the meta-reviewer so the
  attribution is real rather than absent.

### What is given up, and why neither reason argues for generating

- **Pipeline coverage.** A pair with no generated wording cannot carry a
  `pipeline_assessment` and must say `not_applicable`. That is a real loss of
  coverage for the benchmark's second purpose, but generating a wording against
  a corrected query is its own experiment and not a precondition for the gold
  pair.
- **Independent confirmation.** A review round gives multi-reviewer agreement
  that the question matches the SPARQL; a meta-review wording has one expert's.
  That difference in evidential weight is a reason to record the origin
  distinctly, which a separate `gold_question_source` does, not a reason to
  manufacture a review.

### Found while implementing: indexed alternative formulation ids were invalid

`update_from_initial_review` writes a meta-review alternative's
`formulation_id` as `<review event>::formulation::alternative::<n>`, but
`FORMULATION_RE` in `musparql.reviewer_provenance` admitted only the roles
`candidate`, `preferred`, `literal` and `sparql`. Any snapshot carrying such an
alternative therefore failed `audit_snapshot`, since it runs
`validate_public_reviewer_ids` over `alternatives.jsonl`. No shipped snapshot
contains one — the feature landed after v10 — but the next build would have hit
it, because eleven decisions in the current decisions file record alternatives.

Fixed by adding an indexed `alternative` role to the pattern. The unindexed
roles stay one-per-review-event; `alternative` is indexed because one review
event may approve several alternative wordings at once. An unknown role is
still refused.

### Also fixed: the updater dropped the previous snapshot's policy keys

`update_from_initial_review` wrote a fixed `gold_question_policy`, which is how
v9 and v10 came to drop v8's `source_prompt_used_for_curated_linkedmusic` while
carrying its 20 records forward. It now unions the previous snapshot's policy,
as `add_authored_pairs` does. Caught by building v12 on top of v11 and watching
v11's authored-pair keys disappear.

### Related: the audit is a build-time check only

`audit_snapshot` resolves "latest retained" against the live
`var/queries/kg_queries.jsonl`, so a shipped snapshot starts failing its own
audit as soon as one of its pairs is edited. v8 and v9 already fail on
jazzontology-0002, and v10 now fails on meetups-0008 after the 9 November
correction. Nothing is wrong with the snapshots; the audit is only meaningful
before release. Resolving pins against the catalogue as of the manifest's
`captured_through` would make a later re-audit mean something.

## Resolved: NL provenance and query-comment handling

Resolved in `db9a8b2` and hardened after review in `c5e8d8b`.

### Historical problem: authored NL was dropped at the generation-input boundary

The IPL rehearsal first exposed a concrete case in `nfdi4culture-0006`, but an
audit of all 112 workshop pairs shows that the problem is systematic. The
packages contain 106 `generated`, five `paraphrased`, and one `verbatim`
formulation. All 52 Europeana pairs, all six NFDI4Culture pairs, and all 45
Camera dei Deputati pairs are labelled `generated`; only CDEC has any other
origins.

The working query catalogue contains a source-authored structured
`nl_question` for 82 of those pairs: 52 Europeana, six NFDI4Culture, and 24
Camera dei Deputati records. All 82 also have only a `curated_query` evidence
item. `extract_queries.py` gained support for storing curated prompts in August
2026, but the older `build_llm_inputs.py` boundary had not been extended with
it. That builder excluded `curated_query` as SPARQL-block evidence by default
and did not pass the catalogue's structured `nl_question` to generation. All 82
therefore reached the model with an empty evidence list, and all 82 were
classified as `generated`. This was not an ID-alignment or workshop-package
join failure: every workshop query ID occurred in the expected ledger, input,
output, and package records.

`nfdi4culture-0006` is the clearest symptom. Its authored prompt also appears
verbatim as a leading comment inside the SPARQL. Although the structured prompt
and evidence were absent from the model input, the model copied the comment
word for word and described the result as `generated` with no evidence IDs.
It is the only exact whole-question copy of a SPARQL comment found in this
workshop run, but the other 81 curated questions were still needlessly replaced
by newly generated wording.

The remaining 30 pairs reached the model with one `web_query_desc` item. Of
these, 24 were classified as `generated`, five as `paraphrased`, and one as
`verbatim`. Seven `generated` outputs nevertheless cited retained evidence. The
Quagga candidate builder emitted an empty `sources` array for every `generated`
output, so those seven citations and their retained phrases were also lost
before packaging. One CDEC output additionally had the inconsistent combination
`mode=generated` with a non-null `primary_evidence_id`; the schema and citation
validator allowed it at the time of the audit.

The implementation now retains a structured question as
`curated_nl_question` evidence only when it has the explicit source-authored
shape (`generator: null`, `generated_at: null`) and its source resolves in the
record. Question-like SPARQL comments are extracted without duplicating a
question already contained in existing `query_comment` evidence and inherit
metadata from the selected SPARQL version rather than an unrelated source.
Origin validation rejects inconsistent primary evidence, generated wording
cannot exactly copy retained authored-question evidence, generated outputs keep
their cited evidence in Quagga candidates, and schema validation fails closed
when the `jsonschema` dependency is unavailable.

Historical generation outputs and workshop packages are immutable records of
the earlier behavior. Where corrected candidate or package artifacts are still
needed, rerun generation and rebuild them with new immutable digests; do not
rewrite the historical artifacts in place.

The corrected IPL artifacts were rebuilt locally on 12 September 2026 from
generation run `2026-09-12-014200-minimax-m2-5`. The historical August run and
package directory remain untouched. The corrected frozen package set is
`30a5695ba92519fe`; its manifest is under the ignored operational path
`var/review/bundles/ipl-2026-20260912-provenance-fix/`. That package set is
registered and deployed in production.

## Dependency maintenance

The test suite currently emits deprecation warnings from `rdflib` using legacy
`pyparsing` APIs. They do not affect correctness today, but dependency upgrades
should remove or re-evaluate the warnings before they become runtime failures.

## Production readiness

The application and corrected four-package IPL release are deployed on the
ICF-owned VPS at `musparql.industrycommons.net`. The unprivileged web and worker
services run behind Caddy, and repeated external synthetic rehearsals have
covered admission, required names, batch-first team formation, outstanding
forms, durable team-attributed submission, and return to in-progress work.

The database was returned to a clean synthetic rehearsal baseline on
13 September 2026 after taking a recoverable server snapshot. Remaining work
before real-reviewer invitations is governed by
[`ICF_HOSTING_BOUNDARY.md`](ICF_HOSTING_BOUNDARY.md):

- deploy and verify the approved `musparql-workshop-2026-09-16-v1` participant
  notice and affirmative-consent wording;
- record completion of ICF's data-management-plan registration check;
- complete and verify production SMTP delivery and its failure/retry behaviour;
- validate an isolated coherent database-plus-files restore, deletion replay,
  and owner-visible backup/service alerts;
- record the accepted recovery-point objective and complete the required
  restart/reboot check; and
- keep the workshop laptop-only; mobile-browser use was explicitly removed from
  the 16 September workshop scope by the owner on 15 September 2026.

The linguistic-dimensions instruction-page redesign is a pre-pilot usability
item rather than an infrastructure dependency. The automated narrow-viewport
contract passes; human mobile-browser validation remains future work and is not
a gate for the laptop-only IPL workshop.
