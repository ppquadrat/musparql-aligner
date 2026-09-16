# IPL Musparql–Quagga Workshop Experiment

**Date:** 16 September 2026

**Location:** Brussels, Belgium

**Workshop window:** 09:00–10:45 Brussels local time (CEST, UTC+2)

**Status:** post-workshop operational and quantitative record

## Purpose

The main aim of the workshop was to add human-reviewed data to the Quagga
benchmark, with particular emphasis on knowledge graphs that were
underrepresented in the existing review data.

The investigator's additional aims were to:

1. obtain reviews from people other than the investigator and extend the
   reviewer base beyond musicology;
2. test the usability of the review interface in a facilitated workshop;
3. test the profile form, KG-specific pre-review form, and team functionality;
4. collect participant profiles and graph-familiarity data; and
5. collect data on inter-reviewer variation that could be examined in relation
   to reviewers' expertise and familiarity with the relevant knowledge graph.

The workshop therefore tested both data collection and the supporting
workflow: whether participants could register individually, provide profile
and KG-context information, form batch-specific reviewing teams, and review
frozen natural-language/SPARQL pairs in a short facilitated session. The
quantitative questions were:

1. How much review coverage did the workshop add overall and per KG, especially
   for underrepresented graphs?
2. How many participants completed profiles and KG-specific pre-review forms?
3. Did participants form one-person and multi-person reviewing teams as
   intended?
4. How many deduplicated/new pairs received review, and how many pairs received
   independent judgments from at least two teams?
5. Did admission, submission, and processing behave reliably under real venue
   conditions?

This report uses aggregate and pseudonymous production metadata only. It does
not reproduce reviewer names, profile answers, familiarity answers, comments,
or review text. Names remain available only through the protected owner
interface on the production server.

## Workshop blurb

The following description was circulated for the workshop:

> **Quagga Benchmark – Can Humans, Knowledge Graphs and LLMs Speak the Same
> Language?**
>
> How faithfully can an LLM translate between a human question and the formal
> language of a knowledge graph? In this hands-on workshop, participants will
> help shape Quagga, a new benchmark for natural-language-to-SPARQL systems.
>
> We will review new question–SPARQL pairs compiled by Musparql, our system for
> collecting SPARQL queries found in the wild and aligning each one with an
> existing natural-language formulation (such as a competency question), or
> generating a question when none can be found. Participants can contribute
> SPARQL expertise, knowledge of any domain, or experience with cultural
> heritage or SSH research more broadly. Where useful, we will pair SPARQL
> specialists with domain experts so that each example can be assessed from
> both perspectives.
>
> The knowledge graphs span areas including Ancient Greek poetry and Holocaust
> scholarship, as well as broader resources such as Europeana, the Culture
> Knowledge Graph and Italian parliamentary data. Whatever their field,
> participants can help identify questions that are technically accurate,
> meaningful and genuinely useful. Their domain knowledge and familiarity with
> individual KGs will be recorded as part of the review process.
>
> This is the final IPL opportunity to contribute directly to the content and
> quality of the Quagga benchmark.

## Experimental setup

### Admission, consent, and profiles

The workshop round was configured for a maximum of 30 participants. Production
SMTP was not yet available, so participants used one shared six-digit entry
code. A successful redemption created a distinct pseudonymous reviewer account
and server-side session; the shared code was not a shared account.

Before reaching the workshop catalogue, each participant had to:

1. acknowledge the approved `musparql-workshop-2026-09-16-v1` participant
   notice and consent statement;
2. provide a required first name, last name, and contact email, with optional
   title and future-contact preference; and
3. complete the general profile questions covering technical experience,
   languages, and domains of expertise.

The profile store is confidential. Review bundles and submissions contain only
pseudonymous reviewer IDs and team attribution, never names, contact details,
or profile answers.

### KG-specific pre-review forms

Starting a KG batch created a one-person reviewing group and an assignment tied
to that package's frozen KG seed. Before entering the workbench, the reviewer
completed the seed's complete set of:

- subject/domain expertise questions; and
- resource, data-model, and KG-familiarity questions.

The answers are append-only and keyed by reviewer, KG, seed version, assignment,
and context. A completed form could be reused when the same reviewer later
joined another team using the same KG seed. A collaborator joining an already
active review could defer the form, but the system retained the outstanding
requirement and showed it after submission.

### Reviewing teams

Every batch began as a one-person team. From the workbench, a reviewer could
add a consenting collaborator using that person's pseudonymous reviewer ID.
Teams were scoped to a particular batch: working with the same person on a
second graph created a separate group and assignment. A reviewer could
therefore participate in several teams and graphs.

The team, rather than an individual participant, was the independent rater unit.
Any current team member could submit the browser's current state. Each receipt
retained the group ID, authenticated submitter, and contributor IDs. Submission
was non-terminal: later submissions created immutable numbered revisions, and
the latest valid revision per team was used for the coverage analysis below.

### Frozen review bundles

The deployed package set was frozen as package set `633160069243b144`. It used
reviewer-neutral, annotation-free selection pins and
`holdout_input_policy: identity_private_filtered_upstream`; private holdout
pairs were absent before the packages reached the application.

The four Quagga graphs were prepared from an all-pairs candidate report and a
deduplicated-candidate report. Records retained a two-pass marker:

- `deduplicated`: the deduplicated/new subset shown first; and
- `all_pairs`: the remaining previously reviewed/all-pairs material.

Europeana and Camera dei Deputati selected only the deduplicated subset and
used deterministic assignment-level shuffling to improve partial-session
coverage. CDEC and CKG retained both passes in ascending pair order. MusoW was
built from a separately prepared unreviewed, non-holdout source filtered
against public benchmark v10; all selected MusoW records were therefore in the
deduplicated/new pass. Musical Meetups and Organs consisted of public benchmark
v10 pairs offered for reinspection.

| KG package | Available pairs | Deduplicated/new | Previously reviewed/all-pairs |
|---|---:|---:|---:|
| Europeana | 37 | 37 | 0 |
| CKG / NFDI4Culture | 6 | 3 | 3 |
| Camera dei Deputati | 18 | 18 | 0 |
| CDEC | 9 | 2 | 7 |
| Musical Meetups | 30 | 0 | 30 |
| MusoW | 70 | 70 | 0 |
| Organs | 9 | 0 | 9 |
| **Total** | **179** | **130** | **49** |

Within each team, the browser stored drafts locally and submitted complete or
partial snapshots to the server. The server validated assignment identity,
bundle digest, schema, team attribution, review identities, and completion
counts before writing an immutable receipt and queueing isolated processing.

## What happened

### Admission incident

The room contained far fewer than the configured capacity of 30, but admission
stopped after several participants had entered. The cause was a separate
security throttle: only ten workshop-code attempts were allowed from one
trusted remote address in a rolling 15-minute window, and both successful and
unsuccessful attempts consumed the allowance. Devices on the Brussels venue
Wi-Fi appeared behind a shared NAT address and exhausted that context limit.

The first code recorded 17 successful redemptions in total, with its last
successful redemption at 09:27:11 Brussels time. It was replaced at 09:28:42,
but issuing a new code revoked only the old code; it did not reset the
independent address throttle. One anonymised context bucket was observed at the
limit of ten. The replacement code's first successful redemption occurred at
09:46:11, after the 15-minute rolling window had expired, and it received two
redemptions. The replacement code had been valid throughout.

This was a design and verification failure rather than participant error. The
synthetic concurrency tests had assigned a different IP address to each browser
and therefore did not simulate a room of independent devices behind one NAT.
The same unresolved risk also exists in the current email-login design, which
allows ten email-code requests per remote address per 15 minutes even when the
participants use distinct invited email addresses.

### Profiles and pre-review forms

Production recorded 19 workshop-code redemptions. At the audit point:

- 16 workshop accounts remained active;
- 12 active accounts had complete profiles;
- four active accounts had incomplete profiles and never joined a team;
- at least one additional reviewer had necessarily completed a profile before
  joining a team but later withdrew, which erased the identifying profile as
  designed; therefore the historical minimum number of completed profiles is
  13, while the exact historical total cannot be reconstructed after erasure;
- the 23 assignments represented 28 reviewer–assignment memberships;
- 24 of those 28 memberships had complete KG-specific forms; and
- all 17 reviewer–assignment memberships belonging to teams with valid
  submissions had complete forms. There were no recorded form deferrals.

Thus the submitted review corpus has complete pre-review KG context for every
recorded contributor, even though admission disruption prevented several
accounts from progressing into the team workflow.

### Teams and submissions

The workshop created 23 batch-scoped team records:

- 18 were one-person teams;
- five were two-person team records;
- those five records represented three distinct reviewer pairings, because two
  pairings created a second team for another batch;
- three two-person teams produced valid submissions, all for Europeana;
- 14 teams in total produced at least one valid submission;
- nine team assignments produced no receipt, including unused alternative
  teams belonging to reviewers who submitted through another team; and
- two active reviewers had started assignments but produced no receipt.

The 14 submitted teams created 49 immutable revisions. Every stored file passed
its digest check, every receipt was schema-valid, and all 49 processing jobs
succeeded. The number of physical devices cannot be reconstructed: submissions
store pseudonymous team and submitter attribution, but not device, User-Agent,
session, or IP identifiers.

## Review outcomes

The table uses the latest valid, digest-intact, successfully processed
non-holdout revision for each submitted team. **Pair reviews** counts one team's
judgment of one pair. **Distinct reviewed** counts each KG/query pair once.
**Deduplicated/new** uses the frozen workshop-pass metadata, not the broader
`review_scope` label. **Previously reviewed pairs** is the report label for the
frozen `all_pairs` pass. The final column counts new pairs receiving judgments
from at least two different teams.

| KG | Pair reviews | Distinct reviewed | Deduplicated/new | Previously reviewed pairs | New pairs with at least 2 reviews |
|---|---:|---:|---:|---:|---:|
| Camera dei Deputati | 11 | 11 | 11 | 0 | 0 |
| CDEC | 7 | 6 | 2 | 4 | 1 |
| Europeana | 74 | 35 | 35 | 0 | 24 |
| CKG / NFDI4Culture | 8 | 6 | 3 | 3 | 2 |
| Musical Meetups | 0 | 0 | 0 | 0 | 0 |
| MusoW | 28 | 27 | 27 | 0 | 1 |
| Organs | 0 | 0 | 0 | 0 | 0 |
| **Total** | **128** | **85** | **78** | **7** | **28** |

The workshop therefore produced 128 team–pair judgments covering 85 distinct
pairs. Seventy-eight of those pairs were in the deduplicated/new pass, and 28
new pairs received at least two independent team judgments. Europeana accounted
for most overlap: 35 distinct pairs were reviewed, 24 by at least two teams.
Neither Musical Meetups nor Organs received a submission.

For MusoW, the frozen workshop source explicitly classified all 70 available
records as unreviewed relative to public benchmark v10; 27 were reviewed during
the workshop. Earlier reviews that were not represented in that benchmark would
not be detectable from the frozen deduplication metadata and remain a limitation
of this classification.

## Interpretation and follow-up

The workflow successfully preserved the work that reached submission: every
receipt and processing job validated, team attribution survived, every
submitted contributor had completed the KG-specific form, and the workshop
achieved substantial new-pair and repeat-review coverage. The non-terminal
revision model was also exercised heavily: 49 receipts represented 14 teams,
without inflating the reported review counts because analysis selected only the
latest valid revision per team.

However, the workshop did not validate the admission design. The shared-IP
throttle conflicted with the venue topology, the replacement-code operation
could not clear that state, and the owner had no visible warning or narrow
recovery control. The resulting interruption materially reduced profile,
form, team, and review collection. The admission and email-login limits must be
redesigned and retested with at least 30 independent browsers behind one proxy
address before another real cohort.

Additional follow-up should:

1. retain the per-account, per-code, capacity, expiry, nonce, and failed-guess
   protections while preventing normal valid cohort use from exhausting a
   small venue-wide budget;
2. add an owner-visible, privacy-preserving throttle warning and audited reset
   or recovery procedure;
3. rehearse the exact deployed proxy path with shared-NAT admission and SMTP
   requests;
4. preserve the current aggregate analysis method—latest valid revision per
   team—for subsequent inter-rater work; and
5. review whether earlier MusoW judgments outside benchmark v10 need a separate
   identity-based comparison before calling every selected MusoW pair new.

## Related records

- [IPL Musparql–Quagga workshop slides](<../papers/IPL Musparql-Quagga workshop/Musparql_workflow_workshop_slides_with_ambiguity_sidecar.pptx>)
- [`../IPL_WORKSHOP_RELEASE_PLAN.md`](../IPL_WORKSHOP_RELEASE_PLAN.md)
- [`../IPL_WORKSHOP_UX_IMPLEMENTATION.md`](../IPL_WORKSHOP_UX_IMPLEMENTATION.md)
- [`../IPL_WORKSHOP_PACKAGE_RUNBOOK.md`](../IPL_WORKSHOP_PACKAGE_RUNBOOK.md)
- [`../OPEN_ISSUES.md`](../OPEN_ISSUES.md)
- Production submission-export fix: `a61ba62`
