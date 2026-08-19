# Linguistic-dimensions workbench specification

Status: design agreed; implementation not started.

This document specifies a separate Musparql annotation task for comparing
natural-language formulations of the same SPARQL query. It is not an extension
of initial review or comparative review. Those modes decide whether a candidate
faithfully represents a query and whether it belongs in the benchmark. This
task measures linguistic differences only after the query, literal reference,
and eligible formulations have been prepared.

## 1. Purpose and boundary

The task studies how non-literal formulations differ from a validated literal
verbalisation of the SPARQL along three provisional dimensions:

- naturalness;
- pragmatism or communicative salience; and
- room for interpretation or ambiguity.

The literal formulation is a common semantic and numerical reference. It is
assigned position `0` on every dimension by design; that is a coordinate origin,
not an empirical rating and not a claim that the literal is the minimum possible
value.

Negative ratings must remain possible. A candidate can be less natural than a
competent literal formulation, less focused on a plausible human information
need, or more constrained through unsupported specificity. Greater room for
interpretation is not presented as inherently better or worse.

The task does not:

- approve or reject benchmark membership;
- edit the canonical benchmark question;
- validate the literal formulation as part of the ordinary rating flow;
- expose formulation provenance to the reviewer;
- select or annotate private holdout material; or
- require a reviewer to finish every item in an assignment.

## 2. Terminology and eligible stimuli

A **stimulus triple** contains:

1. one selected SPARQL query;
2. one pre-validated literal formulation used as the reference; and
3. two eligible non-literal formulations, displayed as A and B.

The ordinary experimental pool contains only a query identity that:

- is non-holdout and has been filtered before the bundle is built;
- has a selected, versioned SPARQL text and hash;
- has exactly one active pre-validated literal reference; and
- has at least two eligible, versioned non-literal formulations.

The literal reference is treated as validated gold input for this task. Literal
validation or correction is a separate expert workflow. A correction proposed
from this workbench does not silently replace the reference or change ratings
already collected against its version.

Eligible non-literal formulations may originate from verbatim source wording,
an LLM paraphrase, LLM generation without usable source wording, a human
rephrasing, or another controlled source. These origins remain authoritative
metadata for assignment construction and analysis but are not rendered in the
reviewer interface.

## 3. Reviewer screen

The rating screen shows, in this order:

1. the SPARQL;
2. the validated literal reference;
3. formulation A and formulation B; and
4. two anchor-relative rating controls for each linguistic dimension.

A and B are randomised independently for each presentation and the displayed
order is recorded. The literal reference is not randomised because it has a
fixed role. The ordinary interface does not show model, prompt, run, origin,
approval history, or reviewer provenance.

Source evidence is not shown. For this experiment the SPARQL and the validated
literal are the intended semantic context; additional evidence could influence
pragmatic judgements differently across records.

Each formulation receives its own rating relative to the literal. The interface
does not ask for a separate direct A-versus-B score. For a dimension `d`, it
collects `d(A,L)` and `d(B,L)`; the paired contrast is derived as
`d(B,L) - d(A,L)`.

## 4. Dimensions and scale

Each control is a continuous-looking visual analogue slider with a normalized
range from `-100` to `+100`. The stored value is an integer so the interface is
fine-grained without implying meaningful sub-integer precision.

The scale shows `0` clearly and provides visible reference ticks at least at
`-100`, `-50`, `0`, `+25`, `+50`, `+75`, and `+100`. It therefore provides at
least five salient positions from the expected literal baseline through the
positive half while retaining the complete negative range.

Every slider starts **unanswered**, even if the visual thumb or track is drawn
at the centre. An untouched slider must never be stored as a deliberate zero.

### 4.1 Naturalness

- `-100`: much less natural than the literal formulation;
- `0`: about as natural as the literal formulation; and
- `+100`: much more natural than the literal formulation.

Naturalness concerns the difference between awkward, formal, or query-like
wording and fluent wording that a person could plausibly use.

### 4.2 Pragmatism or communicative salience

- `-100`: much less focused than the literal on a plausible human information
  need;
- `0`: about as focused as the literal; and
- `+100`: much more focused than the literal on communicatively salient
  content.

This dimension concerns movement from exhaustive verbalisation of query
structure towards a concise expression of what appears important to ask. It is
not a second semantic-correctness judgment.

### 4.3 Room for interpretation or ambiguity

- `-100`: much more constrained or specific than the literal formulation;
- `0`: about as open to interpretation as the literal formulation; and
- `+100`: much more open to interpretation than the literal formulation.

More openness may represent useful conceptual breadth or harmful
underspecification. The interface must not label either direction as better.

The instructions and training material must include examples near the centre
and at both ends of every dimension. A pilot must check scale use, heaping,
negative-value use, completion time, and whether reviewers interpret the three
dimensions consistently before the main study begins.

## 5. Trial outcomes and navigation

A normal completed rating contains all six values: two formulations across
three dimensions. The six values are committed atomically. A partly completed
screen remains a draft and is not analysed as a completed rating.

The screen provides the following actions.

### Submit rating and continue

Available only when all six sliders have been deliberately answered. It stores
the completed trial and moves to the next random eligible item.

### Skip for now

Stores no linguistic judgment. The trial leaves the immediate queue and may be
offered again after the reviewer has seen the remaining available items. Skips
may be recorded as operational events so repeated avoidance can be audited, but
they are not annotations and do not count as completion.

### Cannot assess

Records a completed non-rating outcome for this exact stimulus presentation and
moves on. It is distinct from a temporary skip and is not normally presented to
the same reviewer again. An optional comment or controlled reason may be added
without making it compulsory.

### Literal formulation appears inaccurate

Stops the linguistic rating for the whole triple. The interface opens optional
fields for:

- a proposed corrected literal formulation; and
- a comment explaining the problem.

Submitting the flag records an anchor-correction proposal and moves on. Any
slider draft for that trial is discarded and no linguistic scores are retained.
The proposal enters a separate owner/expert validation process; it never mutates
the active literal reference directly.

### Finish for now

Commits any already completed trials, leaves an incomplete current screen as a
draft or discards it after an explicit choice, and returns a clear partial
progress state. The reviewer may resume later while the assignment remains
active.

## 6. Assignment, randomisation and progress

The owner or assignment builder determines:

- eligible KGs and queries;
- eligible formulation versions;
- formulation-origin, model, prompt, or run contrasts to sample;
- the target number of trials;
- balance across KGs and contrast types;
- overlap between reviewers for reliability analysis; and
- any calibration subset.

These constraints are fixed in the assignment or reviewer-neutral bundle. A
reviewer-neutral bundle omits reviewer identity; it does not leave the
experimental population or sampling rules unspecified. Attribution occurs when
the assignment is opened by an authenticated reviewer.

The ordinary reviewer cannot manually select stimuli or filter by formulation
origin. The workbench supplies the next item randomly from the assignment's
remaining eligible, balanced queue. Already completed or `cannot_assess` trials
are excluded automatically, so **Unrated** is queue behaviour rather than a
reviewer filter. KG scope is normally fixed during assignment construction,
using the reviewer's expertise and KG-familiarity assessments where applicable.

Manual selection by query and formulation IDs may exist only in a clearly
separated owner/development test route. Test selection must not be available in
the workshop rating flow or mixed into study data.

A reviewer is never required to exhaust the queue. Progress is presented as,
for example, `12 completed of up to 30`, not as an obligation. The assignment
may be partially complete, explicitly finished by the reviewer, closed at a
deadline, or resumed later. A minimum number of usable ratings required for a
particular analysis belongs in the prospective analysis protocol; the UI must
not enforce it by preventing the reviewer from stopping.

Stopping, completing an assignment, and withdrawing contributed data are
separate actions. Withdrawal follows the approved consent, governance, and
retention policy.

## 7. Two-way presentations

The main linguistic-dimensions experiment uses only three-way presentations:
literal reference plus A plus B. A query with only one eligible non-literal
formulation does not enter that main pool.

A two-way capability, containing a literal reference plus one candidate, may be
built for a separate study, later coverage, or calibration. It must not be
interleaved or naively pooled with the main three-way task. Seeing a second
candidate can change how a reviewer uses the scale for the first candidate.

If two-way data are collected, the system records `presentation_arity` and the
complete displayed formulation set. A planned bridge sample should assign some
candidates to both formats across different reviewers so a presentation-format
effect can be estimated. Until such a calibration supports pooling, two-way and
three-way observations are separate analysis strata.

## 8. Data contract

The linguistic task requires its own versioned bundle and annotation schemas.
It must not reuse the historical single-record `interpretive` object, which
cannot represent two jointly displayed candidates, randomized order, or the
literal anchor version.

At minimum, an authoritative stimulus record contains:

- assignment and trial IDs;
- dataset and KG IDs;
- query ID and label;
- selected SPARQL version and hash;
- literal formulation ID, version, text, validation provenance, and digest;
- candidate formulation IDs, versions, texts, and authoritative provenance;
- eligibility and non-holdout assertions;
- presentation arity; and
- sampling stratum or contrast identifiers used by the assignment builder.

At minimum, a submitted trial record contains:

- schema version;
- assignment, dataset, and trial IDs;
- pseudonymous reviewer ID derived from the authenticated assignment;
- query, SPARQL, literal, and candidate version identifiers/digests;
- randomized display order;
- presentation arity and complete displayed formulation set;
- outcome: `rated`, `cannot_assess`, or `literal_inaccurate`;
- for `rated`, six integer values in `[-100, 100]` and a touched marker for
  every control;
- for `cannot_assess`, an optional reason/comment;
- for `literal_inaccurate`, optional corrected wording and comment;
- started, completed, and submitted timestamps; and
- the workbench/task-design version.

Skip events and unfinished drafts are operational state rather than linguistic
annotations. They may be retained separately with assignment, trial, reviewer,
and timestamp references.

The server ignores or rejects browser-supplied identity, assignment, stimulus,
or provenance fields that conflict with the authoritative assignment. It stamps
reviewer identity and validates every formulation and digest before accepting a
trial.

## 9. Analysis design

The primary observation is a candidate's anchor-relative score on one
dimension. The main within-screen contrast is the paired difference between B
and A. Analyses must account for the fact that A and B ratings from one screen
are correlated and contextual rather than independent absolute scores.

Initial substantive analysis should be dimension-specific and may include:

- score distributions, medians, means, uncertainty intervals, and scale-use
  diagnostics;
- paired A/B differences within triples;
- differences between formulation-origin, model, prompt, or review-history
  strata retained in authoritative metadata; and
- mixed-effects models with reviewer and query effects and a trial/screen effect
  for jointly collected ratings.

Presentation order is recorded and should be checked as a possible design
effect. If two-way observations exist, presentation arity is also a design
factor and those data remain a separate stratum unless calibration supports a
combined model.

For inter-rater reliability, the unit must preserve the exact query, literal
version, candidate version, dimension, presentation arity, and displayed
candidate context. Ratings collected with and without a co-present candidate
must not be treated as the same reliability unit by default. Interval
Krippendorff's alpha or another prospectively selected coefficient can
accommodate unequal numbers of raters and missing ratings, but it does not
remove a systematic presentation-format effect.

The assignment design should prioritize replicated judgments on a planned
overlap subset rather than maximizing the number of singly rated triples.

## 10. Privacy, holdout and release boundary

The workbench receives only ordinary non-holdout stimuli. It has no holdout
selection, private export, selector, or private-state controls. Assignment
builders must filter holdout identities before bundles enter the hosted
application.

Reviewer-facing pages show only the pseudonymous ID and ordinary assignment
context. Profile answers and personal fields do not enter stimulus bundles or
annotation exports. Formulation provenance is not rendered but remains
available to controlled analysis.

Linguistic annotations remain internal research records unless a later
governance and release decision explicitly changes that boundary. Existing
public-release builders exclude linguistic-annotation sidecars.

## 11. Implementation placement and exit criteria

This work is Musparql v2 Phase 6b. It is separate from the completed Phase 6
initial/comparative integration and should reuse its authenticated assignment,
digest verification, reviewer attribution, browser-draft isolation, and hosted
holdout-exclusion boundaries where appropriate.

Implementation work includes:

- versioned linguistic stimulus and submission schemas;
- deterministic bundle construction and sampling metadata;
- a dedicated rating interface and instruction/calibration screen;
- assignment-mode allowlisting and authenticated attribution;
- random queue, skip, cannot-assess, literal-error, partial-finish, and resume
  behaviour;
- atomic per-trial draft/submission handling;
- controlled owner handling of literal-correction proposals;
- export/submission validation and analysis-ready normalized output; and
- synthetic browser, isolation, accessibility, randomisation, and concurrency
  tests.

Phase 6b is complete only when:

- a synthetic reviewer can complete, skip, flag, pause, resume, and finish a
  partial assignment without losing completed trials;
- the UI never exposes holdout controls or formulation provenance;
- A/B order is randomized and recorded;
- untouched sliders cannot become zero ratings;
- every accepted rated trial contains six valid values and matches its frozen
  assignment stimulus exactly;
- literal-error trials contain no linguistic ratings and cannot mutate the
  anchor;
- reviewer identity and stimulus provenance are derived and verified
  server-side;
- ordinary reviewers cannot manually select stimuli;
- two-way data, if enabled, are visibly and structurally separated from the
  three-way main task; and
- synthetic overlapping assignments produce analysis-ready records without
  cross-reviewer draft leakage.
