# Human review policy

Human review is where Musparql decides whether a formal graph operation can be
used as a meaningful natural-language benchmark question. It is not a final
proofreading step.

## What the reviewer decides

For each pair, the reviewer checks:

- whether the SPARQL expresses a plausible information need rather than an
  administrative or intermediate operation;
- whether the question is faithful to the selected variables, constraints,
  aggregation, ordering, and graph assumptions;
- whether source or ontology terminology should be translated into normal
  domain language;
- whether a preferred canonical rewrite is needed;
- whether the problem belongs to the generated wording, prompt, evidence,
  source data, execution environment, or SPARQL; and
- whether an eligible pair should enter the private holdout.

Execution success does not settle these questions. A literal verbalization can
also be formally accurate but pragmatically misleading.

## Reviewer identity and formulation provenance

Each review is attributed to a pseudonymous `reviewer-NNNN` identifier. The
confidential profile behind that ID is not a review or benchmark artifact.
Reviews link prior reviews of the same item and separately identify formulations
the reviewer authored and formulations the review approved. Thus "reviewed
before", "wrote this wording", and "approved this wording" are distinct facts.

Reviewer activity is derived from review and formulation links rather than
stored as mutable backlink arrays on the confidential profile.

Browser draft state is namespaced by both dataset and reviewer ID. A reviewer
must not inherit or overwrite another reviewer's local draft merely because
they open the same bundle on the same browser profile.

## Review modes

Initial review is for pairs with no earlier reviewer decision. Comparative
review is for a known pair after a model, prompt, evidence, extraction, or SPARQL
change. Comparative review must preserve the previous decision and show the
change rather than presenting the current candidate as new.

## Canonical and literal wording

The canonical question should express the intended information need faithfully
and naturally. Optional literal wording may record a closer verbalization of the
SPARQL when that helps explain a reviewer rewrite. Literal wording is not a
second canonical answer.

The current review UI does not collect linguistic-dimension ratings or a
graph-context checkbox. Historical values remain readable, but future
linguistic annotation will use a separate interface with its own task design.

## Comments

Public comments explain decisions useful to benchmark users and may enter public
provenance. Internal comments are working review material and are excluded from
the public release. Neither kind becomes private merely because of its field
name: every annotation attached to a selected holdout pair is private.

## Holdout eligibility

A pair is eligible only when it has no prior reviewer annotation and its query
identity has no retained SPARQL edit. Eligibility is pair-wide. Selecting
version `0` does not make an edited identity eligible again.

Holdout selection follows [HOLDOUT_SECURITY.md](HOLDOUT_SECURITY.md) and
[HOLDOUT_RUNBOOK.md](HOLDOUT_RUNBOOK.md). Full holdout exports never enter this
repository or an agent workflow.

## Export boundary

The browser produces separate exports and, for the identity-visible policy, an
annotation-free selector update:

- a sanitized `non_holdout_review_export` for agent-facing benchmark tools; and
- a full private holdout export for the human-controlled private repository;
  and
- a replacement selector JSONL made by merging an existing human-chosen
  selector with holdout additions/removals explicitly touched in the current
  review.

Removing selector membership retires an identity from the active holdout but
does not declassify its annotations. They remain in the full private export and
absent from the sanitized non-holdout export.

Sanitized exports go to `var/review/exports/`. Agent-facing tools reject
private, mixed, legacy, or mislabeled review files rather than attempting to
sanitize them. The browser downloads selector updates; a human verifies and
places them at `var/holdout/selectors.jsonl`.

Names, affiliations, email addresses, expertise profiles, KG familiarity, and
privacy-notice acknowledgments stay in `confidential/reviewers/`. Only the
pseudonymous reviewer ID may cross the export and publication boundary.

## Benchmark authority

A review export records a human decision but does not mutate the benchmark.
Benchmark build or update commands validate it, apply it to a new versioned
snapshot, and preserve its provenance. Automatic evaluation never has this
authority.
