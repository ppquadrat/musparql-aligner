# SPARQL correction follow-up

This document captures the changes identified during the first manual
end-to-end test of the SPARQL correction stream on 5 August 2026. It is a
working list for the next implementation session, not a replacement for
`SPARQL_CORRECTION_RUNBOOK.md` or `SPARQL_EDITING_POLICY.md`.

## Existing foundation to preserve

- Source SPARQL remains immutable version 0; approved edits append v1, v2, and
  so on.
- Approved versions have a tracked, public-safe durable representation in
  `catalog/curated/Approved_SPARQL_Edits.jsonl` and can be restored after a
  clean extraction.
- Holdout identities are filtered before correction candidates, evidence, or
  prompts are inspected.
- Execution remains evidence rather than an approval or eligibility gate.
- The correction queue deliberately includes successful, empty, failed,
  parameterized, and previously edited non-holdout queries.

## P0: decision integrity and durability

### Persist non-approval decisions

Approved SPARQL text is durable, but `defer` and `no_edit` decisions and their
reviewer notes currently live only in the ignored working catalogue. Deleting
`var/` can therefore lose a useful human decision even though an approved edit
would survive.

Add a tracked public-safe decision archive, or an equivalent durable
projection, for non-holdout correction decisions. It should retain at least:

- query identity and reviewed base version/hash;
- decision (`approve_edit`, `no_edit`, or `defer`);
- reviewer note and rationale where supplied;
- edit type and reviewed timestamp; and
- enough provenance to reject a stale decision after the source query changes.

Do not copy raw review-export paths, credentials, unrestricted execution logs,
or private reviewer/holdout data into the durable projection. Fresh extraction
must restore the safe decision history and must fail closed on a hash mismatch.

Acceptance test: apply a synthetic defer with a note, delete the synthetic
working catalogue, extract again, and confirm that the note and decision are
restored without restoring unsafe local details.

### Do not retain stale suggestion metadata after manual changes

Observed failure: an agent returned `no_edit`; the reviewer then changed the
SPARQL manually. The UI correctly changed the proposal origin to `human` and
discarded the agent-suggestion object, but it silently retained the agent's old
rationale, edit type, and evidence IDs. Approval then succeeded because the
stale rationale satisfied validation. The applied record consequently could
claim both that no edit was needed and that a human-authored edit was approved.

When a reviewer changes agent-proposed SPARQL:

- either retain explicit `agent_assisted_human_modified` provenance and require
  the reviewer to reconfirm every inherited metadata field; or
- clear suggestion-derived rationale, edit type, and evidence IDs together
  with the suggestion provenance.

Changing an agent `no_edit` response into an edit must always require fresh edit
metadata. Defer and no-edit decisions must not inherit edit-only metadata unless
it is deliberately represented as a rejected suggestion.

Acceptance tests should cover agent edit, agent no-edit, manual modification,
defer, returning to a decision, re-approval, export, and apply.

### Make saved decisions visible

Reopening an approved candidate retained the changed SPARQL but did not show a
clear current decision. Add a prominent decision badge or selected control in
the detail panel, show the saved timestamp, and say whether approval created or
updated the existing local decision. Re-approving the same candidate should be
visibly an overwrite, not look like a second hidden record.

## P0: exclude unsuitable queries before pair review

Add an explicit **Exclude from benchmark** control to SPARQL correction review.
This is distinct from `defer`: a reviewer may already know that a utility,
administrative, external-target, or non-standalone query should not become an
NL–SPARQL benchmark pair, so asking them to review the pair later wastes effort.

The exclusion must:

- require or allow a short reason;
- be saved durably with query version/hash provenance;
- be visible and reversible through an explicit action;
- prevent the excluded candidate from entering ordinary LLM generation and
  pair review by default; and
- remain separate from private holdout selection.

Before implementation, decide whether exclusion is version-specific by default
and whether an explicit identity-wide option is needed for permanently
non-benchmark utility queries. A later materially changed SPARQL version should
not be silently excluded under an ambiguous old decision.

## P0: evidence must support the decision

### Include source context, not only the extracted SPARQL block

For `linkedmusic-0005`, the three evidence items displayed identical abbreviated
SPARQL from three crosswalk files. The bundle omitted the surrounding material
that showed that the text was a documentation example, that the executable code
constructed `VALUES` from a runtime batch, and that the request targeted
Wikidata. The human and the suggestion model therefore lacked the evidence
needed to interpret the ellipsis.

Evidence extraction should provide bounded contextual excerpts around a match
and, when relevant, nearby query-construction and endpoint code. Group identical
snippets while retaining all source paths and commits. The UI should distinguish
duplicated corroboration from three independent pieces of semantic evidence.

Acceptance test: the LinkedMusic crosswalk example must show the abbreviated
documentation, dynamic batch construction, and Wikidata target without requiring
manual repository inspection.

### Separate source provenance from execution target

A query can be sourced from the LinkedMusic repository while targeting
Wikidata. The current `kg_id` is used for both attribution and endpoint
selection, so `linkedmusic-0005` was incorrectly executed against the SIMSSA
LinkedMusic endpoint and its empty result looked meaningful.

Represent at least these concepts separately:

- source project/repository;
- target knowledge graph or service;
- execution profile/endpoint; and
- whether the query is a standalone benchmark candidate or pipeline utility.

If the actual target is not configured, record execution as unavailable or
external-runtime-required. Do not rewrite the query with `SERVICE` merely to fit
the workbench's endpoint model.

### Recognize documentary ellipses and generated templates

A literal `...` inside a documented `VALUES` example is invalid standalone
SPARQL, but it is not necessarily a source error. Static triage should recognize
documentary ellipses, formatted-string placeholders, and runtime query builders.
Such records should normally be `instantiation_required` or
`runtime_context_required`, not promoted to `likely_correction` solely because
an unsuitable endpoint returned HTTP 400.

## P0: ground parameter instantiation in the graph

The suggestion model currently receives SPARQL, evidence, triage, a bounded
execution summary, and basic endpoint availability. It cannot query an endpoint
or inspect a local dump. For `jazzontology-0002`, it therefore recommended
`no_edit` for `%s` instead of proposing a verified concrete value.

Do not give the model unrestricted dump access. Add a deterministic, bounded
value-discovery step for parameterized queries:

1. derive or define a safe exploratory query;
2. run it against the configured endpoint or local graph;
3. retain a small result sample with query/hash/runtime provenance;
4. include those verified candidates in the suggestion input; and
5. execute the completed proposal against the same target.

The prompt must state whether benchmark-ready SPARQL is required to be fully
instantiated. A model must not call an unresolved template “no edit” merely
because the source program substitutes it at runtime.

## P1: semantic adaptation needs explicit metadata

The Jazz dump combines modelling patterns. `jazzontology-0002` used
`mo:SignalGroup`, which retrieves only five *Encyclopedia of Jazz* parts. The
ordinary album catalogue uses `mo:Release`. Changing the query to retrieve a
verified ordinary release answered a more useful benchmark question, but it was
more than parameter substitution: it was a semantic adaptation to the merged
dataset.

Add an edit type such as `semantic_model_adaptation`, or document clearly when
`benchmark_specialization` should be used. The UI should support a query variable
rename such as `?album` to `?release` when it improves alignment with the graph
class and eventual natural-language question. Evidence for the original query
must not automatically be presented as evidence for the adapted graph pattern.

## P1: make actions and outcomes obvious

### Suggestion feedback

An agent `no_edit` response currently looks as though nothing happened because
there is no diff. Display a visible result card near **Generate suggestion** with
the recommendation, rationale, uncertainty, model, and timestamp. Distinguish
“no edit suggested” from request failure.

### Execution feedback

**Execute proposal** adds a result card much lower in the page, so repeated
tests appeared to do nothing. Show immediate status beside the button and move
or link to the new result. Explain the status vocabulary in place:

- `ok`: executed with rows;
- `empty`: executed successfully with zero rows;
- `error`: endpoint or query failure; and
- `unsupported`/`unavailable`: execution context could not run it.

State prominently that execution does not save or approve SPARQL.

### Metadata and decision controls

Make edit type, rationale, evidence IDs, reviewer note, proposal origin, and
current decision visible before approval. For manual edits, do not allow hidden
or off-screen stale metadata to satisfy validation. Consider a short approval
summary rather than a modal that interrupts every straightforward decision.

## P1: model and service operation

### Make provider configuration explicit

The service defaulted to `gpt-5`, which the configured Graphia LiteLLM provider
did not offer. A listed hosted model then failed on `responses.create` but worked
on `chat.completions.create`.

Keep support for an explicit API method and model. Prefer a required CLI value
or documented environment setting over a provider-specific silent default, and
fail at startup with a useful message when possible. The runbook should show a
known compatible LiteLLM example without implying that its model list is
permanent.

### Stop the service cleanly

Document that the foreground service is stopped with **Ctrl+C**. Catch
`KeyboardInterrupt`, close the HTTP server, and print a short normal shutdown
message instead of a traceback. This does not indicate data loss: browser-local
drafts remain local, while only exported and applied decisions affect canonical
data.

## Regression walkthrough

Before the next release candidate, repeat a short manual round that covers:

1. start the service with an explicit valid model and API method;
2. generate both an edit and a no-edit suggestion;
3. manually change an agent suggestion and verify metadata/provenance handling;
4. execute a proposal that returns rows, one that is empty, and one with an
   unavailable execution context;
5. approve, defer with a note, exclude from benchmark, navigate away, and return
   to verify visible saved decisions;
6. export and apply;
7. rebuild from a clean synthetic working catalogue and verify durable edits,
   non-approval decisions, exclusions, and notes; and
8. stop the service with Ctrl+C and confirm a clean shutdown.

Use synthetic fixtures for automated holdout-related tests. Never use private
holdout annotations to exercise this workflow.
