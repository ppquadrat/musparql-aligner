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

### Completed: do not retain stale suggestion metadata after manual changes

Observed failure: an agent returned `no_edit`; the reviewer then changed the
SPARQL manually. The UI correctly changed the proposal origin to `human` and
discarded the agent-suggestion object, but it silently retained the agent's old
rationale, edit type, and evidence IDs. Approval then succeeded because the
stale rationale satisfied validation. The applied record consequently could
claim both that no edit was needed and that a human-authored edit was approved.

Implemented policy: when a reviewer changes agent-proposed SPARQL, the UI
clears suggestion provenance, rationale, edit type, and evidence IDs together.
The reviewer must enter fresh edit metadata before approval.

The rejected alternative was to retain explicit
`agent_assisted_human_modified` provenance and require reconfirmation of every
inherited field.

Changing an agent `no_edit` response into an edit must always require fresh edit
metadata. Defer and no-edit decisions must not inherit edit-only metadata unless
it is deliberately represented as a rejected suggestion.

Automated coverage verifies that manual proposal changes clear all inherited
agent metadata. The broader regression walkthrough below still covers defer,
returning to a decision, re-approval, export, and apply.

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

## Specific queries awaiting correction (added 8 November 2026)

Unlike the rest of this document, these are not tooling changes: they are named
non-holdout queries whose SPARQL does not express the information need of its
paired question. Both were identified while analysing the IPL workshop review
data; see
[`experiments/2026-11-07-ipl-review-conflict-findings.md`](experiments/2026-11-07-ipl-review-conflict-findings.md).

Neither is an execution problem. Consistent with the principle above that
execution is evidence rather than a gate, the defect in each case is the
correspondence between the natural-language question and the query, which fails
independently of what any endpoint returns.

### Hardcoded licence enumeration excludes the licences it claims

Affects `europeana-0017` ("openly licensed video resources and their respective
objects") and `europeana-0054` ("datasets with openly licensed content"). Both
restrict `edm:rights` with `FILTER (?Rights IN (...))` over four URIs: Public
Domain Mark 1.0, CC0 1.0, CC BY **1.0** and CC BY-SA **1.0**.

The licence version is part of the URI and `IN` compares whole URIs, so the two
CC entries name a version superseded in 2002 that occurs nowhere in the graph.
The CC BY and BY-SA material that does exist carries 2.5, 3.0 and 4.0, and is
excluded. The filter therefore admits public-domain material only, and no future
ingest of openly licensed content could change that.

Correction options to decide between:

- widen the filter to the intended sense of "openly licensed", matching licence
  families by prefix rather than enumerating URIs; or
- narrow both questions to public-domain content and drop the two inert entries.

A prefix match is the more robust option because exact-URI matching also fails
on real values in this graph: jurisdiction ports (`/at/`, `/de/`, `/es/`,
`/pl/`), inconsistent trailing slashes on otherwise identical licences, an
`https` variant alongside `http`, `rightsstatements.org/page/` alongside
`/vocab/`, and one value carrying a trailing zero-width space (U+200B). Decide
also whether the `NoC-*` rights statements fall inside "openly licensed".

### Two ORDER BY clauses make the convoy query unparseable

Affects `cdec-0006` ("For each convoy, what are the departure places, dates, and
the number of deported people per arrest location and Nazi camp?"). The query
ends:

```
}ORDER BY ?convoy ORDER BY ?labelDeparPlace
```

SPARQL allows one `ORDER BY` clause with several expressions, not two clauses.
`rdflib.plugins.sparql.prepareQuery` rejects it with
`Expected end of text, found 'ORDER'`; collapsing the two into
`ORDER BY ?convoy ?labelDeparPlace` parses. This is a syntax error, so unlike
the other entries here the pair fails even the minimal bar for inclusion: a
query that expresses an information need and parses.

A second issue to settle in the same edit: the projection mixes
`(count(?ArrestPlace) as ?count)` with ungrouped variables and the query has no
`GROUP BY`, which SPARQL 1.1 also disallows. Decide what the intended
aggregation is — a count of deportees per convoy, arrest place and camp —
rather than only removing the duplicate `ORDER BY`.

A reviewing team reported the `ORDER BY` defect during the workshop, in a public
comment, and marked the pair `input_data_improvement_recommended`; a second team
had marked it `accepted`. The finding was nearly lost, because resolving that
disagreement by submission order would have kept the `accepted` review and
discarded the comment.

### Type pattern and date handling in the newspapers-per-decade query

Affects `europeana-0039`. Investigated against the live endpoint on 8 November
2026 (`https://api.europeana.eu/sparql`, 6,250,044,585 triples). The findings
below replace the open questions in the earlier version of this entry; two of
them change what the defect is.

#### The recorded 403 is a gateway content filter, not a query error

The August pipeline run recorded `query_error` with HTTP 403 and a Cloudflare
body, and triage read that as "the endpoint classified the submitted text as a
query or parse error". It is neither. The gateway rejects any request whose body
contains `concat(`, `substr(` or `replace(` — the function name immediately
followed by its parenthesis. This was verified with queries that touch no data
at all:

- `SELECT (CONCAT("186","0") as ?d) WHERE { }` → 403
- `SELECT (SUBSTR("1864",1,3) as ?d) WHERE { }` → 403
- `SELECT (REPLACE("1864","4","0") as ?d) WHERE { }` → 403
- `SELECT (STRBEFORE("1864-01","-") as ?d) WHERE { }` → 200
- `SELECT (FLOOR(xsd:integer("1864")/10)*10 as ?d) WHERE { }` → 200
- `GROUP BY` on a plain variable, and every aggregate without those three
  functions, → 200

It is a request-body signature rule of the SQL-injection kind, matching on
characters rather than on SPARQL. Writing a space between the function name and
the parenthesis is accepted, but the query was deliberately **not** re-run that
way: slipping a payload past the provider's filter is not ours to do. If the
project needs these queries to execute, the options are to ask Europeana to
exempt the SPARQL endpoint from that rule, or to rewrite the affected queries in
functions the filter does not match — and the second is a corpus decision, not
a correction to one query.

In the corpus this affects exactly four queries, and they account for every 403
observed on this endpoint:

| query | blocked function | recorded triage |
|---|---|---|
| europeana-0008 | `replace(` | likely_correction / high |
| europeana-0037 | `substr(` | likely_correction / high |
| europeana-0038 | `concat(`, `substr(` | likely_correction / high |
| europeana-0039 | `concat(`, `substr(` | likely_correction / high |

Europeana's other outcomes are 36 `ok`, 4 `empty`, 5 `request_error`, 2 HTTP 500
and 3 HTTP 502. So all four high-priority "endpoint rejected query" entries are
false positives of the same cause, and `endpoint_rejected_query` should not be
inferred from a 403 whose body is a WAF challenge page. Triage needs to
distinguish a gateway rejection from an engine parse error; the remaining three
queries need re-triage on their content alone.


##### Reproduced in Europeana's own console

Checked on 8 November 2026 in a real browser at
`https://api.europeana.eu/console/sparql/` (the console that
`sparql.europeana.eu` now redirects to), so this is not an artifact of our HTTP
client, our User-Agent, or automation:

- `SELECT ?s WHERE { ?s ?p ?o } LIMIT 3` → **3 results in 109 ms**. The console
  works.
- Europeana's own published example "Europeana Newspapers by Year", from the
  console's own Examples menu → **`Attention Required! | Cloudflare`**.
- The corrected query below, which uses none of `concat(`, `substr(` or
  `replace(` → **`Just a moment...`**, Cloudflare's managed challenge.

The console renders both pages as `Unknown error`, which is how a user first
meets this: a published example that fails with no usable diagnosis.

So the browser route does not recover the numbers either, and the rule is not
confined to the three functions — a rejection arrives in 65–79 ms, before the
engine sees the query, for aggregate forms that are otherwise unremarkable.
Removing `HAVING`, `ORDER BY`, the `FILTER` or the `BIND` does not avoid it, and
each construct passes in isolation over no data. The exact rule was deliberately
not mapped further: establishing which byte sequence passes a WAF is
reverse-engineering someone else's security control, and a gold query shaped by
an invisible filter is not a gold query.

**Operational consequence.** Treat execution of `europeana-0039` — and of
`europeana-0008`, `-0037`, `-0038` — as unavailable rather than failed, and
verify corrections against a local dump if a reproducible result is needed. The
graph facts this correction rests on were all established with small queries
that the gateway does accept.


##### Two different Cloudflare behaviours, and only one of them is a wall

The challenge page's own parameters identify what is happening. `cType:
'managed'` is a Cloudflare **managed challenge** — bot mitigation that a real
browser clears by running JavaScript and accepting a cookie. It is not the same
control as the `Attention Required` page returned for `concat(`, `substr(` and
`replace(`, which is a WAF rule that blocks outright and offers nothing to
clear.

The same parameters also show that the console submits queries by **GET**, with
the whole query URL-encoded into the query string:
`/sparql?query=SELECT%20%3Fdecade%20(COUNT(*)...`. A long URL carrying `*`,
parentheses, quotes, `$` and `{4}` scores high on generic attack heuristics,
which is why a short query passes and an aggregate does not. Our pipeline uses
POST and is challenged as well, having neither JavaScript nor cookies.

The console cannot clear a managed challenge, because it issues the request as
an XHR: the interstitial comes back as the response body, and the UI prints it
as `Unknown error`. A person can clear it, by opening the endpoint URL as an
ordinary top-level navigation in their own browser, letting the interstitial
run, and then returning to the console — the `cf_clearance` cookie then applies
to the console's own requests for that session. That is passing the check as the
human user it is meant for, not working around it.

Two limits on that route. It does nothing for the three blocked functions, so
the original published example stays dead. And a cookie tied to one browser
session and address is not a basis for pipeline execution, which is why these
four queries should still be recorded as execution-unavailable rather than
failed.


##### Decision: the query is accepted as not runnable

Taken on 8 November 2026, after three routes were tried and failed: our HTTP
client, Europeana's own console in a real browser, and a top-level navigation
intended to clear the challenge, which instead served the console UI because
`/sparql?query=...` content-negotiates on `Accept: text/html`.

A fourth attempt assembled the distribution from sixty small per-decade queries
of a shape the gateway had accepted repeatedly earlier the same day. It was
aborted after thirty-two consecutive 403s. By then the rejection had spread to
request shapes that had answered normally an hour before, which suggests our
address had accumulated a reputation state after roughly a hundred requests.

Two consequences for anyone reading a 403 from this endpoint later:

- A 403 here is not evidence about the query. It depends on the request's
  recent history as much as its content, so it must never feed triage.
- The small-query measurements recorded in this entry were taken while the
  endpoint was answering normally and stand as observations. They are not
  reproducible on demand.

**What this settles and what it leaves open.** The correction is unaffected:
every fact it rests on — five `edm:hasType` nodes against 773,250 dated objects
under `dc:type`, 915,521 solutions, 4,534 malformed date literals, the
per-decade inflation figures — was measured before the endpoint closed. What is
not established, and should not be claimed, is the decade distribution itself.
The pair's published result cannot be re-derived from this endpoint, so if
`europeana-0039` is to carry a result, it must come from a local dump.

Execution for `europeana-0008`, `-0037`, `-0038` and `-0039` is recorded as
unavailable, not failed.

##### Superseded on 9 November 2026: the corrected query runs

`scripts.run_queries` executed the corrected `europeana-0039` against
`https://api.europeana.eu/sparql` and it returned **34 rows in 961 ms**, status
`ok`. The decision above is wrong about the corrected query, and the claim that
a result "must come from a local dump" does not hold: the decade distribution
can be read from the endpoint.

The reason is visible in retrospect, and it was in the evidence all along. The
WAF rule blocks `concat(`, `substr(` and `replace(`; the corrected query uses
none of them. Every 403 recorded against the corrected text came through the
correction workbench, whose service issues the request from the browser's
session and so meets the managed challenge. The claim in the section above that
"our pipeline uses POST and is challenged as well" was true of that afternoon's
request history, not of the request shape: the pipeline's own client ran the
same query cleanly the next day.

So the two Cloudflare behaviours separate more cleanly than that section
concluded:

- the `Attention Required` WAF block is about query content, and it still kills
  the three published examples that use the blocked functions, which is why
  `europeana-0008`, `-0037` and `-0038` remain unavailable until they are
  corrected;
- the `Just a moment...` managed challenge is about the requester, and it is
  what the workbench hits. It says nothing about the query, and a query that
  fails there may run from the pipeline.

The practical rule: before accepting a query as unrunnable, run it through
`scripts.run_queries`, not only through the workbench. The workbench's
execution button is a convenience for the reviewer, not the pipeline's verdict.

#### The type pattern: settled, and the concept is on `dc:type`

`edm:hasType <aat:300026656>` matches exactly five nodes, all provider proxies
of provider 1183, and they are genuinely newspapers — four `Újság` and one
`Nimród Vadászlapok`. None carries `dc:date`. Their years live in
`dct:created`: 1898, 1929, 1939, 1973, 1987. The query's join is therefore empty
and the query returns no rows, independently of the 403.

The property itself is not the problem: `edm:hasType` carries 13,851,762
statements over 21,663 distinct objects and co-occurs with `dc:date` 8,143,448
times. It is this one concept that is not expressed through it.

Where the newspaper material with dates actually is:

- `?x dc:type <aat:300026656>` → 830,767 nodes;
- with `dc:date` → 915,521 solutions over 773,250 distinct proxies;
- those proxies map one-to-one to 773,250 distinct `ore:proxyFor` objects, so
  only provider proxies carry `dc:type` here and there is no proxy-level
  double count.

So the correction is `edm:hasType` → `dc:type`, which answers the paired
question at the scale it implies. Keeping `edm:hasType` and moving to
`dct:created` would answer it over five objects. The curated source example
("Europeana Newspapers by Year", from Europeana's own console samples) presumably
ran against the older `sparql.europeana.eu` backend; whether `edm:hasType` ever
carried this concept with dates cannot be established from the current endpoint,
and does not need to be in order to choose the correction.

#### The date values: malformed literals, not IRIs

Inside the `dc:type` newspapers slice all 915,521 `dc:date` values are literals
and none is an IRI. The earlier note's IRIs-with-URL-encoded-fragments and the
`"2"` literal came from sampling `dc:date` across the whole graph, not this
slice, so `SUBSTR` on an IRI is not a live failure mode here.

What is live: 910,940 values (99.5%) are bare four-character years, and 4,534
(0.5%) are not year-prefixed at all. They are overwhelmingly German day-first
strings — `28.05.1954 (Beauftragt)`, `Mai 1984 (Beauftragt)`,
`November 1960 (Beauftragt)`. `substr(?dat,1,3)` then yields `28.`, `Mai`, `Nov`
and the bucket becomes `28.0`, `Mai0`, `Nov0`. The same strings also contaminate
any prefix test: filtering on `"15"` matches `15.03.1897`, which is why a naive
century count reports 117 values in the 1500s where the real sparse decades are
the 1680s (51) and 1690s (187).

A correction should therefore guard the input form, for example
`FILTER (REGEX(STR(?dat), "^[0-9]{4}"))`, and may bucket arithmetically from
`xsd:integer` rather than with string functions — which, given the section
above, is also the only form that currently reaches the engine.

#### Counting and `HAVING`: weaker than previously stated

Per decade, counting solutions rather than objects changes the figure by about a
percent, not by the 18% that the overall totals suggest:

| decade | `count(*)` | distinct objects | inflation |
|---|---|---|---|
| 1860s | 64,665 | 63,787 | +1.4% |
| 1900s | 113,295 | 111,933 | +1.2% |
| 1910s | 93,861 | 93,505 | +0.4% |
| 1750s | 532 | 532 | 0% |

The 915,521 vs 773,250 gap across the whole slice is mostly objects whose several
`dc:date` values fall in *different* decades, so they appear in more than one
bucket rather than inflating one. Unlike `europeana-0048`, this does not destroy
the comparison the question exists to make. The open decision is a modelling one:
should a newspaper dated both 1864 and 1910 count once in each decade — which is
arguably what a distribution over decades should show — or once overall?

On `HAVING (?count>1)`: the sparse decades that could be lost hold 51 (1680s),
187 (1690s), 2,184 (2000s) and 15 (2010s) solutions, so no real decade in this
slice holds a single newspaper. What the clause actually suppresses is the junk
buckets from malformed dates, several of which are singletons. It is still worth
removing, because the paired question does not ask for it and because it would
start discarding real decades as soon as the data changed — but it is not
currently dropping an answer. This could not be confirmed bucket by bucket: the
grouped aggregate needs `concat`/`substr`, which the gateway filter blocks.

#### What remains undecided

Settled: use `dc:type`, and guard the date form. Three decisions remain.

The counting convention above, and whether to carry the five
`edm:hasType`/`dct:created` newspapers along with a `UNION`.

And `HAVING (?count>1)`, which an earlier version of this entry wrongly listed
as settled. Once the date guard is in place it changes nothing: every surviving
bucket is a real decade, and the smallest holds 15. So this is not a defect to
fix but a choice about what the gold query should say, and it runs both ways.

Dropping it: the benchmark scores a system's translation against this query, and
no faithful translation of "newspapers per decade" would emit a clause excluding
decades with one newspaper. A system that got the question right would be marked
wrong on shape. That the clause is currently inert is what makes removing it
cheap — no published figure moves.

Keeping it: an approved edit should be the minimum that repairs the defect, and
an inert clause inherited from the curated source is not a defect. The
alternative repair is to leave the SPARQL alone and narrow the paired question
to decades holding more than one newspaper, which is how wording divergence was
handled for `europeana-0048`.

The first is preferred, because the question is the thing a system is given and
the query is the thing it is scored against, so the query should follow the
question rather than the reverse. But it is a decision, not a finding.

### A redundant join double-counts aggregator material (`europeana-0048`)

Reviewed in the IPL workshop round. The gold question is "What is the
distribution of objects per country and Europeana type?" and the query is:

```sparql
SELECT ?Country ?EuropeanaType (count(*) as ?count)
WHERE {
  ?AggregationEuropeana edm:country ?Country ;
                        edm:aggregatedCHO ?ProvidedCHO .
  ?AggregationProvider edm:aggregatedCHO ?ProvidedCHO ;
                       rdf:type ore:Aggregation ;
                       edm:provider ?Provider .
  ?Proxy ore:proxyIn ?AggregationEuropeana ;
         edm:type ?EuropeanaType .
} GROUP BY ?Country ?EuropeanaType
ORDER BY ?Country ?EuropeanaType
```

`?AggregationProvider` and `?Provider` are neither projected nor grouped, so the
middle clause cannot affect the result except through the number of solutions —
and `count(*)` counts solutions, not objects.

Verified on the live endpoint:

- `edm:country` occurs only on `edm:EuropeanaAggregation`, so the first clause is
  the Europeana aggregation and the middle one reaches for a different node.
- The subclass inference is not materialised: no `edm:EuropeanaAggregation` is
  also typed `ore:Aggregation`, and no `edm:EuropeanaAggregation` carries
  `edm:provider`. So `rdf:type ore:Aggregation` already discriminates, and
  `edm:provider ?Provider` adds nothing.
- No aggregation carries two different `edm:provider` values.
- **A CHO can have two aggregations typed `ore:Aggregation`.** Material that
  arrived through an aggregator has both
  `data.europeana.eu/aggregation/aggregator/...` and
  `.../aggregation/provider/...`, and both carry `edm:provider`. For
  `item/2024904/www_topfoto_co_uk_Europeana_WEAVE_1458820_jpg` the query body
  yields **2 solutions for one object**.

The effect on the published answer, measured per country:

| country | `count(*)` as written | distinct objects |
|---|---|---|
| Montenegro | 84 | 84 |
| Georgia | 34 | 34 |
| **Azerbaijan** | **54** | **27** |

Every Azerbaijani object came in through an aggregator, so the figure is exactly
doubled; Montenegro and Georgia are unaffected. The inflation is therefore not a
uniform scale factor but varies by country, which destroys the comparison
between countries that the question exists to make. A system that answered with
the true distribution would be scored wrong against this gold query.

The Montenegro and Georgia figures also show the clause filters nothing in those
slices, and that one proxy per Europeana aggregation holds there, since
`count(*)` and `count(distinct ?ProvidedCHO)` agree.

The fix is to drop the middle clause and count objects rather than solutions:
`(count(distinct ?ProvidedCHO) as ?count)`. If the intent was to restrict to
objects that have a provider — which the clause does not achieve, since it does
not exclude anything in the slices tested — that belongs in a `FILTER EXISTS`,
which cannot multiply solutions.

The meta-review decision is **correction first, then include**: the pair does not
re-enter the benchmark until the count is right, and its canonical wording is
decided afterwards with the corrected query in hand.

The wording also needs "Europeana type" replaced. `edm:type` takes exactly
five values on this endpoint — `IMAGE`, `TEXT`, `SOUND`, `VIDEO`, `3D` — so it is
the media type, and the reviewer who objected that a researcher unfamiliar with
EDM would not ask for "Europeana type" was right about the phrase. That is a
wording fix, not the reason the pair fails.

### Requested coordinates are computed and discarded (`meetups-0008`)

Published in benchmark v10. The gold question is "List all places in the graph
with their coordinates", and the query binds both coordinates and then drops
them:

```sparql
select DISTINCT ?s ?label
where {
 ?s rdf:type mtp:Location ;
    rdfs:label ?label ;
    geo:lat ?lat ;
    geo:long ?long .
}
```

`?lat` and `?long` are never projected, so the query answers "which places have
coordinates recorded" rather than "what are their coordinates". The two readings
differ in their result shape, and a system that returned the coordinates would be
a better translation of the question and would score wrong against this gold
query. Either project the coordinates or narrow the question; the former is
likely intended, since the triple patterns are otherwise pointless.

Note also that the `geo:` prefix is declared as
`<https://www.w3.org/2003/01/geo/wgs84_pos>` with no trailing `#`, so `geo:lat`
expands to a URI that is not the WGS84 property. This needs checking against the
endpoint: if it matches nothing, the pattern is silently restricting the result
to the empty set, and the published result for this pair should be re-derived.


## Wording changes applied on 9 November 2026

Recorded in `var/review/analysis/ipl-2026-09-16-meta-review-decisions.md` and
regenerated into `catalog/curated/meta_review_decisions.jsonl` with
`scripts/benchmark/decisions_from_notes.py`. That notes file is the only place a
canonical wording may be edited; the JSONL is generated and is overwritten on
every run.

### europeana-0017 — replaced, and given a decision it never had

The pair was uncontested, so it had no meta-review row at all. It has one now,
because neither the evidence label ("List openly licensed video resources and
its respective object") nor the pipeline wording ("…and their respective
objects") survives the correction: the query returns 55 collections with counts,
not a list of resources.

Canonical: *"Which collections hold openly licensed video, and how much does
each hold?"* No earlier wording is kept as an alternative. Disposition
`included`, assessment `input_data_improvement_recommended` — the defect was in
the stored query, not in the generated wording.

### europeana-0048 — the wording the meta-review deferred

The meta-review deliberately left the slot blank until the corrected query was
in hand, and required "media type" in place of "Europeana type".

Canonical: *"What is the distribution of objects per country and media type?"*
No alternative, since both earlier wordings use the EDM property name.

### europeana-0039 — reverted to the pipeline wording

The decided wording carried "that has more than one newspaper", which described
`HAVING (?count>1)`. The corrected query drops it, so the canonical wording
reverts to the kept pipeline wording, *"How many newspapers are in Europeana for
each decade?"*, and the minimum-count rewrite is **not** retained as an
alternative: it no longer describes the query. The assessment becomes `accepted`,
since the canonical wording is now the pipeline's own.

The evidence label "Europeana Newspapers by Year" was wrong about the bucketing
before this correction as well — the query has always grouped by decade.

### europeana-0054 and cdec-0006 — no change

"List datasets with openly licensed content" still describes 0054 exactly: the
correction changed which licences count as open, not what is being listed.

cdec-0006's question already asks for the number of deported people per arrest
location and Nazi camp, which is what the query returns. Verified on the
endpoint: `count(?ArrestPlace)` and `count(DISTINCT ?person)` agree on every one
of the 1,158 rows and on the total of 10,666, so the existing projection already
counts people and the wording needs no qualification.

### meetups-0008 — no wording decision

The pair has no meta-review row and needs none: the correction is a prefix fix,
not a change of meaning. Its wording is untouched.

## Applying a review export after the bundle has been rebuilt

`apply_sparql_corrections` pins every review to the digest of the browser bundle
it was made against, and rebuilding the bundle changes that digest. The 13:03
export of 9 November was made in a workbench page still holding the 8 November
bundle, so it could not be applied as exported.

What was done, and what it is safe to do: the two new reviews — europeana-0048
and meetups-0008 — were repinned to the current bundle digest after confirming
that **both candidates are byte-identical** in the rebuilt bundle and in
`var/queries/sparql_correction_candidates.jsonl`, by comparing their
`candidate_digest` values against the export. The four reviews already applied
were dropped from the repinned export rather than repinned, since their base
pins are now stale by design. The repinned export is
`var/review/exports/musparql-sparql-correction-review-7159ddd8a3ac8e87-2026-11-09T13-repinned.json`.

A repin is only legitimate when the candidate digests match. If a candidate has
changed, the review was made against different material and must be redone in a
reloaded workbench.

## Known tooling defects

- A 403 whose body is Cloudflare HTML is classified `query_error` /
  `endpoint_rejected_query`. It must map to `unavailable`: the gateway never
  showed the query to the endpoint. This produced four false
  `likely_correction` candidates at high priority — europeana-0008, -0037,
  -0038 and -0039, exactly the four queries using `concat(`, `substr(` or
  `replace(`.
- **Fixed 9 November 2026.** The runtime refused meetups-0008 with
  `skip_reason: non_read_only_operation` and "Only SELECT queries can be
  executed by this runtime." The query is a `select`; `read_only_query_reason`
  strips `PREFIX` and `BASE` declarations in a loop, but `clean_query` only
  drops leading and trailing comment-only lines, so the
  `# ==================== PLACES` comment sitting between two `PREFIX` lines
  stopped the scan and the text no longer began with a keyword. The prologue
  scanner now consumes comments as well. meetups-0008 executes and returns
  **empty**, which is the endpoint problem below rather than a query defect.
- The correction UI still has no "exclude from benchmark" control (P0 in the
  runbook's regression walkthrough, step 5).

## Resolved: what was missing before the next benchmark

**Built on 9 November 2026 as `benchmark/v11` and `benchmark/v12`.** Everything
below is the trace that led there, kept because it records why the build took
the shape it did. Two snapshots rather than one, because each script writes a
complete snapshot with its own manifest and a single directory would have left
one of them naming a scratch path as its previous benchmark:

- **v11** admitted the five meta-review pairs through
  `scripts/benchmark/add_authored_pairs.py` and re-pinned meetups-0008, which
  also cleared the version-pin failure v10 had acquired. 105 pairs.
- **v12** applied the round's 79 reviews over 11 merged exports in one
  invocation, `update_from_initial_review` having gained paired, repeatable
  `--bundle` / `--reviews` arguments so every stage is named in the manifest.
  177 pairs over nine knowledge graphs.

Both pass `audit_snapshot`. Neither is published as a public release yet; the
release-projection blocker is in [`OPEN_ISSUES.md`](OPEN_ISSUES.md).

The two gaps this section identified were both real. The frozen bundle pinned
version 0, which the supplementary-bundle route in item 1a resolved; and no
corrected version had a pipeline observation, which `run_queries` then
supplied, in the course of which europeana-0039 turned out to run after all.

v10 was built on 6 August 2026 from the pre-IPL review. The IPL round of
16 September had never been folded into a snapshot, so the next snapshot was
that whole round, not just these six pairs. Traced on 9 November 2026 against
the actual scripts.

The merge step already runs. With the decisions file as it now stands:

```bash
.venv/bin/python -m scripts.benchmark.merge_adjudicated_reviews \
  --exports var/review/exports/ipl-2026-09-16 \
  --bundles var/review/bundles/ipl-2026-20260916-seven-kg \
  --decisions catalog/curated/meta_review_decisions.jsonl \
  --meta-reviewer-id reviewer-NNNN \
  --outdir var/review/exports/ipl-2026-09-16-merged --round ipl-2026-09-16
```

it resolves 84 reviews from 14 teams into 12 exports across the seven datasets.
So the wording work is done. What stands between that and a snapshot that passes
`audit_snapshot` is below, hardest first.

### 1. The corrected pairs are pinned to the uncorrected SPARQL

`make_benchmark_record` takes the gold SPARQL from the **review bundle**
(`record["input"]["sparql_clean"]`), not from the query catalogue. The IPL
bundle was frozen on 16 September and pins version 0 for all five corrected
europeana/cdec pairs. The snapshot policy is `latest_retained`, and
`audit_snapshot` enforces it from v8 onwards, so each of the five would fail
with "does not select the latest retained SPARQL".

The wording side needs nothing: the adjudicated canonical becomes
`preferred_question`, and `preferred_question_used_when_present` means it wins
over whatever wording the bundle record carries. Only the SPARQL pin is wrong.
A supplementary review bundle pinned to version 1, ingested as a second review
stage — v10's manifest shows `review_stages` already carries two — would fix
it.

**Checked on 9 November 2026: no, that bundle cannot be built without a
generation run.** `build_review_bundle` iterates over the generation *outputs*
(`for idx, rec in enumerate(output_records)`), indexing the inputs by
`(kg_id, query_id, query_label)` to decorate each one. A pair absent from the
outputs file produces no record at all, and `--query-label` only filters that
loop. `build_next_review_round` is the same: it is driven by `--current-run`.
Each record also carries `output_meta` — model, `generated_at`,
`run_signature`, `request_config`, `response_metadata` — and the bundle
auto-freezes a run snapshot under `var/runs`, so a hand-written outputs file
would be fabricated model provenance and is not an option.

The run is small, though, and the generated wording is discarded. `--input`
accepts any inputs file, so filtering `var/llm/inputs.jsonl` to the affected
labels gives a six-record generation rather than 524. Those inputs already
carry version 1, confirmed after the rebuild: cdec-0006, europeana-0017,
-0039, -0048 and -0054 all resolve to `sparql_version: 1`. The generated
wording never reaches the benchmark, because the adjudicated canonical arrives
as `preferred_question` and `preferred_question_used_when_present` is true.

### 1a. The stage-2 export still has to come from somewhere

Pass `--skip-correction-required` to the merge and the five drop out of stage 1,
which is what that flag is for. They are then not in v11 at all, so a stage-2
bundle inserts them as ordinary new pairs — `existing_keys` is computed from the
previous snapshot, they are not in it, and no overlap arises. meetups-0008 is
the one that needs the replacement exception, since v10 already holds it at
version 0: `build_review_bundle --previous-benchmark` sets
`has_prior_pair_review: true`, the version-1 hash differs so `review_scope`
stays `new`, and `is_reviewed_sparql_replacement` then permits the swap.

What has no mechanism is the stage-2 **review export**. The canonical wordings
reach a benchmark record through `preferred_question` in a review export, and
`merge_adjudicated_reviews` synthesizes that export from the decisions only for
a bundle that has team exports to take its `bundle_digest` and `assignment_id`
from. A fresh six-pair bundle has none. Either review those six in the review
workbench and enter the decided wording there, which needs no new code, or
teach the merge to synthesize a meta-review export against a bundle it has no
team exports for.

### 2. No corrected version has a pipeline execution observation

Every one of the six has version 1 stored and `latest_execution` still on
version 0. The successful runs happened in the correction workbench, and those
live in the correction provenance, not in `execution_history`.

Inclusion does not depend on it — "missing or failed observations do not affect
inclusion" — but the audit does: for a query with retained edits it requires
`sparql_provenance.execution_observation` to carry a status. A failed
observation satisfies that; no observation does not. So `scripts.run_queries`
has to run over the corrected versions before the snapshot, including for the
pairs that will fail: europeana-0039 cannot pass the Cloudflare filter at all,
and europeana-0048's corrected form has so far only timed out.

This is also where the 403 triage defect bites: 0039's observation must be
recorded as `unavailable`, not as a query error.

### 3. `sparql_correction_required` has no "applied" state

The flag is set by `slots["sparql correction"].startswith("NEEDED")`, so it
stays true for all five corrected pairs. The merge reports them as "needing
SPARQL correction and INCLUDED in this merge", and
`--skip-correction-required` would now drop exactly the pairs that have been
fixed. The notes format needs a third state, or the merge needs to compare the
flag against the retained edit count. Until then the flag must be read as "a
correction was required", not "a correction is outstanding".

### 4. Nine multi-team pairs have no decision

`musow-0080`, `nfdi4culture-0006`, `europeana-0001`, `-0002`, `-0015`, `-0016`,
`-0024`, `-0030`, `-0044`. They are resolved by tie-break — earliest
`reviewed_at`, then review id — so one team's review stands and the others are
dropped. They agree on disposition and wording by construction; their
`pipeline_assessment` may not. Either decide them or accept the tie-break
deliberately, and do not pass `--strict`.

### 5. Smaller things

- `reviewer-0001` was used as the meta-reviewer id for the merge that built
  v12. It is the project owner's own workbench id, so the attribution is
  correct, but it was never separately confirmed as the right id to publish.
- europeana-0037 and -0038 remain high-priority correction candidates on static
  diagnostics, and their Cloudflare 403s are genuine content blocks: both use a
  function the WAF rule refuses, so they need rewriting to run at all.
  europeana-0008 is no longer high priority — its 403 is the managed challenge,
  which says nothing about the query.
- meetups-0008's correction cannot be verified: the configured `meetups`
  endpoint serves musOW, and the pair returns empty. It is re-pinned in v11 and
  carried into v12 at version 1.
- The correction queue holds 524 candidates since the 9 November re-triage,
  which is every non-holdout pair; 179 of them are `not_attempted`, almost all
  from one LinkedMusic source that was never executed. See
  [`OPEN_ISSUES.md`](OPEN_ISSUES.md).

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
