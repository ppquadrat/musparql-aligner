# What the IPL round taught us

**Date:** 10 November 2026

**Scope:** the 16 September 2026 IPL workshop round, read for what it added to
what we understood beforehand. Sources are the
[workshop report](2026-09-16-ipl-workshop.md), the
[conflict analysis](2026-11-07-ipl-review-conflict-findings.md), the
[meta-review](2026-11-08-ipl-meta-review-decisions.md) and the SPARQL
corrections of 8–9 November. No new retrieval.

**Standing caveat.** One round, one cohort, 26 pairs with more than one judgment,
and choices that are correlated within reviewer. Nothing below is a rate, and
where a finding rests on a handful of pairs those pairs are named so the evidence
can be weighed directly.

**What this document is not.** The round also exposed process gaps, a reviewer-ID
exposure, endpoint behaviour and pipeline limitations. Those are recorded where
they belong and listed in §10.

## 1. The design underreports wording disagreement

Reviewers were shown a wording and asked to react to it. So what the round
measures is agreement *with a suggestion*, not agreement between independently
produced wordings, and the first is likely to be higher. Had reviewers written
from scratch, the spread would probably have been wider. This limits what the
low number of competing rewrites can be taken to mean.

Only 4 of the 26 multi-judgment pairs had two or more teams each supplying a
wording with differing text. That number should be read as a floor, not as
evidence that phrasing is largely settled.

A second reason for caution: some of the variation we did see is not two
candidates for one slot. On `europeana-0019` one team changed the interrogative
form and another unpacked the schema term *agent* into "people or
organisations". Those are edits to different aspects of the same sentence, and
both are defensible — the resolution can record both rather than choose, which
`accepted_alternatives` and `literal_formulations` already allow.

*Examples:* `europeana-0019` (two non-competing edits); the four competing
rewrites are `europeana-0004`, `-0019`, `-0022`, `-0053`.

## 2. Provider example sources vary in quality and purpose more than expected

The pipeline treats a curated source as a source of information needs. This
round suggests that how well that holds depends a great deal on what the source
was *for*, and that the variation is wider than the design assumed.

Europeana's SPARQL console and API example files are the worst batch so far in
this respect. They exist to demonstrate the schema, so their labels are written
in the schema's voice — a label like "Listing of `edm:Agent`" is documentation,
not a question. All five bare class enumerations in the round trace to
`europeana-sparql-console-select-examples.jsonl` or
`europeana-sparql-api-examples.jsonl`. v10's corpora, drawn from repositories
and site snapshots, contain none of that shape.

Two consequences, both modest. Schema vocabulary in a gold question is often
inherited rather than generated, so the model is not always the place to look.
And it is worth assessing a new source's purpose before trusting its labels —
weighting triage by source would have caught these five before the workshop.

*Examples:* "Listing of `edm:Agent`" as the evidence label behind
`europeana-0004`; inherited vocabulary in `europeana-0034` and `-0035`
("resource"), `-0004`, `-0019`, `-0022` ("agent"), `-0026` ("spatial or coverage
information"). Drift in the other direction, towards the SPARQL and away from
the stated label, in `europeana-0039` ("by Year" → "for each decade") and
`-0034` (the label's "associated to objects" dropped).

## 3. Removing schema vocabulary can remove the discriminant

Mostly, de-jargonising improves a question. At least once it would not have.

In `europeana-0035` the discriminating content of the query is that the time
period is a typed `edm:TimeSpan` entity and not a date literal — and the literal
form does occur elsewhere in the same bundle, since `europeana-0039` does string
arithmetic on `dc:date`. The word "resource" is carrying that distinction. The
useful move is to say it in ordinary language ("linked to a time period") rather
than to delete it.

So a reviewer asked to remove schema vocabulary is being asked for a judgment
rather than an edit, at least some of the time.

*Examples:* `europeana-0035` against `europeana-0039`.

## 4. A reviewer's criteria can be read off their decisions, without asking them

Where the round produced something more durable than individual rewordings, it
was by looking across one team's decisions rather than at one pair.

The four include/exclude splits look like four disagreements. They are not:
every one is the same two teams, one consistently including and the other
consistently excluding, and neither left a reason on any of the four. Reading
them together, it is one disagreement about what belongs in the benchmark. The
same holds for one reviewer unpacking *agent* on three separate pairs — a
consistent stance rather than three independent judgments.

Importantly, this needed nothing from the reviewers. The pattern was visible in
the submissions. That matters because the stances were almost certainly not
explicit to the reviewers themselves, and asking them to name a strategy — least
of all without seeing what others did — would probably not recover it, and would
add a phase to a process we want to simplify.

What follows is smaller than a method. The inference is opportunistic: those two
teams happened to have overlapping assignments, and reading across a reviewer's
decisions costs nothing when it is done after the fact. Assigning pairs of
similar shape deliberately so that stances become legible is *not* worth it —
it would be dull for the reviewer, there are not enough pairs of any one shape
to spend that way, and it needs someone supervising the assignment. Possibly
justified for a specific question; not in the general case.

The one thing worth adding to the form is a **reason list on exclusion**: a list
of grounds we supply, with the option to add one. Open rather than closed, since
the five exclusions in this round rested on five different grounds and the list
will not be complete. A reviewer picks rather than articulates, and the stance
shows up when the picks are grouped — this is the "record the exclusion reason as
a field" item already on the list, in its smallest form. Two notes on running it:
the added entries are the interesting signal, and an open list drifts, so it
needs consolidating between rounds.

Worth being clear about the limit. This works for include/exclude, where the
decision space is small and discrete. There is no equivalent for wording — no
closed or open list of ways to rephrase a question — which is probably part of
why the wording side of the experiment was the harder one to learn from.

*Examples:* `europeana-0011`, `-0032`, `-0034`, `-0049` (the same two teams, four
times); *agent* unpacked across `europeana-0004`, `-0019`, `-0022`.

## 5. Some pairs have no faithful natural-language wording at all

`europeana-0029` is `SELECT DISTINCT ?Concept WHERE { ?Concept rdf:type
skos:Concept }` and `europeana-0004` is the same shape for `edm:Agent`. The
answer is a page of opaque URIs. Inside a larger task that is the right output;
as a standalone need it is not a question anyone asks, so any wording is either
jargon or a loose gloss.

Three things here were not understood beforehand.

The criterion was already in use and had never been named. v10's dismissal
comments make the argument twice, in the reviewer's own prose, and nothing
downstream could tell that the two dismissals shared a reason:

> `jazzontology-0013` — "This seems to be a disambiguation query rather then
> search, since we already have the name. I would dismiss it"

> `jazzontology-0003` — "on its own, this query does not make much sense to
> anyone not intimately [familiar with the ontology]"

It does not seem recoverable from the query text. Three syntactic tests against
v10: "returns no labels" flags 22 of 100, "no selective constraint" 15 of 100,
"projects only triple-subject variables" 19 of 100. All three flag
`linkedmusic-0053`, "Find all MusicBrainz recordings made by Taylor Swift",
which is a sound need whose answer set happens to be URIs.

And the test is not whether the answer is an identifier. `jazzontology-0020`,
"find duplicate audio signals in the graph", returns only signal URIs and a
shared fingerprint, and is a standard MIR question — for duplicate detection the
identity of the two signals is the answer. The question is whether an identifier
answers the need.

*Examples:* `europeana-0029`, `-0004` (no faithful wording); `jazzontology-0013`,
`-0003` (the criterion in use, unnamed); `linkedmusic-0053` (false positive for
all three syntactic tests); `jazzontology-0020` (identifier *is* the answer).

## 6. Three terms, and four ways a pair can fail

The working method so far has been to infer the information need from the
question — for source-derived pairs the question is the only statement of it —
and then check whether the query supplies the answer. The round showed that
sometimes neither the question nor the query expresses a need a researcher could
plausibly have, which the two-term check cannot see. Reading the pair as a
relation between three things — the need, the question and the query — separates
failures that look alike and need different handling.

**No need behind the pair.** `europeana-0029`, `SELECT DISTINCT ?Concept WHERE {
?Concept rdf:type skos:Concept }`, and `europeana-0004` for `edm:Agent`. Nobody
asks these standalone; there is nothing for a wording to be faithful *to*. These
are the §5 cases, and the right outcome is exclusion.

**Real need, question drifted away from it.** `europeana-0049` — the evidence
label "Distribution of objects per language of the metadata" states a need
clearly, and the generated question, "How many objects are in the metadata for
each language?", loses the genitive and has no coherent reading. The query and
the label are both sound. Only the question needs repair, which makes this the
cheapest kind to fix.

**Real need, query does not implement it.** `europeana-0039`'s decade bucketing
is correct given a `YYYY`-prefixed string literal, and `dc:date` does not
reliably supply one. The need and the question are fine; the SPARQL is the
problem, and it goes to the correction workflow rather than the wording one.

**Real need that no question or query fully determines.** The licence pairs,
`europeana-0017` and `-0054`, belong here rather than in the first category, and
that was the correction to this section's first draft. The need is real and
valuable — does this KG hold openly licensed material I can use, roughly how
much of it, and how is it distributed across collections, subjects or media
types? What is missing is the step between the need and any query: *which
licences count as open*. CC BY? BY-SA? Jurisdiction ports? Any version, or
specific ones? That is a policy and domain decision, not a schema question, and
neither the researcher nor the graph supplies it. The pair's filter made the
decision implicitly and made it wrong — four exact URIs, with CC BY and BY-SA
named at version 1.0 only, while the graph's CC BY material is 2.5, 3.0 and 4.0
— and "openly licensed" in the question is exactly as vague as a researcher's
own framing would be, so nothing in the pair reveals the mistake.

That last category is the interesting one, and it is where this round changed
what the problem looks like.

**It explains why the defect survived several reviewers.** The question reads
naturally, the query looks like a licence filter, and the mismatch is visible
only by listing the graph's actual `edm:rights` values — which turn out to carry
jurisdiction ports (`/at/`, `/de/`, `/es/`, `/pl/`), inconsistent trailing
slashes, an `https` variant alongside `http`, and one value with a trailing
zero-width space. No amount of reading the pair gets you there.

**The repair does not belong in the question.** Making the question name the
licence families would fix the scoring and spoil the pair, because the point of
the question is to be the one a researcher would actually ask — and a researcher
asking about open material would not enumerate licence URIs, would not know
which ones this graph holds, and would not know how this schema records them.
Specifying the question is repairing the wrong term.

The repair belongs in the mediator, and it has two branches:

1. **Resolve it from the data.** Inspect the graph, work out what the options
   are, and decide on the researcher's behalf.
2. **Where that is genuinely impossible, ask the researcher** — present the
   options, and say what is missing and why it matters.

The licence case splits across both, which is what makes it a good example.
Branch 1 gets most of the way: a mediator that enumerated `edm:rights` would
discover the CC BY material at 2.5, 3.0 and 4.0, the ports, and the dirty
values, none of which the pair's author knew. What it cannot settle alone is
whether, say, NC or ND counts as "open" for this researcher's purpose. That is
branch 2 — and the stored query did neither. It guessed silently, and guessed
wrong.

**This is the researcher-to-KG direction, and it is the hard one.** Going the
other way the problem does not arise: whoever wrote the SPARQL already made the
decision, so a SPARQL-to-natural-language task never meets the
under-determination. It appears only when starting from what a person wants to
know. Which also means these pairs are not awkward cases to be cleaned out of
the corpus — for the direction Musparql cares about they are the pairs that
test the thing that is actually difficult.

**What that implies for scoring is open.** An under-determined pair cannot be
scored by matching one stored query, so one of the following is needed, and the
choice is not made here:

- an equivalence class of acceptable queries rather than a single gold query;
- scoring the behaviour instead of the output — does the system notice the
  under-determination and ask, or state its assumption, rather than guessing
  silently? On that reading `europeana-0017`'s own SPARQL would fail;
- recording the under-determination as a property of the pair and holding such
  pairs out of exact-match scoring, using them for the mediation question
  instead.

The third would be a sibling of `interpretive.requires_graph_context_knowledge`
rather than the same thing: that flag marks pairs where the expected SPARQL needs
knowledge of how the graph was built, which branch 1 can supply. This would mark
pairs needing a *decision*, which branch 1 may not be able to supply at all.

It is also distinct from the "room for interpretation or ambiguity" dimension in
the [linguistic workbench](../LINGUISTIC_DIMENSIONS_WORKBENCH_SPEC.md), which
rates how open a formulation is relative to the literal verbalisation of the
query. That is a property of the wording. Under-determination here is a property
of the need relative to the graph, and it survives any rephrasing.

A general note on detection, across all four failure modes:
question-against-query can be checked on paper. Question-against-need and
query-against-graph cannot — both need either a researcher's judgment or a query
against the live data.

*Examples:* `europeana-0029`, `-0004` (no need); `europeana-0049` (question
drifted); `europeana-0039` (query defective); `europeana-0017`, `-0054`
(under-determined need).

## 7. One natural wording can cover two different queries

`europeana-0005` and `europeana-0026` both retrieve objects linked to a typed
`edm:Place` and differ in one clause. The routing difference is cosmetic — proxy,
aggregation and CHO form a triangle and both reach the same set. The property is
the real difference: `-0026` pins it to `dcterms:spatial|dc:coverage`, while
`-0005` leaves it a wildcard, which on this graph also admits `dc:subject`,
`edm:currentLocation` and `dc:creator`.

The obvious wording — "objects with a place" — fits both, and settling which
query it describes took a long discussion. A wording that distinguishes them does
exist, so this is not a limit of the language; it is that the natural phrasing
underdetermines, and that these property names mean something narrower inside
this graph than in general use. That second part is a tension we have met before.

What is new is the consequence for the corpus: if two near-duplicate pairs carry
the same gold question, the benchmark asks one question with two correct answers
and marks a correct system wrong. Near-duplicates need questions that state what
separates them. A wildcard property has the same effect on its own — asked for
"objects associated with a place", a competent system would name specific
properties, which is better modelling and scores wrong.

*Examples:* `europeana-0005` against `europeana-0026`.

## 8. Four worked instances of pairs correct only by ingestion convention

The `interpretive.requires_graph_context_knowledge` flag already existed, so the
phenomenon is not new. What the round adds is four cases documented in enough
detail to be used as reference, and the sense that they are not rare:

- `europeana-0011` — `edm:europeanaProxy "false"` picks out provider proxies only
  because Europeana's ingestion always asserts the property. On the endpoint it
  takes exactly two plain-literal values and no proxy lacks it; nothing in EDM
  guarantees that, and the untyped literal would fail silently against
  `"false"^^xsd:boolean`;
- `europeana-0026` — the `dcterms:spatial|dc:coverage` alternation is a
  workaround for inconsistent legacy metadata;
- `europeana-0049` — `edm:language` sits on the Europeana aggregation, so one
  triple pattern silently restricts the query to those aggregations; without
  that the counts would be wrong;
- `nfdi4culture-0001` — the two-type intersection is redundant on current data,
  so the query's recall is the coverage of TA4's curated list rather than of the
  graph.

For these, the expected SPARQL is not derivable from the question plus the
schema. That bears on what a translation benchmark can fairly ask of a system.

## 9. The exclusions carried more information than the inclusions

Five of the seventeen contested pairs were excluded, on five different grounds:
publication convention (`europeana-0011`), unconstrained property traversal
(`-0005`), and bare enumeration (`-0004`, `-0029`, `-0034`). Between them they
describe the boundary of what the benchmark is for better than any inclusion
does — which is an argument for treating exclusion as a reasoned outcome with a
recorded reason, as in §4, rather than a deletion.

Two of them also turned up something about Europeana's graph that nothing else in
the project records. Run as written, `europeana-0004` returns 100 of 100 results
of the form
`file:///home/jenkins/workspace/download-datasets-run/#Adolf_Graf_Schwarzenberg_Besitzer`
— filesystem paths from an ingestion run that resolved relative URIs against a
build worker's disk, published as entity URIs. The same artefacts dominate
`edm:TimeSpan`. A constrained query over the same classes never meets them,
which is why `europeana-0019` (agents filtered to VIAF) survives the pass and
`-0004` does not.

*Examples:* the five exclusions above; `europeana-0004` and `-0034` for the
`file:` URIs; `europeana-0019` as the constrained contrast.

## What the round cannot tell us

The rate of disagreement, which reviewer stances dominate, and — the question
that would matter most — whether reviewers converge on wording once the SPARQL
is correct and the unsuitable pairs are gone. That one is untouched, because the
round never presented a clean set. §1 adds that even the disagreement we did see
is conditioned on reviewers reacting to a suggestion.

Which of the bare-enumeration pairs belong also depends on what the benchmark is
for: they are a liability for natural-language-to-SPARQL and merely hard for the
reverse direction. Musparql's purpose settles it here, but the question had not
been put in that form before, and it decides cases that look like wording
problems.

## 10. Recorded elsewhere

- the instrument and the triage-first lesson — the 8 November addendum in
  [`2026-09-16-ipl-workshop.md`](2026-09-16-ipl-workshop.md);
- reviewer-ID exposure, the release blocker and the withdrawal contradiction —
  [`../OPEN_ISSUES.md`](../OPEN_ISSUES.md);
- endpoint behaviour, the correction queue re-triage and the unobserved pairs —
  [`../SPARQL_CORRECTION_FOLLOW_UP.md`](../SPARQL_CORRECTION_FOLLOW_UP.md);
- benchmark admission without a generation round —
  [`../REVIEW_RUNBOOK.md`](../REVIEW_RUNBOOK.md) and
  [`../DATA_MODEL.md`](../DATA_MODEL.md).

## Related records

- [`2026-09-16-ipl-workshop.md`](2026-09-16-ipl-workshop.md)
- [`2026-11-07-ipl-review-conflict-findings.md`](2026-11-07-ipl-review-conflict-findings.md)
- [`2026-11-08-ipl-meta-review-decisions.md`](2026-11-08-ipl-meta-review-decisions.md)
- [`../REVIEW_POLICY.md`](../REVIEW_POLICY.md)
