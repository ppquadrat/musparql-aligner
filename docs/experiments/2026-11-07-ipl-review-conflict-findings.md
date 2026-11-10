# IPL workshop review data: wording conflicts and pair-level findings

**Date:** 7 November 2026

**Scope:** first analytical pass over the review data collected at the
[16 September 2026 IPL workshop](2026-09-16-ipl-workshop.md)

**Status:** findings and recommendations; no benchmark or production change made

## Why this pass was needed

Until the workshop there was a single reviewer, so the canonical question for a
pair was whatever that reviewer decided. The benchmark builder encodes exactly
that rule: the gold question is the reviewer's `preferred_question` when present
and the approved model output otherwise
([`scripts/benchmark/update_from_initial_review.py:186`](../../scripts/benchmark/update_from_initial_review.py)).
The workshop produced overlapping judgments from independent teams for the
first time, and there is no mechanism to choose between them.

The builder does not silently guess. Its `conflict_policy` is
`replace_only_attested_re_reviewed_sparql_revisions; otherwise fail`, so
overlapping normal reviews are a hard failure rather than an arbitrary merge.
That is the right default — it forces the decision instead of hiding it — but it
also means the workshop data cannot enter the benchmark until a resolution
procedure exists.

## Data and method

The 14 latest valid revisions, one per reviewing team, were copied from the
production server on 7 November 2026 and analysed with
[`scripts/analyze_wording_variants.py`](../../scripts/analyze_wording_variants.py).
Analysis uses the same selection rule as the workshop report: latest valid,
digest-intact, successfully processed revision per team. Only pseudonymous
`reviewer-NNNN` and `group-…` identifiers were used; no profile, familiarity or
contact data entered the analysis.

Reproduce the figures with:

```
python3 scripts/analyze_wording_variants.py var/review/exports/ipl-2026-09-16 \
    --bundles var/review/bundles/ipl-2026-20260916-seven-kg --exclude-non-reviews
```

A full per-pair rendering, including verbatim reviewer comments, is written to
`var/review/analysis/ipl-2026-09-16-wording-conflicts.md`. That file is
deliberately untracked: it reproduces `internal_comment` material, which is
working review data and excluded from publication by
[`REVIEW_POLICY.md`](../REVIEW_POLICY.md). This document paraphrases those
comments instead.

## Correction to the workshop coverage figures

One team used the optional `literal_wording` field as a status box, writing
"Not reviewed" on eight items. All eight carry the untouched form defaults —
`included`, and seven of eight `accepted` — whereas that team's nine genuine
reviews mostly carry `prompt_improvement_recommended` with a substantive
comment. The eight are therefore not judgments and should not count as coverage
or as one side of a disagreement.

| | as submitted | excluding self-declared non-reviews |
|---|---:|---:|
| pair reviews (team × pair) | 128 | **120** |
| distinct pairs reviewed | 85 | **84** |
| pairs judged by ≥ 2 teams | 28 | **26** |
| pairs needing human adjudication | 14 | **17** |

Per KG, excluding the non-reviews: Europeana 66 pair reviews over 34 distinct
pairs (22 with ≥ 2 teams); MusoW 28 over 27 (1); Camera dei Deputati 11 over 11
(0); CKG/NFDI4Culture 8 over 6 (2); CDEC 7 over 6 (1).

The figures in the workshop report remain correct as a description of what was
submitted and processed. The numbers above are the analytical figures to use for
inter-rater and adjudication work.

## The conflicts are fewer and more tractable than the overlap suggests

Of the 26 pairs with two or more team judgments, 16 show no disagreement about
wording: every team left `preferred_question` empty and accepted the pipeline
wording. Three of those 16 are nevertheless contested, because a team excluded
the pair without proposing any rewording. A further four agree on wording and
disposition but differ on `pipeline_assessment`, which decides whether a team's
comment survives into the benchmark, so only nine pairs are uncontested on every
axis and 17 need a decision. The contested ones fall into four types, which need
different handling.

### Competing rewrites — 4 pairs

Two or more teams each supplied a wording and the texts differ. All four are
Europeana: `europeana-0004`, `-0019`, `-0022`, `-0053`.

These are the only cases where arbitration in the ordinary sense is needed, and
in at least two of them the rival wordings are not competing on the same
dimension. On `europeana-0019` one team changed only the interrogative form
while another unpacked the EDM term *agent* into "people or organisations". Both
are defensible; the resolution mechanism should be able to record "both are
acceptable" rather than being forced to pick a winner. The data model already
supports this through `accepted_alternatives` and `literal_formulations`.

### One rewrite against teams that kept the pipeline wording — 6 pairs

`europeana-0011`, `-0026`, `-0039`, `-0045`, `-0054`, `nfdi4culture-0001`.

This is the more dangerous shape, because it does not look like a conflict.
Under the current rule a single team's rewrite silently becomes the gold
question even where two or three other teams were content with the original,
and nothing records that the point was contested.

### Include/exclude disagreement — 4 pairs

`europeana-0011`, `-0032`, `-0034`, `-0049`.

No wording rule touches these. They are also not four independent cases: every
one is the same two teams, one consistently including and the other
consistently excluding, and neither left a reason on any of the four. Elsewhere
the excluding team justified rejections on the grounds that the item is not a
research question. The efficient resolution is to ask that team for their
standard rather than adjudicating pair by pair.

### Assessment-only disagreement — 4 pairs

`cdec-0006`, `europeana-0005`, `-0029`, `-0048`. `europeana-0005` was
subsequently excluded on modelling grounds unrelated to the assessment, which
settles it: a dismissed pair carries no assessment. Three remain open.

Two teams agree that the pair belongs in the benchmark and neither proposes a
rewording, but one marks it `accepted` and the other recommends a prompt or
input-data improvement. Nothing about the gold question is at stake; what is at
stake is whether the dissenting team's comment reaches the benchmark at all,
since the comment travels with the review that is applied.

These were initially left to a documented tie-break on submission order. That
was abandoned once it became clear what it discards: on `cdec-0006` the earlier
review says `accepted` while the later one reports, in a public comment, that
the query has two `ORDER BY` clauses and does not parse. Resolving by timestamp
would have kept the `accepted` review, dropped the comment, and admitted an
unparseable pair. Four cases is small enough to decide, and the general lesson is
that an assessment disagreement is not a formality: it can carry the only record
of a defect.

## Cross-cutting findings

### Schema vocabulary leaks into gold questions through the curated evidence

The pipeline's questions are copied `verbatim` or lightly paraphrased from
curated SPARQL-console example labels, and those labels are written in schema
vocabulary. "Resource" in `europeana-0034` and `-0035`, "agent" in `-0004`,
`-0019` and `-0022`, "spatial or coverage information" in `-0026` all originate
in the evidence snippet, not in the model.

Reviewers push consistently in the opposite direction. One reviewer
de-jargonised *agent* to "people or organisations" on three separate pairs — a
coherent reviewer-level stance rather than three independent judgments, which
also means wording choices are correlated within reviewer and are not
independent draws.

The distinction is not always noise. In `europeana-0035` the whole
discriminating content of the query is that the time period is a typed
`edm:TimeSpan` entity rather than a date literal, and the literal form really
does occur elsewhere in the same bundle (`europeana-0039` does string
arithmetic on `dc:date`). "Resource" carries that distinction in the source
label; the fix is to express it in natural language ("linked to a time
period"), not to delete it.

Recommendation: a gold question should describe what the query retrieves, not
the schema accidents it routes around. Curated evidence labels are a source of
the information need, not a drop-in gold question.

### Paraphrase and generation drift

Where the pipeline did not copy the label verbatim, it drifted:

- `europeana-0049` — "Distribution of objects per language of the metadata"
  became "How many objects are in the metadata for each language?", losing the
  genitive and leaving no coherent reading. The query and the original label are
  both sound; only the generated question is bad. This is the cheapest of the 13
  to settle.
- `europeana-0039` — evidence says "by Year", the question says "for each
  decade", origin mode `generated`. The question happens to match the query,
  which does bucket by decade, so generation drifted *toward* the SPARQL and
  away from its stated source.
- `europeana-0034` — the label's constraint "associated to objects" was dropped;
  here too the question matches the query better than the label does.

Recommendation: record origin mode as a review-time signal. `verbatim` pairs
risk inherited jargon; `paraphrased` and `generated` pairs risk drift, in both
directions.

### Gold-query faithfulness defects, independent of wording

Four findings are defects in the pair itself rather than disagreements about
phrasing, and none is reachable by choosing between the reviewers' wordings:
`europeana-0017` and `-0054` need a corrected filter or a narrowed question,
`europeana-0039` needs its type pattern and its date handling examined,
`cdec-0006` does not parse at all, and `europeana-0048` double-counts every
object that arrived through an aggregator because a redundant join multiplies the
solutions that `count(*)` then counts (Azerbaijan: 54 reported against 27 real
objects, while Montenegro and Georgia are exact — so the distortion varies by
country and destroys the comparison the question exists to make). All three are routed to the SPARQL correction
workflow rather than the adjudication round, even where a reviewer's rewrite also
improves the wording.

**The licence filter (`europeana-0017`, `europeana-0054`).** Both queries ask for
"openly licensed" content and both implement it as
`FILTER (?Rights IN (...))` over four hardcoded URIs:

```
http://creativecommons.org/publicdomain/mark/1.0/     occurs in the graph
http://creativecommons.org/publicdomain/zero/1.0/     occurs in the graph
http://creativecommons.org/licenses/by/1.0/           occurs nowhere
http://creativecommons.org/licenses/by-sa/1.0/        occurs nowhere
```

The licence version is part of the URI, and `IN` compares whole URIs, so these
are four exact strings rather than a pattern. CC BY and BY-SA appear in the list
at **version 1.0 only**, which was superseded in 2002. The graph's actual CC BY
material carries 2.5, 3.0 and 4.0 — all verified present on the live endpoint —
and none of it matches any of the four entries. `by/4.0/` is as different from
`by/1.0/` as it is from `by-nc-nd/3.0/at/`.

So the filter admits public-domain material and nothing else, while openly
licensed material sits in the graph untouched. This is not sparse data that will
fill in later: no quantity of newly ingested CC BY content would ever activate
those two entries, because the version they name does not occur. The queries
therefore cannot express their own stated information need in any state of the
graph. A system that translated "openly licensed" into a filter covering CC BY
4.0 would be a better translation of the question and would be scored wrong
against this gold query.

This is what separates the case from data decay. On `europeana-0010` the
question and the query correspond and only the data moved; restore the item and
the pair is sound. Here the correspondence between the natural-language question
and the SPARQL is itself false, independently of what the endpoint returns, so no
choice between reviewers' phrasings can repair it.

The rights values are also dirty in ways exact URI matching cannot survive:
jurisdiction ports (`/at/`, `/de/`, `/es/`, `/pl/`), inconsistent trailing
slashes on otherwise identical licences, an `https` variant alongside `http`,
`rightsstatements.org/page/` alongside `/vocab/`, and one value carrying a
trailing zero-width space (U+200B). Any corrected filter should match licence
families by prefix rather than enumerating URIs.

**`europeana-0039`.** Two separate problems sit under the year/decade drift
already noted above.

*The type pattern may not denote newspapers in this graph.* The query selects
resources whose `edm:hasType` points at the Getty AAT newspapers concept
(`aat:300026656`). On the live endpoint nothing carries both that and a
`dc:date`, while `dc:type` with the same concept and `dc:date` do co-occur. If
newspaper-ness is expressed through `dc:type` here, then the query's pattern
does not pick out newspapers in this KG, and the question "how many newspapers
per decade" is not what the SPARQL asks — a correspondence failure rather than
an empty result. This one is **not settled**, unlike the licence filter: the
pairing may have matched when the console example was curated and the data may
have been remodelled since. What would settle it is checking whether
`edm:hasType` ever carried this concept alongside dates, and what the curated
source example actually ran against.

*The decade bucketing assumes a string format the data does not guarantee.*
`SUBSTR(?dat, 1, 3)` takes three characters from position 1 (SPARQL strings are
1-indexed) and `CONCAT` appends `"0"`, so `"1897-05-12"` becomes `"1890"`. That
logic is correct, and unambiguously computes decades, **given a `YYYY`-prefixed
string literal**. Real `dc:date` values are not reliably that: sampling returned IRIs such as
`file:///home/jenkins/workspace/download-datasets-run/#1438%3F` — an ingestion
artifact with the date in a URL-encoded fragment — and literals as short as
`"2"`. `SUBSTR` on an IRI is a type error, and a `BIND` that errors leaves the
variable unbound rather than dropping the row, so every such object collapses
into a single unbound group; `"2"` yields the decade `"20"`. Years of fewer than
four digits give the wrong magnitude, and because `?Date` is a plain string,
`ORDER BY` is lexicographic and only coincides with chronological order while
every key is four characters.

Two smaller observations on the same pair: `COUNT(*)` counts solutions rather
than newspapers, so an object with two `dc:date` values is counted twice and
`?x` is not constrained to one proxy per object; and `HAVING (?count>1)`
silently drops decades holding a single newspaper, which the pipeline question
omits and one team's rewrite restored. The rewrite is the better wording on that
specific point, which is why this pair also appears in the contested-wording
list.

### Execution behaviour is provenance, not an exclusion criterion

A query that returns nothing still expresses an information need, and the
benchmark measures NL→SPARQL translation rather than result sets.
Execution-independent scoring is also what keeps the benchmark stable across KG
re-ingests. Empty results should therefore be recorded as a dated provenance
flag, not used to exclude a pair.

One distinction is worth keeping, because it separates decay from defect:

- **No longer matches** — `europeana-0010` and `europeana-0014` hardcode an
  item URI (`…/item/08501/Athena_Update_ProvidedCHO_Bildarchiv_Foto_Marburg_obj_00023128_LA_2_088_40`)
  that is now absent from the endpoint in both subject and object position. One
  team spotted this during the workshop. The queries still say what they meant.
- **Never matched** — `europeana-0039`, where the pattern itself looks wrong
  rather than stale.

The first is the KG moving on. The second hints at a modelling error in the
pair.

### Several pairs are correct only by publication convention

These are candidates for a graph-context flag, because their correctness cannot
be read off the vocabulary:

- `europeana-0011` — `edm:europeanaProxy "false"` picks out provider proxies
  only because Europeana's ingestion always asserts the property. Verified on
  the live endpoint: the property takes exactly two plain-literal values, no
  `ore:Proxy` lacks it, and even objects with an `edm:intermediateProvider` have
  exactly two proxies, so the binary holds. Nothing in EDM guarantees it, and
  the untyped literal would silently fail against `"false"^^xsd:boolean`.
- `europeana-0026` — the `dcterms:spatial|dc:coverage` alternation is a
  workaround for inconsistent legacy metadata. `dc:coverage` is the unrefined
  parent covering place, time and jurisdiction; only the
  `?Place rdf:type edm:Place` constraint keeps the query coherent.
- `europeana-0049` — `edm:language` is carried by the `edm:EuropeanaAggregation`
  and not by the provider aggregation, so that one triple pattern silently
  restricts the query to Europeana aggregations. Without that accident the
  counts would be wrong. The property means the language of the metadata record,
  not of the object; values are ISO codes including `mul`.
- `nfdi4culture-0001` — the two-type intersection is redundant on current data:
  all 22 `fabio:DataRepository` instances are also `nfdicore:NFDI_0000123`
  ("data portal", a subclass of "website"), while 69 data portals are not
  repositories. The query's recall is therefore the coverage of TA4's curated
  repository list, not of the graph. `SAMPLE(?label)` is non-deterministic in
  principle but harmless today, as all 22 resources carry exactly one label;
  `SAMPLE(?resource)` around the grouping key is redundant.

### Near-duplicate pairs need gold questions that separate them

`europeana-0005` and `europeana-0026` both retrieve objects linked to a typed
`edm:Place`, and differ in one clause:

```sparql
# europeana-0026
?Place rdf:type edm:Place .
?Proxy dcterms:spatial|dc:coverage ?Place ; ore:proxyFor ?ProvidedCHO

# europeana-0005
?Place rdf:type edm:Place .
?Proxy ?property ?Place ; ore:proxyIn ?Aggregation .
?Aggregation edm:aggregatedCHO ?ProvidedCHO
```

The routing difference is cosmetic. A proxy, its aggregation and the CHO form a
triangle: `ore:proxyFor` is the direct edge, while `ore:proxyIn` plus
`edm:aggregatedCHO` is the long way round to the same object. No proxy in the
graph has one edge without the other (`ASK { ?p ore:proxyIn ?a FILTER NOT EXISTS
{ ?p ore:proxyFor ?cho } }` is false), so both reach the identical set and
`europeana-0005` simply pays for an extra join.

The property is the real difference. Constrained to actual proxies, the
properties that link an object to an `edm:Place` are `dcterms:spatial`,
`dc:coverage`, `dc:subject`, `edm:currentLocation` and `dc:creator`. So
`europeana-0026` asks what place an object *is about*, while `europeana-0005`
also admits where it is currently held, the place as its subject, and records
where the creator is mistakenly a place. English hides this: "the place of a
thing" can mean the place depicted or the place it is kept, and the data
distinguishes them.

A caution for anyone repeating this analysis: querying `?Proxy ?property ?Place`
without requiring `?Proxy` to be a proxy returns a much longer property list,
including `rdvocab:placeOfBirth` and `skos:broader`, which belong to agents and
concepts rather than objects. The first pass of this analysis made that mistake.

Two consequences. First, if both pairs carry the same gold question the benchmark
asks one question with two different correct answers, and a system that produced
the other pair's query would be marked wrong for a distinction the question never
stated. Near-duplicate pairs have to state what separates them: "which objects
are about a place?" against "which objects have a place associated with them?".

Second, `europeana-0005` was excluded, and the reasoning generalises. Its breadth
comes from a wildcard property rather than from an information need, which is the
shape of an exploratory query — the same ground on which `europeana-0011` was
excluded. A wildcard also makes the pair unusable as ground truth: asked for
objects associated with a place, a competent system would name specific
properties, which is better modelling and would be scored wrong. And part of its
result set is a data error, so no wording can describe it faithfully. The SPARQL
is syntactically correct; the modelling is the problem, and syntactic
correctness is not sufficient for inclusion.

`europeana-0005` is the only pair reaching `edm:currentLocation`, so excluding it
leaves "where is this object held?" uncovered. That need deserves a pair built
for it rather than one that reaches the property by accident.

### Queries whose answer is an identifier: standalone need or intermediate operation

Added 8 November 2026. This began as a question about two Europeana pairs and
turned out to be a benchmark-wide criterion that was already in use without
being named.

`europeana-0029` is `SELECT DISTINCT ?Concept WHERE { ?Concept rdf:type
skos:Concept }`, and `europeana-0004` is the same shape for `edm:Agent`. Neither
returns a name: the answer is a page of opaque URIs. As a step inside a larger
task — get the concept URIs, then ask something about them — that is exactly the
right output. As a standalone research need it is not a question anyone asks, and
the project's own [`../REVIEW_POLICY.md`](../REVIEW_POLICY.md) already names
"intermediate operation" and "non-standalone" among its exclusion grounds.
[`../SPARQL_CORRECTION_FOLLOW_UP.md`](../SPARQL_CORRECTION_FOLLOW_UP.md) goes
further: its P0 "exclude unsuitable queries before pair review" was written
before the workshop, names "non-standalone" in as many words, and gives the
reason that "asking them to review the pair later wastes effort". It is not
implemented, and the workshop paid exactly the cost it predicted.

What the benchmark is for decides the answer. For natural-language-to-SPARQL
these pairs are a liability: there is no natural question whose answer is a bare
identifier list, so any wording is either jargon or a loose gloss. For
SPARQL-to-natural-language they are merely hard. Musparql's stated purpose is
the first, so the pairs are a liability.

**No syntactic test identifies them.** Two were tried against v10 and both
over-flag badly. "Returns no labels or literals" flags 22 of 100, including
`linkedmusic-0053` "Find all MusicBrainz recordings made by Taylor Swift", which
is a perfectly good need whose answer set happens to be recording URIs. "Has no
selective constraint" flags 15 of 100 and also catches the Taylor Swift query,
because the constraint is a literal the pattern failed to parse. Narrowing to
pairs that project only variables appearing as the subject of a triple — that
is, entity identifiers and nothing else — gives a defensible 19 of 100, but the
19 contain both kinds. The judgment is not recoverable from the query text, which
is why it belongs to review rather than to validation.

**Returning identifiers is not the test; needing a name is.** `meetups-0006`,
`meetups-0008`, `organs-0001` and `linkedmusic-0009` all look like bare
enumerations and all in fact project `?s ?label` with an `rdfs:label` binding, so
they answer in names and are sound. Conversely `jazzontology-0020`, "Find
duplicate audio signals in the graph", returns only signal URIs and a shared
fingerprint, and is a standard MIR research question: for duplicate detection the
*identity* of the two signals is the answer, and a label would not help. The
operative question is therefore whether an identifier answers the need, not
whether the projection contains a literal.

**v10's own dismissals already apply this criterion.** Of the six pairs dismissed
from v10, the recorded reasons give it in the reviewer's own words:

> `jazzontology-0013` — "This seems to be a disambiguation query rather then
> search, since we already have the name. I would dismiss it"

> `jazzontology-0003` — "The wording is fine but the concept of ReleaseEvent is
> not very NL friendly ... on its own, this query does not make much sense to
> anyone not intimately [familiar with the ontology]"

So the criterion has been in consistent use since before the workshop; it simply
never became a recorded field, which is why nothing downstream can tell that two
of the six dismissals share a reason with each other or with `europeana-0005`.

**v10 is internally inconsistent on it.** The four dismissed jazzontology pairs
are structurally identical to seven that were published. All take a name or title
in and return an entity identifier out:

| dismissed | published |
|---|---|
| `-0002` title → album | `-0004` title → record label |
| `-0003` title + time → release event | `-0005` title + count → release |
| `-0006` title + number → record | `-0012` name → band |
| `-0013` name + performer → artist | `-0010`, `-0011` name → artist |
|  | `-0007`, `-0008` given CD or performance → tracks, performers |

`jazzontology-0010` and `-0011` are the weakest of these on a second ground as
well: the gold questions are "Is there more than one composer with this name in
the graph?" and the arranger equivalent, but the queries are `SELECT DISTINCT`
over the artist, so answering the question means counting the rows rather than
reading the result. The question form and the query form do not correspond.

**Dismiss rather than flag.** A `split`-style label distinguishing standalone
needs from intermediate operations was considered and rejected. The pairs that
would carry the label are the pairs with no faithful natural-language wording at
all — that is what makes them intermediate — so keeping them means either
publishing jargon or publishing a gloss only an expert in that one schema can
read, and the benchmark should contain neither. The label would preserve the
defect and record that it is a defect. Dismissal with the reason recorded
achieves the same traceability without putting the wording into the published
set.

This leaves one incidental defect worth routing separately: `meetups-0008`'s
gold question is "List all places in the graph with their coordinates", but the
query binds `?lat` and `?long` and then projects only `?s ?label`. The
coordinates are requested, computed and discarded, which is a faithfulness defect
of the same family as the three recorded above and is queued in
[`../SPARQL_CORRECTION_FOLLOW_UP.md`](../SPARQL_CORRECTION_FOLLOW_UP.md).

**Provenance.** All five workshop bare enumerations are Europeana pairs, and all
five trace to `europeana-sparql-console-select-examples.jsonl` or
`europeana-sparql-api-examples.jsonl`. Console and API examples exist to
demonstrate a schema, not to answer questions — "Listing of `edm:Agent`" is
documentation. v10's corpora came from repositories and site snapshots and
contain none of this shape. A query harvested from a console-examples source
therefore deserves a presumption of being a demonstration until a reviewer says
otherwise, which would have caught all five before three teams in Brussels spent
time on them.

**Applying the criterion overturned two decisions already made.** `europeana-0004`
(`?Agent rdf:type edm:Agent`, `LIMIT 100`) and `europeana-0034`
(`?TimeSpan rdf:type edm:TimeSpan`) had both been adjudicated as *include* with
reworded gold questions before the criterion was articulated. Both are bare class
enumerations, and checking them against the live endpoint produced a second,
independent ground for excluding them.

**A build-server path leak makes those two answers junk.** Run exactly as
written, `europeana-0004` returns **100 of 100** results of the form

```
file:///home/jenkins/workspace/download-datasets-run/#Adolf_Graf_Schwarzenberg_Besitzer
file:///home/jenkins/workspace/download-datasets-run/#Agostino_Barbarigo_Besitzer
```

These are filesystem paths from an ingestion run that resolved relative URIs
against the local disk of a Jenkins worker, and they are published in the graph
as entity URIs. The same artefacts dominate `edm:TimeSpan`
(`…/LX3390073XBRAHMSX1884X06X01X1884X06X30`) and occur among `skos:Concept` as
well. Real instances of these classes exist and are reachable — non-`file:` time
spans carry Spanish-language period labels from Hispana, agents carry
`skos:prefLabel` — but an unconstrained enumeration returns the artefacts first,
and many of them carry no label at all.

This is why the two grounds reinforce rather than merely coincide. A constrained
query over the same classes is unaffected: `europeana-0019` filters agents to
`strstarts(str(?Agent), "http://viaf.org/viaf/")` and never sees a `file:` URI,
which is one reason it survives the pass while `europeana-0004` does not. It is
the *absence* of a constraint that both removes the information need and exposes
the ingestion junk. A benchmark pair whose gold answer is a page of build-server
paths would score a correct system wrong.

**What the criterion does not reach.** Four pairs return bare object URIs but
survive, because the need is real and the fix is a projection rather than an
exclusion: `europeana-0017`, `-0022`, `-0026` and `-0032` are all constrained
searches ("objects that reference an agent from VIAF", "objects about a place")
whose answer set is legitimately a set of objects; they would be more usable if
they also projected `dc:title`, which is a query improvement to note rather than
grounds for dropping them. `europeana-0019` keeps its bare URIs for a different
reason again: the question is which agents carry VIAF identifiers, so the
identifier *is* the content, exactly as with `jazzontology-0020`. And
`europeana-0053` and `-0054` only look like URI enumerations — `edm:datasetName`
binds a literal, so they return names.

### The excluded set is a record, not a bin

Five of the seventeen contested pairs were excluded, and the exclusions are worth
more than the pairs were: each one now carries a written rationale naming the
class of defect, and together they describe the boundary of what the benchmark is
for in a way no inclusion does. `europeana-0011` marks the limit of publication
convention; `europeana-0005` marks unconstrained property traversal; `-0004`,
`-0029` and `-0034` mark the bare enumeration, with `-0004` and `-0034` also
documenting a live data-quality defect in Europeana's own graph that nothing else
in the project records.

This argues for treating exclusion as a first-class, reasoned outcome rather than
a deletion. The practical consequence is that the exclusion *reason* should be a
field rather than free text, so that pairs dropped on the same grounds can be
counted and so that a later round does not re-derive the same judgment from
scratch — which is what happened here, since v10's dismissal comments had already
made this argument twice in prose and nothing downstream could see it.

## Instrument findings

The review form shaped the data in ways worth fixing before the next cohort.

1. **No "not assessed" state.** One team improvised eight "Not reviewed"
   markers into `literal_wording`, and the items went out with default
   dispositions. Add an explicit skip state, and do not emit a disposition for
   skipped items.
2. **No place to propose corrected SPARQL.** One team pasted a complete
   corrected query into `literal_wording` after diagnosing a double `ORDER BY`.
   Of 13 `literal_wording` values, only 3 are the closer verbalization the
   policy intends.
3. **Advice without an actionable rewrite.** Of 29 reviews recommending a
   prompt or input-data improvement, 19 supplied a `preferred_question`. None
   was silent once internal comments are counted, but 8 of the 22
   `prompt_improvement_recommended` reviews left the one field the builder reads
   empty. Require `preferred_question` when the assessment recommends a prompt
   change.
4. **The reasoning is in the field excluded from publication.** 17 reviews carry
   internal comments against 5 public ones, and the internal comments are where
   the substance is — an ambiguity diagnosis on `europeana-0053`, a licence-list
   challenge on `-0054`, the dead item on `-0010`. Adjudication needs them as
   input, which means the round runs on internal data and publishes only its
   conclusions. Decide that deliberately.
5. **Question form was requested and mostly honoured.** 21 of 28 supplied
   wordings are phrased as questions, and 6 of the 10 teams that supplied any
   were fully compliant. The imperatives cluster in two reviewers rather than
   being spread evenly.

## Recommendation

Run a human adjudication round over the 17 contested pairs before any benchmark
update. The round is small enough to be done in one sitting, and 17 cases are
far too few to validate an automatic aggregation rule — several are not wording
disputes at all.

The round needs, per conflict type:

- competing rewrites (4) — a decision, with the option to record both wordings
  as accepted alternatives rather than choosing;
- rewrite against pipeline-kept (6) — a decision plus a record that the point
  was contested, so a lone rewrite cannot pass silently;
- include/exclude (4) — the excluding team's standard, not a pair-by-pair vote;
- assessment only (4, of which 3 remain) — which team's assessment and comment
  stand, remembering that the dissenting comment may be the only record of a
  defect;
- the faithfulness defects (`europeana-0017`, `-0054`, `-0039`) — a SPARQL
  correction or a narrowed question, routed to the correction workflow rather
  than the wording decision.

## Not verified

- That `edm:language` appears only on Europeana aggregations across the whole
  graph. Every sampled subject was one, and the provider aggregation of the
  object inspected in full had none, but the exhaustive query timed out.
- The total number of distinct `edm:rights` values. The count query timed out;
  the 40-value sample is a lower bound.
- Whether `edm:hasType` ever carried `aat:300026656` alongside `dc:date`. The
  present absence is established; the history is not, so `europeana-0039`'s type
  pattern cannot yet be called an error rather than a remodelling casualty. The
  identity of `aat:300026656` as the newspapers concept is confirmed by the
  project owner, not by a retrieval recorded here.
- Whether earlier MusoW judgments outside public benchmark v10 would change the
  MusoW coverage classification. This limitation is carried over unchanged from
  the workshop report.

## Related records

- [`2026-09-16-ipl-workshop.md`](2026-09-16-ipl-workshop.md)
- [`2026-11-10-ipl-round-lessons.md`](2026-11-10-ipl-round-lessons.md)
- [`../REVIEW_POLICY.md`](../REVIEW_POLICY.md)
- [`../SPARQL_CORRECTION_FOLLOW_UP.md`](../SPARQL_CORRECTION_FOLLOW_UP.md)
- [`../OPEN_ISSUES.md`](../OPEN_ISSUES.md)
- [`scripts/analyze_wording_variants.py`](../../scripts/analyze_wording_variants.py)
