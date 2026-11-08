# IPL workshop meta-review decisions

**Date:** 8 November 2026

**Scope:** owner adjudication of the 13 contested pairs identified in
[`2026-11-07-ipl-review-conflict-findings.md`](2026-11-07-ipl-review-conflict-findings.md)

**Status:** decided; not yet applied to the benchmark

## What this record is

The 16 September 2026 workshop produced the first reviews by people other than
the investigator, and with them the first conflicting judgments about the same
pair. The benchmark builder has no rule for choosing between competing canonical
wordings and fails closed on overlapping reviews, so the choice is made here, by
the project owner acting as meta-reviewer.

The meta-reviewer's decision overrides a reviewing team's where they differ.
That authority is recorded deliberately rather than exercised silently: every
override carries a rationale, and a rejected wording is kept as an alternative
rather than deleted, so the reviewers' contributions remain visible in the
provenance. Reviewer wordings are reproduced as submitted; reviewing teams are
labelled Team A–I, consistently across pairs so that per-reviewer patterns stay
legible. The mapping to pseudonymous group identifiers is not reproduced here.

Reviewers' `internal_comment` material is not quoted in this record. Where a
rationale explains a graph or schema quirk, that explanation is the
meta-reviewer's own.

## Decisions at a glance

| | pairs |
|---|---:|
| adjudicated | 17 |
| awaiting decision | 0 |
| include | 12 |
| exclude | 5 |
| SPARQL correction required before the pair re-enters | 4 |
| canonical wording deferred to the post-correction review | 1 |
| flagged as requiring graph context | 6 |

All seventeen contested pairs are decided as of 8 November 2026.

Excluded: europeana-0004, europeana-0005, europeana-0011, europeana-0029,
europeana-0034. Correction required: europeana-0039, europeana-0048,
europeana-0054 and cdec-0006 — queued in
[`../SPARQL_CORRECTION_FOLLOW_UP.md`](../SPARQL_CORRECTION_FOLLOW_UP.md).
Included over an excluding team: europeana-0032, europeana-0049.
Graph context required: europeana-0011, europeana-0019, europeana-0022, europeana-0026, europeana-0049, nfdi4culture-0001.

A blank slot means the decision does not apply to that pair: an excluded pair
needs no canonical wording, and `sparql correction` is blank where no defect is
known.

## Decisions by pair

### europeana-0004

Conflict: competing rewrites

Candidate wordings:

- evidence label : Listing of edm:Agent
- pipeline       : List agents in Europeana
- Team A [included]: Can you list 100 agents from Europeana?
- Team B [included]: List of people or organisations in Europeana
- Team C [included]: (kept pipeline wording)

Decision:

- **include/exclude**: exclude — **reversed 8 November 2026**
- **canonical wording**: — (was "Can you list 100 people or organisations in Europeana?")
- **record as alternative**: none
- **sparql correction**: no
- **graph context required**: no
- **rationale**: Reversed on the standalone-need criterion, on two grounds. It is
  a bare class enumeration with no constraint and no label, so the answer is a
  page of opaque URIs and no natural question has that as its answer. And the
  answer it actually returns is junk: run as written against the live endpoint,
  **100 of the 100** agent URIs are
  `file:///home/jenkins/workspace/download-datasets-run/#…` — build-server
  filesystem paths that leaked into the published graph through an ingestion run
  that resolved relative URIs against local disk. No wording is a faithful
  question for that result set.

### europeana-0011

Conflict: lone rewrite vs kept, include/exclude

Candidate wordings:

- evidence label : List provider proxies
- pipeline       : What are the provider proxies?
- Team B [included]: (kept pipeline wording)
- Team C [excluded]: (kept pipeline wording)
- Team D [included]: “Which proxies are provider proxies in Europeana?”

Decision:

- **include/exclude**: exclude
- **canonical wording**: —
- **record as alternative**: —
- **sparql correction**: —
- **graph context required**: yes: edm:europeanaProxy "false" picks out provider proxies only because Europeana’s ingestion always asserts the property. Verified on the live endpoint: the property takes exactly two plain-literal values, no ore:Proxy lacks it, and even objects with an edm:intermediateProvider have exactly two proxies, so the binary holds. Nothing in EDM guarantees it, and the untyped literal would silently fail against "false"^^xsd:boolean.
- **rationale**: provider proxies is really very far from research questions and hard to explain to a researcher

### europeana-0019

Conflict: competing rewrites

Candidate wordings:

- evidence label : List agent resources from VIAF
- pipeline       : List agent resources from VIAF (up to 10)
- Team E [included]: (kept pipeline wording)
- Team F [included]: (kept pipeline wording)
- Team B [included]: List people or organisation resources from VIAF (up to 10)
- Team C [included]: Which agent resources from VIAF (up to 10) are there?

Decision:

- **canonical wording**: Can you list any ten people or organisations from the graph that have VIAF identifiers?
- **record as alternative**: all
- **sparql correction**: no
- **graph context required**: yes
- **rationale**: Europeana reuses VIAF identifiers alongside other identifiers

### europeana-0022

Conflict: competing rewrites

Candidate wordings:

- evidence label : List objects that reference agents from VIAF
- pipeline       : List objects that reference agents from VIAF
- Team F [included]: (kept pipeline wording)
- Team B [included]: List objects that reference people or organisations from VIAF
- Team C [included]: What are the objects that reference agents from VIAF?

Decision:

- **canonical wording**: Can you list ten objects that reference people or organisations from VIAF?
- **record as alternative**: all
- **sparql correction**: no
- **graph context required**: yes
- **rationale**: —

### europeana-0026

Conflict: lone rewrite vs kept

Candidate wordings:

- evidence label : List objects that have spatial or coverage information
- pipeline       : What objects have spatial or coverage information?
- Team F [included]: What objects have spatial or any information about location ?
- Team B [included]: (kept pipeline wording)
- Team C [included]: (kept pipeline wording)

Decision:

- **canonical wording**: Which objects have a place associated with them?
- **record as alternative**: none
- **sparql correction**: no
- **graph context required**: yes
- **rationale**: the dcterms:spatial|dc:coverage alternation is a workaround for inconsistent legacy metadata. dc:coverage is the unrefined parent covering place, time and jurisdiction; only the ?Place rdf:type edm:Place constraint keeps the query coherent.

### europeana-0032

Conflict: include/exclude

Candidate wordings:

- evidence label : List objects that have type or subject information
- pipeline       : What objects have type or subject information?
- Team B [included]: (kept pipeline wording)
- Team C [excluded]: (kept pipeline wording)

Decision:

- **include/exclude**: include
- **canonical wording**: Which objects have a subject, a keyword or a type associated with them in the graph?
- **record as alternative**: all
- **sparql correction**: no
- **graph context required**: no
- **rationale**: —

### europeana-0034

Conflict: include/exclude

Candidate wordings:

- evidence label : List time period resources that are associated to objects
- pipeline       : What time period resources are available?
- Team B [included]: (kept pipeline wording)
- Team C [excluded]: (kept pipeline wording)

Decision:

- **include/exclude**: exclude — **reversed 8 November 2026**
- **canonical wording**: — (was "Which time periods are recorded in the graph?")
- **record as alternative**: none
- **sparql correction**: no
- **graph context required**: no
- **rationale**: Reversed on the same two grounds as europeana-0004. A bare class
  enumeration returning opaque URIs, and the URIs are largely the same
  `file:///home/jenkins/workspace/download-datasets-run/…` ingestion artefacts,
  most carrying no `skos:prefLabel` at all. Real `edm:TimeSpan` instances do
  exist — Spanish-language period labels from Hispana, for example — but they are
  not what the query returns, so the question as worded cannot be answered
  faithfully from the result.

### europeana-0039

Conflict: lone rewrite vs kept

Candidate wordings:

- evidence label : Europeana Newspapers by Year
- pipeline       : How many newspapers are in Europeana for each decade?
- Team B [included]: (kept pipeline wording)
- Team C [included]: (kept pipeline wording)
- Team D [included]: How many newspapers are in Europeana for each decade that has more than one newspaper?

Decision:

- **canonical wording**: How many newspapers are in Europeana for each decade that has more than one newspaper?
- **record as alternative**: all except evidence label
- **sparql correction**: NEEDED — see SPARQL_CORRECTION_FOLLOW_UP.md
- **graph context required**: no
- **rationale**: —

### europeana-0045

Conflict: lone rewrite vs kept

Candidate wordings:

- evidence label : Distribution of objects per country
- pipeline       : Distribution of objects per country
- Team C [included]: What is the distribution of objects per country?
- Team D [included]: (kept pipeline wording)

Decision:

- **canonical wording**: What is the distribution of objects per country?
- **record as alternative**: all
- **sparql correction**: no
- **graph context required**: no
- **rationale**: —

### europeana-0049

Conflict: include/exclude

Candidate wordings:

- evidence label : Distribution of objects per language of the metadata
- pipeline       : How many objects are in the metadata for each language?
- Team B [included]: (kept pipeline wording)
- Team C [excluded]: (kept pipeline wording)

Decision:

- **include/exclude**: include
- **canonical wording**: For each language, how many objects have metadata in that language?
- **record as alternative**: only evidence, not pipeline
- **sparql correction**: no
- **graph context required**: yes
- **rationale**: edm:language is carried by the edm:EuropeanaAggregation and not by the provider aggregation, so that one triple pattern silently restricts the query to Europeana aggregations. Without that accident the counts would be wrong. The property means the language of the metadata record, not of the object; values are ISO codes including mul.

### europeana-0053

Conflict: competing rewrites

Candidate wordings:

- evidence label : List datasets with video content from the Netherlands
- pipeline       : List datasets with video content from the Netherlands
- Team G [included]: Which datasets with video content from the Netherlands are in the graph?
- Team F [included]: (kept pipeline wording)
- Team B [included]: (kept pipeline wording)
- Team C [included]: Are there datasets with video content from the Netherlands?

Decision:

- **canonical wording**: Which datasets with video content from the Netherlands are in the graph?
- **record as alternative**: pipeline
- **sparql correction**: —
- **graph context required**: no
- **rationale**: It's a select query returning a list - Are there datasets would correspond to an ask query

### europeana-0054

Conflict: lone rewrite vs kept

Candidate wordings:

- evidence label : List datasets with openly licensed content
- pipeline       : List datasets with openly licensed content
- Team F [included]: (kept pipeline wording)
- Team B [included]: (kept pipeline wording)
- Team C [included]: Are there datasets with openly licensed content?

Decision:

- **canonical wording**: List datasets with openly licensed content
- **record as alternative**: Are there datasets with openly licensed content? (not canonical: the query returns a list, so the existence form would be answered by ASK)
- **sparql correction**: NEEDED — see SPARQL_CORRECTION_FOLLOW_UP.md
- **graph context required**: —
- **rationale**: —

### nfdi4culture-0001

Conflict: lone rewrite vs kept

Candidate wordings:

- evidence label : List all research data repositories described in the Culture Knowledge Graph.
- pipeline       : List all research data repositories described in the Culture Knowledge Graph.
- Team H [included]: (kept pipeline wording)
- Team I [included]: Which research data repositories are described in the Culture Knowledge Graph?

Decision:

- **canonical wording**: Which research data repositories are described in the Culture Knowledge Graph?
- **record as alternative**: all
- **sparql correction**: no
- **graph context required**: yes
- **rationale**: the two-type intersection is redundant on current data: all 22 fabio:DataRepository instances are also nfdicore:NFDI_0000123 (“data portal”, a subclass of “website”), while 69 data portals are not repositories. The query’s recall is therefore the coverage of TA4’s curated repository list, not of the graph. SAMPLE(?label) is non-deterministic in principle but harmless today, as all 22 resources carry exactly one label; SAMPLE(?resource) around the grouping key is redundant.


### europeana-0005

Conflict: assessment only

Candidate wordings:

- evidence label : Objects provided to Europeana linking to edm:Place
- pipeline       : What objects provided to Europeana link to a place?
- Team B [included], submitted 2026-09-16: assessment `accepted`, no rewording
- Team F [included], submitted 2026-09-16: assessment `prompt_improvement_recommended`, no rewording
    - an internal comment was supplied (not quoted here)

Decision: **exclude**

Rationale: the query reaches a place through any property at all, which is an
exploration of how places are linked rather than an information need. Its results
include objects whose `dc:creator` is mistakenly a place, so no natural question
describes them faithfully. `europeana-0026` keeps the same need in its "about a
place" sense. The SPARQL is syntactically correct; the modelling is the problem.
Excluding it also settles the assessment disagreement, since a dismissed pair
carries no assessment.

## Assessment-only disagreements

Added 8 November 2026. Four further pairs were judged by two teams that agreed
on the wording — no team proposed a rewording — but differed on
`pipeline_assessment`, and with it on whether a team's comment survives into the
benchmark. A tie-break on submission order was considered and abandoned: it would
have discarded a reviewing team's report that `cdec-0006` does not parse. They
are therefore adjudicated. All four are now decided. `europeana-0005` is
recorded above, excluded for reasons that have nothing to do with the
assessment.

Team letters continue the mapping used above. The working slots are in the
untracked notes file; these blocks record the conflict so the tracked set is
complete.

### cdec-0006

Conflict: assessment only

Candidate wordings:

- evidence label : Luoghi e date di partenza dei convogli e numero dei deportati per località di arresto
- pipeline       : For each convoy, what are the departure places, dates, and the number of deported people per arrest location and Nazi camp?
- Team J [included], submitted 2026-09-16: assessment `accepted`, no rewording
- Team K [included], submitted 2026-09-16: assessment `input_data_improvement_recommended`, no rewording
    - public comment: Double ORDER BY is not allowed; you only need one ORDER BY clause for multiple columns

Decision:

- **include/exclude**: include, after correction
- **pipeline assessment**: `input_data_improvement_recommended` — Team K's
- **canonical wording**: unchanged from the pipeline wording
- **sparql correction**: required, see [`../SPARQL_CORRECTION_FOLLOW_UP.md`](../SPARQL_CORRECTION_FOLLOW_UP.md)
- **rationale**: The wording was never contested; both teams kept the pipeline
  question. Team K's comment is the only record of the defect, so its assessment
  stands over the accepting one. The pair re-enters the benchmark once the
  duplicate `ORDER BY` and the missing `GROUP BY` are corrected.

### europeana-0029

Conflict: assessment only

Candidate wordings:

- evidence label : 
- pipeline       : What are all the concept resources in the dataset?
- Team F [included], submitted 2026-09-16: assessment `prompt_improvement_recommended`, no rewording
    - an internal comment was supplied (not quoted here)
- Team C [included], submitted 2026-09-16: assessment `accepted`, no rewording

Decision:

- **include/exclude**: exclude
- **rationale**: It returns a list of IRIs, not labels. A bare class enumeration
  with no constraint: the answer is a page of opaque concept URIs. As a step
  inside a larger task that is the right output, but as a standalone information
  need there is no natural question whose answer is an identifier list. Excluding
  it overrides **both** teams rather than a minority, which no other decision in
  this round does — one accepted the pair and the other asked for a better
  prompt, so neither proposed dropping it.

### europeana-0048

Conflict: assessment only

Candidate wordings:

- evidence label : Distribution of objects per country and Europeana type
- pipeline       : What is the distribution of objects per country and Europeana type?
- Team F [included], submitted 2026-09-16: assessment `prompt_improvement_recommended`, no rewording
    - public comment: Researcher not aware with EDM may not ask for Europeana type
- Team B [included], submitted 2026-09-16: assessment `accepted`, no rewording

Decision:

- **include/exclude**: SPARQL correction first, then include
- **pipeline assessment**: `input_data_improvement_recommended` — neither team's
  reasoning, but the nearer of the two labels
- **canonical wording**: deferred to the post-correction review, decided with the
  corrected query in hand. It must replace "Europeana type" with "media type":
  `edm:type` takes exactly `IMAGE`, `TEXT`, `SOUND`, `VIDEO` and `3D` on this
  endpoint, so Team F was right about the phrase.
- **sparql correction**: required first, see [`../SPARQL_CORRECTION_FOLLOW_UP.md`](../SPARQL_CORRECTION_FOLLOW_UP.md)
- **rationale**: Neither team's assessment stands. Team F was right that
  "Europeana type" is EDM vocabulary a researcher would not use, but the pair's
  real defect is arithmetic and both teams missed it. `?AggregationProvider` and
  `?Provider` are neither projected nor grouped, so the middle clause only
  multiplies solutions, and `count(*)` counts solutions. Objects that arrived
  through an aggregator carry two aggregations typed `ore:Aggregation` and are
  counted twice: Azerbaijan reports 54 against 27 real objects, while Montenegro
  and Georgia are exact. The distortion varies by country, so it destroys the
  between-country comparison the question exists to make.
## Working copy

The hand-edited working copy, which carries the group identifiers, is
`var/review/analysis/ipl-2026-09-16-meta-review-decisions.md`. It is untracked
and outside the ICF server backup; this document is the durable record.
