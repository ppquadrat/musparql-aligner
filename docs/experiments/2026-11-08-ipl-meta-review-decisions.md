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
| adjudicated | 13 |
| awaiting decision (assessment only) | 4 |
| include (overriding an exclusion) | 3 |
| exclude | 1 |
| SPARQL correction required | 3 |
| flagged as requiring graph context | 6 |

Included: europeana-0032, europeana-0034, europeana-0049. Excluded: europeana-0011. Correction required:
europeana-0039, europeana-0054 and cdec-0006 — queued in
[`../SPARQL_CORRECTION_FOLLOW_UP.md`](../SPARQL_CORRECTION_FOLLOW_UP.md).
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

- **canonical wording**: Can you list 100 people or organisations in Europeana?
- **record as alternative**: all
- **sparql correction**: no
- **graph context required**: no
- **rationale**: —

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

- **include/exclude**: include
- **canonical wording**: Which time periods are recorded in the graph?
- **record as alternative**: all except evidence label
- **sparql correction**: no
- **graph context required**: no
- **rationale**: —

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


## Still to decide: assessment-only disagreements

Added 8 November 2026. Four further pairs were judged by two teams that agreed
on the wording — no team proposed a rewording — but differed on
`pipeline_assessment`, and with it on whether a team's comment survives into the
benchmark. A tie-break on submission order was considered and abandoned: it would
have discarded a reviewing team's report that `cdec-0006` does not parse. They
are therefore adjudicated, and are not yet decided.

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

Decision: **pending** — SPARQL correction required regardless, see [`../SPARQL_CORRECTION_FOLLOW_UP.md`](../SPARQL_CORRECTION_FOLLOW_UP.md)

### europeana-0005

Conflict: assessment only

Candidate wordings:

- evidence label : Objects provided to Europeana linking to edm:Place
- pipeline       : What objects provided to Europeana link to a place?
- Team B [included], submitted 2026-09-16: assessment `accepted`, no rewording
- Team F [included], submitted 2026-09-16: assessment `prompt_improvement_recommended`, no rewording
    - an internal comment was supplied (not quoted here)

Decision: **pending**

### europeana-0029

Conflict: assessment only

Candidate wordings:

- evidence label : 
- pipeline       : What are all the concept resources in the dataset?
- Team F [included], submitted 2026-09-16: assessment `prompt_improvement_recommended`, no rewording
    - an internal comment was supplied (not quoted here)
- Team C [included], submitted 2026-09-16: assessment `accepted`, no rewording

Decision: **pending**

### europeana-0048

Conflict: assessment only

Candidate wordings:

- evidence label : Distribution of objects per country and Europeana type
- pipeline       : What is the distribution of objects per country and Europeana type?
- Team F [included], submitted 2026-09-16: assessment `prompt_improvement_recommended`, no rewording
    - public comment: Researcher not aware with EDM may not ask for Europeana type
- Team B [included], submitted 2026-09-16: assessment `accepted`, no rewording

Decision: **pending**
## Working copy

The hand-edited working copy, which carries the group identifiers, is
`var/review/analysis/ipl-2026-09-16-meta-review-decisions.md`. It is untracked
and outside the ICF server backup; this document is the durable record.
