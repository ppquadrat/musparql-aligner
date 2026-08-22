# Knowledge-Graph Source Discovery Evaluation

Date: 2026-08-22

## Question

What can the source-discovery script realistically surface for a knowledge
graph, and what review process should Musparql use when applying it to new
graphs?

The evaluation compares saved discovery reports for Organs, MEETUPS, MuSOW,
LinkedMusic, and Jazz Ontology with the curated KG seeds, source catalog, and
the queries already extracted into Musparql. It asks three things:

1. Did discovery recover the sources we already knew were valuable?
2. Which known sources did it miss or hide?
3. Which new findings have a realistic chance of containing additional SPARQL
   or competency questions?

## Materials

- Implementation commit before the catalog update: `0986139`
- Seeds: `catalog/seeds.yaml`
- Sources: `catalog/sources.yaml`
- Extracted query ledger: `var/queries/kg_queries.jsonl`
- Discovery reports:
  - `var/source-discovery/organs.json`
  - `var/source-discovery/meetups.json`
  - `var/source-discovery/musow.json`
  - `var/source-discovery/linkedmusic.json`
  - `var/source-discovery/jazz-ontology-grouped-aliases.json`

Discovery reports are non-authoritative search results. “Found” below means
that a useful source appeared in the saved shortlist unless explicitly stated
otherwise. This is stricter than merely having been returned somewhere in the
search backend's unshown result set.

## Comparison with the curated corpus

<table>
  <colgroup>
    <col width="8%">
    <col width="9%">
    <col width="23%">
    <col width="23%">
    <col width="37%">
  </colgroup>
  <thead>
    <tr><th>Graph</th><th>Queries extracted</th><th>Curated sources recovered</th><th>Curated sources not recovered</th><th>New findings worth reviewing</th></tr>
  </thead>
  <tbody>
    <tr><td>Organs</td><td>11</td><td>Knowledge-graph repository; ontology README</td><td>Local curated derivative (not independently discoverable)</td><td><code>Polifonia-Corpus</code> and its Organs Zenodo metadata are useful context, but unlikely to add query pairs.</td></tr>
    <tr><td>MEETUPS</td><td>31</td><td>Knowledge-graph repository; useful-query page; 2023 SEMMES paper; 2024 ESWC paper</td><td>None of the four substantive public sources</td><td><code>meetups-ontology</code> and <code>meetups_pilot</code> may add schema or provenance, although the publications point to the already-curated query material.</td></tr>
    <tr><td>MuSOW</td><td>94</td><td>Licence-experiments repository</td><td><code>registry-data</code> repository; exact project page</td><td><code>musow-pipeline</code>, <code>registry_app</code>, and possibly CLEF merit a targeted search for embedded SPARQL; authored CQs are less likely.</td></tr>
    <tr><td>LinkedMusic</td><td>70</td><td>Data-lake and queries repositories; project website; OpenReview SESEMMI paper; integrating-databases article</td><td>Sample-query and prefix wikis; public query spreadsheet; sustainable-archiving paper; local derivatives</td><td>The 2026 ACM SESEMMI paper and the cutoff-hidden <code>DDMAL/SESEMMI</code> repository are the strongest new leads.</td></tr>
    <tr><td>Jazz Ontology</td><td>24</td><td>Dig That Lick repository; 2022 Jazz Ontology paper</td><td>The open paper supplement, despite being linked as publisher supplementary material</td><td>The separate <code>ppquadrat/JazzOntology</code> repository and DTL1000 dataset page improve coverage, but do not currently look likely to add query pairs.</td></tr>
  </tbody>
</table>

The totals are counts in the working extraction ledger at comparison time.
Rediscovering a source that already accounts for many queries demonstrates
recall, but does not make the result novel.

## Six findings

### 1. The script generally rediscovers the principal public sources

The strongest result is recall of the obvious query-bearing material. It found
the Organs and MEETUPS knowledge-graph repositories, the MEETUPS query page and
papers, the MuSOW licence experiment, the main LinkedMusic repositories and
website, and the Jazz Ontology paper and Dig That Lick repository. This gives a
useful baseline for applying the method to an unfamiliar graph: its first page
usually identifies the central project ecosystem.

Recall is not complete. MuSOW's `registry-data` repository—the source of most
of its 94 extracted queries—did not appear in the saved shortlist. Several
specialized LinkedMusic wiki, spreadsheet, and publication sources were also
absent. Search discovery therefore cannot replace seed knowledge or a curated
catalog.

### 2. Names and aliases determine whether the result set is usable

The first Jazz Ontology search was overwhelmed by philosophical writing about
the ontology of jazz and by IBM Jazz software. Adding the project name “Dig
That Lick” and the aliases `DTL1000` and “100 years of jazz” transformed the
result: the final shortlist recovered the correct paper and repositories and
found the relevant DTL1000 dataset page.

This is a reusable lesson for new graphs. Discovery should begin with the KG's
name, but project names, dataset names, acronyms, and known aliases must be
added when the name is generic or overloaded. Alias selection is itself a
small research task and cannot be inferred reliably from lexical ranking alone.

### 3. A high-ranked result is often contextual rather than query-bearing

Organs illustrates the distinction. The script found the Polifonia Corpus,
Zenodo metadata for the Organs pilot, project pages, and press material. These
are relevant and may improve provenance or explain the graph, but they do not
look likely to add SPARQL or competency questions beyond the already-curated
knowledge-graph repository and ontology documentation.

The same applies to many deliverables and portal pages returned for MEETUPS and
MuSOW. They are useful source leads, not automatic extraction inputs. A reviewer
must ask whether a candidate contains actual queries, explicit CQs, links to
query files, or only a general description of the project.

### 4. Plausible results still require semantic and provenance judgment

Three borderline cases show why. SESEMMI's test queries are valid and sometimes
useful SPARQL, but they are synthetic fixtures rather than automatically suitable
benchmark candidates. The search also surfaced the Linked Jazz ontology, which
is distinct from the Jazz Ontology even though the latter implements some
Linked Jazz relationships. Finally, the older MEETUPS ontology repository may
have been overtaken by the knowledge graph's newer development. All three are
relevant search hits; a human must still decide whether each is authored,
current, in scope, and novel.

### 5. The shortlist cutoff can hide the best new implementation lead

The LinkedMusic report found 19 GitHub repositories but displayed only five.
`DDMAL/SESEMMI` was among the 14 omitted results. It was recovered by noticing
the non-zero omission count and replaying the report's recorded GitHub query
without the five-result cutoff. The generic README did not identify the project
well; the GitHub description did: “Front-end tool that uses agentic LLMs to
query LinkedMusic.”

The saved report did directly surface the associated 2026 ACM paper,
[Facilitating Access to LinkedMusic with SESEMMI](https://doi.org/10.1145/3815723.3815727),
as its top publication. The paper contains one complete NLQ–SPARQL example,
transcribed separately by the owner. The repository may contain additional
examples, so it should be added to the LinkedMusic source catalog and passed
through the normal extraction pipeline.

This case distinguishes backend recall from shortlist recall: the search found
the repository, but the default report view effectively hid it. Omission counts
are therefore review signals, not decorative statistics.

### 6. Jazz Ontology shows both the value and the noise ceiling

With project context and aliases, the script found the curated 2022 paper,
`ppquadrat/DigThatLick`, the separate `ppquadrat/JazzOntology` repository, and
the DTL1000 data deposit. It also returned an enhanced Linked Jazz ontology,
workshop materials, a modern solo-analysis toolkit, and many false positives.

The separate ontology repository and dataset landing page are reasonable
catalog-completeness candidates, but do not currently promise new query pairs.
More importantly, the script missed the paper supplement even though it is
openly available as the publisher's
[supplementary PDF](https://ars.els-cdn.com/content/image/1-s2.0-S1570826822000245-mmc1.pdf).
The public file is byte-identical to the local curated copy. It should therefore
be considered discoverable; the miss illustrates a common weakness because
supplementary attachments often have opaque filenames and poor search indexing.
Publication candidates should prompt an explicit check of their landing pages
for supplementary material.

## Overall evaluation

The findings are a useful starting point for an assistant. They record
repeatable searches across repositories, publications, and web pages, group
some duplicates, and expose the hidden result tail through omission counts.
For a new graph, this is better grounding than an unrecorded general web search.

The process cannot be automated end to end. Relevance is not evidence
that a source is authoritative, query-bearing, novel, semantically compatible
with the graph, or suitable for a benchmark. Conversely, a generic README, an
unexpected name, a result below the cutoff, or a paper supplement can conceal
the best lead. Assistant inspection can reduce the candidate set, but a human
must check the prioritization and decide which sources enter the catalog.

## Adopted source-discovery process

For new graphs, use the following review loop:

1. **Run the discovery script.** Supply the KG name and project context; add
   aliases when the name is ambiguous. Save the report so searches, ranks,
   omissions, and provenance remain inspectable.
2. **Assistant review and prioritization.** Compare the report with any existing
   seeds, sources, and query corpus. Inspect promising repositories, publication
   supplements, and papers; look beyond non-zero cutoffs; identify likely SPARQL
   or CQ locations; and separate context from extraction candidates.
3. **Human review.** The owner checks the prioritized findings, corrects
   mistaken relevance judgments, obtains inaccessible material where possible,
   and decides which sources should be added.
4. **Catalog and pipeline update.** Add only the approved sources, preserve
   provenance, rerun extraction for the graph, and review newly extracted
   queries for duplicates and suitability.

This experiment adopts the process, not any automatic acceptance threshold.
The immediate approved follow-up was to add `DDMAL/SESEMMI` to LinkedMusic and
rerun the pipeline. Other candidates remain review leads.

Future runs will test a standardized agent-review prompt that explicitly checks
publication supplements, since these often contain competency questions and
SPARQL examples omitted from the main paper.

## Approved follow-up result

`DDMAL/SESEMMI` was added as `sesemmi-linkedmusic-repository`, and the
LinkedMusic pipeline was rerun. The normal corpus grew from 70 to 248 queries:
the original 70 plus 178 examples from `llm-service/app/graph/examples.py`.

The first pass also extracted 11 plausible queries from synthetic tests and
assigned them ordinary query IDs, showing that valid SPARQL is not enough to
establish benchmark provenance. SESEMMI test files are now excluded by default,
with an explicit diagnostic opt-in.

The pipeline does not yet recover the natural-language questions paired with
the 178 Python examples, so they still need source-aware pairing and review.
This reinforces the central result: extraction can surface a valuable candidate
set, but source semantics and human judgment determine what belongs in
Musparql.
