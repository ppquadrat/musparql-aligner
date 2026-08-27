# Knowledge-Graph Source Discovery Experiment

Dates: 2026-08-22–2026-08-23

## Question

What can the source-discovery script realistically surface for a knowledge
graph, and what review process should Musparql use when applying it to new
graphs?

The first phase compares saved discovery reports for Organs, MEETUPS, MuSOW,
LinkedMusic, and Jazz Ontology with the curated KG seeds, source catalog, and
the queries already extracted into Musparql. It asks three things:

1. Did discovery recover the sources we already knew were valuable?
2. Which known sources did it miss or hide?
3. Which new findings have a realistic chance of containing additional SPARQL
   or competency questions?

The second phase applies the resulting review process to five previously
uncatalogued graphs—ALyrA, Camera dei Deputati, CDEC, NFDI4Culture CKG, and
Europeana—and records what the script, authoritative-source inspection, and
human review contributed at each stage.

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
- New-graph discovery reports:
  - `var/source-discovery/alyra.json`
  - `var/source-discovery/camera-dei-deputati.json`
  - `var/source-discovery/cdec.json`
  - `var/source-discovery/nfdi4culture.json`
  - `var/source-discovery/europeana.json`
- Detailed new-graph reviews:
  - [`2026-08-23-alyra.md`](../graph-discovery/2026-08-23-alyra.md)
  - [`2026-08-23-camera-dei-deputati.md`](../graph-discovery/2026-08-23-camera-dei-deputati.md)
  - [`2026-08-23-cdec.md`](../graph-discovery/2026-08-23-cdec.md)
  - [`2026-08-23-nfdi4culture.md`](../graph-discovery/2026-08-23-nfdi4culture.md)
  - [`2026-08-23-europeana.md`](../graph-discovery/2026-08-23-europeana.md)

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
as its top publication. The paper contains one complete NLQ–SPARQL example in
Appendix A; that example is now retained in a source-faithful curated derivative.
The repository may contain additional examples, so it should be added to the
LinkedMusic source catalog and passed through the normal extraction pipeline.

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

## Follow-up lesson: ALyrA link traversal and mixed-KG publications

The 2026-08-23 ALyrA review exposed two additional failure modes that should be
part of the standard discovery assessment.

First, the discovery report recovered the official TALOS ALyrA page but missed
the main [Zenodo dataset record](https://zenodo.org/records/13371967), even
though the recovered page explicitly directs readers there for the RDF export,
competency questions, and SPARQL examples. The missed page contains all five
primary human-authored question-query pairs. Recovering an authoritative
landing page is therefore not enough: review must traverse its dataset,
download, supplement, repository, and “more information” links and treat those
linked records as first-class recall targets. A result should not be credited
with recovering the linked evidence merely because it found a page that
mentions it.

Second, the later paper [*Creating ontoterminologies for antiquity: workflow,
challenges and solutions*](https://ceur-ws.org/Vol-3990/paper4.pdf) contains
three complete SPARQL examples for three different ontoterminologies:

1. **Ancient Greek Oratory Ontology:** a query for ancient Greek terms and
   natural-language definitions of legal proceedings supervised by the
   Thesmothetai, using graph `http://www.ontologia.fr/OTB/oratory_v.1.0.rdf`.
   The paper cites the dataset at <https://doi.org/10.5281/zenodo.13379144>.
2. **ALyrA:** a query for the essential characteristics defining “hymn” and
   the poetess who composed that type of poem, using graph
   `http://ontologia.fr/OTB/ALyrA_v.1.0.rdf`.
3. **Göbekli Tepe Ontoterminology:** a query for English terms and definitions
   of archaeological concepts, using graph
   `http://www.ontologia.fr/OTB/Gobekli_Tepe_v.1.0.rdf`. The paper cites the
   dataset at <https://doi.org/10.5281/zenodo.13370343>.

A relevant paper can therefore be both a source for the graph under review and
a discovery lead for other graphs. Publication inspection must classify every
query by namespace, graph IRI, described dataset, and surrounding prose before
assigning it to a KG. Bulk extraction of the PDF as an ALyrA source would have
silently misattributed the Oratory and Göbekli Tepe queries. The adopted pattern
is to retain the complete paper for provenance, disable indiscriminate query
extraction when it is mixed-KG, and create a source-faithful per-KG curated
record for the applicable query. The other queries remain recorded discovery
leads until their own KGs are reviewed and approved.

## Five-new-graph extension

The detailed reports remain the evidence record. The table below captures only
the approved source outcome and the principal lesson contributed by each new
graph.

| Graph | Approved query-bearing outcome | Main lesson |
|---|---|---|
| ALyrA | Five Zenodo question–query pairs, one related paper query, and the complete v1.0 RDF/XML dump | Follow authoritative pages to deposits; classify every query in a mixed-KG paper before extraction. |
| Camera dei Deputati | Italian documentation with 21 displayed query blocks, endpoint XML with 22 NL–SPARQL records, and a third-party article with English-described examples | A visible endpoint UI may load a better machine-readable example source; preserve language and version context and deduplicate the rendered page against its backing data. |
| CDEC | Nine Italian NL–SPARQL examples, the current xDams endpoint, and the historical Shoah named graph | Distinguish a current service from the older named graph targeted by its examples; broken ontology documentation does not invalidate the graph IRI. |
| NFDI4Culture CKG | Six retained authored examples: three public domain queries and three richer deep-dive presentation queries | Prepared-query URL fragments and slide links can contain the best evidence; repository SPARQL may instead be maintenance, validation, or generic inspection material. |
| Europeana | Eight documentation examples and 52 console examples preserved; 46 console SELECTs curated while six DESCRIBEs remain source evidence | Trace dynamic interfaces to official JSON/configuration files, and separate complete source preservation from the narrower query forms admitted to the corpus. |

### Lessons added by the five reviews

1. **Traverse authoritative links, not only search results.** Landing pages must
   be followed to versioned deposits, downloads, supplements, repositories,
   prepared-query links, and additional files. ALyrA's most valuable Zenodo
   page was one explicit link away from the page the script recovered.
2. **Inspect how interactive pages obtain their examples.** Camera dei
   Deputati's endpoint loads an official XML file, Europeana's console loads an
   official 52-record JSON file, and NFDI4Culture encodes complete queries in
   prepared-query links. The backing artifact is often more complete and more
   reproducible than the rendered interface.
3. **Preserve the source collection separately from corpus eligibility.** The
   Europeana JSON remains a 52-example source even though the current corpus is
   SELECT-only and its curated derivative contains 46 records. Generic probes,
   DESCRIBEs, synthetic fixtures, maintenance queries, and malformed examples
   should remain auditable without being silently promoted.
4. **Treat identity and currency as query-level questions.** A current endpoint
   can expose examples for an older named graph; a paper can contain several
   ontologies; closely related projects can reuse the same namespace. Graph
   IRI, namespace, endpoint, version, and surrounding prose must be checked for
   each candidate.
5. **Authorship and intent matter more than syntax.** Syntactically valid
   SPARQL can be a generic graph probe, validation routine, application-internal
   operation, or synthetic test. Conversely, a short imperative label in
   Italian or English may be genuine authored natural-language evidence.
6. **Human review is part of the method, not a final formality.** The owner
   supplied graph identity, recognized genuine examples, rejected wrong
   artifact types, approved source boundaries, and decided which ambiguous
   materials merited preservation. The script supplied leads and reproducible
   search evidence, not catalog decisions.
7. **Use parent sources plus curated derivatives when rendering is lossy or a
   source is mixed.** Keep the official page, paper, XML, or JSON as provenance;
   give it `query_role: none` when indiscriminate extraction would be unsafe;
   and create a source-faithful per-KG or SELECT-only derivative as the canonical
   extraction input.

The five new seeds and approved sources were added to the catalog with immutable
seed history and source snapshots. The subsequent explicitly approved pipeline
run also covered the new LinkedMusic paper query. Existing holdout identities
were excluded before execution and model-input construction, and the corpus's
literal `WHERE`-keyword requirement was applied before generation.

### Pipeline outcome

| Graph | New extracted | Excluded: no `WHERE` | Ready for review | Model classified as generated | Executed successfully/empty |
|---|---:|---:|---:|---:|---:|
| ALyrA | 6 | 0 | 6 | 2 | 6 |
| Camera dei Deputati | 46 | 1 | 45 | 45 | 34 |
| CDEC | 9 | 0 | 9 | 3 | 8 |
| Europeana | 54 | 2 | 52 | 52 | 40 |
| LinkedMusic | 1 | 0 | 1 | 1 | 1 |
| NFDI4Culture | 6 | 0 | 6 | 6 | 3 |
| **Total** | **122** | **3** | **119** | **109** | **92** |

All 119 eligible inputs produced schema-valid model outputs with no generation
errors or citation warnings. The remaining alignment modes were nine
paraphrased formulations and one verbatim formulation. Execution success is
reported separately because remote endpoint failures do not determine whether
a source-authored query remains eligible for human review or SPARQL editing.

## Overall result

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
   supplements, and papers; follow authoritative pages to linked deposits and
   additional files; look beyond non-zero cutoffs; identify likely SPARQL or CQ
   locations; classify every query in a mixed-KG publication before assigning
   it; and separate context from extraction candidates.
3. **Human review.** The owner checks the prioritized findings, corrects
   mistaken relevance judgments, obtains inaccessible material where possible,
   and decides which sources should be added.
4. **Catalog and pipeline update.** Add only the approved sources, preserve
   provenance, rerun extraction for the graph, and review newly extracted
   queries for duplicates and suitability.

This experiment adopts the process, not any automatic acceptance threshold.
The immediate approved follow-up was to add `DDMAL/SESEMMI` to LinkedMusic and
rerun the pipeline. Other candidates remain review leads.

The standardized agent-review prompt was tested in the five-new-graph extension
and updated with the lessons above. Its maintained form is in
[`../KG_SOURCE_DISCOVERY.md`](../KG_SOURCE_DISCOVERY.md).
