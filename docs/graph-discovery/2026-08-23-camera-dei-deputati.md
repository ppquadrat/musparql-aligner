# Source-discovery review: Camera dei Deputati

- **Knowledge graph:** Camera dei Deputati, Italy
- **Project/context:** Italian Chamber of Deputies; economies and finances, law, and sociology
- **Aliases:** Camera dei Deputati; dati.camera.it; Camera open data; OCD
- **Discovery report:** [`var/source-discovery/camera-dei-deputati.json`](../../var/source-discovery/camera-dei-deputati.json)
- **Review date:** 2026-08-23
- **Authority note:** Human review fixed the intended identity as the Italian Camera dei Deputati activity graph and supplied `https://dati.camera.it/sparql` as its endpoint; discovery rankings are not authoritative.

## A. Recall and findings summary

| Graph | Current extracted queries | Existing catalog sources recovered | Curated public sources missed | Prioritized finding |
|---|---:|---|---|---|
| Camera dei Deputati | 0 | None: no Camera entry existed before this review | The official technical-documentation page and the endpoint-backed examples XML | Seed the working official endpoint; add both official example sources and the three-example Sparna article; treat large third-party query collections separately |

The discovery run found a relevant Sparna/Sparnatural article and several repositories, but omitted the official query-bearing documentation despite the human input naming the endpoint and noting examples. Forty-seven publications, ten repositories, and six web candidates were hidden below display cutoffs, so the displayed shortlist was not sufficient.

No existing catalog source could be recovered because this graph is new to both catalogs. The official public pages were discoverable and should have ranked above derived research projects and topical publications.

## B. Prioritized review

### Likely sources of new SPARQL or competency questions

1. **Official semantic representation and documentation — highest priority.** Its `SPARQL` / `Query examples` section contains 21 displayed `SELECT` blocks with execution links, representing 20 distinct intents because the first bibliography formulation is repeated. Subjects include parliamentary members and offices, groups, acts and signatories, votes, governments, and debates. The second bibliography block appears syntactically incomplete at `?s ?classificazione .`; it requires manual rejection or correction rather than automatic import.
2. **Official endpoint examples XML — highest priority.** The endpoint interface loads 22 Italian natural-language/SPARQL pairs from `https://dati.camera.it/ocd/dump/custom_endpoint/sparql.xml`. These substantially overlap the documentation but include updated legislature variants and an additional debate query, so both official sources must be retained and deduplicated.
3. **Sparna article — medium priority.** The article describes three visual examples in English. Their SPARQL is displayed in screenshots, so they require source-faithful transcription rather than ordinary HTML extraction.
4. **`italyParlR` — deferred.** Its source contains parameterized SPARQL and useful adjacent English function documentation, but the queries are application-internal and combine Camera and Senato concerns.
5. **`italianparliament-mcp` — deferred.** The archive contains substantial SPARQL mixed across user-facing tools, documentation, helpers, and tests, and combines Camera and Senato. Volume is not evidence of benchmark suitability.

### Provenance or schema context

- The official portal is [dati.camera.it](https://dati.camera.it/), with ontology base `http://dati.camera.it/ocd/`.
- The [official ontology page](https://dati.camera.it/ocd-ontologia-della-camera-dei-deputati) links the current ontology RDF at [classi.rdf](http://dati.camera.it/ocd/classi.rdf) and states CC BY 4.0 terms.
- The endpoint `https://dati.camera.it/sparql` returned SPARQL JSON for a minimal read-only query on 2026-08-23.
- Portal counts and update dates are live operational metadata and should be captured with a retrieval date rather than treated as permanent facts.

### Duplicates, synthetic material, obsolete sources, or wrong ontologies

- The official examples have no overlap with the current corpus at the KG or namespace level; `dati.camera.it` does not occur in the 408 extracted queries. There is, however, at least one internal near-duplicate in the official list.
- ParliamentRAG is a derived Neo4j graph with its own schema. It uses Camera data but is not the Camera RDF graph and should be rejected as a seed/query source for this KG.
- Repository tests, generated requests, query builders, and application-internal templates must not be promoted merely because they are valid SPARQL.
- Historical GitHub issues and old parliamentary analysis scripts may target earlier data/model states; retain only as low-priority provenance leads.

### Inaccessible or unresolved candidates

- The dynamic Sparnatural interface may expose more examples than its static article. Those should be captured and attributed manually if they prove materially different from the 21 official examples.
- Each official example still needs execution and answer-shape validation against the current endpoint before extraction.

## C. High-priority candidates

| Candidate and exact URL | Evidence inspected | Where SPARQL/CQs occur | Authorship | Likely corpus overlap | Recommendation |
|---|---|---|---|---|---|
| [Official semantic representation and documentation](https://dati.camera.it/ocd-rappresentazione-semantica-e-documentazione) | Full page and all displayed code blocks, including links beyond the initially visible shortlist | `SPARQL` / `Query examples`; 21 displayed `SELECT` blocks representing 20 intents | Official authored examples; one duplicated bibliography block appears malformed | No Camera overlap; one internal duplicate | **Added** as a canonical source; filter and deduplicate during extraction |
| [Official endpoint](https://dati.camera.it/sparql) | Minimal read-only endpoint response on 2026-08-23 | Query service rather than a corpus source | Official service | Not applicable | **Added as the KG seed endpoint** |
| [Official endpoint examples XML](https://dati.camera.it/ocd/dump/custom_endpoint/sparql.xml) | Complete XML loaded dynamically by the endpoint interface | 22 `<query>` elements pairing Italian formulations with SPARQL | Official authored examples | Substantial versioned overlap with the documentation; two additional intents | **Added** as a canonical source |
| [Official ontology page](https://dati.camera.it/ocd-ontologia-della-camera-dei-deputati) and [ontology RDF](http://dati.camera.it/ocd/classi.rdf) | Namespace, ontology description, RDF link, and license | Schema context; no CQ list | Official | Not applicable | **Do not add separately**; legitimate but unnecessary for current query collection |
| [Sparnatural article](https://www.sparna.fr/en/posts/dati-camera-it-s-sparnatural-instance-a-query-builder-for-the-italian-chamber-of-deputies/) | Article and its three English-described visual-query examples | Three example sections; SPARQL is displayed in screenshots | Third-party authored examples built for the official graph | Likely topical overlap with official examples; exact comparison still needed | **Added** as a canonical source for later curated transcription |
| [`paride92/italyParlR`](https://github.com/paride92/italyParlR) | Complete repository archive; 13 SPARQL-bearing files | Parameterized R query-building functions | Authored but application-internal and mixed Camera/Senato | No exact corpus overlap; likely overlap with official parliamentary topics | **Investigate**, do not bulk-import |
| [`ondata/italianparliament-mcp`](https://github.com/ondata/italianparliament-mcp) | Complete repository archive; SPARQL-bearing source, docs, and tests | Application templates and examples throughout the project | Mixed authored application logic, docs, and tests | High risk of internal duplication and Camera/Senato mixing | **Investigate as a separate source type**, not an initial source |

## D. Assessment

The run found useful third-party leads but missed the strongest, obvious official source. Its broad publication results were mostly about parliamentary data or downstream research rather than sources of authored Camera queries. Inspecting beyond the displayed repository cutoff confirmed substantial query material, but also showed why repository hit counts are misleading: much of it is parameterized, mixed-source, synthetic, or internal.

The approved initial catalog consists of the official endpoint seed, the Italian query documentation, the endpoint-backed examples XML, and the Sparna article. The ontology page/RDF were not added separately, and `italyParlR` and `italianparliament-mcp` remain deferred. Human review is still needed for the malformed/duplicate official blocks, translations of the Italian formulations, semantic suitability, and overlap among versioned examples. The seed and sources have been cataloged, but query extraction and execution have not been run.
