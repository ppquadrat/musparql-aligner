# Source-discovery review: NFDI4Culture Culture Knowledge Graph

- **Knowledge graph:** Culture Knowledge Graph
- **Project/context:** NFDI4Culture; architecture and space management, art and art history, musicology, and performing arts
- **Aliases:** NFDI4Culture KG; Culture Knowledge Graph; CKG; NFDI4Culture Research Information and Research Data Knowledge Graph
- **Discovery report:** [`var/source-discovery/nfdi4culture.json`](../../var/source-discovery/nfdi4culture.json)
- **Review date:** 2026-08-23
- **Authority note:** Human review fixed the intended identity as NFDI4Culture's cross-domain Culture Knowledge Graph for architecture, art history, musicology, performing arts, archives, and audiovisual collections. This review distinguishes the public graph from LinkedMusic datasets that reuse its CTO ontology.

## A. Recall and findings summary

| Graph | Current extracted queries | Existing catalog sources recovered | Curated public sources missed | Prioritized finding |
|---|---:|---|---|---|
| NFDI4Culture CKG | 0 | None for this graph before this review; the catalog only mentioned NFDI4Culture as ontology context for LinkedMusic | Three complete question/query permalinks embedded in an official deep-dive slide deck were not promoted as candidates | Retain the three domain examples from the public page and the three deck queries; exclude four generic inspection queries and ontology-maintenance SPARQL |

This was the strongest discovery run. It recovered the official service page, public query interface, integration guidelines, official ontology repository, and relevant publications. Its main miss was inside publication content: the displayed shortlist did not reveal three complete, authored queries linked from the deep-dive slides.

No existing CKG source exists in the catalogs. The current corpus does contain 13 LinkedMusic records that mention `https://nfdi4culture.de/ontology/`; those queries run against LinkedMusic named graphs and are schema reuse, not extracted CKG queries.

## B. Prioritized review

### Likely sources of new SPARQL or competency questions

1. **Official public query page — highest priority.** It exposes three domain-specific authored examples for research data repositories, research data portals, and project-partner/Wikidata mappings. These are retained. Its four additional prepared queries—triple count, types, instance counts per type, and properties used for types—are generic graph-inspection operations without useful benchmark NL and are excluded.
2. **Official “Culture Knowledge Graph deep dive” deck — highest priority.** Three pages link complete queries with natural-language questions: Gregorovius letters versus RISM musical sources (page 22), Epidat people with Wikidata federation (page 26), and community standards in data portals (page 39). These are substantially richer than the metric examples.
3. **Repository query material — exclude.** `docs/queries.md` duplicates the four rejected generic queries. The files under `src/sparql/` are maintenance, validation, and ontology-quality-control routines rather than NL examples.

### Publication supplements

The complete file inventories of the relevant Zenodo records were inspected. No separate supplementary query files were deposited: each slide record contains its main PDF only, and the papers contain PDF/XML publication files. Full document text searches covered `SPARQL`, `SELECT`, `ASK`, `CONSTRUCT`, `DESCRIBE`, `PREFIX`, “competency question(s)”, and `CQ`.

- [Culture Knowledge Graph deep dive](https://zenodo.org/records/18292744): pages 22, 26, and 39 contain the three query links above. Relevant pages were visually checked.
- [Data Integration into the NFDI4Culture Knowledge Graph](https://zenodo.org/records/18816017): architecture/context and a repeated Gregorovius link; no separate supplement and no additional unique complete query found.
- [Data integration into the NFDI4Culture knowledge graph (2023)](https://zenodo.org/records/7715510), [2024 follow-up](https://zenodo.org/records/10698301), and the Base4NFDI abstract: endpoint and architecture context, but no complete CQ collection or additional query body.

### Provenance or schema context

- The endpoint `https://nfdi4culture.de/sparql` returned SPARQL JSON for a minimal read-only query on 2026-08-23.
- The official ontology repository reports CTO 3.0.0, built on NFDIcore v3 and BFO 2020, with current ontology namespace `https://nfdi4culture.de/ontology/` and NFDI ontology namespace `https://nfdi.fiz-karlsruhe.de/ontology/`.
- Earlier publications and reports describe older NFDICO/first-version designs. Version information must accompany any source-derived query.

### Duplicates, synthetic material, obsolete sources, or wrong ontologies

- The 21 files under the repository's `src/sparql/` are ontology-development reports, quality-control checks, and violation queries. They are authored tooling, not user examples or competency questions; exclude them from benchmark extraction.
- The four repository metric queries duplicate the four rejected public graph-inspection examples and are not imported.
- LinkedMusic's CTO-using queries are not duplicates of the CKG endpoint examples, but they create schema-level overlap that should be disclosed.
- Older NFDICO material may be superseded by CTO 3.0.0 and NFDIcore v3.

### Inaccessible or unresolved candidates

- The three `/go/` links resolve to endpoint-query permalinks and are public, but their stability depends on project redirects and the query interface. Preserve both the redirect URL and its resolved query when curating.
- Query result stability and version compatibility still need per-query execution review.

## C. High-priority candidates

| Candidate and exact URL | Evidence inspected | Where SPARQL/CQs occur | Authorship | Likely corpus overlap | Recommendation |
|---|---|---|---|---|---|
| [Official public CKG query page](https://nfdi4culture.de/resources/knowledge-graph.html) | Full page, prepared-query list, and encoded permalink targets | Three retained domain queries and four rejected generic inspection queries | Official authored examples | No CKG query overlap | **Added** as provenance for the three retained examples |
| [Gregorovius/RISM query](https://nfdi4culture.de/go/kg-gregorovius-rism-example-musical-sources-letters) | Deep-dive deck page 22 and resolved query permalink | Natural-language question plus complete `SELECT` | Official authored demonstration | No exact corpus overlap; domain overlap with music-oriented LinkedMusic queries | **Added** to the curated six-example source |
| [Epidat/Wikidata persons query](https://nfdi4culture.de/go/kg-query-epidat-wikidata-persons) | Deep-dive deck page 26 and resolved query permalink | Natural-language formulation plus federated complete query | Official authored demonstration | No exact corpus overlap | **Added** to the curated source; federation flagged |
| [Community standards/data portals query](https://nfdi4culture.de/go/kg-query-community-standards-data-portals) | Deep-dive deck page 39 and resolved query permalink | Natural-language question plus complete `SELECT` | Official authored demonstration | No exact corpus overlap | **Added** to the curated source |
| [Culture Knowledge Graph deep dive](https://zenodo.org/records/18292744) | Full 50-page PDF, file inventory, text search, and visual inspection of query-bearing pages | Pages 22, 26, and 39; no separate supplement | Official project presentation | Contains the three links above, so it is their provenance rather than four new sources | **Added** as provenance/container source |
| [Official ontology repository](https://github.com/ISE-FIZKarlsruhe/nfdi4culture) | Complete repository archive, README, ontology version, `docs/queries.md`, and all 21 `src/sparql` files | Four rejected generic examples in `docs/queries.md`; maintenance/QC queries under `src/sparql/` | Mixed official examples and internal ontology tooling | Generic duplication; maintenance files unsuitable | **Do not add** to the CKG source set |
| [CKG service page](https://nfdi4culture.de/services/details/culture-knowledge-graph.html) and [integration guideline](https://docs.nfdi4culture.de/ta5-research-data-culture-knowledge-graph/en) | Service identity, scope, architecture, and integration documentation | No additional unique complete CQ set located | Official documentation | Not applicable | **Do not add separately**; no unique queries |

## D. Assessment

The discovery run was highly useful: it found nearly all major authoritative source classes and correctly identified the official repository and publications. Its weakness was document-depth recall. Three of the best authored questions were inside an official presentation and would have been missed by reviewing titles and landing pages alone.

The approved source set contains six authored examples: three domain queries linked from the public page and three richer queries linked from the official deep-dive presentation. Their exact permalink-derived SPARQL and NL formulations are preserved in a curated derivative because ordinary HTTP snapshots lose URL fragments. The four generic metric/inspection queries and all repository maintenance material are excluded. The seed uses `https://nfdi4culture.de/sparql`; `lod.xdams.org` belongs to CDEC. Query extraction and execution have not been run. Human review remains necessary for federation, result stability, ontology-version alignment, and semantic suitability.
