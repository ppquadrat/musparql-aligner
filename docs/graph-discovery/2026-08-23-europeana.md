# Source-discovery review: Europeana

- **Knowledge graph:** Europeana
- **Project/context:** European cultural-heritage aggregation platform; cultural heritage and museology
- **Aliases:** Europeana; Europeana SPARQL API; Europeana Data Model graph; EDM
- **Discovery report:** [`var/source-discovery/europeana.json`](../../var/source-discovery/europeana.json)
- **Review date:** 2026-08-23
- **Authority note:** Human review fixed the intended identity as Europeana's cross-domain cultural-heritage graph and supplied the official SPARQL API documentation as the source-of-truth lead. The supplied context described more than 50 million digitised objects from more than 3,700 institutions; these scale figures are contextual and may change. Endpoint URLs in the documentation require a currentness correction.

## A. Recall and findings summary

| Graph | Current extracted queries | Existing catalog sources recovered | Curated public sources missed | Prioritized finding |
|---|---:|---|---|---|
| Europeana | 0 | None: Europeana is absent from both catalogs | The run missed the supplied official SPARQL documentation, current console/backend endpoint, and official EDM documentation | Curate the official examples, but seed `https://api.europeana.eu/sparql`; the documented `https://sparql.europeana.eu/` now redirects to a UI |

This was the weakest discovery run. It returned no web-document candidates and failed to recover the exact authoritative page supplied by the human reviewer. Its publication results were broad policy/history/API context, while its displayed repositories were unrelated to authored Europeana SPARQL examples.

There are no existing curated Europeana sources to recover. All important missed sources are public and should reasonably have been found.

## B. Prioritized review

### Likely sources of new SPARQL or competency questions

1. **Official SPARQL API documentation — highest priority.** Its “More examples” section contains eight labelled authored SELECT examples. Topics include providers, Italian datasets, eighteenth-century French objects, agents, places, Getty AAT concepts, and federated Wikidata/Sophox lookups. Examples 6 and 8 were recovered from the official percent-encoded query-service links because Confluence mangles their rendered code blocks.
2. **Current SPARQL console — highest priority.** Its official `europeana/sparql-ui` source file contains 52 titled examples: 46 SELECT and six DESCRIBE queries across general, media, contextual-resource, and provenance categories. Several overlap the documentation; all are preserved, with only the SELECT subset eligible for the current corpus.

### Provenance or schema context

- [Official EDM documentation](https://pro.europeana.eu/index.php/page/edm-documentation) is the authoritative model-documentation hub; the current [web mapping guidelines](https://europeana.atlassian.net/wiki/spaces/EF/pages/987791389) describe the implemented subset.
- The SPARQL documentation names `https://sparql.europeana.eu/`. On 2026-08-23 this URL returned a redirect to the console at `https://api.europeana.eu/console/sparql/`, not a SPARQL JSON response.
- The console's public configuration identifies `https://api.europeana.eu/sparql` as its backend. That backend returned SPARQL JSON for a minimal read-only query on 2026-08-23.
- Important model namespaces include EDM `http://www.europeana.eu/schemas/edm/`, ORE, Dublin Core terms/elements, SKOS, and FOAF.

### Duplicates, synthetic material, obsolete sources, or wrong ontologies

- No Europeana endpoint or EDM namespace occurs in the current 408-query corpus, so official examples are prospectively new.
- The inspected [`europeana/Europeana-Cloud`](https://github.com/europeana/Europeana-Cloud) archive contained no SPARQL text and is storage/backend infrastructure, not a query source. Reject it for this purpose.
- CrowdHeritage, awesome-list, and unrelated cultural-heritage repositories in the discovery shortlist are not Europeana query corpora.
- Older Europeana API publications provide historical context but may predate the current endpoint and model implementation. They should not override current operational documentation.

### Publication supplements

The discovery shortlist did not identify a publication whose accessible supporting materials contained a Europeana competency-question list or complete SPARQL collection. Its publications were therefore treated as context rather than query sources. The absence of web-document results, despite the official query page supplied in advance, is a more important recall failure than the lack of publication supplements.

### Inaccessible or unresolved candidates

- Exact overlap between the eight documentation examples and 52 console examples remains to be established during extraction and deduplication.
- The official documentation's examples must be tested against the current backend. Confluence mangles the rendered code for examples 6 and 8, but their source text has been recovered from the official encoded query-service links; both still depend on external services and historical endpoint assumptions.
- Europeana's APIs may impose authentication, rate, or acceptable-use requirements that are not established by a successful minimal query; review current service terms before sustained extraction.

## C. High-priority candidates

| Candidate and exact URL | Evidence inspected | Where SPARQL/CQs occur | Authorship | Likely corpus overlap | Recommendation |
|---|---|---|---|---|---|
| [Official SPARQL API documentation](https://europeana.atlassian.net/wiki/spaces/EF/pages/2385870903/SPARQL%2BAPI%2BDocumentation) | Rendered page, Confluence storage representation, and percent-encoded query-service links | “More examples” section; eight labelled SELECT examples | Official authored API examples | None in current corpus; overlaps the console set | **Added** with a source-faithful curated derivative; retain syntax and semantic review |
| [Current SPARQL backend](https://api.europeana.eu/sparql) | Console configuration plus successful minimal SPARQL JSON response on 2026-08-23 | Query service, not a corpus source | Official operational endpoint | Not applicable | **Added as the endpoint seed** |
| [SPARQL console](https://api.europeana.eu/console/sparql/), [query-samples JSON](https://raw.githubusercontent.com/europeana/sparql-ui/master/europeana/query-samples.json), and [public console configuration](https://api.europeana.eu/console/sparql/custom-config.json) | Live Examples dialog, official repository source, all 52 records, and configured backend URI | 46 SELECT and six DESCRIBE examples in four categories | Official authored console examples | Partly overlaps documentation; exact deduplication pending | **Added**; preserve all 52 as evidence and curate the 46 SELECT records |
| [Europeana Data Model documentation](https://pro.europeana.eu/index.php/page/edm-documentation) | Documentation hub, current implementation guidance, schema and mapping links | Schema examples rather than a SPARQL/CQ corpus | Official | Not applicable | **Retain as review context**; no separate query source needed |
| [Current EDM mapping guidelines](https://europeana.atlassian.net/wiki/spaces/EF/pages/987791389) | Current web guidance and implemented-model caveats | Data-model examples; no dedicated CQ list | Official | Not applicable | **Retain as review context**; no separate query source needed |
| [`europeana/Europeana-Cloud`](https://github.com/europeana/Europeana-Cloud) | Complete repository archive; no SPARQL text located | None | Official infrastructure, wrong artifact type | None | **Reject as query source** |

The former URL `https://sparql.europeana.eu/` should be retained only as a documented historical/redirect URL. It should not be configured as a programmatic endpoint unless behavior changes and is reverified.

## D. Assessment

The discovery run added little reliable value for Europeana: it missed the known official query page, current endpoint configuration, and core EDM documentation, while ranking broad publications and unrelated repositories. Manual authoritative-source review produced a compact catalog set—the official query documentation, console JSON, curated derivatives, and working endpoint seed—while retaining EDM documentation as schema context rather than a separate query source.

Human judgment is required on endpoint/API terms, console/documentation overlap, federation-dependent queries, dated or defective examples, and benchmark semantics. Successful SPARQL execution alone would not settle those questions. The sources, curated derivatives, endpoint seed, source snapshots, and KG metadata have now been added; query extraction and execution have not been run.
