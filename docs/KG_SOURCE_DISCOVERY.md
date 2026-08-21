# KG source discovery

`musparql-discover-sources` searches for possible repositories, publications,
and documentation associated with a named knowledge graph. Its output is a
non-authoritative research aid. It never edits `catalog/sources.yaml`, a KG seed
file, or any benchmark artifact.

Run all available backends:

```console
musparql-discover-sources --name "Jazz Ontology"
```

Add project context when it helps disambiguate the name, or select backends:

```console
musparql-discover-sources \
  --name "MUSOW Knowledge Graph" \
  --project "Polifonia" \
  --backend github \
  --backend openalex
```

The optional environment variables are:

- `GITHUB_TOKEN`: authenticates GitHub API requests and raises rate limits;
- `OPENALEX_MAILTO`: identifies requests to OpenAlex;
- `BRAVE_API_KEY`: enables Brave web search. Brave is skipped without it.

To retain the evidence for manual review, explicitly save a new JSON report:

```console
musparql-discover-sources \
  --name "Jazz Ontology" \
  --output var/source-discovery/jazz-ontology.json
```

The command refuses non-JSON output names and refuses to overwrite an existing
report. `var/` is ignored by Git. A saved report includes every returned result
within the configured per-query API limit, including low lexical relevance
results, the query that produced each candidate, backend warnings, and search
limitations.

Human review remains mandatory. Inspect candidate authority and relevance,
search separately for omissions, and only then make a distinct, reviewed change
to the source catalogue and KG seed catalogue.
