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
report. `var/` is ignored by Git. By default a report shows a ranked shortlist
of at most five repositories, five publications, and five web documents. It
records each candidate's original API rank and query, explains its lexical
ranking signals, and reports how many unique results were omitted.

Use `--expanded` only when the shortlist is insufficient and every returned
unique candidate is worth inspecting:

```console
musparql-discover-sources --name "Jazz Ontology" --expanded
```

Human review remains mandatory. Inspect candidate authority and relevance,
search separately for omissions, and only then make a distinct, reviewed change
to the source catalogue and KG seed catalogue.
