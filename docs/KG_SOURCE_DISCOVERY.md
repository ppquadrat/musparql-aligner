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

Alternative names can be supplied explicitly. Each alias creates recorded
search queries and contributes to ranking; it is not promoted into a catalogue:

```console
musparql-discover-sources \
  --name "Jazz Ontology" \
  --project "Dig That Lick" \
  --alias "DTL1000" \
  --alias "100 years of jazz"
```

At most five distinct aliases are accepted so one invocation cannot silently
expand into an unbounded number of API requests.

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
report. When `--output` is supplied, the command prints only a short saved-file
confirmation; add `--also-print` to display the report as well. `var/` is
ignored by Git. By default a report shows a ranked shortlist
of at most five repositories, five publications, and five web documents. It
records each candidate's original API rank and query, explains its lexical
ranking signals, and reports how many unique results were omitted.

Publication copies are grouped conservatively when they share a DOI or OpenAlex
identifier, or when a publication result has the same normalized long title.
The candidate retains every discovered location and labels title-based grouping
as probable rather than authoritative. Weakly similar titles remain separate.

Use `--expanded` only when the shortlist is insufficient and every returned
unique candidate is worth inspecting:

```console
musparql-discover-sources --name "Jazz Ontology" --expanded
```

Human review remains mandatory. Inspect candidate authority and relevance,
search separately for omissions, and only then make a distinct, reviewed change
to the source catalogue and KG seed catalogue.

## Provisional agent-review prompt

The following prompt formalizes lessons from the initial source-discovery
evaluation. It has not yet been tested on a new graph and should be revised in
response to future runs. Publication supplements are inspected by the agent;
the discovery script does not automate that step.

```text
Review the saved source-discovery report(s) for:

Knowledge graph: [KG NAME]
Project/context: [PROJECT]
Aliases: [ALIASES]
Report: [PATH TO REPORT]

Treat the report as non-authoritative discovery output. Compare it with:

- catalog/seeds.yaml
- catalog/sources.yaml
- the current extracted query corpus
- any existing public documentation for this KG

Do not promote sources, edit the catalog, or rerun extraction until I approve
the recommendations.

Perform the following review:

1. Curated-source recall
   - Identify which existing curated sources the report recovered.
   - Identify which curated public sources it missed or hid below a cutoff.
   - Distinguish genuinely undiscoverable local derivatives from public sources
     that the process should reasonably have found.

2. Publication supplements
   - For every relevant publication, inspect its landing page and all accessible
     supplementary or supporting material, appendices, additional files, data
     deposits, and linked repositories.
   - Give this special attention because competency-question lists and complete
     SPARQL examples are often placed in supplements rather than the main paper.
   - Search accessible documents for SPARQL, SELECT, ASK, CONSTRUCT, DESCRIBE,
     PREFIX, competency question, competency questions, and CQ.
   - Record the exact public URL and the section, page, or file containing the
     evidence. Report mentioned but inaccessible supplements explicitly.

3. Repository inspection
   - Look beyond the displayed shortlist whenever omission counts are non-zero.
   - Inspect promising repositories for .rq and .sparql files, embedded query
     strings, examples, notebooks, documentation, and CQ lists.
   - Distinguish authored examples from synthetic tests, validation fixtures,
     generated probes, and application-internal queries.

4. Identity, currency, and novelty
   - Check that each result concerns this KG rather than a similarly named or
     related ontology.
   - Note whether a repository or ontology appears obsolete, superseded, or
     tied to an older graph version.
   - Compare prospective queries and CQs with the existing corpus so that
     rediscovered or duplicated material is not presented as new.

Return:

A. A compact table containing the graph, current extracted-query count, curated
   sources recovered, curated public sources missed, and prioritized findings.

B. A prioritized list divided into likely sources of new SPARQL or CQs;
   provenance or schema context; duplicates, synthetic material, obsolete
   sources, or wrong ontologies; and inaccessible or unresolved candidates.

C. For every high-priority candidate, give its exact URL, the evidence inspected,
   where SPARQL or CQs occur, whether the material is authored, synthetic, or
   uncertain, likely overlap with the corpus, and a recommendation to add,
   investigate manually, or reject it.

D. A short assessment of how useful the discovery run was, what it missed and
   should have found, and which conclusions require human judgment.

Do not equate syntactically valid SPARQL with benchmark suitability. Preserve
uncertainty and flag semantic, provenance, version, and authorship judgments for
human confirmation.
```
