# Graph discovery reviews

This directory contains the durable, human-reviewed source pre-check for each
knowledge graph considered for addition to Musparql. It is the working record
between a non-authoritative source-discovery run and approved changes to
`catalog/sources.yaml` and `catalog/seeds.yaml`.

Use one file per graph, named `YYYY-MM-DD-<kg-id>.md`. Keep the raw discovery
report under `var/source-discovery/`; `var/` is intentionally ignored, while the
review in this directory records the public evidence, decisions, unresolved
questions, and approved catalog outcome that need to remain in version control.

Each review should:

1. identify the graph, project, aliases, raw report, and human-reviewed identity;
2. compare discovery output with the catalog and current query corpus;
3. inspect authoritative links, publication supplements, repositories, dynamic
   interface backing files, and query permalinks;
4. separate authored examples from generic probes, tests, maintenance queries,
   obsolete versions, and wrong graphs;
5. record exact URLs, evidence locations, overlap, uncertainty, and the human
   decision for every high-priority candidate; and
6. finish with the catalog/seed outcome and state whether extraction and
   execution have or have not been run.

The maintained review prompt and command documentation are in
[`docs/KG_SOURCE_DISCOVERY.md`](../KG_SOURCE_DISCOVERY.md). The method was
developed and tested in the
[`2026-08-22 KG source discovery experiment`](../experiments/2026-08-22-kg-source-discovery-experiment.md).

Current reviews:

- [ALyrA](2026-08-23-alyra.md)
- [Camera dei Deputati](2026-08-23-camera-dei-deputati.md)
- [CDEC](2026-08-23-cdec.md)
- [Europeana](2026-08-23-europeana.md)
- [NFDI4Culture Culture Knowledge Graph](2026-08-23-nfdi4culture.md)
