# KG Query Execution Baseline

Date: 2026-08-03

## Question

What is the current execution status of the extracted queries for the remaining
knowledge graphs, and which failures are evidence for a SPARQL correction rather
than an endpoint, dataset, extraction, or runtime-context problem?

## Execution

- Execution and classification commit: `19c3bae`
- Remote KGs were run with `.venv/bin/python run_queries.py --kg-id <kg-id>`.
- Jazz Ontology was run locally against the retained
  `dumps/jazzontology.ttl` dataset selected by its seed configuration.
- MusicBO has no extracted query records, so there was nothing to execute.
- Every recorded execution resolves to the exact retained SPARQL version and
  hash that was run.

The runner now distinguishes standalone SPARQL from queries that require
SPARQL Anything, a local file, or runtime parameter substitution. These are
reported as `skipped_local_query` with a `skip_reason`, not as failed standalone
SPARQL.

## Results

| KG | Extracted | Executed | `ok` | `empty` | Error | Non-standalone |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Organs | 11 | 11 | 0 | 10 | 1 | 0 |
| MEETUPS | 31 | 30 | 5 | 25 | 0 | 1 |
| MUSOW | 94 | 92 | 90 | 2 | 0 | 2 |
| Jazz Ontology | 24 | 9 | 8 | 1 | 0 | 15 |
| MusicBO | 0 | 0 | 0 | 0 | 0 | 0 |
| **Total** | **160** | **142** | **103** | **38** | **1** | **18** |

The 18 non-standalone records comprise one MEETUPS and two MUSOW SPARQL
Anything/local-file queries, plus 15 Jazz Ontology Python `%s`/`%i`
templates. The five MEETUPS `ok` results are aggregate rows whose totals are
zero; they do not demonstrate that the intended data is present.

## Dataset and endpoint caveats

- The configured Organs endpoint was unavailable. The runner used
  `https://data.open.ac.uk/sparql`, which is not an Organs-specific dataset.
  The ten empty results therefore do not validate the queries against the
  intended KG.
- The configured MEETUPS endpoint was unavailable and its fallback graph probe
  exposed `http://data.open.ac.uk/musow/`. The MEETUPS outcomes are therefore
  not reliable semantic evidence for the intended KG.
- MUSOW returned meaningful MUSOW resources and labels from its configured
  endpoint; its results are the strongest remote baseline in this run.
- Jazz Ontology was evaluated against the retained local dump. Its 15 templates
  need parameter values before they can be executed and should not be treated
  as malformed standalone queries.

These distinctions should be preserved in paper statistics. A single
"percentage executable" would conflate query validity with endpoint
availability, target-dataset presence, and required runtime context.

## Correction triage

| Query or group | Observation | Classification / next action |
| --- | --- | --- |
| `organs-0011` | The source query uses `xsd:Boolean(?isFirstProject) = True` and returned HTTP 400. A probe using `FILTER(?isFirstProject = true)` was accepted by the fallback endpoint, although it returned no rows. | Demonstrated SPARQL correction. The source remains version 0; the approved correction is retained as version 1. |
| `jazzontology-0001`–`jazzontology-0015` | Queries contain Python runtime placeholders. | Extraction/runtime-context issue, not fifteen SPARQL corrections. |
| `meetups-0005`, `musow-0093`, `musow-0094` | Queries depend on SPARQL Anything and local CSV input. | Execute in the required local environment; do not edit merely to make them remote-endpoint queries. |
| `musow-0064`, `musow-0092`, `jazzontology-0020` | Standalone queries executed but returned empty. Variants probed for `musow-0064` also returned empty. | No demonstrated syntax correction. Investigate data and graph context before proposing an edit. |

The baseline run itself did not add corrected SPARQL. In the subsequent release
preparation step, `organs-0011` version 1 was added with
`FILTER(?isFirstProject = true)` and executed successfully against the fallback
endpoint with an empty result. Endpoint/dataset repair and runtime-aware
extraction should still precede broad query editing.

## Benchmark decision

This experiment records execution evidence only. It does not update the
benchmark; corrected versions still require the normal provenance and human
review workflow.
