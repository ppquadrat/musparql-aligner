# Musparql workflow

Musparql turns existing SPARQL and scattered human-language evidence into a
reviewed NL–SPARQL benchmark. It is a curation workflow, not a query-generation
system: every query identity and intended information need must originate in a
repository, paper, guide, or other identified human source. A benchmark may
select a human-approved corrected SPARQL version, but that correction repairs
the retained source query; it does not introduce a new information need.

The main workflow is natural-language review: recover, align, or formulate a
faithful expression of the information need embodied in an existing query, then
have a human decide whether the pair belongs in the benchmark. SPARQL correction
is an exceptional supporting track for source queries that are malformed,
incomplete, parameterized, or otherwise erroneous. Corrections should be rare,
source-grounded, append-only, and limited to making the existing information
need usable; version `0` always preserves the source text.

This document explains the flow. Use the [pipeline runbook](PIPELINE_RUNBOOK.md)
for commands and [DATA_MODEL.md](DATA_MODEL.md) for field-level detail.

## Where determinism, models, and human judgment enter

Musparql does not use an LLM throughout the pipeline. Most artifact construction
is deterministic: given the same tracked inputs, configuration, code, and local
working records, it produces the same identities, normalized records, prompt
payloads, version pins, benchmark files, audits, and release package. The parts
that depend on external systems, an LLM, or human judgment remain explicit and
carry separate provenance.

| Workflow activity | Kind | Role and boundary |
| --- | --- | --- |
| Capture sources | Deterministic after capture | Fetching can observe changing external content, but tracked snapshots provide the stable input used downstream. |
| Extract queries and evidence | Deterministic | Parsers and enrichment rules transform configured source artifacts. They do not invent benchmark SPARQL or gold questions. |
| Normalize and version SPARQL; build model inputs | Deterministic | Code selects retained versions, calculates hashes, packages evidence, and applies dismissal and holdout filters. |
| Execute queries | External observation | The execution procedure and recorded pins are deterministic, but results can change with endpoints, datasets, federation, and time. Execution never decides benchmark inclusion. |
| Triage possible SPARQL problems | Deterministic rules | Static checks and execution diagnostics select cases for investigation. An optional LLM may propose a correction, but only a human can approve an append-only SPARQL version. |
| Align evidence and formulate questions | **LLM-assisted** | The LLM selects, aligns, paraphrases, or proposes provisional natural-language wording. Its output is non-deterministic model output, not gold data. |
| Automatically evaluate provisional generation runs | Mixed diagnostics | Exact comparisons and optional LLM-based judges compare batches of proposed questions during pipeline experiments. These diagnostics do not evaluate the published benchmark or write review decisions or gold data. |
| Review candidates and corrections | **Human judgment** | A reviewer decides semantic adequacy, canonical wording, inclusion or dismissal, correction approval, and eligible holdout selection. |
| Build, audit, and package a benchmark | Deterministic | Validated human decisions and pinned artifacts are transformed into versioned benchmark files and an allowlisted public release. |

In short, LLMs may **propose language, suggest a SPARQL correction, or provide an
evaluation signal**. They do not create benchmark information needs, alter
source SPARQL, approve corrections, select gold pairs, choose canonical
questions, or publish a benchmark decision. Those boundaries are enforced by
separate artifacts rather than relying only on prompt instructions.

## 1. Collect sources

`catalog/sources.yaml` assigns stable IDs to repositories, web pages, papers,
local documents, and curated derivatives. `catalog/seeds.yaml` selects the
sources used for each knowledge graph and records endpoint or dump information.

The catalogue build captures text under `catalog/snapshots/` and writes the
tracked KG overview to `catalog/kgs.jsonl`. Curated source material lives under
`catalog/curated/`; reference PDFs live under `catalog/pdfs/`.

Tracked catalog files answer: what source did we use, where did it come from,
and what local artifact represents it?

## 2. Extract queries and evidence

Query extraction reads configured repositories, source snapshots, curated
files, and papers. It writes the working query catalogue to
`var/queries/kg_queries.jsonl`.

Each query receives a stable identity, the original text, normalized SPARQL,
source provenance, and any nearby comments or descriptions. Evidence enrichment
then adds competency questions, paper passages, documentation text, and other
candidate descriptions without treating them as gold wording.

The working query catalogue is intentionally ignored. Once holdout identities
exist, publishing the full candidate pool could reveal or redistribute selected
pairs.

## 3. Execute and diagnose queries

Execution uses the endpoint or local dump configured for each KG. It records the
exact SPARQL version and hash, status, result count, timing, and a bounded error
summary.

Execution helps distinguish a working query from a broken endpoint, unsupported
feature, federation failure, or query error. It does not decide whether a pair
belongs in the benchmark. A query may execute successfully but express an
administrative operation; a meaningful query may fail because a remote service
is unavailable.

Automatic triage writes correction candidates to
`var/queries/sparql_correction_candidates.jsonl`. Approved corrections are
append-only versions; version `0` remains the normalized source query. See the
[SPARQL editing policy](SPARQL_EDITING_POLICY.md).

## 4. Build model inputs

The input builder selects the effective SPARQL version, packages the available
evidence, and excludes dismissed or held-out identities before constructing a
prompt record.

Every agent-facing builder requires an explicit holdout choice:

- `--holdout-selectors var/holdout/selectors.jsonl` for the identity-visible
  policy;
- `--no-holdout` only while no holdout identities exist; or
- `--holdout-filtered-upstream` for a genuinely human-only upstream filter.

Prompt inputs are written to `var/llm/inputs.jsonl`.

## 5. Align evidence or formulate a question

The model first tries to use language already present in the sources. Its output
records whether the proposed question is:

- a direct source formulation;
- aligned or paraphrased from cited evidence; or
- generated because no suitable source wording was selected.

Outputs include evidence citations and confidence metadata and are written to
`var/llm/`. They remain provisional. Model fluency is not evidence of semantic
correctness.

## 6. Freeze a generation run

A run snapshot under `var/runs/<run-id>/` copies the prompt inputs, outputs,
prompt, schema, examples, optional query catalogue, and configuration hashes.
This makes later comparison possible even after working files change.

Runs are persistent local state, not build products: deleting them can remove
the provenance needed to understand a review or benchmark update.

## 7. Automatically evaluate generation changes

Automatic evaluation compares generation runs and writes reports under
`var/evals/reports/`. These reports are diagnostics for pipeline development;
they do not update the benchmark.

When an experiment changes evidence, prompts, models, extraction, or execution,
record the conclusion in `docs/experiments/`. Preserve the reasoning and the run
IDs, not every transient judge cache in Git.

## 8. Human review

Initial review assesses candidates that have not previously received a reviewer
decision. Comparative review places an earlier and current candidate side by
side after a pipeline change.

Review determines:

- whether the query expresses a meaningful information need;
- whether the proposed question matches the graph pattern;
- whether a canonical rewrite is needed;
- whether a problem belongs to the model, prompt, source data, or SPARQL; and
- whether an eligible pair is selected for the private holdout.

Sanitized non-holdout exports go to `var/review/exports/`. Full private holdout
exports go to the separate human-controlled private repository and must never be
handled by an agent. Under the identity-visible policy, the review UI also
merges explicitly touched membership changes into an existing selector (or an
empty set) and downloads a replacement that the human verifies and places at
`var/holdout/selectors.jsonl`. See the [review policy](REVIEW_POLICY.md),
[review runbook](REVIEW_RUNBOOK.md), and [holdout security policy](HOLDOUT_SECURITY.md).

## 9. Build or update a benchmark snapshot

Benchmark commands combine a generation run with a validated non-holdout review
export. They create a new `benchmark/vN/` directory rather than mutating an old
version.

The scoring file contains one canonical question and one selected SPARQL version
per pair. Public alternatives and provenance are kept in sidecars. Internal
partitions and reviewer-only material are ignored and excluded from public
release packages.

## 10. Audit and package a public release

Snapshot audits verify identity, SPARQL version pins, review provenance,
partition completeness, and the absence of forbidden public fields.

The public-release command then derives an allowlisted package under
`build/public-releases/vN/`. That package contains sanitized benchmark data,
approved public alternatives, a release manifest, and checksums. It excludes
holdouts, internal annotations, raw reviews, model request metadata, local
paths, and working query state.

`build/` is disposable: a public release must be reproducible from the reviewed
snapshot and release command. `benchmark/` is versioned project data and is not
disposable.

## What may change what

The dependency direction is deliberate:

```text
catalog sources
    → working query catalogue
    → model inputs and generation runs
    → human review
    → versioned benchmark snapshot
    → sanitized public release
```

Automatic evaluation may inform a new experiment, but it must not write review
decisions or benchmark gold data. SPARQL corrections may add a new version, but
they must not overwrite the source query. Private holdout annotations never
flow back into this repository.
