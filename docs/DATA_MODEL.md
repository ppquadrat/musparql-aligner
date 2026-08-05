# Artifact and data model reference

This is the technical supplement to the readable [workflow](WORKFLOW.md). It
describes the main artifacts and the contracts between stages. JSONL files
contain one JSON object per line.

## Tracked catalog

### `catalog/sources.yaml`

Defines stable `source_id` values. A source records its type, title, and either
an external locator or an explicitly justified local artifact. Supported types
are repository, web document, publication, local document, and derivative.
Derivatives identify their parent sources.

### `catalog/seeds.yaml`

Selects source IDs for each KG and stores operational KG configuration:

- `kg_id` and display name;
- source IDs;
- SPARQL endpoint, authentication mode, and optional graph;
- optional local dump path and format; and
- project-specific notes.

Repository and document lists are hydrated from the source catalogue rather
than duplicated manually.

### `catalog/kgs.jsonl`

One generated catalogue record per KG. It contains endpoint and dump metadata,
resolved source URLs, captured revisions, local snapshot paths, source IDs, and
catalog provenance. This file is tracked because it is a compact, reviewable
description of the collected source state.

### `catalog/curated/Approved_SPARQL_Edits.jsonl`

A tracked, public-safe projection of every approved SPARQL version. It retains
the query identity, immutable base/version hashes, corrected SPARQL, concise
rationale, edit type, and non-private approval source. It deliberately excludes
raw review exports, reviewer notes, service logs, local paths, and execution
details.

Extraction restores missing versions from this archive after preserving any
richer matching local state. A fresh working directory therefore reconstructs
approved query versions without treating the ignored working catalogue as the
only copy.

## Working query catalogue

### `var/queries/kg_queries.jsonl`

One record per extracted query. Important fields include:

- `kg_id`, `query_id`, and `query_label`;
- `sparql_raw`, `sparql_clean`, and normalized hashes;
- source URL, path, commit, and extraction metadata;
- evidence records with stable evidence IDs;
- append-only `sparql_edits`;
- execution and run history pinned to SPARQL version and hash;
- correction and review provenance where applicable.

Version `0` is always `sparql_clean`. Later versions live in `sparql_edits` and
must have increasing integer versions, text, hash, reason, source, and approval
provenance.

The query catalogue is persistent local state and is ignored by Git. Approved
SPARQL versions are additionally projected into the tracked archive above;
execution history and detailed correction provenance remain local.

### `var/queries/sparql_correction_candidates.jsonl`

An automatic triage ledger. A candidate identifies the query and base SPARQL
version, summarizes the observed problem, and records the evidence safe for the
correction workbench. It is not an approved edit.

## Model inputs and outputs

### `var/llm/inputs.jsonl`

Prompt-ready records containing the selected SPARQL version, query identity,
evidence, and schema reference. Holdout and dismissed identities must be removed
before this file is written.

### `var/llm/outputs.jsonl`

Provisional model responses. A successful response contains:

- ranked evidence phrases and evidence IDs;
- proposed `nl_question`;
- an origin mode and cited evidence IDs;
- confidence and rationale; and
- model/request metadata needed for reproducibility.

Malformed or failed responses are written separately and never treated as
benchmark candidates. Model outputs remain separate from the working query
catalogue; frozen runs, review builders, and evaluation tools join them by query
identity without mutating the extracted query records.

## Generation runs

### `var/runs/<run-id>/manifest.json`

A frozen run manifest identifies:

- run ID and creation time;
- copied input and output filenames and SHA-256 hashes;
- prompt, schema, and example hashes;
- requested and returned model information;
- generation parameters and request configuration; and
- optional query and KG catalog snapshots.

Review bundles refer to run IDs and manifests rather than an unversioned output
file whenever possible.

## Review bundles and exports

The browser bundle is generated and ignored. It contains candidate records,
their run provenance, source evidence, SPARQL provenance, and holdout eligibility
information.

A sanitized review export has `kind: non_holdout_review_export` and contains no
holdout entry. Review records can contain a benchmark decision, preferred
question, literal wording, public and internal comments, and copied-review
provenance. Historical interpretive fields remain readable but are not collected
by the current UI.

Sanitized exports belong in `var/review/exports/`. Private or mixed exports are
rejected by agent-facing tools.

## Holdout selectors

### `var/holdout/selectors.jsonl`

Under the identity-visible policy, each record may contain only:

```json
{
  "kg_id": "example-kg",
  "query_id": "example-query",
  "sparql_version": 0,
  "sparql_hash": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
}
```

The version and hash are optional but must appear together. Reviewer decisions,
wording, comments, ratings, timestamps, and provenance are forbidden. Exclusion
is pair-wide, and a selector is invalid when the query identity retains a SPARQL
edit.

The full holdout export is not part of this data model because it belongs to the
separate human-controlled private repository.

## Benchmark snapshot

### `benchmark/vN/benchmark.jsonl`

The scoring dataset. Each record contains a benchmark identity, KG and query
identity, selected SPARQL text/version/hash, one canonical gold question, gold
question provenance, evidence summary, and safe source provenance.

For an edited SPARQL pin, public `sparql_provenance` is an allowlisted
projection. It may retain the edit count, selected version/hash, history digest,
neutral execution status, and stable human-approved edit facts such as the
decision, edit type, rationale, proposal origin, review time, and approved
version/hash. It excludes working correction identifiers and artifacts,
including candidate IDs/digests, review-export hashes, and UI execution-attempt
histories.

### `benchmark/vN/alternatives.jsonl`

Public, human-accepted non-canonical formulations and literal formulations.
Alternatives never replace the single canonical scoring question.

### `benchmark/vN/manifest.json`

Records the snapshot version, build time, counts, input runs and reviews,
sidecar names, and policy assertions. Historical manifests retain the paths that
were true when they were built.

### Ignored internal sidecars

Working snapshots may also have ignored included, dismissed, linguistic, or
holdout partitions. They are not public repository artifacts and are never
copied into a public release. In particular, `included.jsonl` may retain the
complete internal SPARQL correction provenance from the reviewed query record;
the public benchmark and alternatives files receive only the projection above.

## Public release

### `build/public-releases/vN/`

A derived publication package containing only allowlisted fields. It normally
contains sanitized `benchmark.jsonl`, public `alternatives.jsonl`, and a manifest
with checksums. The release builder rejects private fields, internal review
metadata, filesystem paths, credentials, and correction artifacts.

The directory is ignored and may be deleted and rebuilt.

## Automatic evaluation reports

### `var/evals/reports/<eval-id>/`

May contain a manifest, per-record scores, a readable summary, and a judge
cache. Reports compare runs or systems; they do not update benchmark gold data.
