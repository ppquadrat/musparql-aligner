# Benchmark

This directory stores versioned, human-curated NL–SPARQL benchmark snapshots.
Every record in `benchmark.jsonl` is part of the benchmark and its canonical
natural-language formulation has been confirmed by a human reviewer.

The tracked portion of each `vN/` directory is a public data artifact. Local
working snapshots may additionally contain ignored internal files used for
curation and reconstruction. Commands under `scripts/benchmark/` build, update,
audit, and package those artifacts; the public-release builder enforces the
smaller DOI/publication boundary.

## Snapshot files

- `benchmark/vN/benchmark.jsonl`
  - the compact scoring dataset
  - one canonical `gold_question` and SPARQL query per record
  - `sparql_version` and `sparql_hash` identify the retained query text
  - benchmark membership is implicit: every record in this file is included
  - optional public reviewer rationale is stored as `provenance.reviewer_comment`
- `benchmark/vN/included.jsonl`
  - ignored local detailed curation records for the included pairs
  - preserves generation provenance and `pipeline_assessment`
  - keeps `review.public_comment` and `review.internal_comment` separate
- `benchmark/vN/alternatives.jsonl`
  - public sidecar containing human-accepted non-canonical formulations
  - `accepted_alternatives` contains accepted model outputs and previous canonical questions
  - `literal_formulations` contains reviewer-authored literal descriptions and marks each with
    `source_type: "literal_sparql_wording"`
- `benchmark/vN/linguistic_annotations.jsonl`
  - ignored local exploratory ratings such as naturalness and pragmatism
  - not part of the public release because the annotation scheme has not been validated
- `benchmark/vN/dismissed.jsonl`
  - ignored local candidates excluded during curation; never part of `benchmark.jsonl`
- `benchmark/vN/manifest.json`
  - snapshot metadata, file inventory, and counts
  - records the release builder and the intended public file set

The local version directory may be a working snapshot, but its tracked tree
contains no internal sidecars. A DOI/public release
must be constructed with `scripts.benchmark.build_public_release`, which serializes only allowed
fields into `manifest.json`, `benchmark.jsonl`, and `alternatives.jsonl`.
Detailed curation records, exploratory ratings, and dismissed candidates remain
outside the release. Private holdout annotations remain outside the repository
entirely; see the [holdout security overview](../docs/HOLDOUT_SECURITY.md).

## Pipeline assessment

Pipeline assessment is independent of benchmark membership. It describes the
pre-review formulation process, not the validity of the final canonical pair:

- `accepted`: the presented candidate formulation was acceptable
- `prompt_improvement_recommended`: the canonical pair is valid, but prompt or model behaviour should improve
- `input_data_improvement_recommended`: the canonical pair is valid, but generation inputs should improve
- `not_applicable`: no generated natural-language candidate was assessed, for example a source-authored prompt

Excluded candidates use `benchmark_disposition: "excluded"`. The compact
scoring file omits this field because its contents are uniformly included and
human-confirmed. Public benchmark tools reject private holdout records rather
than storing them in a snapshot partition.

Generated formulations associated with an improvement recommendation are not
published as accepted alternatives. Literal formulations are published only in
the explicitly named `literal_formulations` array.

## Canonical-question policy

For each included pair:

- use the reviewer-preferred formulation when supplied;
- otherwise use the human-accepted model formulation;
- for curated source prompts, retain the source-authored question.

Alternative accepted wordings remain in `alternatives.jsonl`; exploratory
linguistic ratings remain in the internal annotation file.

Reviewer comments are not alternative formulations. Public comments explain
semantic or wording decisions and are copied into compact benchmark provenance.
Internal comments are working notes and are excluded by the public release
allowlist. Literal formulations remain exclusively in `literal_formulations`.
The normalization tool removes a legacy `Literal: ...` note line only when it
matches the dedicated `literal_wording` value. Other legacy note text defaults
to `internal_comment` and requires an explicit later review before publication:

```bash
.venv/bin/python -m scripts.benchmark.normalize_review_comments
```

## Build and update

Review decisions follow `schemas/review-decision.schema.json`. Builders reject
unknown disposition and assessment values instead of treating them as included.

Build a snapshot from an initial-review bundle and export:

```bash
.venv/bin/python -m scripts.benchmark.build_benchmark \
  --bundle review/review_data.js \
  --reviews var/review/exports/<non-holdout-review-export>.json \
  --outdir benchmark/vN
```

A comparative-review bundle is constructed from the previous and current
generation outputs. Previous decisions normally come from the latest benchmark
snapshot; an earlier review export may also supply review context. Apply the new
comparative-review decisions to that previous benchmark snapshot:

```bash
.venv/bin/python -m scripts.benchmark.update_benchmark \
  --previous-benchmark benchmark/vN \
  --bundle review/review_data.js \
  --reviews var/review/exports/<non-holdout-comparative-export>.json \
  --outdir benchmark/vNext
```

An initial review can also add newly reviewed pairs to an existing snapshot
without comparing two generation runs side by side:

```bash
.venv/bin/python -m scripts.benchmark.update_from_initial_review \
  --previous-benchmark benchmark/vN \
  --bundle review/review_data.js \
  --reviews var/review/exports/<non-holdout-initial-export>.json \
  --outdir benchmark/vNext
```

The evaluator reads every record in `benchmark.jsonl`:

```bash
.venv/bin/python -m scripts.evals.evaluate_runs \
  --benchmark benchmark/vN \
  --runs var/runs/<run-id> \
  --baseline var/runs/<baseline-run-id> \
  --judge-model gpt-5 \
  --out var/evals/reports/<eval-id>
```

New working snapshots and public releases (release schema `1.1`) retain `sparql_version` and
`sparql_hash`. Evaluation rejects version/hash mismatches. For snapshots that
predate these fields, a missing historical query ID may be mapped only to a
unique normalized-SPARQL match within the same KG; the score records both the
benchmark `query_id` and resolved `run_query_id`.

Regenerate all compact scoring files, accepted alternatives, and manifest counts
from the detailed snapshots, auditing each version as it is written:

```bash
.venv/bin/python -m scripts.benchmark.regenerate_snapshots
```

Run the snapshot and saved-evaluation consistency audits independently:

```bash
for snapshot in benchmark/v*; do
  .venv/bin/python -m scripts.benchmark.audit_snapshot "$snapshot"
done
.venv/bin/python -m scripts.benchmark.audit_eval_reports
```

In a clean public clone, the snapshot audit validates only the tracked compact
benchmark and alternatives because the three ignored working files are absent.
If any working file is present, all three are required and the full provenance
audit runs. Snapshot update commands deliberately fail when `included.jsonl` is
absent; compact public data is not a lossless source for curation provenance.

Build a new, empty public-release directory from a validated snapshot:

```bash
.venv/bin/python -m scripts.benchmark.build_public_release \
  --snapshot benchmark/v8 \
  --outdir build/public-releases/v8
```

The release builder uses field allowlists and rejects private filesystem paths
and internal review, API-response, and linguistic-annotation fields. It also
writes SHA-256 checksums into the public manifest. Licensing and repository-level
release documentation must still be completed before publishing the directory.

`benchmark/v8` is the first snapshot in which every query is explicitly pinned
to a retained `sparql_version` and `sparql_hash`. Rebuild its adjudicated Organs
questions, canonical LinkedMusic identities, and latest-version selections from
v7 with:

```bash
.venv/bin/python -m scripts.migrations.migrate_benchmark_v8
.venv/bin/python -m scripts.benchmark.audit_snapshot benchmark/v8
```

The v8 manifest summarizes the execution observations available for its 100
selected SPARQL versions at build time. Full observations and histories remain
in `var/queries/kg_queries.jsonl`; they are deliberately not duplicated into the public
NL–SPARQL release records.
