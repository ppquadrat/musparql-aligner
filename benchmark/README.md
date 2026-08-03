# Benchmark

This directory stores versioned, human-curated NL–SPARQL benchmark snapshots.
Every record in `benchmark.jsonl` is part of the benchmark and its canonical
natural-language formulation has been confirmed by a human reviewer.

The `vN/` directories are data artifacts. The Python modules alongside them are
maintenance tools for building, updating, auditing, and packaging those
artifacts; they are part of the working repository but are not copied into the
DOI dataset release. Keeping the tools next to the snapshots makes the
reconstruction path visible, while `build_public_release.py` enforces the much
smaller publication boundary.

## Snapshot files

- `benchmark/vN/benchmark.jsonl`
  - the compact scoring dataset
  - one canonical `gold_question` and SPARQL query per record
  - benchmark membership is implicit: every record in this file is included
  - optional public reviewer rationale is stored as `provenance.reviewer_comment`
- `benchmark/vN/included.jsonl`
  - detailed internal curation records for the included pairs
  - preserves generation provenance and `pipeline_assessment`
  - keeps `review.public_comment` and `review.internal_comment` separate
- `benchmark/vN/alternatives.jsonl`
  - public sidecar containing human-accepted non-canonical formulations
  - `accepted_alternatives` contains accepted model outputs and previous canonical questions
  - `literal_formulations` contains reviewer-authored literal descriptions and marks each with
    `source_type: "literal_sparql_wording"`
- `benchmark/vN/linguistic_annotations.jsonl`
  - internal exploratory ratings such as naturalness and pragmatism
  - not part of the public release because the annotation scheme has not been validated
- `benchmark/vN/dismissed.jsonl`
  - candidates excluded during curation; never part of `benchmark.jsonl`
- `benchmark/vN/holdout.jsonl`
  - reviewer-only records withheld from the public benchmark
- `benchmark/vN/manifest.json`
  - snapshot metadata, file inventory, and counts
  - records the release builder and the intended public file set

The version directory is a working snapshot, not a DOI archive. A public release
must be constructed with `build_public_release.py`, which serializes only allowed
fields into `manifest.json`, `benchmark.jsonl`, and `alternatives.jsonl`.
Detailed curation records, exploratory ratings, dismissed candidates, and
holdout records remain internal.

## Pipeline assessment

Pipeline assessment is independent of benchmark membership. It describes the
pre-review formulation process, not the validity of the final canonical pair:

- `accepted`: the presented candidate formulation was acceptable
- `prompt_improvement_recommended`: the canonical pair is valid, but prompt or model behaviour should improve
- `input_data_improvement_recommended`: the canonical pair is valid, but generation inputs should improve
- `not_applicable`: no generated natural-language candidate was assessed, for example a source-authored prompt

Excluded candidates use `benchmark_disposition: "excluded"`; private holdout
records use `benchmark_disposition: "withheld"`. The compact scoring file omits
both fields because its contents are uniformly included and human-confirmed.

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
.venv/bin/python benchmark/normalize_review_comments.py
```

## Build and update

Review decisions follow `schemas/review-decision.schema.json`. Builders reject
unknown disposition and assessment values instead of treating them as included.

Build a snapshot from an initial-review bundle and export:

```bash
.venv/bin/python benchmark/build_benchmark.py \
  --bundle review/review_data.js \
  --reviews review/exports/<review-export>.json \
  --outdir benchmark/vN
```

A comparative-review bundle is constructed from the previous and current
generation outputs. Previous decisions normally come from the latest benchmark
snapshot; an earlier review export may also supply review context. Apply the new
comparative-review decisions to that previous benchmark snapshot:

```bash
.venv/bin/python benchmark/update_benchmark.py \
  --previous-benchmark benchmark/vN \
  --bundle review/review_data.js \
  --reviews review/exports/<comparative-review-export>.json \
  --outdir benchmark/vNext
```

An initial review can also add newly reviewed pairs to an existing snapshot
without comparing two generation runs side by side:

```bash
.venv/bin/python benchmark/update_from_initial_review.py \
  --previous-benchmark benchmark/vN \
  --bundle review/review_data.js \
  --reviews review/exports/<initial-review-export>.json \
  --outdir benchmark/vNext
```

The evaluator reads every record in `benchmark.jsonl`:

```bash
.venv/bin/python evals/evaluate_runs.py \
  --benchmark benchmark/vN \
  --runs runs/<run-id> \
  --baseline runs/<baseline-run-id> \
  --judge-model gpt-5 \
  --out evals/reports/<eval-id>
```

Regenerate all compact scoring files, accepted alternatives, and manifest counts
from the detailed snapshots, auditing each version as it is written:

```bash
.venv/bin/python benchmark/regenerate_snapshots.py
```

Run the snapshot and saved-evaluation consistency audits independently:

```bash
for snapshot in benchmark/v*; do
  .venv/bin/python benchmark/audit_snapshot.py "$snapshot"
done
.venv/bin/python benchmark/audit_eval_reports.py
```

Build a new, empty public-release directory from a validated snapshot:

```bash
.venv/bin/python benchmark/build_public_release.py \
  --snapshot benchmark/v7 \
  --outdir build/public-v7
```

The release builder uses field allowlists and rejects private filesystem paths
and internal review, API-response, and linguistic-annotation fields. It also
writes SHA-256 checksums into the public manifest. Licensing and repository-level
release documentation must still be completed before publishing the directory.
