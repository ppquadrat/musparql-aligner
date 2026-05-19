# Benchmark

This directory stores **curated benchmark snapshots** derived from:

- model outputs and their review bundle
- exported human-review judgments
- frozen LLM generation runs in `runs/<run-id>/`

The benchmark is distinct from:

- raw generation artefacts such as `llm_outputs.jsonl`
- reviewer exports in `review/exports/`
- the run snapshot itself

## Structure

- `benchmark/vN/manifest.json`
  - metadata for a benchmark snapshot
  - source files used to build it
  - counts of approved / pending / dismissed items

- `benchmark/vN/benchmark.jsonl`
  - the clean evaluation dataset
  - approved items plus pending items that have a reviewer-supplied gold question
  - one canonical `gold_question` per NL–SPARQL pair

- `benchmark/vN/approved.jsonl`
  - detailed approved records only
  - preserves reviewed model output and provenance

- `benchmark/vN/pending.jsonl`
  - reviewed but not benchmark-approved items
  - typically `needs_prompt_fix` or `needs_data_fix`
  - records are included in `benchmark.jsonl` only when the reviewer supplied a gold question

- `benchmark/vN/dismissed.jsonl`
  - reviewed items explicitly excluded from the benchmark
  - useful for provenance/data-quality inspection, but not semantic scoring
  - can be reused as an exclusion list when building future LLM prompt inputs

## Gold question policy

For each reviewed item:

- if the reviewer supplied a preferred rewrite, use that as `gold_question`
- otherwise, if the model output was approved as-is, use the approved model output as `gold_question`

This keeps a single canonical wording per benchmark item, while preserving provenance about whether that wording came from the reviewer or the model. `benchmark.jsonl` intentionally omits the generated model wording; the detailed `approved.jsonl` and `pending.jsonl` files keep it for audit and review workflows.

## Builder

Build a benchmark snapshot from a review bundle and an exported review file:

```bash
.venv/bin/python benchmark/build_benchmark.py \
  --bundle review/review_data.js \
  --reviews review/exports/musparql-review-830748f26ceb9031.json \
  --outdir benchmark/v1
```

Apply a compare-review export to an existing benchmark snapshot:

```bash
.venv/bin/python benchmark/update_benchmark.py \
  --previous-benchmark benchmark/v1 \
  --bundle review/review_data.js \
  --reviews review/exports/<compare-review-export>.json \
  --outdir benchmark/v2
```

The update routine carries forward unchanged records from the previous benchmark
and replaces only the pairs that received decisions in the compare review.

## Record design

Records in `benchmark.jsonl` are intentionally compact:

- `sparql`
- `gold_question`
- traceability metadata (`query_id`, `query_label`, `kg_id`, source review file)
- benchmark metadata (`benchmark_version`, `benchmark_built_at`, status group)
- light analysis metadata (evidence type summary, review provenance)

The benchmark should be easy to evaluate against, while still traceable back to the reviewed LLM generation run.

In other words, the intended chain is:

```text
runs/<run-id>/ -> review/exports/<review-file>.json -> benchmark/vN/
```

## Automatic evaluation

Use `evals/evaluate_runs.py` to compare frozen prompt/model generation runs against a
benchmark snapshot:

```bash
.venv/bin/python evals/evaluate_runs.py \
  --benchmark benchmark/v2 \
  --runs runs/<baseline-run> runs/<candidate-run> \
  --baseline runs/<baseline-run> \
  --judge-model gpt-5 \
  --out evals/reports/<eval-id>
```

The evaluator scores `benchmark.jsonl`. That file already contains approved
items plus pending items with reviewer-supplied gold questions. Dismissed items
are excluded from semantic scoring.

SPARQL is treated as fixed input. If a run input's SPARQL differs from the
benchmark SPARQL for the same `query_id`, the evaluator reports a deterministic
`sparql_mismatch` warning and skips semantic judge scoring for that item.
