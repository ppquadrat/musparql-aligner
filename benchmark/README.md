# Benchmark

This directory stores **curated benchmark snapshots** derived from:

- model outputs and their review bundle
- exported human-review judgments
- frozen generation runs in `runs/<run-id>/`

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
  - approved benchmark items only
  - one record per NL–SPARQL pair

- `benchmark/vN/pending.jsonl`
  - reviewed but not benchmark-approved items
  - typically `needs_prompt_fix` or `needs_data_fix`

## Gold question policy

For each reviewed item:

- if the reviewer supplied a preferred rewrite, use that as `gold_question`
- otherwise, if the model output was approved as-is, use the approved model output as `gold_question`

This keeps a single canonical wording per benchmark item, while preserving provenance about whether that wording came from the reviewer or the model.

## Builder

Build a benchmark snapshot from a review bundle and an exported review file:

```bash
.venv/bin/python benchmark/build_benchmark.py \
  --bundle review/review_data.js \
  --reviews review/exports/musparql-review-830748f26ceb9031.json \
  --outdir benchmark/v1
```

## Record design

Benchmark items are intentionally compact:

- `sparql`
- `gold_question`
- traceability metadata (`query_id`, `query_label`, `kg_id`, source review file)
- light analysis metadata (model origin mode, evidence type summary, review provenance)

The benchmark should be easy to evaluate against, while still traceable back to the reviewed generation run.

In other words, the intended chain is:

```text
runs/<run-id>/ -> review/exports/<review-file>.json -> benchmark/vN/
```
