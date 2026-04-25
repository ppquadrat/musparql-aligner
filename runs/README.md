# Runs

This directory stores **frozen generation runs** that are worth preserving.

A run is an immutable generation artefact. It is distinct from:

- reviewer judgments in `review/exports/`
- benchmark snapshots in `benchmark/vN/`

The intended relationship is:

```text
run -> review(s) -> benchmark snapshot
```

## What belongs in `runs/`

Capture a run when it is:

- used for manual review
- used to build a benchmark
- used for prompt/model comparison
- used in reporting or later analysis

Do **not** capture scratch/debugging probes that were never reviewed and do not affect decisions.

## Recommended structure

Each run gets its own directory:

```text
runs/<run-id>/
```

Typical contents:

- `manifest.json`
- `llm_inputs.jsonl`
- `llm_outputs.jsonl`
- `llm_outputs.errors.jsonl` (if present)
- `prompt.txt`
- `schema.json`
- `examples.jsonl` (if used)
- optional source snapshots such as `kgs.jsonl` and `kg_queries.jsonl`

## Build a run snapshot

```bash
.venv/bin/python runs/build_run_snapshot.py \
  --run-id 2026-04-25-sample-review-gpt5 \
  --inputs prompts/llm_nl_generation.sample.jsonl \
  --outputs llm_outputs.sample_current.jsonl \
  --errors llm_outputs.sample_current.errors.jsonl \
  --prompt prompts/llm_nl_generation.prompt.txt \
  --schema schemas/llm_output.schema.json \
  --examples prompts/llm_nl_generation.examples.jsonl \
  --kgs kgs.jsonl \
  --kg-queries kg_queries.jsonl \
  --purpose "manual review sample"
```

## Review linkage

- One run can have many review exports.
- One review export should point to exactly one run.
- `build_review_bundle.py` should be the normal entry point for review, because it auto-freezes a run when needed and writes the run linkage into the review bundle before anyone starts annotating.
