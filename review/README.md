# Review Workbench

This folder contains a lightweight local reviewer for LLM question-generation outputs.

## Usage

1. Build the browser data bundle:

```bash
.venv/bin/python build_review_bundle.py
```

By default, the builder ensures the bundle points to exactly one frozen LLM generation run. If the
selected output is not already inside `runs/<run-id>/`, it will auto-freeze a generation run
snapshot first and then build the review bundle from that run.

If you want to review an already-frozen generation run explicitly:

```bash
.venv/bin/python build_review_bundle.py \
  --outputs runs/<run-id>/llm_outputs.jsonl \
  --run-manifest runs/<run-id>/manifest.json
```

To compare a previous reviewed run with a new run:

```bash
.venv/bin/python build_next_review_round.py \
  --previous-run runs/<old-run-id> \
  --current-run runs/<new-run-id> \
  --previous-reviews review/exports/<previous-review-export>.json \
  --previous-benchmark benchmark/vN \
  --benchmark-only
```

`--current-run` defaults to `llm_outputs.jsonl`, so it can be omitted when the
new outputs are in the repo-root current output file.

The compare bundle shows only added, removed, and review-worthy changed pairs by
default. Use `--include-unchanged` if you want unchanged pairs visible too.
Rationale, confidence, model, and full-input evidence changes are treated as
metadata-only unless the question, origin, retained evidence, or SPARQL also
changed. Use `--include-metadata-only` if you want those records visible.
Pairs that were dismissed in the previous review export are excluded by default;
use `--include-dismissed` only when intentionally revisiting those exclusions.

For normal benchmark review rounds, pass `--previous-benchmark benchmark/vN
--benchmark-only`. Review exports are incremental, so the latest export may only
contain decisions from the latest review round; the benchmark snapshot contains
the carried-forward approved and pending decisions.

Compare mode shows the previous run and current run side by side. Previous
review decisions are read-only context; current decisions are stored separately
and exported as a new compare-mode review file.

New examples can be marked as **Private holdout** before export. In compare
mode, this control is shown for added pairs. Holdout decisions are saved in the
review export as `split: "private_holdout"` and are intended for reviewer-only
sanity checks, not prompt development.

2. Serve the repo locally:

```bash
python3 -m http.server 8000
```

3. Open:

```text
http://localhost:8000/review/
```

Reviewer decisions are stored in browser local storage and can be exported/imported as JSON.
The recommended repo location for exported reviewer decisions is `review/exports/`.

## Review labels

- `approve`
  - Keep this example in the benchmark as-is.

- `dismiss`
  - Exclude this example from the benchmark going forward.
  - Dismissed pairs are also omitted from future compare-review queues by default.
  - Use when the underlying pair is bad benchmark material, not merely when the model behaved badly.

- `needs_prompt_fix`
  - The example is valid, but the model behavior should improve through prompt changes.
  - Typical cases: wrong `generated` vs `paraphrased`, awkward wording, poor evidence selection by the model.

- `needs_data_fix`
  - The example may be valid, but the model inputs are wrong, incomplete, noisy, or missing important signals.
  - Typical cases: missing query-specific evidence, bad provenance matching, irrelevant evidence attached by enrichment.

## Notes

- Model outputs remain separate from reviewer judgments.
- Review exports should point to exactly one run.
- `build_review_bundle.py` is responsible for making that true before review starts.
- `build_next_review_round.py` is the usual entry point after changing extraction,
  enrichment, prompts, or models.
- The generated review file is `review/review_data.js`.
- Exported reviewer judgments can be stored in `review/exports/` and committed when you want them versioned alongside the benchmark work.
- Review exports can be shared with other evaluators without changing the original model output files.
- Compare-mode exports contain the reviewer decisions for the current run, while
  the imported previous review remains read-only context.
- Private holdout records are excluded from normal benchmark snapshots,
  compare-review queues, future prompt-input generation, and automatic
  evaluation by default. Do not paste or print holdout labels in model-visible
  conversations if the goal is to keep them clean.
