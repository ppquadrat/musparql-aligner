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

For later initial-review rounds, pass the latest benchmark snapshot:

```bash
.venv/bin/python build_review_bundle.py \
  --outputs runs/<run-id>/llm_outputs.jsonl \
  --run-manifest runs/<run-id>/manifest.json \
  --previous-benchmark benchmark/vN
```

With `--previous-benchmark`, initial review excludes already reviewed pairs by
default and always excludes private holdout pairs. Use `--include-reviewed` only
for a deliberate audit pass over non-holdout reviewed pairs. Previous decisions
and pipeline assessments are not included unless `--reveal-previous-decision` is passed.

To build a comparative review of a previous generation run and a new run:

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

The comparative-review bundle shows only added, removed, and review-worthy changed pairs by
default. Use `--include-unchanged` if you want unchanged pairs visible too.
Rationale, confidence, model, and full-input evidence changes are treated as
metadata-only unless the question, origin, retained evidence, or SPARQL also
changed. Use `--include-metadata-only` if you want those records visible.
Pairs previously excluded from the benchmark are omitted by default; use
`--include-dismissed` only when intentionally revisiting those exclusions.

For comparative-review rounds against a curated benchmark, pass
`--previous-benchmark benchmark/vN --benchmark-only`. Review exports are
incremental, so the latest export may only contain decisions from the latest
review round; the benchmark snapshot contains the carried-forward decisions.

Comparative mode shows the previous run and current run side by side. Previous
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
The initial-review form also exports optional interpretive dimensions
(`naturalness`, `pragmatism`, `room_for_interpretation`) and the
`requires_graph_context_knowledge` flag when you set them. Benchmark builders
copy these into the internal `benchmark/vN/linguistic_annotations.jsonl`, not
into the scoring dataset or public release.

The form also exports optional literal SPARQL wording in `literal_wording`.
Use this for wording that follows the SPARQL more exactly than the preferred
natural-language question. Older exports that stored these as `Literal:` note
lines are still supported by the benchmark builders.

Reviewer comments use two deliberately simple fields:

- `public_comment` is published with the benchmark and should explain semantic
  or wording decisions that are useful to benchmark users.
- `internal_comment` is an operational working note and is excluded from the
  public release.

Legacy `note` fields are imported as internal comments because they predate the
public/private contract. A later review round can explicitly promote selected
text to `public_comment`. If a legacy comment also contains a `Literal: ...`
line that matches `literal_wording`, that duplicate line is removed while the
rest of the comment is retained privately.

## Review fields

- `benchmark_disposition: included`
  - Publish the human-confirmed canonical pair.

- `benchmark_disposition: excluded`
  - Exclude this example from the benchmark going forward.
  - Dismissed pairs are also omitted from future comparative-review queues by default.
  - Use when the underlying pair is bad benchmark material, not merely when the model behaved badly.

- `pipeline_assessment: accepted`
  - The presented formulation is acceptable.

- `pipeline_assessment: prompt_improvement_recommended`
  - The example is valid, but the model behavior should improve through prompt changes.
  - Typical cases: wrong `generated` vs `paraphrased`, awkward wording, poor evidence selection by the model.

- `pipeline_assessment: input_data_improvement_recommended`
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
- Literal SPARQL wording is preserved separately in
  `benchmark/vN/alternatives.jsonl` under `literal_formulations`, with source
  type `literal_sparql_wording`.
- Private holdout records are excluded from normal benchmark snapshots,
  comparative-review queues, future prompt-input generation, and automatic
  evaluation by default. Do not paste or print holdout labels in model-visible
  conversations if the goal is to keep them clean.
