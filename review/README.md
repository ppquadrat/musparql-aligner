# Review Workbench

This folder contains a lightweight local reviewer for LLM question-generation outputs.

## Usage

1. Build the browser data bundle:

```bash
.venv/bin/python build_review_bundle.py
```

By default, the builder ensures the bundle points to exactly one frozen run. If the
selected output is not already inside `runs/<run-id>/`, it will auto-freeze a run
snapshot first and then build the review bundle from that run.

If you want to review an already-frozen run explicitly:

```bash
.venv/bin/python build_review_bundle.py \
  --outputs runs/<run-id>/llm_outputs.jsonl \
  --run-manifest runs/<run-id>/manifest.json
```

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
- The generated review file is `review/review_data.js`.
- Exported reviewer judgments can be stored in `review/exports/` and committed when you want them versioned alongside the benchmark work.
- Review exports can be shared with other evaluators without changing the original model output files.
