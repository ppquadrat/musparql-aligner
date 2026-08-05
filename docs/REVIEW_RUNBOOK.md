# Human review runbook

This runbook covers ordinary initial and comparative review. If selecting or
handling holdouts, also follow [HOLDOUT_RUNBOOK.md](HOLDOUT_RUNBOOK.md).

## Before review

1. Finish the relevant pipeline run and freeze it under `var/runs/`.
2. Decide whether this is initial or comparative review.
3. Gather every applicable previous benchmark and sanitized review export.
4. Supply an explicit holdout selector, or use `--no-holdout` only when the
   human owner confirms no holdout exists.
5. Use `--assert-complete-review-provenance` only after confirming that the
   supplied sources cover all earlier reviewer decisions. This assertion enables
   new holdout selection and must not be guessed.

## Initial review

Build the bundle:

```bash
.venv/bin/python -m scripts.build_review_bundle \
  --outputs var/runs/<run-id>/llm_outputs.jsonl \
  --run-manifest var/runs/<run-id>/manifest.json \
  --previous-benchmark benchmark/vN \
  --holdout-selectors var/holdout/selectors.jsonl
```

Omit `--previous-benchmark` only for a genuinely first review. Use
`--include-reviewed` for intentional reinspection, not normal initial review.

## Comparative review

Build a side-by-side bundle:

```bash
.venv/bin/python -m scripts.build_review_diff_bundle \
  --previous-outputs var/runs/<old-run>/llm_outputs.jsonl \
  --current-outputs var/runs/<new-run>/llm_outputs.jsonl \
  --previous-run-manifest var/runs/<old-run>/manifest.json \
  --current-run-manifest var/runs/<new-run>/manifest.json \
  --previous-benchmark benchmark/vN \
  --previous-reviews var/review/exports/<previous-export>.json \
  --holdout-selectors var/holdout/selectors.jsonl
```

Use `--benchmark-only` when the comparison is specifically about existing
benchmark pairs. Include unchanged, dismissed, or metadata-only records only
when the review question requires them.

## Open the workbench

Serve only the review directory on loopback:

```bash
python3 -m http.server 8000 --bind 127.0.0.1 --directory review
```

Open `http://127.0.0.1:8000/`. Review state is stored in browser local storage;
closing the tab does not clear it.

## Review each pair

1. Read the proposed question and its origin.
2. Inspect the selected SPARQL version and evidence.
3. Check whether returned variables and technical operations belong in the
   human information need.
4. Accept, exclude, recommend a pipeline improvement, or write a preferred
   question.
5. Use literal wording only when a close SPARQL verbalization helps preserve the
   distinction from the preferred question.
6. Keep public comments suitable for publication. Put working notes in the
   internal comment field.
7. Follow the separate holdout procedure before marking any private holdout.

The current UI intentionally has no linguistic-dimension controls.

## Export

1. Select **Export Non-Holdout**.
2. Move the downloaded sanitized export to `var/review/exports/`.
3. If holdouts were selected, export and verify the private package using the
   human-only holdout procedure and store it in the separate private repository.
4. Clear private browser state only after verifying the private export.

Do not ask an agent to inspect, move, count, validate, or migrate a private
holdout export.

## Apply the review

Use the appropriate command under `scripts/benchmark/` to build or update a new
benchmark version. Then run the snapshot audit and inspect the diff from the
previous version.

Check that:

- included and excluded counts match the intended decisions;
- canonical questions and SPARQL pins are correct;
- no holdout identity appears in the public files;
- previous review provenance is retained; and
- automatic scores have not been copied into gold decisions.

## Close the session

Confirm that the sanitized export is saved, complete any holdout clearing steps,
close the browser, and stop the local server. Do not rely on browser cache
clearing as a substitute for the explicit private-state flow.
