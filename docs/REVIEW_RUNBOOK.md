# Human review runbook

This runbook covers ordinary initial and comparative review. If selecting or
handling holdouts, also follow [HOLDOUT_RUNBOOK.md](HOLDOUT_RUNBOOK.md).

## Before review

1. Confirm the reviewer's pseudonymous ID in the confidential registry. Do not
   copy profile fields into the bundle or command line.
2. Finish the relevant pipeline run and freeze it under `var/runs/`.
3. Decide whether this is initial or comparative review.
4. Gather every applicable previous benchmark and sanitized review export.
5. Supply an explicit holdout selector, or use `--no-holdout` only when the
   human owner confirms no holdout exists.
6. Use `--assert-complete-review-provenance` only after confirming that the
   supplied sources cover all earlier reviewer decisions. This assertion enables
   new holdout selection and must not be guessed.

## Initial review

Build the bundle:

```bash
.venv/bin/python -m scripts.build_review_bundle \
  --reviewer-id reviewer-0001 \
  --latest-run \
  --previous-benchmark benchmark/vN \
  --holdout-selectors var/holdout/selectors.jsonl \
  --assert-complete-review-provenance
```

Omit `--previous-benchmark` only for a genuinely first review. Use
`--include-reviewed` for intentional reinspection, not normal initial review.
If the workbench says holdout selection is disabled because complete
prior-review provenance was not attested, rebuild the bundle with
`--assert-complete-review-provenance` after verifying the condition in step 5
above. The **Private holdout / selector member** text is the checkbox label; it
does not mean that every displayed candidate is already a selector member.

## Comparative review

Build a side-by-side bundle:

```bash
.venv/bin/python -m scripts.build_review_diff_bundle \
  --reviewer-id reviewer-0001 \
  --previous-outputs var/runs/<old-run>/llm_outputs.jsonl \
  --current-outputs var/runs/<new-run>/llm_outputs.jsonl \
  --previous-run-manifest var/runs/<old-run>/manifest.json \
  --current-run-manifest var/runs/<new-run>/manifest.json \
  --previous-benchmark benchmark/vN \
  --previous-reviews var/review/exports/<previous-export>.json \
  --holdout-selectors var/holdout/selectors.jsonl \
  --assert-complete-review-provenance
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
3. Under the identity-visible policy, use **Update Holdout Selectors** to merge
   the current review's explicit holdout additions/removals into the existing
   selector, or create a new selector when none exists. Verify the download and
   move/rename it to `var/holdout/selectors.jsonl`.
4. If holdouts were selected, export and verify the private package using the
   human-only holdout procedure and store it in the separate private repository.
5. Clear private browser state only after verifying both applicable selector
   and private exports.

Do not ask an agent to inspect, move, count, validate, or migrate a private
holdout export.

Legacy sanitized non-holdout exports can be backfilled explicitly:

```bash
.venv/bin/python -m scripts.migrations.add_reviewer_provenance \
  --reviewer-id reviewer-0001 \
  --write var/review/exports/<legacy-export>.json
```

The migration accepts only explicit paths under `var/review/exports/`; it
cannot target a benchmark snapshot or holdout artifact. Published v1-v10
snapshots remain unchanged.

To backfill matching correction histories in the local query catalogue, add
`--kg-queries var/queries/kg_queries.jsonl` to the command. That option is
restricted to the canonical local path.

## Apply the review

Follow the [benchmark runbook](BENCHMARK_RUNBOOK.md) to choose the command from
the exported review artifact's mode, build a new version, run the snapshot
audit, and inspect the diff from the previous version. In particular, a normal
review of a changed SPARQL pin uses the initial-review updater when the bundle
explicitly attests a re-reviewed SPARQL revision; it does not become a
comparative update merely because an existing benchmark record is replaced.

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
