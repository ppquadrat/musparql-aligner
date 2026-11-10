# Benchmark snapshot and release runbook

Run commands from the repository root. This runbook starts after human review
and the export procedure are complete. Use the [review runbook](REVIEW_RUNBOOK.md)
for review and export, and the [holdout runbook](HOLDOUT_RUNBOOK.md) for the
human-only private holdout procedure.

In the command templates, replace `vN` with the previous version, `vN+1` with
the actual next version, and angle-bracketed filenames with real paths. For
example, an update from `v8` writes to `benchmark/v9`, not a directory literally
named `benchmark/vN+1`.

## 1. Confirm the inputs

Before building a snapshot, identify:

- the previous snapshot, unless this is the first benchmark;
- the new version number, which must name a new `benchmark/vN/` directory;
- the review bundle used in the browser, normally `review/review_data.js`; and
- the matching sanitized review export under `var/review/exports/`.

Benchmark tools may consume only an export whose `kind` is
`non_holdout_review_export`. Never pass a private, mixed, or legacy review
export. The holdout policy is applied while constructing the review bundle and
exporting review state; benchmark commands do not accept private holdout data.

Always pass the review export explicitly. There is deliberately no "latest
export" default: filesystem recency does not prove that an export matches the
bundle, run, dataset, or review mode.

## 2. Choose the command from the review artifact

Choose the command according to how the review bundle and export were built,
not merely according to whether an existing benchmark pair will change.

### First benchmark snapshot

Use `build_benchmark` only when no previous benchmark snapshot exists:

```bash
.venv/bin/python -m scripts.benchmark.build_benchmark \
  --bundle review/review_data.js \
  --reviews var/review/exports/<sanitized-export>.json \
  --outdir benchmark/v1
```

This creates a snapshot solely from decisions represented in the supplied
initial-review bundle and export. Do not use it to create `vN+1`; it does not
carry forward an earlier snapshot.

### Initial-review update to an existing snapshot

Use `update_from_initial_review` when an ordinary initial-review bundle adds
newly reviewed public pairs to an existing snapshot:

```bash
.venv/bin/python -m scripts.benchmark.update_from_initial_review \
  --previous-benchmark benchmark/vN \
  --bundle review/review_data.js \
  --reviews var/review/exports/<sanitized-export>.json \
  --kg-queries var/queries/kg_queries.jsonl \
  --outdir benchmark/vN+1
```

The updater carries forward previous included, dismissed, alternative, and
internal linguistic-annotation records. It normally rejects a review that
overlaps an already reviewed pair.

A narrowly defined exception permits a corrected SPARQL version to replace the
same benchmark identity. The review-bundle record must attest both
`has_prior_pair_review: true` and `review_scope: new`, and its SPARQL version or
hash must differ from the previous snapshot. This is still an initial-review
update when the bundle and export are not compare-mode artifacts.

### A round exported per team and per dataset

A workshop round produces one export per reviewing team plus a synthesized
meta-review, each pinned to its own dataset's bundle. Repeat `--bundle` and
`--reviews`, paired in order, so the whole round is one update to one snapshot:

```bash
.venv/bin/python -m scripts.benchmark.update_from_initial_review \
  --previous-benchmark benchmark/vN \
  --bundle <bundle-a> --reviews <export-a1> \
  --bundle <bundle-a> --reviews <export-a2> \
  --bundle <bundle-b> --reviews <export-b1> \
  --decisions catalog/curated/meta_review_decisions.jsonl \
  --outdir benchmark/vN+1
```

Build the exports with `merge_adjudicated_reviews` first, which resolves pairs
several teams judged and leaves the builder's overlap guard intact. Order does
not matter, because the merged exports contain no pair twice; a pair applied by
an earlier stage is still an overlap for a later one.

The manifest then records every stage in `review_stages`, with the bundle,
export, dataset and applied count for each, and `update_type` becomes
`multi_export_review_update`. `source_bundle`, `source_review_export`,
`dataset_id` and `current_run` become lists. A single-export update is
unchanged: the scalar fields and `normal_review_update` stay as they were.

Do not chain one invocation per export through throwaway directories. Only the
last stage would be named in the snapshot it writes, and `previous_benchmark`
would point at a scratch path, so the rest of the round's provenance would be
lost.

### Comparative-review update

Use `update_benchmark` only with a bundle and export produced by the comparative
review workflow. Both must have `mode: compare`:

```bash
.venv/bin/python -m scripts.benchmark.update_benchmark \
  --previous-benchmark benchmark/vN \
  --bundle review/review_data.js \
  --reviews var/review/exports/<sanitized-compare-export>.json \
  --kg-queries var/queries/kg_queries.jsonl \
  --outdir benchmark/vN+1
```

This applies reviewed side-by-side decisions while carrying forward unchanged
records and their provenance. The command rejects an initial-review bundle or
export rather than guessing the intended update semantics.

### Authored pairs, with no generation round

Use `add_authored_pairs` when a pair's question is already settled and there is
nothing for a model to propose or a reviewer to judge:

```bash
.venv/bin/python -m scripts.benchmark.add_authored_pairs \
  --previous-benchmark benchmark/vN \
  --pairs var/benchmark/<authored-pairs>.jsonl \
  --decisions catalog/curated/meta_review_decisions.jsonl \
  --meta-reviewer-id reviewer-NNNN \
  --outdir benchmark/vN+1 \
  --holdout-selectors var/holdout/selectors.jsonl
```

Each line of the pairs file names `kg_id`, `query_label` and an `origin`:

- `meta_review_decision` — the wording was decided in meta-review. The
  question, disposition and alternatives are read from the decisions file, so
  no wording is retyped and there is one source of truth. The decision must
  record a canonical question; a blank slot is refused.
- `source_prompt` — the source published question and SPARQL together, as the
  LinkedMusic public query database did. The line carries `gold_question` and a
  `source` block, because that provenance lives outside this repository. It
  cannot carry alternatives: an accepted alternative needs an approving
  reviewer, and a source-authored pairing has none.
- `retained_sparql_repin` — the pair is already in the previous snapshot and
  only its SPARQL has changed. The question and the review that approved it are
  carried forward untouched and just the version pin moves. Use this rather
  than re-admitting a reviewed pair, which would discard its attribution.

An authored pair carries `pipeline_assessment: not_applicable`, because there is
no model output in the record to assess. A meta-review decision's own
`pipeline_assessment` judges the wording the pipeline produced, which is not the
wording being admitted, so it is preserved in the `source` block as
`meta_review_pipeline_assessment`. The `source` block also records
`confirmation`: `source_authored_pairing`, `single_meta_reviewer`, or
`carried_forward_from_previous_review`. That distinction matters — a review
round gives multi-reviewer agreement that question and SPARQL match, while a
meta-review wording has one expert's, and the record says which.

Replacing a pair already in the previous snapshot needs `--replace-existing`,
which is never implicit because the replaced record loses its reviewer
attribution. `--dry-run` reports the counts without writing.

The manifest's `gold_question_policy` is unioned with the previous snapshot's,
so an assertion made when content was admitted keeps travelling with later
snapshots. This matters: v9 and v10 dropped v8's
`source_prompt_used_for_curated_linkedmusic` key while still carrying its 20
LinkedMusic records forward.

Historical migration and snapshot-regeneration scripts are not normal
benchmark-building commands.

## 3. Check the build output

Read the command summary. Confirm that the number of applied reviews matches
the sanitized export and that the total included and dismissed counts are
plausible. A sanitized export may contain fewer records than the browser review
when the remaining records are holdouts; those private records must not appear
in the public snapshot.

Inspect the new manifest:

```bash
jq '.benchmark_version, .update_type, .counts, .execution_snapshot' \
  benchmark/vN+1/manifest.json
```

For an update, confirm that `previous_benchmark` and
`previous_benchmark_version` identify the intended predecessor.

## 4. Audit the snapshot

Run the audit before packaging or committing:

```bash
.venv/bin/python -m scripts.benchmark.audit_snapshot benchmark/vN+1
```

The audit checks snapshot membership, manifest counts, SPARQL version and hash
pins, retained edit provenance, execution-observation summary, sidecar
references, and the public/private boundary. Public benchmark and alternative
records may contain only the stable SPARQL-provenance projection; the audit
rejects working correction fields such as candidate identities, review-export
hashes, and UI execution-attempt histories. Resolve every error; do not edit a
manifest merely to silence the audit.

## 5. Inspect the change from the previous version

Start with a compact identity and gold-field comparison. This avoids mistaking
an unchanged JSONL context line for a removed record:

```bash
diff -u \
  <(jq -r '[.kg_id, .query_id, .query_label, .sparql_version, .gold_question] | @tsv' benchmark/vN/included.jsonl) \
  <(jq -r '[.kg_id, .query_id, .query_label, .sparql_version, .gold_question] | @tsv' benchmark/vN+1/included.jsonl)
```

In unified diff output, only lines beginning with `-` or `+` were removed or
added. Lines beginning with a space are unchanged context.

Then inspect the complete working partitions, where carried-forward records
remain stable and substantive changes are easiest to see:

```bash
git diff --no-index -- benchmark/vN/included.jsonl benchmark/vN+1/included.jsonl
git diff --no-index -- benchmark/vN/dismissed.jsonl benchmark/vN+1/dismissed.jsonl
git diff --no-index -- benchmark/vN/alternatives.jsonl benchmark/vN+1/alternatives.jsonl
git diff --no-index -- benchmark/vN/linguistic_annotations.jsonl benchmark/vN+1/linguistic_annotations.jsonl
```

Then inspect the manifest and public scoring file:

```bash
git diff --no-index -- benchmark/vN/manifest.json benchmark/vN+1/manifest.json
git diff --no-index -- benchmark/vN/benchmark.jsonl benchmark/vN+1/benchmark.jsonl
```

`git diff --no-index` exits with status `1` when differences are found; that is
normal. The public scoring file is regenerated, so version and build-time fields
will change on every row. Verify the substantive question and SPARQL changes in
addition to that expected metadata noise.

Check that:

- applied-review counts match the intended public decisions;
- included and dismissed membership changed only as intended;
- canonical questions use the intended reviewer wording;
- selected SPARQL text, version, and hash are correct;
- unchanged alternatives and provenance were retained;
- no holdout identity or reviewer-only material appears in public files; and
- automatic scores were not copied into gold decisions.

If the semantic diff is wrong, correct the upstream human review or bundle and
rebuild the new snapshot. Do not hand-edit benchmark gold data.

## 6. Build and inspect the public release

Build into a new or empty ignored directory:

```bash
.venv/bin/python -m scripts.benchmark.build_public_release \
  --snapshot benchmark/vN+1 \
  --outdir build/public-releases/vN+1
```

The release builder audits the working snapshot again and writes an allowlisted
package with a release manifest and checksums. It excludes raw reviews,
internal annotations, run request metadata, local working paths, and private
holdout material. It also reapplies the stable SPARQL-provenance projection as
defense in depth and rejects every reviewer-bearing value that is not a
`reviewer-NNNN` pseudonym. It copies the CC BY 4.0 benchmark license and third-party
notices into the package; publishing a package without either document is not
allowed. Inspect the package; do not publish the working snapshot directory
directly.

Confirm that the five declared files are present and that the manifest records
the expected license:

```bash
find build/public-releases/vN+1 -maxdepth 1 -type f -print | sort
jq '.release_schema_version, .licensing, .files' \
  build/public-releases/vN+1/manifest.json
```

The package must contain `manifest.json`, `benchmark.jsonl`,
`alternatives.jsonl`, `LICENSE`, and `THIRD_PARTY_NOTICES.md`. Before assigning
a DOI, replace `[VERSION]` in the suggested attribution when displaying it in
release metadata and add the DOI to the repository citation metadata when one
is available. Do not alter the packaged license scope to claim ownership of
third-party queries or referenced knowledge graphs.

## 7. Final checks

```bash
.venv/bin/python -m pytest -q
.venv/bin/python scripts/check_public_tree.py --rev HEAD
git status --short
```

Review all tracked changes before committing. Do not overwrite or delete the
previous benchmark version, and do not bypass repository commit or push hooks.
