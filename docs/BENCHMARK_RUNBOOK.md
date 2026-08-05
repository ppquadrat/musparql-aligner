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
defense in depth. Inspect the package; do not publish the working snapshot
directory directly.

## 7. Final checks

```bash
.venv/bin/python -m pytest -q
.venv/bin/python scripts/check_public_tree.py --rev HEAD
git status --short
```

Review all tracked changes before committing. Do not overwrite or delete the
previous benchmark version, and do not bypass repository commit or push hooks.
