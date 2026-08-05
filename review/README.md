# Review Workbench

This folder contains a lightweight local reviewer for LLM question-generation outputs.

## Usage

Before selecting any holdout pairs, read the
[holdout security overview](../HOLDOUT_SECURITY.md) and follow the
[holdout review runbook](../HOLDOUT_RUNBOOK.md).

Every bundle build requires exactly one of `--holdout-selectors <path>`,
`--no-holdout`, or `--holdout-filtered-upstream`. The examples use
`--no-holdout` only for a first review before any holdout exists; later
identity-visible examples use the selector path.

1. Build the browser data bundle:

```bash
.venv/bin/python build_review_bundle.py --no-holdout
```

By default, the builder ensures the bundle points to exactly one frozen LLM generation run. If the
selected output is not already inside `runs/<run-id>/`, it will auto-freeze a generation run
snapshot first and then build the review bundle from that run.

If you want to review an already-frozen generation run explicitly:

```bash
.venv/bin/python build_review_bundle.py \
  --no-holdout \
  --outputs runs/<run-id>/llm_outputs.jsonl \
  --run-manifest runs/<run-id>/manifest.json
```

For later initial-review rounds, pass the latest benchmark snapshot:

```bash
.venv/bin/python build_review_bundle.py \
  --holdout-selectors review/public_exports/<holdout-selectors>.jsonl \
  --outputs runs/<run-id>/llm_outputs.jsonl \
  --run-manifest runs/<run-id>/manifest.json \
  --previous-benchmark benchmark/vN
```

With `--previous-benchmark`, initial review excludes already reviewed SPARQL
versions by default. Under the identity-visible holdout policy, pass an
annotation-free selector file with `--holdout-selectors`; private annotations
are never read by this command. Previous
decisions match by version/hash, with legacy SPARQL text as a compatibility
fallback. Use `--include-reviewed` only
for a deliberate audit pass over non-holdout reviewed pairs. Previous decisions
and pipeline assessments are not included unless `--reveal-previous-decision` is passed.

To build a comparative review of a previous generation run and a new run:

```bash
.venv/bin/python build_next_review_round.py \
  --holdout-selectors review/public_exports/<holdout-selectors>.jsonl \
  --previous-run runs/<old-run-id> \
  --current-run runs/<new-run-id> \
  --previous-reviews review/public_exports/<previous-non-holdout-export>.json \
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

New examples can be marked as **Private holdout** before export. **Export
Non-Holdout** creates a sanitized decision file with those entries absent;
**Export Private Holdout** separately creates the self-contained
`musparql-holdout-private-*.json` file. The private file contains the candidate
and its complete annotation and must remain outside Git.

The summary bar shows the current holdout count. Use **Set → Holdout only** to
inspect the selected set. After a private export starts, the workbench reports
the exported count so the reviewer can compare it with the downloaded file
before clearing browser state.

A pair may be selected in either the initial or comparison interface only when
no reviewer decision was attached before the current review session. Earlier
generation, inclusion in another run, or passive display does not disqualify it.
An earlier status, corrected wording, rating, or comment does. The workbench
enables the control only for eligible pairs and explains ineligibility otherwise.
The bundle must be built with all applicable prior-review sources and the human
`--assert-complete-review-provenance` assertion; without it, eligibility is
unknown and the control fails closed.

2. Serve only the review application on loopback:

```bash
python3 -m http.server 8000 --bind 127.0.0.1 --directory review
```

3. Open:

```text
http://127.0.0.1:8000/
```

### SPARQL correction mode

Query execution automatically creates the correction-candidate ledger. Build a
holdout-filtered bundle and open the separate correction page:

```bash
.venv/bin/python build_sparql_correction_bundle.py --no-holdout
# open http://127.0.0.1:8000/corrections.html
```

This mode shows the immutable base query, execution observation, automatic
triage, aligned evidence, and an append-only proposal form. It has no
private-holdout control, import, or private-export path. Apply exported
decisions with `apply_sparql_corrections.py` following
`SPARQL_CORRECTION_RUNBOOK.md`.

Any query identity with retained SPARQL edits is permanently ineligible for
holdout inclusion. Initial and comparison NL review enforce this from explicit
edit-history provenance, including when selected SPARQL is version `0`.

Reviewer decisions are stored in persistent browser local storage, so closing a
tab does not clear them and clearing the browser cache is not relevant. After
securely saving and verifying a private export, use **Clear Private State**, then
close the browser before resuming agent-assisted work. Move sanitized non-holdout
exports to ignored, agent-readable `review/public_exports/`. Move the separately downloaded private
export to ignored, agent-forbidden `review/private/` or outside the workspace;
open it and verify its holdout count before using **Clear Private State**. Legacy
or unsanitized exports belong only in agent-forbidden `review/exports/`.
No review export is a paper-repository artifact.
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
- Only sanitized exports from which holdout entries are absent may be supplied
  to agent-facing builders or shared with other evaluators. Review exports are
  not committed.
- Compare-mode exports contain the reviewer decisions for the current run, while
  the imported previous review remains read-only context.
- Literal SPARQL wording is preserved separately in
  `benchmark/vN/alternatives.jsonl` under `literal_formulations`, with source
  type `literal_sparql_wording`.
- Private holdout records are never retained in repository snapshots. Public
  builders reject mixed/private exports. See the
  [security overview](../HOLDOUT_SECURITY.md) and
  [review runbook](../HOLDOUT_RUNBOOK.md).
