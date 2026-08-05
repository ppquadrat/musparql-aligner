# Pipeline runbook

Run commands from the repository root. This runbook shows the normal sequence;
commands that expose options document them with `--help`.

## 1. Prepare the environment

```bash
.venv/bin/pip install --no-build-isolation -e .
.venv/bin/python -m pytest -q
```

Enable the repository safety hooks once per checkout:

```bash
git config core.hooksPath .githooks
```

## 2. Build the tracked KG catalogue

```bash
.venv/bin/python -m scripts.build_kgs
```

This reads `catalog/sources.yaml` and `catalog/seeds.yaml`, refreshes captured
text under `catalog/snapshots/`, and writes `catalog/kgs.jsonl`. Review tracked
catalog changes before committing them.

## 3. Extract and enrich queries

```bash
.venv/bin/python -m scripts.extract_queries
.venv/bin/python -m scripts.enrich_evidence
```

The working catalogue is `var/queries/kg_queries.jsonl`. Repository clones and
local dumps are cached under `var/cache/`. Extraction also restores approved
versions from `catalog/curated/Approved_SPARQL_Edits.jsonl`. The reported
restored count is zero when an intact local catalogue already contains the same
versions, or the archive's edit count after rebuilding from scratch.

## 4. Execute queries

Before the first holdout exists:

```bash
.venv/bin/python -m scripts.run_queries --no-holdout
```

After holdout selection:

```bash
.venv/bin/python -m scripts.run_queries \
  --holdout-selectors var/holdout/selectors.jsonl
```

Use `--kg-id`, `--source-id`, or `--sparql-version` for a targeted run. Execution
updates the working query catalogue and the failure and correction-candidate
ledgers under `var/queries/`.

## 5. Build model inputs

```bash
.venv/bin/python -m scripts.build_llm_inputs \
  --holdout-selectors var/holdout/selectors.jsonl \
  --unreviewed-from benchmark/vN
```

Use `--no-holdout` only when the human owner asserts that no holdout identities
exist. `--unreviewed-from` limits generation to pairs that are new since the
given public benchmark plus previously reviewed identities whose retained SPARQL
version/hash has changed. Omit it for a full generation run. The output is
`var/llm/inputs.jsonl`.

## 6. Generate provisional questions

Configure the API key and optional compatible base URL outside tracked files,
then run:

```bash
.venv/bin/python -m scripts.run_llm_generation \
  --unreviewed-from benchmark/vN \
  --output var/llm/outputs-minimax-unreviewed.jsonl \
  --errors var/llm/outputs-minimax-unreviewed.errors.jsonl
```

Generation defaults to the adopted `MiniMax-M2.5` model through
`chat.completions.create`. After the first successful output, the selected model
and API method are saved locally in ignored `var/llm/generation_config.json` and
reused by later runs. Explicit `--model` and `--api-method` arguments take
precedence; `GRAPHIA_MODEL` and `GRAPHIA_API_METHOD` provide persistent shell
overrides.

The command uses explicit output names so the completed generation is not
confused with older working files. Replace `vN` with the benchmark version you
are actually building from. The generation flag can also filter an existing
full input file; omit it when the input builder already produced the subset.
Inspect the error file before freezing the run.

## 7. Freeze the generation run

Use the exact input, output, and error paths supplied to the generation command.
The copyable command below matches the generation command above.

```bash
.venv/bin/python -m scripts.runs.build_run_snapshot \
  --inputs var/llm/inputs.jsonl \
  --outputs var/llm/outputs-minimax-unreviewed.jsonl \
  --errors var/llm/outputs-minimax-unreviewed.errors.jsonl \
  --prompt prompts/llm_nl_generation.prompt.txt \
  --schema schemas/llm_output.schema.json \
  --examples prompts/llm_nl_generation.examples.jsonl \
  --kgs catalog/kgs.jsonl \
  --kg-queries var/queries/kg_queries.jsonl
```

The snapshot builder infers a run ID from the generation start time and the
single requested model recorded in the output. For the generation shown above,
the snapshot is `var/runs/2026-08-05-191155-minimax-m2-5/`. Pass `--run-id NAME`
only to override the inferred name.

## 8. Evaluate a change

This step is optional and applies only when comparing two generation runs, such
as a baseline and an experimental prompt or model. Skip it for an ordinary
initial-review round. The following is a template rather than a copyable command
because both run IDs must be chosen from the runs being compared:

```bash
.venv/bin/python -m scripts.evals.evaluate_runs \
  --runs var/runs/<baseline-run> var/runs/<candidate-run> \
  --baseline var/runs/<baseline-run> \
  --out var/evals/reports/<eval-id>
```

Read the report, inspect representative differences, and record the conclusion
in `docs/experiments/`. Do not update the benchmark from automatic scores.

## 9. Build review material

For initial review:

```bash
.venv/bin/python -m scripts.build_review_bundle \
  --latest-run \
  --previous-benchmark benchmark/vN \
  --holdout-selectors var/holdout/selectors.jsonl \
  --assert-complete-review-provenance
```

`--latest-run` reads the newest frozen manifest under `var/runs/` and resolves
that run's copied inputs and outputs from the manifest. Use explicit `--inputs`,
`--outputs`, and `--run-manifest` instead when reviewing an older run.

`--assert-complete-review-provenance` is a human attestation that the supplied
previous benchmark accounts for every earlier reviewer decision. Use it only
after confirming that `--previous-benchmark` is the complete authoritative
prior-review snapshot. Without this assertion, the bundle is still usable for
ordinary review, but holdout selection is disabled for every candidate because
the workbench cannot safely determine which identities are eligible.

Start the review application on loopback from a separate terminal:

```bash
python3 -m http.server 8000 --bind 127.0.0.1 --directory review
```

Open `http://127.0.0.1:8000/` in a browser. Review state is stored in browser
local storage, so closing the tab does not clear it. When the review and exports
are complete, return to the server terminal and press `Ctrl-C` to stop it. Follow
the [review runbook](REVIEW_RUNBOOK.md) for review, export, selector-update, and
private holdout procedures.

For comparative review, follow [REVIEW_RUNBOOK.md](REVIEW_RUNBOOK.md). The
browser bundle remains an ignored generated file under `review/` because the
static review application loads it from that directory.

### Export and close the review

Do not proceed directly from reviewing to benchmark construction. Complete the
export sequence in the [review runbook](REVIEW_RUNBOOK.md):

1. Select **Export Non-Holdout**, then place the sanitized export under
   `var/review/exports/` for the benchmark-building tools.
2. Under the identity-visible holdout policy, select **Update Holdout
   Selectors**, verify the downloaded selector-only file yourself, and replace
   `var/holdout/selectors.jsonl` with the verified update.
3. If the review contains holdouts, follow the human-only export, verification,
   storage, and browser-state clearing procedure in the
   [holdout runbook](HOLDOUT_RUNBOOK.md). Never place the private holdout export
   in the public repository or ask an agent to inspect it.
4. Confirm that all required exports are safely stored, close the review tab,
   and stop the local server with `Ctrl-C`.

The sanitized non-holdout export—not the browser's local review state or the
private holdout export—is the review input used in the next pipeline step.

## 10. Build or update a benchmark

Follow the [benchmark runbook](BENCHMARK_RUNBOOK.md). It explains how to choose
between a first build, an initial-review update, and a comparative-review
update. The command is determined by the review artifact's mode, not just by
whether an existing pair will change.

Always use the matching validated sanitized review export and write to a new
version after `benchmark/vN/`; never overwrite an earlier version. The explicit
holdout decision was made when constructing and exporting the review material.
Benchmark tools consume only the sanitized non-holdout export and reject
private or mixed review data.

## 11. Audit the snapshot

```bash
.venv/bin/python -m scripts.benchmark.audit_snapshot benchmark/vN
```

Resolve every audit error before release.

Then inspect the snapshot diff using the partition-first procedure in the
[benchmark runbook](BENCHMARK_RUNBOOK.md).

## 12. Build a public release

```bash
.venv/bin/python -m scripts.benchmark.build_public_release \
  --snapshot benchmark/vN \
  --outdir build/public-releases/vN
```

`build/public-releases/vN/` is a sanitized, reproducible package. It is ignored
and may be archived or uploaded after human inspection; it is not a replacement
for the tracked working snapshot.

## 13. Final checks

```bash
.venv/bin/python -m pytest -q
.venv/bin/python scripts/check_public_tree.py --rev HEAD
git status --short
```

Do not bypass the commit or push hooks.
