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
  --unreviewed-from benchmark/vN
```

The default outputs are `var/llm/outputs.jsonl` and
`var/llm/outputs.errors.jsonl`. The generation flag can also filter an existing
full input file; omit it when the input builder already produced the subset.
Inspect the error file before freezing the run.

## 7. Freeze the generation run

```bash
.venv/bin/python -m scripts.runs.build_run_snapshot \
  --run-id <run-id> \
  --inputs var/llm/inputs.jsonl \
  --outputs var/llm/outputs.jsonl \
  --errors var/llm/outputs.errors.jsonl \
  --prompt prompts/llm_nl_generation.prompt.txt \
  --schema schemas/llm_output.schema.json \
  --examples prompts/llm_nl_generation.examples.jsonl \
  --kgs catalog/kgs.jsonl \
  --kg-queries var/queries/kg_queries.jsonl
```

The snapshot is written to `var/runs/<run-id>/`.

## 8. Evaluate a change

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
  --outputs var/runs/<run-id>/llm_outputs.jsonl \
  --run-manifest var/runs/<run-id>/manifest.json \
  --holdout-selectors var/holdout/selectors.jsonl
```

For comparative review, follow [REVIEW_RUNBOOK.md](REVIEW_RUNBOOK.md). The
browser bundle remains an ignored generated file under `review/` because the
static review application loads it from that directory.

## 10. Build or update a benchmark

Use the appropriate command under `scripts/benchmark/` for an initial build,
comparative update, or curated-source addition. Always supply the frozen run,
validated sanitized review export, previous snapshot where applicable, and an
explicit holdout policy.

Create a new `benchmark/vN/`; never overwrite an earlier version.

## 11. Audit the snapshot

```bash
.venv/bin/python -m scripts.benchmark.audit_snapshot benchmark/vN
```

Resolve every audit error before release.

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
