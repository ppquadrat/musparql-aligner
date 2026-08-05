# SPARQL correction workbench runbook

This workflow assumes holdouts exist. In every command, replace
`<holdout-selectors>.jsonl` with the annotation-free selector file chosen by the
human owner under `var/review/exports/`. Never use private annotations as an
input.

## 1. Run the pipeline and update candidates

```bash
.venv/bin/python -m scripts.run_queries \
  --holdout-selectors var/holdout/selectors.jsonl
```

The command removes holdout identities before query jobs are created. For each
remaining selected query it runs the latest retained SPARQL by default and
updates `sparql_correction_candidates.jsonl`. Successful, empty, failed,
unsupported, unavailable, and not-attempted versions are all valid correction
candidates. Execution changes priority and trust; it never decides eligibility.

To use an older retained version deliberately, add `--sparql-version <number>`.
The selected version and hash are recorded explicitly. Without that option,
`latest` is always used and there is no silent fallback.

## 2. Build the browser bundle

```bash
.venv/bin/python -m scripts.build_sparql_correction_bundle \
  --holdout-selectors var/holdout/selectors.jsonl
```

This writes the ignored local file `review/sparql_correction_data.js`. The
builder filters holdout identities before it validates candidate content or
touches evidence.

## 3. Start the local workbench service

```bash
.venv/bin/python -m scripts.correction_service \
  --holdout-selectors var/holdout/selectors.jsonl
```

Open:

```text
http://127.0.0.1:8765/corrections.html
```

No separate static server is needed. The service serves the existing browser UI
and provides same-origin `/api/execute` and `/api/suggest` endpoints. It binds to
loopback by default.

UI execution is deliberately separate from pipeline execution. It uses the same
endpoint, fallback, graph insertion, cleaning, timeout, local-dataset, and
unsupported-runtime rules, but it does not mutate `var/queries/kg_queries.jsonl`. Attempts
are appended to `review/local_workbench_execution_attempts.jsonl` and retained
with an approval. Pipeline execution, by contrast, updates canonical execution
history and refreshes the candidate ledger.

Suggestion generation uses the configured OpenAI environment and defaults to
model `gpt-5`. Override it with `--model <model>`. API keys are read by the SDK;
they are never returned or written to workbench logs.

## 4. Review efficiently

For each candidate:

1. Inspect triage, retained base, latest approved version, source evidence, and
   the base-to-proposal diff.
2. Optionally click **Generate suggestion**. Its edit type, rationale, evidence
   IDs, model/request identity, and prompt/schema hashes are filled and retained
   automatically, but nothing is approved automatically.
3. Optionally execute base, latest, or proposal. Endpoint failure or unsupported
   runtime does not block the decision.
4. Click **Approve and next**, **No edit and next**, or **Defer and next**.

The current shared result parser executes `SELECT` queries. `ASK`, `CONSTRUCT`,
and `DESCRIBE` are recorded as unsupported observations; they remain reviewable
and eligible downstream.

For a manual approval, the human must enter changed, nonempty SPARQL and either
an edit type or a rationale. Evidence IDs and reviewer note are optional. The UI
infers a human origin; no model/tool field is requested. No-edit and defer need
no metadata and accept an optional note.

The browser saves drafts and decisions in local browser storage. Approval marks
only the local decision and advances to the next visible unreviewed candidate.
Canonical records are unchanged until the explicit apply step.

## 5. Export and apply

Click **Export decisions**. Move the downloaded, non-holdout correction export
to the ignored local input directory:

```text
var/review/exports/musparql-sparql-correction-review-<dataset>-<time>.json
```

Do not commit the export or the two local service logs. Validate without writing:

```bash
.venv/bin/python -m scripts.apply_sparql_corrections \
  var/review/exports/musparql-sparql-correction-review-<dataset>-<time>.json \
  --holdout-selectors var/holdout/selectors.jsonl \
  --dry-run
```

Apply it:

```bash
.venv/bin/python -m scripts.apply_sparql_corrections \
  var/review/exports/musparql-sparql-correction-review-<dataset>-<time>.json \
  --holdout-selectors var/holdout/selectors.jsonl
```

The apply command compares browser decisions with the authoritative candidate
ledger and, for agent suggestions and UI executions, the append-only local
service logs. It rejects stale version/hash pins, changed service metadata,
duplicates, and holdout identities. Approved SPARQL is appended atomically as a
new version; no existing version is overwritten.

## 6. Rebuild downstream artifacts

```bash
.venv/bin/python -m scripts.build_llm_inputs \
  --holdout-selectors var/holdout/selectors.jsonl
```

The latest approved SPARQL is included regardless of whether its execution
observation is successful, empty, failed, unavailable, unsupported, or absent.
The observation travels as provenance. Stale hashes and provenance still fail
closed.

Continue with the normal generation and review commands:

```bash
.venv/bin/python -m scripts.run_llm_generation

.venv/bin/python -m scripts.build_review_bundle \
  --holdout-selectors var/holdout/selectors.jsonl \
  --outputs llm_outputs.jsonl \
  --assert-complete-review-provenance
```

## 7. Checks before a commit

```bash
.venv/bin/python -m pytest -q
node --check review/correction_app.js
.venv/bin/python -m py_compile \
  scripts/correction_service.py \
  src/musparql/sparql_corrections.py \
  scripts/run_queries.py
```

After staging the intended source files:

```bash
.venv/bin/python scripts/check_public_tree.py --staged
```

The benchmark audit needs the ignored canonical `var/queries/kg_queries.jsonl` present so it
can resolve immutable version/hash pins:

```bash
.venv/bin/python -m scripts.benchmark.audit_snapshot benchmark/v8
```
