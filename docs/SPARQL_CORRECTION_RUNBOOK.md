# SPARQL correction workbench runbook

Known follow-up work is indexed in [`OPEN_ISSUES.md`](OPEN_ISSUES.md), with the
detailed correction backlog in
[`SPARQL_CORRECTION_FOLLOW_UP.md`](SPARQL_CORRECTION_FOLLOW_UP.md).

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
If extraction recorded a static diagnostic for the still-latest SPARQL version,
that candidate is high priority even without a usable endpoint. A diagnostic on
an older source version is retained as provenance but does not affect triage
after an approved correction exists.

To use an older retained version deliberately, add `--sparql-version <number>`.
The selected version and hash are recorded explicitly. Without that option,
`latest` is always used and there is no silent fallback.

## 2. Build the browser bundle

```bash
.venv/bin/python -m scripts.build_sparql_correction_bundle \
  --reviewer-id reviewer-0001 \
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

The API method defaults to `responses.create`. For a LiteLLM or hosted-vLLM
deployment that exposes the selected model only through chat completions, start
the service with both an explicit model and API method, for example:

```bash
.venv/bin/python -m scripts.correction_service \
  --holdout-selectors var/holdout/selectors.jsonl \
  --model Qwen3-Coder-30B-A3B-Instruct-Q8_0 \
  --api-method chat.completions.create
```

An error that names a valid model but reports `Hosted_vllmException` with HTTP
404 usually means the Responses API was used for a chat-completions-only model.

## 4. Review efficiently

The queue intentionally contains every non-holdout query, including successful
queries, empty results, and identities that already have an approved version.
An approved version may still need a later correction, and execution alone does
not establish semantic correctness. The sidebar does not currently show version
numbers; open a record to see **Retained base** and **Latest approved**.

For a focused first pass, use the **Triage** filter in this order:

1. **Likely correction** — high-priority syntax or endpoint-rejected queries.
2. **Instantiation required** — unresolved parameterized templates that need
   concrete values and source/runtime context; these are not automatically
   SPARQL errors.
3. **Investigate** — medium-priority execution failures that may reflect either
   the query or its environment.
4. **Runtime environment** — informational cases requiring a local file,
   specialist engine, federation, or other unavailable runtime.
5. **General review** — successful, empty, and otherwise ordinary queries; use
   this for a broader semantic audit rather than the initial correction pass.

Combine the triage filter with **KG** or search when useful. Do not mark an
already-approved or low-priority query **No edit** merely to clear it from the
queue: filter it out unless a human no-edit decision is actually intended.

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

The apply command also updates the tracked, public-safe
`catalog/curated/Approved_SPARQL_Edits.jsonl` projection. Commit that projection
with the source change. It contains approved version text and minimal
non-private provenance, not the browser export or local service logs. A clean
extraction restores approved versions from it automatically.

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
  --reviewer-id reviewer-0001 \
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
.venv/bin/python -m scripts.benchmark.audit_snapshot benchmark/vN
```
