# SPARQL correction runbook

This is the practical workflow for reviewing and applying SPARQL corrections.
It assumes that holdout selectors already exist. Replace
`<holdout-selectors>.jsonl` and other angle-bracket placeholders with your
actual paths or values.

## 1. Run the queries and create the correction queue

Run:

```bash
.venv/bin/python run_queries.py \
  --holdout-selectors review/public_exports/<holdout-selectors>.jsonl
```

This executes the latest retained SPARQL version for each non-holdout query and
updates `sparql_correction_candidates.jsonl` automatically. Failed execution is
not a reason to hide a query from correction review: endpoint rejection,
unresolved placeholders, unavailable infrastructure, and other failures can all
appear in the queue with different triage labels. Holdout pairs are removed
before the queue is written.

You do not write a separate correction report. The queue is that report.

## 2. Build and open the correction workbench

Build the browser data:

```bash
.venv/bin/python build_sparql_correction_bundle.py \
  --holdout-selectors review/public_exports/<holdout-selectors>.jsonl
```

Serve the review directory on loopback:

```bash
python3 -m http.server 8000 --bind 127.0.0.1 --directory review
```

Open:

```text
http://127.0.0.1:8000/corrections.html
```

## 3. Review a proposed correction

The workbench shows the retained base SPARQL, the observed failure, the source
evidence, and the proposed SPARQL in one place. Compare the proposal with the
base query and evidence before approving it. Do not approve merely because a
query looks syntactically plausible.

At present, the workbench does **not** call the endpoint and does not generate an
agent correction itself. It also does not yet provide one-click approval of a
fully populated suggestion. These are current UI limitations.

For `Approve New Version`, the current UI requires:

- changed, complete SPARQL;
- an edit type;
- a short rationale;
- at least one evidence ID shown in the evidence panel;
- proposal origin; and
- a model/tool name only when the proposal origin is `Agent`.

The workbench automatically supplies the query identity, base version and hash,
failure observation, candidate digest, review time, and available evidence. You
are not asked to copy those fields manually. `Reviewer note` is optional.

If you are happy with the correction and the required fields are already
populated, click `Approve New Version`, then click `Next`. Approval does not yet
advance automatically. With the current UI, missing required fields must be
completed before export. `No SPARQL Edit` requires only a rationale; `Defer` can
be used when more investigation is needed.

Approving an edit permanently makes that query identity ineligible for holdout
inclusion.

## 4. Export and apply the decisions

Click **Export Correction Reviews**. Put the downloaded JSON in the ignored
local directory `review/public_exports/`, for example:

```text
review/public_exports/musparql-sparql-correction-review-<dataset>-<time>.json
```

Do not commit this export. Apply it with:

```bash
.venv/bin/python apply_sparql_corrections.py \
  review/public_exports/musparql-sparql-correction-review-<dataset>-<time>.json \
  --holdout-selectors review/public_exports/<holdout-selectors>.jsonl
```

The command validates the export against the authoritative candidate queue and
then appends every approved correction as the next version. It refuses stale,
changed, duplicate, unknown, or holdout-selected records.

`--dry-run` is optional. Use it only if you want a validation-only rehearsal;
the normal workflow does not require running the command twice.

## 5. Re-execute approved corrections

You cannot execute a proposed version from the current UI. After applying the
export, re-run the affected KG:

```bash
.venv/bin/python run_queries.py \
  --kg-id <kg-id> \
  --holdout-selectors review/public_exports/<holdout-selectors>.jsonl
```

You do not need to know the new version number: `run_queries.py` uses `latest`
by default. The new version and hash are recorded automatically.

If the endpoint fails or is unavailable, the edit is still retained and can
return to the correction queue. It is not silently discarded. However, the
edited query remains blocked from NL generation and benchmark propagation until
that exact version and hash receives an `ok` or `empty` execution. This gate is
separate from eligibility for correction review.

## 6. Rebuild downstream data

After successful re-execution, rebuild NL inputs:

```bash
.venv/bin/python build_llm_inputs.py \
  --holdout-selectors review/public_exports/<holdout-selectors>.jsonl
```

Run generation if needed:

```bash
.venv/bin/python run_llm_generation.py
```

Build an initial NL review bundle:

```bash
.venv/bin/python build_review_bundle.py \
  --holdout-selectors review/public_exports/<holdout-selectors>.jsonl \
  --outputs llm_outputs.jsonl \
  --assert-complete-review-provenance
```

## 7. Test before committing

Run the test suite:

```bash
.venv/bin/pytest -q
```

After staging only the files you intend to commit, run the publication-boundary
check:

```bash
.venv/bin/python scripts/check_public_tree.py --staged
```

The commit and push hooks run the boundary check again automatically.
