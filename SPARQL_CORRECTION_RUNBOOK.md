# SPARQL correction runbook

Use this procedure after query execution. Policy and rationale are in
[SPARQL_EDITING_POLICY.md](SPARQL_EDITING_POLICY.md).

## 1. Capture candidates automatically

Every `run_queries.py` invocation requires `--holdout-selectors PATH` or the
human assertion `--no-holdout`, then updates
`sparql_correction_candidates.jsonl`. Scoped runs replace candidate state only
for the `(kg_id, query_id, sparql_version)` jobs they attempted and preserve unrelated
queue entries. The queue is diagnostic; it does not assert that every row needs
an edit. Selected holdout identities are excluded before any candidate evidence
or execution detail is written.

## 2. Build the correction workbench

Choose the same holdout policy used by downstream development workflows:

```bash
.venv/bin/python build_sparql_correction_bundle.py --no-holdout
```

After holdouts exist, use either an annotation-free selector or the
identity-private upstream assertion. Serve `review/` on loopback and open
`http://127.0.0.1:8000/corrections.html`.

## 3. Review each candidate

1. Read the automatic category, endpoint observation, base version/hash, and evidence.
2. Decide `Approve New Version`, `No SPARQL Edit`, or `Defer`.
3. For approval, provide the edit type, complete SPARQL, rationale, and evidence IDs.
4. If an agent proposed it, record `Agent` and the model/tool identifier.
5. Remember that approval permanently makes the query identity holdout-ineligible.

Export **Correction Reviews**. This mode has no holdout control or private export.

## 4. Validate and apply append-only

```bash
.venv/bin/python apply_sparql_corrections.py \
  path/to/musparql-sparql-correction-review-*.json \
  --no-holdout \
  --dry-run
```

Then rerun without `--dry-run`. The command loads the authoritative candidate
ledger by default (override with `--candidates PATH`) and rejects unknown queries, duplicate
decisions, unchanged proposals, selected holdout identities, and stale base
versions/hashes or candidate digests. Approved edits become version `N+1`; all decisions are retained
in `sparql_correction_history`.

## 5. Verify and propagate

1. Execute the approved version with `run_queries.py --sparql-version N` and an explicit holdout policy.
2. Compare original and approved observations in the same environment.
3. Confirm semantic alignment, not merely parser acceptance.
4. Rebuild LLM inputs and generation/review bundles.
5. Run the full tests and public-boundary checks before committing.

If intended semantics change, classify the edit as benchmark specialization or
a new source query rather than presenting it as a transparent syntax fix.
