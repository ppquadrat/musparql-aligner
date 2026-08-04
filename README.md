## Musparql: automatically extracting human language question - sparql query pairs from KGs

Following the workflow described in WORKFLOW.md. See Appendix A there for the
JSONL data model reference.

Documentation entry points:

- `WORKFLOW.md`: end-to-end collection, generation, review, curation, audit,
  and release flow.
- `HOLDOUT_SECURITY.md`: what the evaluation holdout protects, the trust model,
  and the safeguards in place.
- `HOLDOUT_RUNBOOK.md`: the human checklist for selecting, exporting, clearing,
  and publishing around holdout records.
- `review/README.md`: review-workbench operation and review-field semantics.
- `benchmark/README.md`: snapshot schemas, update policy, audits, and
  public-release packaging.

Source identities and provenance are defined in `sources.yaml`; `seeds.yaml`
selects those sources for each knowledge graph. Local papers and curated files
must retain an external URL, a derivation reference, or an explicit provenance
description. Validate the catalogue with:

```bash
.venv/bin/pytest -q tests/test_source_provenance.py
```

Use `normalize_source_provenance.py` only to migrate existing artefacts to
catalogue IDs. It preserves captured or commit-pinned `source_url` values and
stores the catalogue locator separately as `source_catalog_url`.

SPARQL corrections are retained as append-only versions. Version `0` is always
the normalized source query; execution history, prompt inputs, and new benchmark
records identify the selected version and hash. See “Query Execution Metadata”
and the `kg_queries.jsonl` model in `WORKFLOW.md`.

This project uses OpenAI API with input/output sharing enabled to save costs.
