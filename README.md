## Musparql: automatically extracting human language question - sparql query pairs from KGs

Following the workflow described in WORKFLOW.md. See Appendix A there for the
JSONL data model reference.

Documentation entry points:

- `WORKFLOW.md`: end-to-end collection, generation, review, curation, audit,
  and release flow.
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

This project uses OpenAI API with input/output sharing enabled to save costs.
