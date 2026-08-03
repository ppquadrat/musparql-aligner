## Musparql: automatically extracting human language question - sparql query pairs from KGs

Following the workflow described in WORKFLOW.md. See Appendix A there for the
JSONL data model reference.

Source identities and provenance are defined in `sources.yaml`; `seeds.yaml`
selects those sources for each knowledge graph. Local papers and curated files
must retain an external URL, a derivation reference, or an explicit provenance
description. Validate the catalogue with:

```bash
.venv/bin/pytest -q tests/test_source_provenance.py
```

This project uses OpenAI API with input/output sharing enabled to save costs.
