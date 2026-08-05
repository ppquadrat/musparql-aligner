# Musparql

Musparql is a human-curated workflow for building natural-language–SPARQL
benchmark pairs from existing domain knowledge graphs. The current benchmark
uses music knowledge graphs, but the workflow is intended to transfer to other
research domains.

The project starts from SPARQL written or published by people. It collects the
query, its source, and nearby human-language evidence; uses an LLM to align that
evidence or propose wording where evidence is missing; and asks a human reviewer
to decide what belongs in the benchmark. Generated wording never becomes gold
data without human review.

The current public benchmark is under [`benchmark/v8`](benchmark/v8). It
contains 100 reviewed pairs from Musical Meetups, the Jazz Ontology, MusOW,
Organs, and LinkedMusic.

## How the system fits together

The workflow has four stages:

1. **Acquire sources and queries.** Musparql records source provenance, captures
   stable source snapshots, and extracts existing SPARQL. It does not generate
   benchmark queries.
2. **Align evidence and formulate questions.** Existing descriptions and
   competency questions are preferred. An LLM may align or paraphrase them, or
   formulate a provisional question when no suitable wording exists.
3. **Review.** A human checks whether the query expresses a meaningful
   information need, whether the question matches it, and whether either needs
   correction. Query execution is diagnostic evidence, not an inclusion rule.
4. **Publish.** A versioned benchmark snapshot keeps the compact scoring pairs
   separate from provenance, alternatives, internal review state, and any
   private holdout.

That separation is the core architecture of Musparql. Source evidence, model
assistance, execution observations, human judgment, and published benchmark
data remain distinguishable instead of being flattened into one file.

## Repository map

- `catalog/` contains tracked source definitions, captured source text,
  curated inputs, reference PDFs, and the KG catalogue.
- `src/musparql/` contains reusable Python implementation.
- `scripts/` contains commands for collection, generation, review, benchmark
  maintenance, evaluation, and migration.
- `review/` contains the browser review applications.
- `benchmark/` contains versioned benchmark snapshots and schemas.
- `prompts/` and `schemas/` contain tracked model instructions and data
  contracts.
- `docs/` contains policies, runbooks, the workflow, experiments, and papers.
- `var/` contains persistent local working state. It is ignored by Git.
- `build/` contains reproducible, disposable publication output. It is ignored
  by Git.

The private holdout is not stored in this repository. The annotation-free,
identity-visible selector used to exclude holdout pairs lives locally at
`var/holdout/selectors.jsonl`.

## Documentation

- [`docs/WORKFLOW.md`](docs/WORKFLOW.md) — readable end-to-end workflow.
- [`docs/DATA_MODEL.md`](docs/DATA_MODEL.md) — artifact and field reference.
- [`docs/PIPELINE_POLICY.md`](docs/PIPELINE_POLICY.md) — rules that pipeline
  changes and runs must preserve.
- [`docs/PIPELINE_RUNBOOK.md`](docs/PIPELINE_RUNBOOK.md) — commands for running
  the pipeline.
- [`docs/REVIEW_POLICY.md`](docs/REVIEW_POLICY.md) — what human review decides
  and how review data is handled.
- [`docs/REVIEW_RUNBOOK.md`](docs/REVIEW_RUNBOOK.md) — initial and comparative
  review procedure.
- [`docs/HOLDOUT_SECURITY.md`](docs/HOLDOUT_SECURITY.md) and
  [`docs/HOLDOUT_RUNBOOK.md`](docs/HOLDOUT_RUNBOOK.md) — holdout boundary and
  human-only procedure.
- [`docs/SPARQL_EDITING_POLICY.md`](docs/SPARQL_EDITING_POLICY.md) and
  [`docs/SPARQL_CORRECTION_RUNBOOK.md`](docs/SPARQL_CORRECTION_RUNBOOK.md) —
  append-only SPARQL correction rules and operation.
- [`benchmark/README.md`](benchmark/README.md) — benchmark snapshot and release
  details.
- [`review/README.md`](review/README.md) — review application reference.

## Development setup

Create or activate a virtual environment, install the repository package in
editable mode, and run the tests:

```bash
.venv/bin/pip install --no-build-isolation -e .
.venv/bin/python -m pytest -q
```

Commands are run from the repository root as Python modules, for example:

```bash
.venv/bin/python -m scripts.build_kgs
.venv/bin/python -m scripts.extract_queries
```

The complete sequence and required holdout-handling flags are in the pipeline
runbook.
