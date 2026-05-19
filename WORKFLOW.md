# Workflow: Collecting NL–SPARQL Pairs for Musicological Knowledge Graphs

This repository provides a **reproducible pipeline for collecting, testing, and curating natural-language question–SPARQL query pairs** for musicological Knowledge Graphs (KGs). The resulting dataset is intended for evaluation, benchmarking, and downstream use in systems such as **Musparql**.

The workflow deliberately separates **configuration/selection**, **deterministic processing**, and **LLM-assisted interpretation** to support auditability and long-term maintainability.

---

## Workflow At A Glance

At a high level, the pipeline works like this:

1. Define which KGs to process in `seeds.yaml`.
2. Collect deterministic KG source snapshots into `kgs.jsonl` and `kg_sources/`, while also allowing curated local source overrides under `curated_sources/`.
3. Extract candidate SPARQL queries from repos, docs, and PDFs into `kg_queries.jsonl`.
4. Enrich those query records with nearby human-readable evidence such as comments, query descriptions, and competency questions.
5. Run the queries against endpoints or local dumps and record execution metadata.
6. Build LLM input payloads into `llm_inputs.jsonl` from the enriched query records, optionally excluding queries already dismissed in a benchmark snapshot.
7. Align SPARQL with source evidence where possible, and generate natural-language questions into `llm_outputs.jsonl` when no fitting source evidence exists.
8. Merge LLM results back into `kg_queries.jsonl` for downstream evaluation and curation.
9. Freeze review-worthy generation outputs into `runs/<run-id>/`.
10. Review examples in a lightweight human-review workbench, export reviewer decisions, and place those exports into `review/exports/`.
11. Build versioned benchmark snapshots such as `benchmark/vN/benchmark.jsonl` and `benchmark/vN/pending.jsonl` from reviewed examples.
12. Run automatic prompt/model evaluation reports under `evals/reports/<eval-id>/`, using deterministic checks plus optional LLM judging against the reviewed benchmark.

The intent is to keep every step inspectable: deterministic collection and SPARQL query execution happen first, and LLM interpretation happens only after provenance and query execution metadata are already attached.

---

## 1. Design Principles

- **YAML = selection plane**  
  Specifies processing scope and source locations

- **JSONL = curated outputs**  
  One record per line for KGs and NL–SPARQL pairs

- **Python = truth layer**  
  Repository cloning, SPARQL execution, timeouts, provenance capture, and filtering.

- **LLMs = language and interpretation layer**  
  KG descriptions, natural-language questions, confidence estimates.

- **Human review = judgment layer**  
  Reviewer decisions, notes, and rewrites are preserved as explicit artefacts rather than folded into model outputs.

- **Benchmark snapshots and eval reports = evaluation layer**  
  Approved/pending gold pairs and prompt/model comparison reports are versioned separately from both raw generations and review judgments.

- **Experiment notes = method history**  
  Pipeline variants that are tested but not necessarily adopted are recorded under `experiments/`, with links to their run snapshots, review exports, evaluation reports, and adoption decisions.

This separation reduces hidden state, supports regeneration, and preserves dataset defensibility.

---

## 2. Seed Definition (`seeds.yaml`)

**Purpose:** define which KGs to process and where their technical resources live.

Each KG entry typically includes:

- `kg_id` (stable identifier)
- human-readable name
- short `description_hint` (prompt hint, not authoritative)
- SPARQL endpoint (if available)
- repository URLs (one or more)
- optional documentation links
- optional curated local documents for repaired or manually transcribed sources
- priority and notes

Example:

    kgs:
      - kg_id: meetups
        name: Polifonia MEETUPS Knowledge Graph
        project: Polifonia
        description_hint: >
          Musical encounters and collaborations extracted from
          musician biographies (c. 1800–1945).
        sparql:
          endpoint: https://polifonia.disi.unibo.it/meetups/sparql
          auth: none
        repos:
          - https://github.com/polifonia-project/meetups-kg

`seeds.yaml` is version-controlled and changes infrequently.

If an upstream source is malformed or intermittently unavailable, add a corrected local copy under `curated_sources/` and list it in the KG's `docs:` alongside the original remote source. The pipeline ingests both; identical SPARQL queries are deduplicated later by normalized hash, while provenance from both sources is preserved.

---

## 3. Data Model (Schemas)

To make provenance, QA, human judgment, and evaluation explicit, we use **six main artefact families**:

- `kgs.jsonl`: one record per KG (metadata, endpoints, datasets)
- `kg_queries.jsonl`: one record per query (SPARQL, evidence, NL artifacts, query execution metadata)
- `runs/<run-id>/`: frozen LLM generation run snapshots
- `review/exports/*.json`: exported reviewer judgments
- `benchmark/vN/*.jsonl`: reviewed benchmark snapshots (approved, pending, dismissed)
- `evals/reports/<eval-id>/`: automatic prompt/model evaluation reports

### `kgs.jsonl` (KG metadata)

Each line is a JSON object. Example:

    {
      "kg_id": "meetups",
      "name": "Polifonia MEETUPS Knowledge Graph",
      "project": "Polifonia",
      "description": "...authoritative KG summary...",
      "sparql": {
        "endpoint": "https://polifonia.disi.unibo.it/meetups/sparql",
        "auth": "none",
        "graph": null
      },
      "dataset": {
        "dump_url": null,
        "local_path": null,
        "format": null
      },
      "repos": ["https://github.com/polifonia-project/meetups-knowledge-graph"],
      "docs": ["https://polifonia.kmi.open.ac.uk/meetups/queries.php"],
      "notes": "...",
      "created_at": "2026-01-30",
      "updated_at": "2026-01-30",
      "source_urls": [
        "https://raw.githubusercontent.com/polifonia-project/meetups-knowledge-graph/<commit>/README.md"
      ],
      "source_files": [
        "kg_sources/meetups__01__raw-githubusercontent-com.txt",
        "curated_sources/example_fixed_readme.txt"
      ],
      "source_details": [
        {
          "source_url": "https://github.com/polifonia-project/meetups-knowledge-graph",
          "resolved_url": "https://raw.githubusercontent.com/polifonia-project/meetups-knowledge-graph/<commit>/README.md",
          "repo_commit": "<commit>",
          "source_path": "README.md",
          "error": null
        },
        {
          "source_url": "curated_sources/example_fixed_readme.txt",
          "resolved_url": "curated_sources/example_fixed_readme.txt",
          "repo_commit": null,
          "source_path": "curated_sources/example_fixed_readme.txt",
          "error": null,
          "is_local_file": true
        }
      ]
    }

### `kg_queries.jsonl` (query-centric record)

One record per query, with provenance and run history:

    {
      "query_id": "musow__sha256:abc123...",
      "query_label": "musow-0001",
      "kg_id": "musow",
      "query_type": "select",
      "sparql_raw": "...as extracted...",
      "sparql_clean": "...normalized...",
      "sparql_hash": "sha256:...clean...",
      "raw_hash": "sha256:...raw...",
      "evidence": [
        {
          "evidence_id": "e1",
          "type": "repo_file",
          "source_url": "https://github.com/.../queries",
          "source_path": "docs/query1.sparql",
          "repo_commit": "abc123",
          "repo_checkout_mode": "fresh_clone|reused_local_clone",
          "repo_default_branch": "main",
          "snippet": "SELECT ...",
          "extracted_at": "2026-01-30",
          "extractor_version": "extract_queries.py@v1"
        }
        {
          "evidence_id": "e2",
          "type": "cq_item",
          "source_url": "https://github.com/.../queries",
          "source_path": "README.md",
          "repo_commit": "abc123",
          "snippet": "CQ1 - Where did the concert take place?",
          "extracted_at": "2026-01-30",
          "extractor_version": "extract_queries.py@v1"
        }
      ],
      "confidence": null,
      "llm_output": {
        "ranked_evidence_phrases": [],
        "nl_question": null,
        "nl_question_origin": {
          "mode": null,
          "evidence_ids": [],
          "primary_evidence_id": null
        },
        "confidence": null,
        "confidence_rationale": null,
        "needs_review": null
      },
      "nl_question": {
        "text": null,
        "source": null,
        "generated_at": null,
        "generator": null
      },
      "verification": {
        "status": "unverified",
        "notes": null
      },
      "latest_execution": {
        "ran_at": "2026-01-30T12:10:00Z",
        "status": "http_error",
        "endpoint": "https://polifonia.disi.unibo.it/meetups/sparql",
        "result_count": null,
        "sample_row": null,
        "duration_ms": 1820,
        "error": "http_500"
      },
      "latest_successful_execution": {
        "ran_at": "2026-01-29T10:40:00Z",
        "status": "ok",
        "endpoint": "https://polifonia.disi.unibo.it/meetups/sparql",
        "result_count": 14,
        "sample_row": {"s": "..."},
        "duration_ms": 1200
      },
      "execution_history": [
        {
          "ran_at": "2026-01-29T10:40:00Z",
          "status": "ok",
          "endpoint": "https://...",
          "duration_ms": 1200
        }
      ]
    }

Notes:

- `evidence` is the place for raw extractions of NL evidence from repos/websites/docs/papers.
- `confidence` is a combined score (LLM confidence + runnability + heuristics).
- `llm_output` stores the generated NL question, provenance, and LLM confidence.
- `latest_execution` and `latest_successful_execution` are convenience fields; `execution_history` is optional. These query execution fields are populated by `run_queries.py` in-place.
- Legacy records may use `latest_run`, `latest_successful_run`, and `run_history`; new writers preserve those aliases for compatibility while preferring the execution terminology.
- Repo-derived evidence may also record `repo_checkout_mode` and `repo_default_branch` so reuse of an existing local checkout is explicit in provenance.

### `runs/<run-id>/manifest.json` (frozen LLM generation run)

One frozen generation run captures the exact generation artefacts that became important enough to
review, compare, or report:

    {
      "run_id": "2026-04-25-sample-review-gpt5",
      "generation_run_id": "2026-04-25-sample-review-gpt5",
      "created_at": "2026-04-25T23:14:14+00:00",
      "purpose": "manual review sample",
      "notes": "Auto-frozen by build_review_bundle.py",
      "record_counts": {
        "outputs": 12,
        "errors": 0
      },
      "models": ["gpt-5"],
      "model_provenance": {
        "request_configs": [
          {
            "hash": "sha256-like-request-config-hash",
            "count": 12,
            "config": {
              "script_version": "run_llm_generation.py@v2",
              "api_method": "responses.create",
              "requested_model": "gpt-5",
              "timeout_s": 180.0,
              "prompt_hash": "...",
              "schema_hash": "...",
              "examples_hash": "...",
              "system_prompt_hash": "...",
              "generation_parameters": {
                "temperature": null,
                "top_p": null,
                "max_output_tokens": null,
                "reasoning_effort": null
              }
            }
          }
        ],
        "legacy_records_without_request_config": 0,
        "response_models": ["gpt-5"]
      },
      "files": {
        "llm_inputs": {"filename": "llm_inputs.jsonl", "sha256": "..."},
        "llm_outputs": {"filename": "llm_outputs.jsonl", "sha256": "..."},
        "prompt": {"filename": "prompt.txt", "sha256": "..."},
        "schema": {"filename": "schema.json", "sha256": "..."}
      }
    }

Notes:

- A generation run is the immutable LLM generation layer.
- One generation run can have many review exports.
- `build_review_bundle.py` should normally create or attach this generation run before review starts.
- Older manifests may only contain `run_id` and `models`; newer manifests also include `generation_run_id` and `model_provenance` so prompt/model comparisons can distinguish model aliases, request parameters, and prompt/schema/example hashes.

`runs/<run-id>/llm_outputs.jsonl` contains one output record per generated
question. New records include:

- query keys: `query_id`, `query_label`, `kg_id`
- `llm_output`: the schema-validated generated question, retained evidence, origin mode, confidence, rationale, and review flag
- `model`: requested model alias
- `run_signature`: prompt/schema/example/system/input hashes plus `request_config_hash`
- `request_config`: API method, requested model, timeout, prompt/schema/example paths and hashes, and explicit generation parameters
- `response_metadata`: API response ID and returned model name when available
- `elapsed_ms` and `generated_at`

Older output records may omit `request_config` and `response_metadata`; tooling should treat those fields as optional.

### `review/exports/*.json` (reviewer judgments)

One exported review file contains the human judgments for a specific review dataset:

    {
      "dataset_id": "830748f26ceb9031",
      "run_id": "2026-04-25-sample-review-gpt5",
      "generation_run_id": "2026-04-25-sample-review-gpt5",
      "run_ids": ["2026-04-25-sample-review-gpt5"],
      "generation_run_ids": ["2026-04-25-sample-review-gpt5"],
      "runs": [
        {
          "run_id": "2026-04-25-sample-review-gpt5",
          "generation_run_id": "2026-04-25-sample-review-gpt5",
          "manifest_path": "runs/2026-04-25-sample-review-gpt5/manifest.json",
          "purpose": "manual review sample",
          "created_at": "2026-04-25T23:14:14+00:00"
        }
      ],
      "exported_at": "2026-04-25T20:10:00Z",
      "reviews": {
        "meetups::meetups-0002::<token>": {
          "status": "approve|dismiss|needs_prompt_fix|needs_data_fix",
          "preferred_question": "",
          "note": "",
          "updated_at": "2026-04-25T20:09:00Z"
        }
      }
    }

Notes:

- Review exports preserve both approved and non-approved judgments.
- They are intentionally separate from model outputs.
- They should point to exactly one frozen generation run.
- They are the source material used to build benchmark snapshots.

### `benchmark/vN/benchmark.jsonl` (gold evaluation snapshot)

One record per benchmark item included in automatic evaluation. This file contains
approved items plus pending items that have a reviewer-supplied gold question,
with exactly one canonical `gold_question` per SPARQL query:

    {
      "benchmark_version": "v3",
      "benchmark_built_at": "2026-05-19T12:00:00+00:00",
      "benchmark_status_group": "approved|pending",
      "benchmark_id": "meetups::meetups-0002::<token>",
      "kg_id": "meetups",
      "query_id": "meetups__sha256:...",
      "query_label": "meetups-0002",
      "sparql": "...normalized SPARQL...",
      "gold_question": "Who are the two people who most frequently participated in meetups with Edward Elgar?",
      "gold_question_source": "approved_model_output|reviewer_rewrite",
      "review_status": "approve",
      "review": {
        "review_id": "meetups::meetups-0002::<token>",
        "review_export": "review/exports/....json",
        "dataset_id": "<review-dataset-id>",
        "note": "",
        "updated_at": "2026-04-25T21:00:00Z"
      },
      "run": {
        "generation_run_id": "2026-04-25-sample-review-gpt5",
        "run_label": "llm_outputs.sample_current",
        "source_file": "llm_outputs.sample_current.jsonl",
        "model": "gpt-5",
        "run_signature": {"model": "gpt-5", "...": "..."}
      },
      "evidence_summary": {
        "evidence_count": 41,
        "evidence_types": ["cq_item", "query_comment"],
        "has_source_evidence": true,
        "has_query_specific_evidence": true
      }
    }

Notes:

- Benchmark snapshots are built from review exports, not directly from raw model output files.
- `gold_question` is the single canonical wording used for evaluation.
- `approved.jsonl` preserves detailed approved records, including reviewed model output.
- `pending.jsonl` preserves detailed pending records, including reviewed model output.
- `dismissed.jsonl` preserves excluded records for audit and future input exclusion.

### `evals/reports/<eval-id>/` (automatic evaluation report)

One evaluation report compares one or more frozen generation runs against a benchmark
snapshot:

    {
      "created_at": "2026-05-14T12:00:00+00:00",
      "script_version": "evaluate_runs.py@v1",
      "benchmark": "benchmark/v2",
      "benchmark_counts": {
        "approved": 41,
        "pending": 9,
        "dismissed": 4
      },
      "scored_status_groups": ["approved", "pending"],
      "dismissed_excluded": 4,
      "runs": [
        {
          "run_id": "2026-04-26-full-review-gpt5",
          "generation_run_id": "2026-04-26-full-review-gpt5",
          "path": "runs/2026-04-26-full-review-gpt5",
          "output_count": 149,
          "input_count": 149,
          "manifest": {"...": "..."}
        }
      ],
      "baseline_run_id": "2026-04-26-full-review-gpt5",
      "baseline_generation_run_id": "2026-04-26-full-review-gpt5",
      "judge": {
        "enabled": true,
        "model": "gpt-5",
        "prompt_hash": "...",
        "timeout_s": 120.0
      },
      "summary": {"...": "..."}
    }

`scores.jsonl` contains one score record per benchmark item per run, including:

- deterministic `errors` and `warnings`
- `sparql_match` and `sparql_mismatch` compatibility warning when applicable
- candidate, gold, and baseline questions
- judge status and cached judge result when semantic judging is enabled
- generation run signature and request configuration when present

Notes:

- Approved and pending benchmark records are scored by default.
- Dismissed records are excluded from semantic scoring.
- SPARQL mismatch is a deterministic provenance warning, not part of LLM quality scoring.


---

## 4. KG Source Capture (`kgs.jsonl`)

**Objective:** produce KG records with deterministic source snapshots and explicit provenance.

### Inputs

- `seeds.yaml`
- KG README files
- project websites
- related academic papers (abstracts or introductions)

### Process

For each KG:

1. Collect textual sources deterministically.
2. Resolve GitHub README and GitHub `blob/...` documentation URLs to commit-pinned raw URLs when possible.
3. Save source snapshots under `kg_sources/`.
4. Record provenance in `kgs.jsonl` via `source_urls`, `source_files`, and `source_details`.

Generated KG descriptions are an optional downstream step, not part of the current deterministic `build_kgs.py` implementation.

### Output

`kgs.jsonl`, one KG per line, for example:

    {
      "kg_id": "meetups",
      "name": "Polifonia MEETUPS Knowledge Graph",
      "description": "...",
      "sparql": {
        "endpoint": "https://...",
        "auth": "none",
        "graph": null
      },
      "repos": ["https://github.com/..."],
      "source_details": [
        {
          "source_url": "https://github.com/...",
          "resolved_url": "https://raw.githubusercontent.com/.../<commit>/README.md",
          "repo_commit": "<commit>",
          "source_path": "README.md",
          "error": null
        }
      ]
    }

---

## 5. SPARQL Query Extraction (`kg_queries.jsonl`)

**Objective:** collect all candidate SPARQL queries with full provenance, without interpretation.

### Inputs

- repositories listed in `seeds.yaml`
- documentation pages with example queries
- academic papers containing SPARQL or competency questions (CQs)

### Process 

- Clone repositories.
- Record whether each query came from a fresh clone or a reused local checkout.
- Extract:
  - `.rq` and `.sparql` files
  - embedded SPARQL in code or documentation
- Normalise whitespace and prefixes.
- Deduplicate by hash.
- Record provenance:
  - repository URL
  - file path
  - commit hash
  - checkout mode (`fresh_clone` or `reused_local_clone`)
  - repository default branch (when available)
  - line spans (if available)

### Output

`kg_queries.jsonl` (query records with raw SPARQL, clean SPARQL, and evidence)

No filtering or LLM use occurs at this stage.

### Tests

SPARQL extraction helpers are covered by standard-library unit tests:

```bash
.venv/bin/python -m unittest tests/test_extract_queries.py
```

The tests use small synthetic fixtures for Markdown fences, HTML/pre blocks,
multiple queries in one file, prefix normalization, malformed/non-SELECT
rejection, and PDF-like broken `PREFIX` / IRI line wrapping. These tests are
intended to protect extractor behavior directly; a separate golden mini-corpus
can be added later for end-to-end extraction drift checks.

---

## 6. Evidence Enrichment (`kg_queries.jsonl`)

**Objective:** enrich query records with human-readable evidence from sources, preserving provenance and evidence types.

### Inputs

- `kg_queries.jsonl`
- repositories listed in `seeds.yaml`
- documentation pages and websites (optional)
- academic papers (PDFs, optional)

### Process (deterministic)

Extraction targets and evidence types:

- **Repo files**
  - `.rq`/`.sparql` comments directly above queries → `query_comment`
  - fenced `sparql` blocks in Markdown with the nearest preceding paragraph → `readme_query_desc`
  - raw file provenance → `repo_file`
- **Web/docs (HTML/MD)**
  - fenced/preformatted `sparql` blocks with the nearest preceding text block → `web_query_desc` / `doc_query_desc`
  - competency questions listed in headings, bullet lists, or tables → `cq_item`
- **PDF papers**
  - SPARQL code blocks in running text → `doc_query_desc` from the nearest preceding paragraphs
  - SPARQL code embedded in tables/figures/algorithms → capture the table/figure/algorithm as a query; attach the caption as `doc_query_desc`
  - competency question sections or tables (including captioned tables) → `cq_item`

All evidence items carry `evidence_id`, `source_url`, `source_path`, timestamps, and extractor version metadata. Repo-derived evidence also carries `repo_commit`, and may carry `repo_checkout_mode` and `repo_default_branch`.

### Output

`kg_queries.jsonl` (updated in-place with evidence items)

### Tests

Evidence and CQ enrichment helpers are covered by standard-library unit tests:

```bash
.venv/bin/python -m unittest tests/test_enrich_evidence.py
```

The tests cover query comments, nearest prose descriptions for Markdown and
HTML/pre query blocks, competency-question tables, competency-question bullet
lists, duplicate evidence suppression, and removal of SPARQL-like lines from
natural-language evidence snippets.

---

## 7. Academic Paper Integration (Parallel Track)

Academic papers belong conceptually to the same source-acquisition layer as query extraction and evidence enrichment.

For each KG:

- Identify canonical papers.
- Extract:
  - SPARQL examples
  - competency questions (CQs), e.g. from headers, tables and figure captions.

Paper-derived material is then added to queries and evidence in the same way as repo-derived material. Unit tests include this functionality.

---

## 8. Query Execution Metadata (`kg_queries.jsonl`)

**Objective:** record execution metadata for queries against endpoints or local dumps.

### Inputs

- `kg_queries.jsonl`
- SPARQL endpoints from `seeds.yaml` (plus fallbacks, if configured)
- local dataset dumps when no endpoint is available

### Process (deterministic)

For each query:

- Execute against the endpoint with a timeout; if configured, attempt fallbacks.
- For local dumps:
  - load the dump into an in-process SPARQL engine
  - execute the query against the local dataset
- Record:
  - execution status (`ok`, `empty`, `timeout`, `parse_error`, `auth`, etc.)
  - timestamp
  - endpoint used (or local dump path)
  - optional first result row
- Store `latest_execution`, `latest_successful_execution`, and append to `execution_history`.
- For backward compatibility, also update legacy aliases `latest_run`, `latest_successful_run`, and `run_history`.

### Output

`kg_queries.jsonl` (updated in-place with query execution metadata)

This step establishes **ground-truth executability** for each query record.

---

## 9. Natural-Language Question And Confidence Generation

**Objective:** align SPARQL with source evidence when possible, and otherwise generate human-readable NL–SPARQL pairs with confidence estimates.

### Inputs

- `kg_queries.jsonl`
- KG descriptions from `kgs.jsonl`
- optional sample result rows
- prompt + schema files in `prompts/` and `schemas/`

### Process (LLM with schema enforcement)

1. Build inputs with `build_llm_inputs.py` → `llm_inputs.jsonl`.
2. Run LLM alignment/generation with `run_llm_generation.py` → `llm_outputs.jsonl`.
3. Merge outputs with `merge_llm_outputs.py` → `kg_queries.jsonl` (updates in-place).

Current implementation notes:

- `run_llm_generation.py` defaults to `llm_inputs.jsonl`.
- When a benchmark already identifies queries that are unsuitable for NL generation, pass
  `--exclude-dismissed-benchmark benchmark/vN` to `build_llm_inputs.py`. This uses
  `benchmark/vN/dismissed.jsonl` as a deterministic query exclusion list.
- `llm_outputs.jsonl` is treated as JSONL; legacy JSON-array files are normalized to JSONL on read.
- Output records carry a `run_signature` containing hashes of the effective prompt/schema/examples/input configuration.
- New output records also carry `request_config`, which records:
  - script version
  - API method (`responses.create`)
  - requested model alias
  - timeout
  - input/prompt/schema/example paths
  - prompt/schema/examples/system-prompt hashes
  - explicit generation parameters such as temperature, top-p, max output tokens, and reasoning effort
- `response_metadata` records response-level metadata exposed by the API, such as response ID and returned model name when available.
- Resume/skip behavior uses `query_id`, `query_label`, `kg_id`, `model`, `system_prompt_hash`, `input_hash`, and, for new records, `request_config_hash`.
- Older runs remain readable even when they only contain `model` and `run_signature`.

For each runnable query, generate an object of the following form (stored in `llm_output`):

    {
      "ranked_evidence_phrases": [
        {
          "text": "...",
          "evidence_id": "e12",
          "source_type": "query_comment",
          "rank": 1,
          "verbatim": true
        }
      ],
      "nl_question": "...",
      "nl_question_origin": {
        "mode": "verbatim|paraphrased|generated",
        "evidence_ids": ["e12", "e7"],
        "primary_evidence_id": "e12"
      },
      "confidence": 92,
      "confidence_rationale": "...",
      "needs_review": false
    }

Guidelines:

- Prefer **clear, concise phrasing**.
- Avoid ontology jargon unless unavoidable.
- Lower confidence if semantics are ambiguous.

### Evidence prioritization for LLM input

Provide the full evidence list to the LLM and specify a preference order by type:

1. `query_comment` (SPARQL comments)
2. `doc_query_desc` / `web_query_desc` / `readme_query_desc`
3. `cq_item`
4. general KG descriptions (`kg_summary`, `doc_summary`, `readme_summary`, `web_summary`, `repo_summary`)

A second **consistency-check pass** may be applied to downgrade overconfident pairs.

### Output

`llm_outputs.jsonl` (versioned JSONL LLM results with `run_signature`) and `kg_queries.jsonl` (updated in-place).

---

## 9A. Experiment History

**Objective:** preserve the outcome of pipeline variants without conflating method
adoption with benchmark curation.

When a run changes extraction, enrichment, prompting, model choice, or evaluation
procedure, add an experiment note under `experiments/`. The note should link to
the implementation commit, generated run, review export, evaluation report, and
any benchmark snapshot produced from the review.

The benchmark may still be updated from a useful review even when the tested
method is not adopted as a default pipeline step.

---

## 10. Frozen Generation Run Capture

**Objective:** preserve review-worthy LLM-generation artefacts in an immutable form before human validation begins.

### Inputs

- `llm_inputs.jsonl`
- one LLM output file to review, such as `llm_outputs.jsonl`
- optional `llm_outputs.errors.jsonl`

### Process

1. Freeze the generation artefacts into `runs/<run-id>/`.
2. Copy the inputs, outputs, prompt, schema, and any relevant source snapshots needed for later traceability.
3. Write `runs/<run-id>/manifest.json` with file hashes, model list, counts, and purpose metadata.
4. Use the frozen generation run as the review target from this point onward.

### Notes

- A generation run is the immutable generation layer.
- One generation run can later accumulate multiple review exports.
- `build_review_bundle.py` can create this generation run automatically when the chosen output is not already inside `runs/<run-id>/`.
- For new runs, `manifest.json` includes `model_provenance`, summarising distinct request configurations, response model names when available, and the count of legacy records without `request_config`.

### Output

- `runs/<run-id>/manifest.json`
- copied generation artefacts inside `runs/<run-id>/`

---

## 11. Human Review

**Objective:** inspect generated NL–SPARQL pairs and capture human judgments in a reusable, versionable form.

### Inputs

- `llm_inputs.jsonl`
- one or more LLM output files such as `llm_outputs.jsonl`
- optional prior reviewer exports in `review/exports/`

### Process

1. For a first review of a run, build a browser review bundle with `build_review_bundle.py` → `review/review_data.js`.
   - If the selected outputs are not already inside `runs/<run-id>/`, the builder should auto-freeze a run snapshot first.
2. Open `review/index.html` through a local web server.
3. Inspect examples with:
   - formatted SPARQL
   - retained evidence phrases
   - full input evidence
   - generated NL question
   - origin mode, confidence, and rationale
4. Record reviewer decisions and optional rewrites.
5. Export reviewer judgments as JSON.
6. Place the exported review file under `review/exports/` so it can be reused for benchmark construction and later prompt/model comparisons.

For later review rounds after changing extraction, enrichment, prompts, or
models, build a compare-review bundle instead:

```bash
.venv/bin/python build_next_review_round.py \
  --previous-run runs/<old-run-id> \
  --current-run runs/<new-run-id> \
  --previous-reviews review/exports/<previous-review-export>.json
```

`--current-run` may also point at a current output file such as
`llm_outputs.jsonl`, which is the default. Compare mode shows added, removed,
and review-worthy changed pairs unless `--include-unchanged` is passed.
Question, origin, retained-evidence, and SPARQL changes are review-worthy.
Confidence, rationale, model, and full-input evidence changes are treated as
metadata-only unless one of the review-worthy fields also changed; pass
`--include-metadata-only` to audit those records. Pairs dismissed in the
previous review export are excluded by default; pass `--include-dismissed` only
when intentionally revisiting those decisions. The UI presents
previous and current records side by side, including SPARQL, generated question,
retained evidence, reviewer choices, preferred wording, and notes. Previous
review decisions are read-only context; the exported compare review contains the
new decisions for the current run.

Current reviewer labels:

- `approve`: keep this example in the benchmark as-is
- `dismiss`: exclude this example from the benchmark, future compare-review queues, and future LLM input generation when dismissed benchmark exclusions are enabled
- `needs_prompt_fix`: example is valid, but model behavior should improve through prompt changes
- `needs_data_fix`: example may be valid, but the model inputs are wrong, incomplete, noisy, or missing key signals

### Notes

- Reviewer judgments are kept separate from model outputs.
- Review exports are keyed to the review dataset and the underlying run provenance, so prompt/model changes naturally produce a new review set.
- One review export should correspond to exactly one run, but one run may accumulate multiple review exports from different reviewers or sessions.
- Compare-review exports are keyed to a comparison dataset and should be applied
  with the compare bundle they were exported from.
- In practice this stage forms an iteration loop:
  1. inspect examples
  2. approve or flag them
  3. improve prompt or enrichment
  4. rerun generation
  5. compare against the reviewed subset

### Output

- `review/review_data.js` – browser-friendly review bundle
- `review/exports/*.json` – reviewer judgments, notes, and preferred rewrites

---

## 12. Benchmark Construction

**Objective:** convert reviewed examples into versioned benchmark snapshots that can be used for prompt comparison, model evaluation, and downstream experiments.

### Inputs

- `review/review_data.js`
- one exported reviewer file from `review/exports/`

### Process

1. For a first reviewed run, build a benchmark snapshot with `benchmark/build_benchmark.py`.
2. Create a versioned directory such as `benchmark/v1/`.
3. Split reviewed items into:
   - approved records
   - pending items that still need prompt/data fixes
   - dismissed items excluded from the benchmark
4. Build `benchmark.jsonl` from approved records plus pending records that have reviewer-supplied gold questions.
5. Set `gold_question` using:
   - reviewer rewrite, if present
   - otherwise the approved model output
6. Preserve provenance linking each benchmark item back to:
   - query identifiers
   - review export
   - generation run metadata

For later review rounds, apply a compare-review export to the previous benchmark
snapshot:

```bash
.venv/bin/python benchmark/update_benchmark.py \
  --previous-benchmark benchmark/v1 \
  --bundle review/review_data.js \
  --reviews review/exports/<compare-review-export>.json \
  --outdir benchmark/v2
```

The update routine carries forward unchanged previous benchmark records and
replaces only pairs that received decisions in the compare review. Approved
current records enter `approved.jsonl`, dismissed records enter `dismissed.jsonl`,
and `needs_prompt_fix` / `needs_data_fix` records enter `pending.jsonl`.
`benchmark.jsonl` is then extracted as the combined evaluation set.

### Output

- `benchmark/vN/manifest.json` – snapshot metadata and counts
- `benchmark/vN/benchmark.jsonl` – combined gold evaluation pairs: approved plus pending records with reviewer-supplied gold questions
- `benchmark/vN/approved.jsonl` – detailed approved records
- `benchmark/vN/pending.jsonl` – detailed reviewed but not yet benchmark-approved items
- `benchmark/vN/dismissed.jsonl` – reviewed items explicitly excluded from the benchmark; this file can also be used as the exclusion list for future generation inputs

### Notes

- The benchmark is distinct from both raw model outputs and review exports.
- Review exports capture human judgments; benchmark snapshots capture the current curated gold set.
- Dismissed records are still preserved with provenance so the exclusion is auditable and reversible.
- Compare-review exports are update instructions for a benchmark version, not a
  full benchmark by themselves.
- This separation makes it possible to compare multiple prompt/model runs against the same approved benchmark, while preserving reviewer provenance and benchmark history.

---

## 13. Automatic Prompt/Model Evaluation

**Objective:** compare prompt and model runs against a reviewed benchmark without
requiring immediate human review of every changed output.

### Inputs

- one benchmark snapshot such as `benchmark/v2/`
- one or more frozen LLM generation runs under `runs/<run-id>/`
- optional baseline run
- optional LLM judge model

### Process

Run deterministic-only checks:

```bash
.venv/bin/python evals/evaluate_runs.py \
  --benchmark benchmark/v2 \
  --runs runs/<candidate-run> \
  --baseline runs/<baseline-run> \
  --skip-judge \
  --out evals/reports/<eval-id>
```

Run semantic evaluation with an LLM judge:

```bash
.venv/bin/python evals/evaluate_runs.py \
  --benchmark benchmark/v2 \
  --runs runs/<baseline-run> runs/<candidate-run> \
  --baseline runs/<baseline-run> \
  --judge-model gpt-5 \
  --out evals/reports/<eval-id>
```

Evaluation uses the combined gold benchmark by default:

- `benchmark.jsonl` items are approved pairs plus pending pairs with reviewer-supplied gold questions.
- `approved.jsonl` and `pending.jsonl` keep the detailed review records for audit and review workflows.
- `dismissed.jsonl` items are excluded from semantic scoring.

### Scoring Policy

- SPARQL is treated as fixed input. The system does not generate new SPARQL.
- A benchmark/run SPARQL mismatch is reported as a deterministic provenance warning (`sparql_mismatch`), not as model-quality failure.
- If SPARQL mismatches or input provenance is missing, semantic judge scoring is skipped for that item.
- Deterministic checks cover coverage, output shape, non-empty questions, placeholder leakage, evidence ID integrity, and question changes against the baseline.
- LLM judging is used only for semantic question quality: faithfulness to the fixed SPARQL and equivalence to the gold question.
- Judge results are cached in `judge_cache.jsonl` so repeated comparisons do not re-score unchanged triples of SPARQL, gold question, and candidate question.

### Output

- `evals/reports/<eval-id>/manifest.json` – inputs, generation run metadata, judge configuration, and aggregate summary
- `evals/reports/<eval-id>/scores.jsonl` – one score record per benchmark item per run
- `evals/reports/<eval-id>/summary.md` – human-readable report
- `evals/reports/<eval-id>/judge_cache.jsonl` – cached semantic judge results

### Notes

- Offline reports are the first evaluation layer; they are not automatically CI gates.
- CI gating should start with deterministic checks only, such as unit tests, schema checks, and SPARQL compatibility warnings.
- LLM judge scores should be calibrated against human review before they are used as blocking quality thresholds.

---

## 14. Outputs And Intended Use

At minimum, the project produces:

- `seeds.yaml` – configuration input
- `kgs.jsonl` – KG catalogue 
- `kg_queries.jsonl` – validated queries with execution metadata and `llm_output`
- `llm_inputs.jsonl` – LLM input payloads
- `llm_outputs.jsonl` – LLM outputs (before merge)
- `runs/<run-id>/manifest.json` – frozen generation run metadata and copied generation artefacts
- `review/review_data.js` – local reviewer bundle
- `review/exports/*.json` – exported human-review judgments
- `benchmark/vN/benchmark.jsonl` – combined gold evaluation pairs
- `benchmark/vN/approved.jsonl` – detailed approved records
- `benchmark/vN/pending.jsonl` – detailed reviewed items pending fixes
- `benchmark/vN/manifest.json` – benchmark snapshot metadata
- `evals/reports/<eval-id>/` – automatic prompt/model evaluation reports

These outputs may be:

- ingested into Musparql
- used for evaluation or benchmarking
- published as a dataset
- extended with additional KGs

---

## 15. Rationale

- Every artefact is reproducible.
- Every query is runnable or explicitly marked otherwise.
- Every NL question has an explicit confidence estimate.
- Human review is versionable and separable from raw model output.
- Benchmark snapshots are versionable and separable from both review judgments and raw model output.
- Provenance is preserved end-to-end.
- LLM use is restricted to tasks where it adds value (language, summarisation).
