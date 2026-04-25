# Workflow: Collecting NL–SPARQL Pairs for Musicological Knowledge Graphs

This repository provides a **reproducible pipeline for collecting, testing, and curating natural-language question–SPARQL query pairs** for musicological Knowledge Graphs (KGs). The resulting dataset is intended for evaluation, benchmarking, and downstream use in systems such as **Musparql**.

The workflow deliberately separates **configuration/selection**, **deterministic processing**, and **LLM-assisted interpretation** to support auditability and long-term maintainability.

---

## Workflow At A Glance

At a high level, the pipeline works like this:

1. Define which KGs to process in `seeds.yaml`.
2. Collect deterministic KG source snapshots into `kgs.jsonl` and `kg_sources/`, with provenance attached.
3. Extract candidate SPARQL queries from repos, docs, and PDFs into `kg_queries.jsonl`.
4. Enrich those query records with nearby human-readable evidence such as comments, query descriptions, and competency questions.
5. Run the queries against endpoints or local dumps and record execution metadata.
6. Build LLM input payloads into `llm_inputs.jsonl` from the enriched query records.
7. Align SPARQL with source evidence where possible, and generate natural-language questions into `llm_outputs.jsonl` when no fitting source evidence exists.
8. Merge LLM results back into `kg_queries.jsonl` for downstream evaluation and curation.
9. Freeze review-worthy generation outputs into `runs/<run-id>/`.
10. Review examples in a lightweight human-review workbench, export reviewer decisions, and place those exports into `review/exports/`.
11. Build versioned benchmark snapshots such as `benchmark/vN/benchmark.jsonl` and `benchmark/vN/pending.jsonl` from reviewed examples.

The intent is to keep every step inspectable: deterministic collection and execution happen first, and LLM interpretation happens only after provenance and run metadata are already attached.

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

- **Benchmark snapshots = evaluation layer**  
  Approved gold pairs are versioned separately from both raw generations and review judgments.

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

---

## 3. Data Model (Schemas)

To make provenance, QA, human judgment, and evaluation explicit, we use **five main artefact families**:

- `kgs.jsonl`: one record per KG (metadata, endpoints, datasets)
- `kg_queries.jsonl`: one record per query (SPARQL, evidence, NL artifacts, run metadata)
- `runs/<run-id>/`: frozen LLM-generation runs
- `review/exports/*.json`: exported reviewer judgments
- `benchmark/vN/*.jsonl`: reviewed benchmark snapshots (approved, pending, dismissed)

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
        "kg_sources/meetups__01__raw-githubusercontent-com.txt"
      ],
      "source_details": [
        {
          "source_url": "https://github.com/polifonia-project/meetups-knowledge-graph",
          "resolved_url": "https://raw.githubusercontent.com/polifonia-project/meetups-knowledge-graph/<commit>/README.md",
          "repo_commit": "<commit>",
          "source_path": "README.md",
          "error": null
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
      "latest_run": {
        "ran_at": "2026-01-30T12:10:00Z",
        "status": "http_error",
        "endpoint": "https://polifonia.disi.unibo.it/meetups/sparql",
        "result_count": null,
        "sample_row": null,
        "duration_ms": 1820,
        "error": "http_500"
      },
      "latest_successful_run": {
        "ran_at": "2026-01-29T10:40:00Z",
        "status": "ok",
        "endpoint": "https://polifonia.disi.unibo.it/meetups/sparql",
        "result_count": 14,
        "sample_row": {"s": "..."},
        "duration_ms": 1200
      },
      "run_history": [
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
- `latest_run` and `latest_successful_run` are convenience fields; `run_history` is optional. These run-related fields are populated by `run_queries.py` in-place.
- Repo-derived evidence may also record `repo_checkout_mode` and `repo_default_branch` so reuse of an existing local checkout is explicit in provenance.

### `runs/<run-id>/manifest.json` (frozen generation run)

One frozen run captures the exact generation artefacts that became important enough to
review, compare, or report:

    {
      "run_id": "2026-04-25-sample-review-gpt5",
      "created_at": "2026-04-25T23:14:14+00:00",
      "purpose": "manual review sample",
      "notes": "Auto-frozen by build_review_bundle.py",
      "record_counts": {
        "outputs": 12,
        "errors": 0
      },
      "models": ["gpt-5"],
      "files": {
        "llm_inputs": {"filename": "llm_inputs.jsonl", "sha256": "..."},
        "llm_outputs": {"filename": "llm_outputs.jsonl", "sha256": "..."},
        "prompt": {"filename": "prompt.txt", "sha256": "..."},
        "schema": {"filename": "schema.json", "sha256": "..."}
      }
    }

Notes:

- A run is the immutable generation layer.
- One run can have many review exports.
- `build_review_bundle.py` should normally create or attach this run before review starts.

### `review/exports/*.json` (reviewer judgments)

One exported review file contains the human judgments for a specific review dataset:

    {
      "dataset_id": "830748f26ceb9031",
      "run_id": "2026-04-25-sample-review-gpt5",
      "run_ids": ["2026-04-25-sample-review-gpt5"],
      "runs": [
        {
          "run_id": "2026-04-25-sample-review-gpt5",
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
- They should point to exactly one frozen run.
- They are the source material used to build benchmark snapshots.

### `benchmark/vN/benchmark.jsonl` (approved benchmark snapshot)

One record per approved benchmark item:

    {
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
        "run_label": "llm_outputs.sample_current",
        "source_file": "llm_outputs.sample_current.jsonl",
        "model": "gpt-5",
        "run_signature": {"model": "gpt-5", "...": "..."}
      },
      "model_output": {
        "nl_question": "...model wording...",
        "origin_mode": "generated|paraphrased|verbatim",
        "confidence": 82,
        "confidence_rationale": "...",
        "needs_review": false,
        "retained_evidence_phrases": []
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
- `pending.jsonl` and `dismissed.jsonl` use the same broad structure but capture non-approved review outcomes.


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

---

## 6. Academic Paper Integration (Parallel Track)

Academic papers belong conceptually to the same source-acquisition layer as query extraction and evidence enrichment.

For each KG:

- Identify canonical papers.
- Extract:
  - SPARQL examples
  - competency questions (CQs)

If only CQs exist:

- Optionally draft SPARQL (marked as `crafted_from_cq`).
- Assign lower confidence unless verified against an endpoint.

Paper-derived material then passes through the **same enrichment, execution, generation, review, and benchmark steps** as repo-derived material.

---

## 7. Evidence Enrichment (`kg_queries.jsonl`)

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

---

## 8. Query Execution And Run Metadata (`kg_queries.jsonl`)

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
- Store `latest_run`, `latest_successful_run`, and append to `run_history`.

### Output

`kg_queries.jsonl` (updated in-place with run metadata)

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
- `llm_outputs.jsonl` is treated as JSONL; legacy JSON-array files are normalized to JSONL on read.
- Output records carry a `run_signature` containing hashes of the effective prompt/schema/examples/input configuration.
- Resume/skip behavior uses `query_id`, `query_label`, `kg_id`, `model`, `system_prompt_hash`, and `input_hash`.

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

## 10. Frozen Run Capture

**Objective:** preserve review-worthy LLM-generation artefacts in an immutable form before human validation begins.

### Inputs

- `llm_inputs.jsonl`
- one LLM output file to review, such as `llm_outputs.jsonl`
- optional `llm_outputs.errors.jsonl`

### Process

1. Freeze the generation artefacts into `runs/<run-id>/`.
2. Copy the inputs, outputs, prompt, schema, and any relevant source snapshots needed for later traceability.
3. Write `runs/<run-id>/manifest.json` with file hashes, model list, counts, and purpose metadata.
4. Use the frozen run as the review target from this point onward.

### Notes

- A run is the immutable generation layer.
- One run can later accumulate multiple review exports.
- `build_review_bundle.py` can create this run automatically when the chosen output is not already inside `runs/<run-id>/`.

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

1. Build a browser review bundle with `build_review_bundle.py` → `review/review_data.js`.
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

Current reviewer labels:

- `approve`: keep this example in the benchmark as-is
- `dismiss`: exclude this example from the benchmark going forward
- `needs_prompt_fix`: example is valid, but model behavior should improve through prompt changes
- `needs_data_fix`: example may be valid, but the model inputs are wrong, incomplete, noisy, or missing key signals

### Notes

- Reviewer judgments are kept separate from model outputs.
- Review exports are keyed to the review dataset and the underlying run provenance, so prompt/model changes naturally produce a new review set.
- One review export should correspond to exactly one run, but one run may accumulate multiple review exports from different reviewers or sessions.
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

1. Build a benchmark snapshot with `benchmark/build_benchmark.py`.
2. Create a versioned directory such as `benchmark/v1/`.
3. Split reviewed items into:
   - approved benchmark items
   - pending items that still need prompt/data fixes
   - dismissed items excluded from the benchmark
4. For approved items, set `gold_question` using:
   - reviewer rewrite, if present
   - otherwise the approved model output
5. Preserve provenance linking each benchmark item back to:
   - query identifiers
   - review export
   - generation run metadata

### Output

- `benchmark/vN/manifest.json` – snapshot metadata and counts
- `benchmark/vN/benchmark.jsonl` – approved benchmark items only
- `benchmark/vN/pending.jsonl` – reviewed but not yet benchmark-approved items
- `benchmark/vN/dismissed.jsonl` – reviewed items explicitly excluded from the benchmark

### Notes

- The benchmark is distinct from both raw model outputs and review exports.
- Review exports capture human judgments; benchmark snapshots capture the current curated gold set.
- This separation makes it possible to compare multiple prompt/model runs against the same approved benchmark, while preserving reviewer provenance and benchmark history.

---

## 13. Outputs And Intended Use

At minimum, the project produces:

- `seeds.yaml` – configuration input
- `kgs.jsonl` – KG catalogue 
- `kg_queries.jsonl` – validated queries with run metadata and `llm_output`
- `llm_inputs.jsonl` – LLM input payloads
- `llm_outputs.jsonl` – LLM outputs (before merge)
- `runs/<run-id>/manifest.json` – frozen run metadata and copied generation artefacts
- `review/review_data.js` – local reviewer bundle
- `review/exports/*.json` – exported human-review judgments
- `benchmark/vN/benchmark.jsonl` – versioned approved benchmark pairs
- `benchmark/vN/pending.jsonl` – reviewed items pending fixes
- `benchmark/vN/manifest.json` – benchmark snapshot metadata

These outputs may be:

- ingested into Musparql
- used for evaluation or benchmarking
- published as a dataset
- extended with additional KGs

---

## 14. Rationale

- Every artefact is reproducible.
- Every query is runnable or explicitly marked otherwise.
- Every NL question has an explicit confidence estimate.
- Human review is versionable and separable from raw model output.
- Benchmark snapshots are versionable and separable from both review judgments and raw model output.
- Provenance is preserved end-to-end.
- LLM use is restricted to tasks where it adds value (language, summarisation).
