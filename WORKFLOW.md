# Workflow: Building And Curating NL-SPARQL Benchmark Data

This repository provides a reproducible pipeline for collecting, testing, and
curating natural-language question and SPARQL query pairs for musicological
Knowledge Graphs (KGs). The resulting dataset is intended for evaluation,
benchmarking, and downstream use in systems such as Musparql.

The workflow separates deterministic processing, LLM-assisted interpretation,
human judgment, benchmark curation, and lightweight automatic evaluation. That
separation is the main design principle: every artefact should be traceable,
regenerable, and clear about whether it came from source material, model output,
human review, or an evaluation script.

---

## 1. Purpose And Principles

The pipeline has four distinct decision layers:

- **Deterministic collection and validation**: source capture, query extraction,
  evidence enrichment, SPARQL execution, hashing, and provenance.
- **LLM generation**: alignment of SPARQL to evidence and generation of
  candidate natural-language questions with confidence metadata.
- **Human review**: costly but authoritative decisions about benchmark
  membership, canonical wording, private holdout membership, and interpretive
  dimensions.
- **Automatic evaluation**: low-cost experiment triage against an existing
  reviewed benchmark snapshot.

Automatic evaluation does not update the benchmark.
Benchmark construction and benchmark updates are based on human review.
Autoeval is used for lower-cost decisions such as whether to continue an
experiment, whether a run deserves human review, and how candidate prompts or
models rank against a current benchmark snapshot.

Private holdout items are reviewer-only. They are preserved separately and
excluded from normal generation inputs, prompt work, public/dev benchmark files,
and normal automatic evaluation.

---

## 2. Pipeline Overview

At a high level, the pipeline works like this:

1. Define source provenance in `sources.yaml` and select source IDs in `seeds.yaml`.
2. Capture KG source snapshots into `kgs.jsonl` and `kg_sources/`.
3. Extract candidate SPARQL queries into `kg_queries.jsonl`.
4. Enrich query records with nearby human-readable evidence.
5. Execute queries against endpoints or local dumps and record execution metadata.
6. Build LLM input payloads from enriched query records.
7. Generate or align natural-language questions into `llm_outputs.jsonl`.
8. Freeze review-worthy generation outputs into `runs/<run-id>/`.
9. Use automatic evaluation for experiment triage against the current benchmark.
10. Review selected outputs in the local review workbench.
11. Build or update versioned benchmark snapshots under `benchmark/vN/`.
12. Record targeted experiments under `experiments/`.

The order is not strictly linear after generation. A candidate run can go to
autoeval, human review, or both:

```text
candidate generation run
  |-- autoeval against current benchmark -> triage / rank / decide whether to review
  `-- human review -> benchmark build or benchmark update
```

---

## 3. Artefact Map

| Artefact | Role |
| --- | --- |
| `sources.yaml` | Stable source identities, external links, derivations, and justified local artefacts |
| `seeds.yaml` | KG selection and references to normalized source IDs |
| `kgs.jsonl` | KG catalogue, endpoint metadata, and captured source provenance |
| `kg_sources/` | Deterministic text snapshots of KG source material |
| `kg_queries.jsonl` | Working query records: SPARQL, evidence, execution metadata, and merged NL output |
| `llm_inputs.jsonl` | Prompt-ready LLM input payloads |
| `llm_outputs.jsonl` | Raw LLM generation/alignment outputs before merge or freezing |
| `runs/<run-id>/` | Frozen generation runs with copied artefacts and model provenance |
| `review/review_data.js` | Browser review bundle |
| `review/exports/*.json` | Human review decisions, notes, rewrites, holdout flags, and interpretive annotations |
| `benchmark/vN/` | Versioned benchmark snapshot and sidecar files |
| `evals/reports/<eval-id>/` | Automatic evaluation reports |
| `experiments/` | Targeted experiment notes and outcomes |

Appendix A contains the detailed data model reference.

---

## 4. Source And Query Collection

**Objective:** collect KG metadata, SPARQL queries, human-readable evidence, and
query execution metadata without LLM interpretation.

### KG Source Capture

Inputs:

- `sources.yaml`
- `seeds.yaml`
- KG README files
- project websites
- related academic papers when available
- curated local source overrides under `curated_sources/`

Process:

1. Validate that each source has an external URL, a `derived_from` reference,
   or a description justifying a local artefact.
2. Resolve GitHub README and GitHub `blob/...` documentation URLs to
   commit-pinned raw URLs when possible.
3. Save deterministic text snapshots under `kg_sources/`.
4. Record source IDs and catalogue provenance in `kgs.jsonl` through
   `source_ids`, `source_files`, and `source_details`.
5. Preserve curated local fixes as explicit derivative sources rather than silently
   replacing upstream material.

Generated KG descriptions are optional downstream interpretation. They are not
part of the deterministic `build_kgs.py` source-capture layer.

### SPARQL Query Extraction

Inputs:

- repositories listed in `seeds.yaml`
- documentation pages with example queries
- academic papers containing SPARQL or competency questions

Process:

1. Clone or reuse source repositories.
2. Extract `.rq` and `.sparql` files.
3. Extract embedded SPARQL from Markdown, HTML/pre blocks, source code, and PDFs.
4. Normalize whitespace and prefixes.
5. Deduplicate queries by normalized SPARQL hash.
6. Record repository URL, source path, commit hash, checkout mode, default
   branch, and line spans where available.

Output:

- `kg_queries.jsonl` with raw SPARQL, normalized SPARQL, query identifiers, and
  extraction provenance.

Tests:

```bash
.venv/bin/python -m unittest tests/test_extract_queries.py
```

### Evidence Enrichment

Evidence enrichment attaches human-readable context to query records.

Evidence types include:

- `repo_file`: raw source-file provenance.
- `query_comment`: comments directly above SPARQL queries.
- `readme_query_desc`, `doc_query_desc`, `web_query_desc`: nearby prose around
  SPARQL examples.
- `cq_item`: competency questions from headings, lists, tables, or paper text.
- `doc_pdf`: SPARQL or evidence extracted from academic PDFs.

All evidence items carry `evidence_id`, source location, timestamps, and
extractor version metadata. Repo-derived evidence also records commit and
checkout information where available.

Tests:

```bash
.venv/bin/python -m unittest tests/test_enrich_evidence.py
```

### Academic Paper Integration

Academic papers are part of the same source-acquisition layer. Every downloaded
paper or supplement must have a `sources.yaml` record preserving its public URL,
its relationship to another source, or an explicit justification. When a KG has a
canonical paper or supplement, the pipeline extracts SPARQL examples,
competency questions, captions, and nearby explanatory text. Paper-derived
queries and evidence enter `kg_queries.jsonl` through the same fields as
repo-derived material.

### Query Execution Metadata

Queries are executed against configured endpoints or local dumps.

For each query, the pipeline records:

- execution status (`ok`, `empty`, `timeout`, `parse_error`, `auth`, etc.)
- endpoint or local dump path
- timestamp
- result count
- optional sample row
- duration and error text when relevant

Current records use `latest_execution`, `latest_successful_execution`, and
`execution_history`. Legacy aliases `latest_run`, `latest_successful_run`, and
`run_history` are preserved for compatibility.

This step establishes whether a query is runnable before LLM generation and
benchmark curation.

---

## 5. LLM Generation Layer

**Objective:** align SPARQL queries with source evidence where possible, and
generate candidate natural-language questions where no suitable source wording
exists.

Inputs:

- `kg_queries.jsonl`
- `kgs.jsonl`
- optional query execution samples
- prompt, schema, and example files under `prompts/` and `schemas/`

Process:

1. Build prompt payloads:

   ```bash
   .venv/bin/python build_llm_inputs.py
   ```

2. Run generation or alignment:

   ```bash
   .venv/bin/python run_llm_generation.py
   ```

   The generation runner validates retained evidence citations before writing
   records. If a generated phrase clearly matches a different input evidence
   snippet than the cited `evidence_id`, the runner repairs the ID and records a
   `citation_validation` report. Ambiguous or weak matches are left unchanged
   and reported as warnings.

3. Merge outputs back into query records:

   ```bash
   .venv/bin/python merge_llm_outputs.py
   ```

When a benchmark snapshot already identifies unsuitable queries, exclude them
from future generation inputs:

```bash
.venv/bin/python build_llm_inputs.py \
  --exclude-dismissed-benchmark benchmark/vN
```

### Generation Output

Each LLM output contains:

- `ranked_evidence_phrases`
- `nl_question`
- `nl_question_origin`
- `confidence`
- `confidence_rationale`
- `needs_review`

Each raw output record also contains `citation_validation`, a runner-side
guardrail report with citation repairs and warnings. This report is provenance
metadata, not model-authored content, and does not change the prompt schema.

Output records also carry model and request provenance:

- requested model alias
- response model when available
- prompt/schema/example/system-prompt hashes
- input hash
- request config hash
- API method
- OpenAI-compatible base URL when configured
- explicit generation parameters

For Graphia/LiteLLM-hosted models, credentials stay in the shell environment:

```bash
export GRAPHIA_API_KEY="..."
export GRAPHIA_BASE_URL="https://llm.graphia-ssh.eu/v1"

.venv/bin/python run_llm_generation.py \
  --api-method chat.completions.create \
  --model MiniMax-M2.5
```

Older runs remain readable even when they only contain `model` and
`run_signature`.

### Frozen Generation Runs

Review-worthy outputs are frozen under `runs/<run-id>/`.

A frozen run contains:

- copied LLM inputs and outputs
- prompt, schema, and examples
- optional error output
- `manifest.json` with file hashes, model list, counts, and provenance

`build_review_bundle.py` can auto-freeze a run when the selected output is not
already inside `runs/<run-id>/`.

---

## 6. Automatic Evaluation Layer

**Objective:** provide a low-cost, synthetic evaluation round for experiment
triage, prompt/model ranking, and deciding whether human evaluation is worth
the cost.

Autoeval compares one or more generation runs against an existing reviewed
benchmark snapshot. It does not update the benchmark.

Run deterministic-only checks:

```bash
.venv/bin/python evals/evaluate_runs.py \
  --benchmark benchmark/vN \
  --runs runs/<candidate-run> \
  --baseline runs/<baseline-run> \
  --skip-judge \
  --out evals/reports/<eval-id>
```

Run deterministic checks plus LLM judging:

```bash
.venv/bin/python evals/evaluate_runs.py \
  --benchmark benchmark/vN \
  --runs runs/<baseline-run> runs/<candidate-run> \
  --baseline runs/<baseline-run> \
  --judge-model gpt-5 \
  --out evals/reports/<eval-id>
```

What the evaluator scores:

- `benchmark.jsonl` only.
- Every human-confirmed pair in that file.

What the evaluator excludes:

- `dismissed.jsonl`
- `holdout.jsonl`
- `alternatives.jsonl`
- `linguistic_annotations.jsonl`

Deterministic checks cover:

- missing outputs
- output schema errors
- empty questions
- placeholder leakage
- evidence ID integrity
- SPARQL mismatch against the fixed benchmark query

LLM judging, when enabled, scores semantic question quality against the fixed
SPARQL and canonical `gold_question`. Judge results are cached in
`judge_cache.jsonl` so unchanged triples of SPARQL, gold question, and candidate
question do not need to be rescored.

Outputs:

- `evals/reports/<eval-id>/manifest.json`
- `evals/reports/<eval-id>/scores.jsonl`
- `evals/reports/<eval-id>/summary.md`
- `evals/reports/<eval-id>/judge_cache.jsonl`

---

## 7. Human Review Layer

**Objective:** capture authoritative human judgments about generated NL-SPARQL
pairs in a reusable, versioned form.

Inputs:

- one frozen generation run or current LLM output file
- optional prior benchmark snapshot
- optional previous review export when building comparative reviews

### Initial Review

Build an initial-review bundle:

```bash
.venv/bin/python build_review_bundle.py \
  --outputs runs/<run-id>/llm_outputs.jsonl \
  --run-manifest runs/<run-id>/manifest.json
```

For later initial-review rounds, pass the latest benchmark snapshot:

```bash
.venv/bin/python build_review_bundle.py \
  --outputs runs/<run-id>/llm_outputs.jsonl \
  --run-manifest runs/<run-id>/manifest.json \
  --previous-benchmark benchmark/vN
```

With `--previous-benchmark`, initial review excludes already reviewed pairs by
default and always excludes private holdout pairs. Use `--include-reviewed` only
for deliberate audit passes over non-holdout reviewed pairs. Previous decisions
remain hidden unless `--reveal-previous-decision` is explicitly passed.

Open the workbench through a local server:

```bash
python3 -m http.server 8000
```

```text
http://localhost:8000/review/
```

Initial review captures:

- reviewer status
- preferred/corrected wording
- reviewer notes
- private holdout flag
- interpretive dimensions: naturalness, pragmatism, room for interpretation
- whether graph/context knowledge is required

### Comparative Review

After changing extraction, enrichment, prompts, or models, build a comparative
review bundle:

```bash
.venv/bin/python build_next_review_round.py \
  --previous-run runs/<old-run-id> \
  --current-run runs/<new-run-id> \
  --previous-reviews review/exports/<previous-review-export>.json \
  --previous-benchmark benchmark/vN \
  --benchmark-only
```

Comparative mode shows previous and current records side by side. It is intended
for deciding whether changed or added outputs should update the benchmark.

By default, compare mode focuses on added, removed, and review-worthy changed
pairs. Question, origin, retained evidence, and SPARQL changes are
review-worthy. Confidence, rationale, model, and full-input evidence changes
are metadata-only unless one of the review-worthy fields also changed.

Useful options:

- `--include-unchanged`: include unchanged pairs.
- `--include-metadata-only`: include metadata-only changes.
- `--include-dismissed`: intentionally revisit previously dismissed pairs.

Review fields:

- `benchmark_disposition: included`: publish the human-confirmed canonical pair.
- `benchmark_disposition: excluded`: exclude unsuitable benchmark material.
- `pipeline_assessment: accepted`: the presented formulation is acceptable.
- `pipeline_assessment: prompt_improvement_recommended`: the canonical pair is valid, but prompt/model behaviour should improve.
- `pipeline_assessment: input_data_improvement_recommended`: the canonical pair is valid, but generation inputs or evidence should improve.

Outputs:

- `review/review_data.js`
- `review/exports/*.json`

---

## 8. Benchmark Curation Layer

**Objective:** convert human-reviewed examples into versioned benchmark
snapshots for evaluation, publication, and downstream experiments.

### Initial Benchmark Build

Build a first benchmark snapshot:

```bash
.venv/bin/python benchmark/build_benchmark.py \
  --bundle review/review_data.js \
  --reviews review/exports/<review-export>.json \
  --outdir benchmark/v1
```

The builder writes:

- `included.jsonl`
- `dismissed.jsonl`
- `holdout.jsonl`
- `alternatives.jsonl`
- `linguistic_annotations.jsonl` (internal only)

It also creates the scoring dataset:

- `benchmark.jsonl`

`benchmark.jsonl` contains every included, human-confirmed pair. Each item has
exactly one canonical `gold_question`; inclusion is implicit in presence.

Gold question policy:

- Use the reviewer rewrite when present.
- Otherwise use the approved model output.

### Benchmark Updates From Comparative Review

Apply a comparative-review export to a previous benchmark snapshot:

```bash
.venv/bin/python benchmark/update_benchmark.py \
  --previous-benchmark benchmark/v1 \
  --bundle review/review_data.js \
  --reviews review/exports/<comparative-review-export>.json \
  --outdir benchmark/v2
```

The update routine carries forward unchanged previous benchmark records and
replaces only pairs that received decisions in the comparative review.

Private holdout records are carried forward separately in `holdout.jsonl`.
Accepted alternative phrasings are carried forward in `alternatives.jsonl`.
Exploratory linguistic annotations are carried forward separately in the
internal `linguistic_annotations.jsonl`.

### Benchmark Updates From Initial Review

Initial-review exports can also update an existing benchmark snapshot. This is
the appropriate path when a reviewer has examined additional pairs from the same
or a later run, but the review was not a side-by-side comparison of old and new
outputs.

Apply an additive initial-review export to a previous benchmark snapshot:

```bash
.venv/bin/python benchmark/update_from_initial_review.py \
  --previous-benchmark benchmark/vN \
  --bundle review/review_data.js \
  --reviews review/exports/<initial-review-export>.json \
  --outdir benchmark/vN_plus_1
```

For an initial-review update:

- Carry forward all records from the previous benchmark snapshot.
- Add newly reviewed pairs that were not already present.
- Preserve dismissed and private holdout records according to the usual split
  policy.
- Preserve accepted non-canonical phrasings and explicitly marked literal
  SPARQL wordings in `alternatives.jsonl`; keep exploratory ratings separately
  in internal `linguistic_annotations.jsonl`.
- Record the update source in the new benchmark manifest.

If an initial-review export only covers pairs that were absent from the previous
benchmark, the update is additive and requires no conflict resolution.

If an initial-review export covers a pair that is already present in the
benchmark, treat the new review as an additional judgment, not as an automatic
replacement. The benchmark must keep one canonical decision for scoring, but it
should preserve all review evidence in sidecar/provenance records.

The additive updater fails fast on overlapping already-reviewed pairs. Use that
failure as a cue to perform an explicit conflict-aware merge or adjudication
step, rather than silently replacing the previous benchmark decision.

### Curated Source Additions

Some public sources already provide both natural-language prompts and SPARQL
queries. They should still pass through source capture, query extraction,
pairing, and a lightweight human review. The command below documents the legacy
LinkedMusic shortcut retained for reconstruction only; it must not be used for
the final DOI release:

```bash
.venv/bin/python benchmark/add_linkedmusic_curated.py \
  --previous-benchmark benchmark/vN \
  --source curated_sources/LinkedMusic_Queries_Corrected.md \
  --outdir benchmark/vN_plus_1
```

Legacy curated-source records use the source prompt as `gold_question` and record
`gold_question_source: "source_prompt"`. Query execution status should be
recorded separately from pair validity: a source-authored NL-SPARQL pair can be
valid benchmark material even when a live federated query is temporarily
unrunnable or dependent on external endpoint limits.

### Multiple Reviews And Conflict Resolution

Multiple reviews of the same pair are methodologically useful: they can support
quality control, inter-reviewer agreement, and intra-reviewer variation checks.
They should not be collapsed silently.

The benchmark curation policy is:

- Preserve every review separately, including benchmark disposition, pipeline assessment, preferred
  wording, literal wording, notes, split, interpretive annotations, timestamp,
  review export, and run provenance.
- Keep exactly one canonical `gold_question` in `benchmark.jsonl`.
- Store accepted alternative wordings and explicitly marked literal formulations
  in `alternatives.jsonl`, with provenance for each formulation.
- Track whether a canonical benchmark decision came from a single review,
  consensus, wording variation, status conflict, or adjudication.

When review decisions agree:

- Matching inclusion decisions: include the pair with the selected canonical wording.
- If only one review supplies a preferred wording, use it as the canonical
  `gold_question`.
- If multiple reviews supply preferred wordings, use the latest as the default
  canonical wording and store the others in `alternatives.jsonl`.
- If multiple literal wordings are supplied, retain all distinct literal
  formulations under `literal_formulations` in `alternatives.jsonl`; do not force a single public literal
  wording unless a downstream export format requires one.

When review decisions differ:

- Different pipeline assessments do not change inclusion when reviewers agree
  on a human-confirmed canonical question.
- Any inclusion decision versus exclusion: require adjudication before including the pair in
  the strict public benchmark.

For public releases and paper results, use the strict benchmark: unresolved
status conflicts involving dismissal or pair validity should be excluded from
`benchmark.jsonl` or explicitly adjudicated. Lenient inclusion is acceptable for
internal prompt iteration, but it must be marked as such and should not be
reported as the main curated benchmark.

### Alternatives and Internal Linguistic Annotations

`alternatives.jsonl` is the public sidecar for accepted non-canonical phrasings
and explicitly marked literal formulations. `linguistic_annotations.jsonl`
stores exploratory ratings internally and is excluded from the public release.

The sidecar records:

- pair identity
- current canonical question
- accepted alternative phrasings and literal formulations in separate arrays
- source type for each phrasing (`model_output`, `human_rewrite`,
  `previous_canonical_question`, `literal_sparql_wording`)
- review/run/model provenance

When an approved human rewrite becomes canonical, the generated model wording
is retained as an accepted alternate if distinct. When a later approved rewrite
replaces an older canonical wording, the older canonical wording is retained as
an accepted alternate.

Dismissed and private holdout records are not exposed through the public
ambiguity sidecar.

---

## 9. Targeted Experiments

**Objective:** capture focused investigations and their outcomes, whether or
not they change the benchmark or become part of the default workflow.

Use `experiments/` for targeted changes, comparisons, and checks that are worth
remembering: extraction variants, enrichment changes, prompt revisions, model
comparisons, evaluation procedures, data-quality investigations, or guardrails.
The result may be positive, negative, inconclusive, or only useful as context
for future work.

An experiment note should usually record:

- the question or hypothesis being tested
- the implementation or configuration change
- generated run, autoeval report, review export, or other evidence, where
  applicable
- observed outcome, including failures and limitations
- whether any follow-up action was taken
- whether the benchmark was updated, left unchanged, or not involved

Experiments are not benchmark updates by themselves. A useful experiment can
lead to a benchmark update through human review, but many experiments should
only leave an audit trail: for example, a rejected prompt, a model comparison
that explains a later choice, or a data-quality check that confirms no change is
needed.

---

## 10. Outputs And Intended Use

At minimum, the project can produce:

- KG source and metadata records.
- Query records with execution metadata and evidence.
- LLM generation runs with model provenance.
- Human review exports.
- Versioned benchmark snapshots.
- Public ambiguity sidecars.
- Automatic evaluation reports.
- Experiment notes.

These outputs may be:

- ingested into Musparql
- used for prompt/model evaluation
- published as a dataset
- extended with additional KGs

---

## Appendix A. Data Model Reference

This appendix describes the main artefact families. The examples are indicative;
tooling should remain tolerant of older records that omit newer provenance
fields.

### `kgs.jsonl`

One record per KG:

```json
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
  "source_ids": ["meetups-knowledge-graph-repository", "meetups-query-examples"],
  "source_files": ["kg_sources/meetups__01__raw-githubusercontent-com.txt"],
  "source_details": [
    {
      "source_url": "https://github.com/polifonia-project/meetups-knowledge-graph",
      "source_id": "meetups-knowledge-graph-repository",
      "catalog_provenance": {
        "type": "repository",
        "title": "Polifonia MEETUPS Knowledge Graph repository",
        "url": "https://github.com/polifonia-project/meetups-knowledge-graph"
      },
      "resolved_url": "https://raw.githubusercontent.com/.../<commit>/README.md",
      "repo_commit": "<commit>",
      "source_path": "README.md",
      "error": null
    }
  ]
}
```

### `kg_queries.jsonl`

One record per query:

```json
{
  "query_id": "musow__sha256:abc123",
  "query_label": "musow-0001",
  "kg_id": "musow",
  "query_type": "select",
  "sparql_raw": "...as extracted...",
  "sparql_clean": "...normalized...",
  "sparql_hash": "sha256:...",
  "raw_hash": "sha256:...",
  "evidence": [
    {
      "evidence_id": "e1",
      "type": "query_comment",
      "source_url": "https://github.com/...",
      "source_path": "queries/example.rq",
      "repo_commit": "abc123",
      "snippet": "Find all concerts...",
      "extracted_at": "2026-01-30T12:00:00Z",
      "extractor_version": "extract_queries.py@v1"
    }
  ],
  "latest_execution": {
    "ran_at": "2026-01-30T12:10:00Z",
    "status": "ok",
    "endpoint": "https://...",
    "result_count": 14,
    "sample_row": {"s": "..."},
    "duration_ms": 1200,
    "error": null
  },
  "latest_successful_execution": {
    "ran_at": "2026-01-30T12:10:00Z",
    "status": "ok",
    "endpoint": "https://...",
    "result_count": 14
  },
  "execution_history": [],
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
  }
}
```

### `runs/<run-id>/manifest.json`

One frozen generation run:

```json
{
  "run_id": "2026-04-25-sample-review-gpt5",
  "generation_run_id": "2026-04-25-sample-review-gpt5",
  "created_at": "2026-04-25T23:14:14+00:00",
  "purpose": "manual review sample",
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
```

### `review/exports/*.json`

One exported human-review file:

```json
{
  "dataset_id": "830748f26ceb9031",
  "run_id": "2026-04-25-sample-review-gpt5",
  "generation_run_id": "2026-04-25-sample-review-gpt5",
  "run_ids": ["2026-04-25-sample-review-gpt5"],
  "runs": [
    {
      "run_id": "2026-04-25-sample-review-gpt5",
      "generation_run_id": "2026-04-25-sample-review-gpt5",
      "manifest_path": "runs/2026-04-25-sample-review-gpt5/manifest.json"
    }
  ],
  "exported_at": "2026-04-25T20:10:00Z",
  "reviews": {
    "meetups::meetups-0002::<token>": {
      "benchmark_disposition": "withheld",
      "pipeline_assessment": "accepted",
      "preferred_question": "",
      "literal_wording": "",
      "note": "",
      "split": "private_holdout",
      "interpretive": {
        "naturalness": 88,
        "pragmatism": 70,
        "room_for_interpretation": 22,
        "requires_graph_context_knowledge": true
      },
      "updated_at": "2026-04-25T20:09:00Z"
    }
  }
}
```

### `benchmark/vN/`

Benchmark snapshots include:

- `manifest.json`
- `benchmark.jsonl`
- `included.jsonl`
- `dismissed.jsonl`
- `holdout.jsonl`
- `alternatives.jsonl`
- `linguistic_annotations.jsonl` (internal only)

`benchmark.jsonl` is the compact scoring dataset:

```json
{
  "benchmark_version": "v3",
  "benchmark_built_at": "2026-05-19T12:00:00+00:00",
  "benchmark_id": "meetups::meetups-0002::<token>",
  "kg_id": "meetups",
  "query_id": "meetups__sha256:...",
  "query_label": "meetups-0002",
  "sparql": "...normalized SPARQL...",
  "gold_question": "Who are the two people who most frequently participated in meetups with Edward Elgar?",
  "gold_question_source": "approved_model_output",
  "review": {
    "review_id": "meetups::meetups-0002::<token>",
    "review_export": "review/exports/....json",
    "dataset_id": "<review-dataset-id>",
    "literal_wording": "",
    "note": "",
    "updated_at": "2026-04-25T21:00:00Z"
  },
  "run": {
    "generation_run_id": "2026-04-25-sample-review-gpt5",
    "run_label": "llm_outputs.sample_current",
    "source_file": "llm_outputs.sample_current.jsonl",
    "model": "gpt-5",
    "run_signature": {"model": "gpt-5"}
  },
  "evidence_summary": {
    "evidence_count": 41,
    "evidence_types": ["cq_item", "query_comment"],
    "has_source_evidence": true,
    "has_query_specific_evidence": true
  }
}
```

`alternatives.jsonl` is a public sidecar, not a scoring file:

```json
{
  "benchmark_version": "v3",
  "benchmark_built_at": "2026-05-19T12:00:00+00:00",
  "benchmark_id": "meetups::meetups-0002::<token>",
  "kg_id": "meetups",
  "query_id": "meetups__sha256:...",
  "query_label": "meetups-0002",
  "sparql": "...normalized SPARQL...",
  "canonical_question": "Canonical reviewed wording?",
  "canonical_question_source": "reviewer_rewrite",
  "accepted_alternatives": [
    {
      "text": "Alternative accepted wording?",
      "normalized_text": "alternative accepted wording?",
      "source_type": "model_output",
      "review_id": "meetups::meetups-0002::<token>",
      "review_export": "review/exports/....json",
      "dataset_id": "<review-dataset-id>",
      "run_id": "2026-04-25-sample-review-gpt5",
      "generation_run_id": "2026-04-25-sample-review-gpt5",
      "model": "gpt-5",
      "updated_at": "2026-04-25T21:00:00Z"
    }
  ],
  "literal_formulations": []
}
```

Exploratory ratings use the same pair identity in the separate internal
`linguistic_annotations.jsonl` file and are not part of the public release.

### `evals/reports/<eval-id>/`

An automatic evaluation report contains:

- `manifest.json`: inputs, benchmark counts, run metadata, judge configuration,
  and aggregate summary.
- `scores.jsonl`: one score record per benchmark item per run.
- `summary.md`: human-readable report.
- `judge_cache.jsonl`: cached semantic judge results.

Score records include:

- deterministic errors and warnings
- `sparql_match`
- candidate, gold, and baseline questions
- judge status and judge result when semantic judging is enabled
- generation run signature and request config when available

---

## Appendix B. Rationale

- Every artefact should be reproducible or explicitly marked as human judgment.
- Every query should be runnable or explicitly marked otherwise.
- Every generated NL question should have provenance and confidence metadata.
- Human review is versionable and separable from raw model output.
- Benchmark snapshots are versionable and separable from both review exports and
  raw model outputs.
- Automatic evaluation is a triage layer, not a benchmark-construction layer.
- Provenance is preserved end to end.
