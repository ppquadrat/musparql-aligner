# Artifact and data model reference

This is the technical supplement to the readable [workflow](WORKFLOW.md). It
describes the main artifacts and the contracts between stages. JSONL files
contain one JSON object per line.

## Tracked catalog

### `catalog/sources.yaml`

Defines stable `source_id` values. A source records its type, title, and either
an external locator or an explicitly justified local artifact. Supported types
are repository, web document, publication, local document, and derivative.
Derivatives identify their parent sources.

### `catalog/seeds.yaml`

Declares schema `musparql.kg-seeds.v2`, selects source IDs for each KG, and
stores operational and reviewer-measurement configuration:

- `kg_id`, display name, and independently frozen `seed_version`;
- source IDs;
- SPARQL endpoint, authentication mode, and optional graph;
- optional local dump path and format; and
- one or more owner-approved `review_domains`, each with a stable ID, label,
  description, and optional owner-verified vocabulary mappings;
- one or more `familiarity_scopes`, each identifying a resource, knowledge
  graph, or federation; and
- project-specific notes.

Repository and document lists are hydrated from the source catalogue rather
than duplicated manually. `schemas/kg_seeds.schema.json` and the Python seed
loader enforce the complete contract, including required and unknown fields,
nested SPARQL/dataset structure, URLs, unique source IDs, domain mappings, and
familiarity scopes. The application asks every domain and familiarity scope
declared by the frozen seed; it does not infer them from queries or named graphs.

### `catalog/kg_seed_snapshots.yaml`

An append-only archive of every complete KG seed version. Each record contains
the canonical SHA-256 digest of its embedded seed and the preceding digest for
that KG, forming one non-branching history. The current entry in
`catalog/seeds.yaml` must equal the unique archived head for its `kg_id`; reusing
a `seed_version` with changed content is rejected. This preserves the full
domain descriptions and familiarity wording needed to reconstruct historical
assignments rather than relying on the mutable current catalogue or Git history.
Normal hydrated/build/query seed loaders require and validate this archive.
Retiring a KG removes it only from the current seed catalogue; its archived
history remains intact and resolvable.

After intentionally incrementing a seed version, append and verify its snapshot:

```bash
.venv/bin/python -m scripts.snapshot_kg_seeds
```

The contracts are `schemas/kg_seed_snapshots.schema.json` and the snapshot
validators in `musparql.source_catalog`. Contract tests execute the Draft
2020-12 schemas with format checking and local `$ref` resolution in addition to
the runtime validators.

### `catalog/expertise_domain_suggestions.yaml`

A small, versioned local suggestion set for general reviewer expertise. It
records a snapshot ID, creation date, source/version metadata, preferred and
alternative labels, language, broader local concepts, and vocabulary URI/version
when a concept has actually been imported and verified. The Phase 1 snapshot
contains owner-reviewed specialist music terms and records EuroSciVoc as a
reference-only source; it does not claim unverified EuroSciVoc mappings.

Free-text entry remains valid even when the file has no matching suggestion.
The contract is `schemas/expertise_domain_suggestions.schema.json`. Its runtime
loader rejects incomplete or unknown fields, invalid snapshot dates and language
tags, references to unknown/broader entries, and vocabulary provenance that does
not provide the concept URI and vocabulary version together.

### `catalog/kgs.jsonl`

One generated catalogue record per KG. It contains endpoint and dump metadata,
including configured named graphs and fallback endpoints; resolved source URLs,
captured revisions, local snapshot paths, source IDs, and catalog provenance.
This file is tracked because it is a compact, reviewable description of the
collected source state.

### `catalog/curated/Approved_SPARQL_Edits.jsonl`

A tracked, public-safe projection of every approved SPARQL version. It retains
the query identity, immutable base/version hashes, corrected SPARQL, concise
rationale, edit type, and non-private approval source. It deliberately excludes
raw review exports, reviewer notes, service logs, local paths, and execution
details.

Extraction restores missing versions from this archive after preserving any
richer matching local state. A fresh working directory therefore reconstructs
approved query versions without treating the ignored working catalogue as the
only copy.

## Working query catalogue

### `var/queries/kg_queries.jsonl`

One record per extracted query. Important fields include:

- `kg_id`, `query_id`, and `query_label`;
- `sparql_raw`, `sparql_clean`, and normalized hashes;
- source URL, path, commit, and extraction metadata;
- evidence records with stable evidence IDs;
- append-only `sparql_edits`;
- execution and run history pinned to SPARQL version and hash;
- correction and review provenance where applicable.

Version `0` is always `sparql_clean`. Later versions live in `sparql_edits` and
must have increasing integer versions, text, hash, reason, source, and approval
provenance.

The query catalogue is persistent local state and is ignored by Git. Approved
SPARQL versions are additionally projected into the tracked archive above;
execution history and detailed correction provenance remain local.

### `var/queries/sparql_correction_candidates.jsonl`

An automatic triage ledger. A candidate identifies the query and base SPARQL
version, summarizes the observed problem, and records the evidence safe for the
correction workbench. It is not an approved edit.

## Model inputs and outputs

### `var/llm/inputs.jsonl`

Prompt-ready records containing the selected SPARQL version, query identity,
evidence, and schema reference. Holdout and dismissed identities must be removed
before this file is written.

### `var/llm/outputs.jsonl`

Provisional model responses. A successful response contains:

- ranked evidence phrases and evidence IDs;
- proposed `nl_question`;
- an origin mode and cited evidence IDs;
- confidence and rationale; and
- model/request metadata needed for reproducibility.

Malformed or failed responses are written separately and never treated as
benchmark candidates. Model outputs remain separate from the working query
catalogue; frozen runs, review builders, and evaluation tools join them by query
identity without mutating the extracted query records.

## Generation runs

### `var/runs/<run-id>/manifest.json`

A frozen run manifest identifies:

- run ID and creation time;
- copied input and output filenames and SHA-256 hashes;
- prompt, schema, and example hashes;
- requested and returned model information;
- generation parameters and request configuration; and
- optional query and KG catalog snapshots.

Review bundles refer to run IDs and manifests rather than an unversioned output
file whenever possible.

## Review bundles and exports

### Confidential reviewer registry

Reviewer profiles are durable human-supplied data under
`confidential/reviewers/`, not rebuildable pipeline state under `var/`. The
legacy JSONL registry remains governed by `schemas/reviewer.schema.json` and
`schemas/reviewer_kg_familiarity.schema.json` until the owner-run Phase 2
migration. Those legacy frequency-like values must not be reinterpreted as v2
expertise levels.

The Phase 1 v2 confidential contracts are:

- `schemas/reviewer_profile_v2.schema.json`: identity, contact and technical
  experience plus a current-state projection of repeatable general-domain
  expertise. Each projected value records the stable domain ID and latest
  assertion ID as well as labels, level, timestamps, and nullable vocabulary
  provenance. It is not the historical source of truth.
- `schemas/reviewer_domain_expertise_assertion.schema.json`: the append-only
  history behind that projection. Each event has an assertion ID, stable domain
  ID, timestamp, and nullable `supersedes_id`; earlier events are retained.
- `schemas/reviewer_kg_domain_assessment.schema.json`: an append-only subject
  expertise assertion for one domain in a frozen KG seed.
- `schemas/reviewer_resource_familiarity_assessment.schema.json`: an append-only
  familiarity assertion for one resource/KG/federation scope in that seed.

Assessment rows snapshot the prompt label and seed version, record whether they
came from a pre-review confirmation or profile change, link the preceding
assertion when present, and require an assignment ID only for pre-review rows.
Their full prompt descriptions resolve through the immutable seed snapshot
archive. Assertion and assessment collection validators reject dangling,
cross-subject, non-chronological, branching, cyclic, or disconnected predecessor
histories. A separate projection validator requires every profile domain to equal
the head of its append-only assertion chain and checks its first/latest
timestamps. Examples under `schemas/examples/` are obviously synthetic.

Only IDs matching `reviewer-NNNN` may cross into review artifacts. Profile,
domain-assessment, and familiarity fields never enter bundles, submitted review
exports, benchmarks, prompts, or logs. Synthetic fixtures are the sole exception
for schema and validator testing.

The browser bundle is generated and ignored. It contains candidate records,
their run provenance, source evidence, SPARQL provenance, and holdout eligibility
information.

A new sanitized review export has schema `musparql.review-export.v2`, kind
`non_holdout_review_export`, and contains no
holdout entry. Review records can contain a benchmark decision, preferred
question, literal wording, public and internal comments, and copied-review
provenance. Historical interpretive fields remain readable but are not collected
by the current UI.

Every v2 review records `reviewer_id`, `reviewed_at`, `prior_review_ids`,
`authored_formulation_ids`, and `approved_formulation_ids`. These normalized
links are authoritative. Reviewer activity is derived from them rather than
duplicated on the confidential profile. A copied or repeated review links its
predecessor through `prior_review_ids`. Review times are timezone-qualified RFC
3339 date-times. A review event ID ends with its reviewer ID, and formulations
attributed to the current reviewer are rooted at that event ID.

The export's review-map key remains the bundle lookup key used to join a
decision to its candidate. The review object's explicit `review_id` is the
globally distinct review-event ID and includes the reviewer ID. Provenance links
must target the explicit event ID, allowing two reviewers to assess the same
bundle candidate without identity collision.

Formulation entries follow `schemas/formulation_provenance.schema.json` and
separate `authored_by_reviewer_id` from the review and reviewer IDs that
approved the formulation.

Sanitized exports belong in `var/review/exports/`. Private or mixed exports are
rejected by agent-facing tools.

## Holdout selectors

### `var/holdout/selectors.jsonl`

Under the identity-visible policy, each record may contain only:

```json
{
  "kg_id": "example-kg",
  "query_id": "example-query",
  "sparql_version": 0,
  "sparql_hash": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
}
```

The version and hash are optional but must appear together. Reviewer decisions,
wording, comments, ratings, timestamps, and provenance are forbidden. Exclusion
is pair-wide, and a selector is invalid when the query identity retains a SPARQL
edit.

The full holdout export is not part of this data model because it belongs to the
separate human-controlled private repository.

## Benchmark snapshot

### `benchmark/vN/benchmark.jsonl`

The scoring dataset. Each record contains a benchmark identity, KG and query
identity, selected SPARQL text/version/hash, one canonical gold question, gold
question provenance, evidence summary, and safe source provenance.

Snapshots created under the v2 review model expose only the pseudonymous reviewer
ID and review/formulation links needed for provenance. Published snapshots v1
through v10 remain immutable legacy single-reviewer artifacts.

When identical formulation text is approved more than once, the alternatives
sidecar merges `approval_review_ids` and `approval_reviewer_ids` rather than
discarding later approvals. `authored_by_reviewer_id` remains distinct.

For an edited SPARQL pin, public `sparql_provenance` is an allowlisted
projection. It may retain the edit count, selected version/hash, history digest,
neutral execution status, and stable human-approved edit facts such as the
decision, edit type, rationale, proposal origin, review time, and approved
version/hash. It excludes working correction identifiers and artifacts,
including candidate IDs/digests, review-export hashes, and UI execution-attempt
histories.

### `benchmark/vN/alternatives.jsonl`

Public, human-accepted non-canonical formulations and literal formulations.
Alternatives never replace the single canonical scoring question.

### `benchmark/vN/manifest.json`

Records the snapshot version, build time, counts, input runs and reviews,
sidecar names, and policy assertions. Historical manifests retain the paths that
were true when they were built.

### Ignored internal sidecars

Working snapshots may also have ignored included, dismissed, linguistic, or
holdout partitions. They are not public repository artifacts and are never
copied into a public release. In particular, `included.jsonl` may retain the
complete internal SPARQL correction provenance from the reviewed query record;
the public benchmark and alternatives files receive only the projection above.

## Public release

### `build/public-releases/vN/`

A derived publication package containing only allowlisted fields. It normally
contains sanitized `benchmark.jsonl`, public `alternatives.jsonl`, `LICENSE`,
`THIRD_PARTY_NOTICES.md`, and a manifest with checksums and machine-readable
license metadata. The release builder rejects private fields, internal review
metadata, filesystem paths, credentials, and correction artifacts.

The directory is ignored and may be deleted and rebuilt.
Both snapshot audit and release packaging validate all reviewer-bearing fields;
names or other non-pseudonymous strings fail closed before publication. Public
review event IDs must end in the matching reviewer pseudonym, formulation IDs
must be rooted at a reviewer-suffixed event with a supported role, and parallel
approval review/reviewer lists must agree entry by entry.

## Automatic evaluation reports

### `var/evals/reports/<eval-id>/`

May contain a manifest, per-record scores, a readable summary, and a judge
cache. Reports compare runs or systems; they do not update benchmark gold data.
