# Pipeline policy

This policy defines the invariants that pipeline runs and pipeline changes must
preserve. The commands are in [PIPELINE_RUNBOOK.md](PIPELINE_RUNBOOK.md).

## Start from human-authored SPARQL

Musparql may extract, normalize, test, and version SPARQL, but it does not invent
benchmark information needs. Every query identity and intended information need
must resolve to an identified repository, document, paper, guide, or curated
derivative with explicit provenance.

The normal curation track aligns natural language with an existing query and
sends the pair to human review. SPARQL editing is an exceptional repair track,
not an alternative source of benchmark tasks. A correction must remain grounded
in the retained query and its evidence, preserve the intended information need,
and never introduce an unrelated query merely because it is useful or easier to
execute.

## Keep provenance attached

Source IDs are stable. Captured URLs and revisions describe what was actually
read; catalogue URLs describe the maintained source identity. Do not replace a
captured commit-pinned URL with a moving catalogue URL.

Generated text is never source evidence. Model outputs must retain their origin
mode and evidence IDs so a reviewer can distinguish direct source wording,
evidence alignment, paraphrase, and unguided formulation.

## Keep source SPARQL immutable

Normalized source SPARQL is version `0`. A correction adds an append-only
version with its own hash and provenance. Execution records, prompt inputs, and
benchmark records identify the exact selected version and hash.

Corrections should be rare and limited to errors, omissions, unresolved
parameters, or other defects that prevent the retained query from faithfully
expressing its source-grounded information need. They are not routine rewriting
or optimization.

Never rewrite version `0`, reuse a version number, or silently replace an
approved edit.

## Treat execution as observation

Execution status is evidence about the query and its current environment. It is
not proof that a pair is suitable, and failure is not proof that a pair is
invalid. Keep query errors, endpoint failures, unsupported features, and
federated-service failures distinct.

## Keep automatic and human decisions separate

Automatic evaluation and triage may create reports or correction candidates.
They must not change benchmark inclusion, canonical wording, reviewer comments,
or holdout membership.

Only a validated human review export can create or update benchmark gold data.

## Exclude holdouts before agent-facing work

Every applicable command must make an explicit holdout-handling choice. Under
the current identity-visible policy, use
`var/holdout/selectors.jsonl`. Selected identities must be removed before prompt
construction, execution jobs, correction evidence, review bundles, or automatic
evaluation are assembled.

The selector contains identities and optional SPARQL pins only. Full holdout
records and reviewer annotations belong exclusively to the separate private
repository.

## Freeze meaningful generation runs

A review or benchmark update should refer to a frozen run with copied inputs,
outputs, prompts, schemas, configuration, and hashes. An unversioned working
output is not sufficient provenance for a published benchmark decision.

## Version benchmark changes

Do not edit an existing `benchmark/vN/` snapshot in place. Build a new version,
audit it, and retain the previous snapshot. Frozen historical manifests keep the
paths and provenance that were true when they were created.

## Publish through an allowlist

Never publish a working tree or a copy-and-delete directory. Use the public
release builder, which includes only approved benchmark fields and checks the
result. Raw reviews, internal annotations, local paths, model request metadata,
working query files, correction artifacts, and holdout material are not public
release files.

## Record experiments honestly

When a prompt, model, source, or extraction change is evaluated, record the
baseline, candidate run, method, result, and decision in `docs/experiments/`.
Negative results and rejected changes are part of the project history.
