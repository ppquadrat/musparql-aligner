# Holdout security

## What this document is about

This document explains how Musparql protects its evaluation holdout. It is for
reviewers, maintainers, and future contributors who need to understand what the
holdout is, which information is confidential, and where the security boundary
lies.

For the step-by-step procedure, use [HOLDOUT_RUNBOOK.md](HOLDOUT_RUNBOOK.md).

## What is the holdout?

The project starts with candidate pairs. Each pair contains a SPARQL query, its
supporting evidence, and a generated natural-language formulation. Agents may
help create and inspect those candidates.

A human reviewer may later select a candidate as a **holdout pair**. Holdout
pairs are reserved for evaluation: they must not become training, prompt-design,
or development material after selection.

The important secret is the **reviewer annotation made for a holdout pair**.
That includes the decision itself, corrected or literal wording, comments,
ratings, timestamps, provenance, and any combined export containing those
fields. Even a field named `public_comment` is private when it belongs to a
holdout.

The candidate's SPARQL, evidence, and generated formulation are not initially
secret from agents—the agent may already have seen them before selection. Once
the pair is selected, however, those materials must not be published or returned
to agent-assisted development workflows.

There are currently no holdout records. This policy establishes the boundary
before the first ones are created.

## Security aim

After a human selects a pair for the holdout, we aim to ensure that:

- its reviewer annotation is never shown to an agent or model;
- the pair is excluded from prompts, development evaluation, and routine review;
- its SPARQL, evidence, and generated formulation are not included in the public
  repository or release;
- only a human-controlled private copy carries the holdout record forward.

This is defence in depth, not an absolute guarantee. The system combines human
procedure, separate exports, ignored paths, fail-closed tools, Git hooks, and CI
checks so that one ordinary mistake is less likely to disclose the data.

## Constraints and trust assumptions

The current workflow is unencrypted. A plaintext file on the same computer and
under the same operating-system account is not a strong access boundary: software
with access to that account could read it. A separate repository on the same
machine improves organisation but does not solve that problem.

For now, the workflow assumes that humans:

- conduct the private part of review without an agent active;
- keep private exports outside agent-readable workflows;
- verify the private export before clearing browser state; and
- never ask an agent to inspect, move, audit, or recover private holdout data.

The preferred future boundary is encryption before the private export touches
disk, with a passphrase or key that is never stored on the machine, in an
environment variable, on a command line, or in an agent-accessible credential
store. A separate OS account, encrypted removable device, or separate review
machine would be stronger still.

## What is in place

### The review workbench separates the data

The browser has two explicit export actions:

- **Export Non-Holdout** creates an agent-readable review export with every
  holdout entry absent.
- **Export Private Holdout** creates a separate, self-contained private package.

The browser checks that every private annotation has exactly one matching record.
It will not clear private state after a missing or duplicate identity, and any
edit invalidates the previous export-ready state. Clearing requires a fresh
private export and human confirmation that the file opens and has the expected
count. Clearing also removes legacy browser-storage copies.

### Agent-facing tools fail closed

Benchmark and comparison tools accept only exports explicitly marked
`non_holdout_review_export`. They reject private, mixed, legacy, or mislabeled
review exports rather than trying to remove private records themselves.

Where the identity-visible policy is chosen, tools may use an annotation-free
selector containing only `kg_id`, `query_id`, `sparql_version`, and
`sparql_hash`. Selector values are validated. Reviewer fields are forbidden.

### The repository excludes working and private data

The public tree contains source code, documentation, synthetic tests, compact
benchmark data, and allowlisted release artifacts. Raw query pools, prompt
inputs, model outputs, frozen runs, generated review bundles, raw review exports,
internal snapshot partitions, and holdout records are ignored and not tracked.

The locations have different meanings:

- `review/public_exports/` — ignored but agent-readable sanitized non-holdout
  exports;
- `review/private/` — ignored and human-only private exports;
- `review/exports/` — ignored and human-only legacy or unsanitized exports.

Agents are instructed never to read or operate on the human-only locations,
`benchmark/v*/holdout.jsonl`, or any `musparql-holdout-private-*` file.

### Publication checks provide tripwires

The public-tree checker parses staged and committed JSON/JSONL for private
markers and rejects forbidden paths or private filenames. The pre-commit hook
checks staged blobs. The pre-push hook checks every outgoing commit, including
records added and deleted within the outgoing history. CI checks the committed
tree again.

These controls are tripwires, not a substitute for the human procedure. CI on a
public remote is too late to be the first place a leak is detected.

## Holdout identity policy

The project owner must choose one of two policies before creating holdouts:

- **Identity visible:** agent-facing tools receive an annotation-free selector
  so they can exclude selected pair identities. This is operationally simpler,
  but the agent can know or infer which candidates were withheld.
- **Identity private:** selection and filtering happen entirely in a human-only
  environment. No selector or before/after membership ledger enters the agent
  workspace. This requires a candidate universe that has not been exposed in a
  way that makes omission reveal membership.

In both policies, reviewer annotations remain human-only.

Because the current workflow selects holdouts from candidates agents may already
have seen, identity visible is the practical default. Identity private is a
future option only when selection begins from a genuinely human-only candidate
universe.

## Publication and incidents

Publish only an allowlisted release, never a copy-and-delete working directory.
Enable the repository hooks with:

```bash
git config core.hooksPath .githooks
```

Removing a file in a later commit does not remove it from earlier Git history.
The first public repository should therefore be created from a history-free,
allowlisted export unless a separate human-audited history rewrite has been
authorised.

If a holdout annotation is shown to an agent or model, retire and replace that
holdout. If a selected pair is published, treat it as public and replace it. If
a private file is staged or committed, stop publication and follow the incident
steps in the runbook.
