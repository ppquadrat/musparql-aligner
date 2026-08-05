# Holdout security

## What this document is about

This document explains how Musparql protects its evaluation holdout. It is for
reviewers, maintainers, and future contributors who need to understand what the
holdout is, which information is confidential, and where the security boundary
lies.

For the step-by-step procedure, use [HOLDOUT_RUNBOOK.md](HOLDOUT_RUNBOOK.md).

## What is the holdout?

The project starts with candidate pairs. Each pair contains a SPARQL query, its
supporting evidence, and a verbatim or generated natural-language formulation. Agents may
help compile and inspect those candidate pairs.

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

This policy applies from the first holdout selection onward and remains in force
when the set is expanded or replaced.

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

Both review modes apply the same eligibility rule: a pair can enter the holdout
only if no reviewer decision or annotation was attached to it before the current
review session. Merely generating the pair in an earlier run, including it in a
comparison bundle, or displaying it in either review interface does not make it
ineligible. An earlier status, wording correction, rating, or comment does.

A query identity that retains any SPARQL edit is also permanently ineligible,
even if version `0` is selected later or a subsequent edit reverts the text.
SPARQL correction review is a development workflow and excludes selected
holdout identities before attaching execution details, evidence, or proposals.

Initial review uses pair-level provenance in its bundle; comparison review checks
the attached previous review. Eligibility remains pair-wide even when a changed
SPARQL version prevents reuse of the old decision. The initial interface disables
the holdout checkbox for ineligible pairs. The comparison interface offers a
selectable checkbox only for eligible current pairs. Both mutation paths also
check eligibility rather than relying on presentation alone.

These controls fail closed. Build the bundle with every applicable previous
benchmark and sanitized previous-review export, then add
`--assert-complete-review-provenance` only after a human confirms that those
sources cover all earlier review decisions, or that no earlier decisions exist.
Without that assertion, neither interface permits new holdout selections.

### Agent-facing tools fail closed

Benchmark and comparison tools accept only exports explicitly marked
`non_holdout_review_export`. They reject private, mixed, legacy, or mislabeled
review exports rather than trying to remove private records themselves.

Where the identity-visible policy is chosen, tools use the annotation-free
selector mechanism described below. Reviewer fields are forbidden.

### The repository excludes working and private data

The public tree contains source code, documentation, synthetic tests, compact
benchmark data, and allowlisted release artifacts. Raw query pools, prompt
inputs, model outputs, frozen runs, generated review bundles, raw review exports,
internal snapshot partitions, and holdout records are ignored and not tracked.

The locations have different meanings:

- `var/review/exports/` — ignored but agent-readable sanitized non-holdout
  exports;
- `var/holdout/selectors.jsonl` — ignored, annotation-free, agent-readable
  selector under the identity-visible policy;
- the separate private holdout repository outside this workspace — full
  records and reviewer annotations;
- `review/private/` and `review/exports/` — forbidden legacy quarantine paths,
  not normal destinations.

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

### Identity-visible selector implementation

The relevant builders require exactly one holdout-handling option:

- `--holdout-selectors <path>` for identity-visible holdouts;
- `--no-holdout` only when no holdout identities currently exist; or
- `--holdout-filtered-upstream` when an identity-private human process has
  already removed holdout pairs from the inputs.

Omitting the choice or supplying conflicting choices is an error. Before the
first holdout review, confirm which option every downstream command will use.
The repository implements and tests this enforcement in:

- `scripts.build_llm_inputs` excludes selected pairs before constructing prompt
  inputs;
- `scripts.build_review_bundle` excludes them from later initial-review bundles;
- `scripts.build_review_diff_bundle` excludes them from comparison bundles; and
- `scripts.build_next_review_round` forwards the selector to the comparison builder.

Under the identity-visible policy, the human creates the selector after choosing
the holdout identities and the same file must be supplied explicitly to every
applicable downstream run. Once a holdout exists, `--no-holdout` is not valid.
Each JSON or JSONL record may contain only:

```json
{
  "kg_id": "example-kg",
  "query_id": "example-query",
  "sparql_version": 1,
  "sparql_hash": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
}
```

The version and hash may both be omitted. The shared validator rejects reviewer
decisions, comments, wording, ratings, timestamps, provenance, and malformed
version pins. Version `0` pins are permitted, but a selector is invalid if the
canonical query identity retains any edit; exclusion remains pair-wide. Under
the identity-private policy, do not create an agent-visible
selector; filtering must occur in the human-only environment before any
agent-facing processing.

The intended holdout proportion and sampling rationale are methodology, not
reviewer annotation, and may be documented publicly. Under an identity-private
policy, that documentation must not identify selected pairs or otherwise reveal
membership.

## Publication and incidents

Publish only an allowlisted release, never a copy-and-delete working directory.
Enable the repository hooks with:

```bash
git config core.hooksPath .githooks
```

Candidate material committed before an identity became a holdout does not by
itself expose a later reviewer annotation. From the first holdout selection
onward, however, private holdout data and newly selected records must never enter
the public Git history. A history rewrite is an incident response, not a normal
part of holdout maintenance.

If a holdout annotation is shown to an agent or model, retire and replace that
holdout. If a selected pair is published, treat it as public and replace it. If
a private file is staged or committed, stop publication and follow the incident
steps in the runbook.
