# Holdout security policy

## Scope

Candidates may be processed by agents before a human designates a holdout. From
the moment of designation, every human field attached to that candidate is
private: disposition, pipeline assessment, preferred or literal wording,
comments, interpretive ratings, timestamps, provenance, and the combined private
record. A field called `public_comment` is still private when it belongs to a
holdout.

The selected candidate's SPARQL, evidence, and generated formulation must be
absent from the paper's public repository and release artifacts. Existing
candidates already reachable through this repository's public Git history cannot
satisfy a "never publicly released" claim; strict holdouts must come from fresh
candidate material created after this boundary is in place.

Holdout membership is a separate policy choice:

- **Identity visible:** agent-facing tools may consume a selector-only file with
  `kg_id`, `query_id`, `sparql_version`, and `sparql_hash`, but no reviewer fields.
- **Identity private:** filtering must happen in a human-only environment. No
  selector or before/after disposition ledger may enter the agent workspace.

Omission from a candidate universe already seen by an agent can reveal membership,
so the identity-private policy requires a fresh human-only candidate universe.

## Repository invariants

The repository must never contain a real private annotation, private holdout
export, annotation-bearing holdout snapshot, or unfiltered candidate pool.

- Legacy/raw browser exports live under ignored, agent-forbidden
  `review/exports/`; private exports live under ignored `review/private/` or
  outside the workspace.
- Sanitized non-holdout decision exports live under ignored
  `review/public_exports/` as agent-readable working inputs. The
  paper tree contains only allowlisted benchmark outputs, not review exports.
- Private browser exports use an opaque
  `musparql-holdout-private-*.json` filename and remain ignored.
- `benchmark/v*/holdout.jsonl` is not a supported snapshot artifact.
- Public builders reject `withheld` and `private_holdout` records instead of
  routing them into a private partition.
- Agent-facing tests use synthetic records only.

The public-tree check and repository hooks are tripwires. They inspect staged
blobs, the committed tree, and every outgoing commit without crawling ignored
private locations.

## Human review procedure (current, unencrypted)

Until encrypted storage is implemented, this procedure is best-effort rather
than a cryptographic access boundary:

1. Conduct review without an agent active.
2. Use the two separate workbench actions: export the sanitized non-holdout
   decisions, then explicitly export the self-contained private holdout.
3. Move the non-holdout file to `review/public_exports/`. Move the private file
   to `review/private/` or, preferably, outside the workspace. Open the private
   file and verify its holdout count before continuing.
4. Use **Clear Private State** (which requires a private export in the current
   session), then close the review browser before resuming
   agent-assisted work.
5. Commit only allowlisted benchmark artifacts, never a review export.

Serve only the review application, bound to loopback:

```bash
python3 -m http.server 8000 --bind 127.0.0.1 --directory review
```

Plaintext under the same OS account remains readable in principle. A separate
plaintext repository on the same computer is an organizational boundary, not an
agent access boundary.

## Preferred future boundary

The private browser export should eventually be encrypted before it touches
disk, using authenticated encryption and a passphrase or key that is never saved
on the computer, placed in an environment variable, passed on a command line, or
stored in an agent-accessible credential store. Decrypt only during a human-only
session and lock the store before resuming agent work. A separate OS account,
removable encrypted device, or separate review machine is stronger.

## Publication

The paper repository contains source code, synthetic fixtures, sanitized public
benchmark artifacts, and documentation. It excludes raw query pools, prompt
inputs, raw model outputs, frozen runs, generated review bundles, raw review
exports, and holdout records. Publish from the tracked allowlisted tree, not a
copy-and-delete workflow.

Local hooks should be enabled with:

```bash
git config core.hooksPath .githooks
```

CI repeats the public-tree check, but CI on a public remote detects a leak only
after data has left the machine. The ignore rules and pre-push check are the
primary publication controls.

Removing old artifacts from the current tree does not purge prior Git history.
Before first publication, create the public repository from an allowlisted
history-free export (or perform a separately authorized, human-audited history
rewrite). Never treat a deletion commit alone as historical erasure. Material
already pushed to a public remote must be treated as public and retired from any
strict holdout.

## Incident response

- If an annotation is shown to an agent or model, retire and replace that
  annotation/holdout.
- If a selected candidate is pushed publicly, treat it as public and replace it.
- If a private file is staged but not committed, unstage it through a human-only
  process and verify that it never reached a remote.
- If a private file is committed, audit all refs, stashes, reflogs, CI artifacts,
  and remotes. History rewriting limits distribution but does not restore secrecy.
- Freeze publication during an incident review.
