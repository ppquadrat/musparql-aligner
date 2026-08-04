# Holdout review runbook

Use this checklist when selecting or handling holdout pairs. The rationale and
security model are in [HOLDOUT_SECURITY.md](HOLDOUT_SECURITY.md).

## Before review

1. Decide whether holdout identities may be visible to agents. Record the choice
   as **identity visible** or **identity private**.
2. Finish or close all agent-assisted tasks.
3. Serve only the review application on the loopback interface:

   ```bash
   python3 -m http.server 8000 --bind 127.0.0.1 --directory review
   ```

4. Open `http://127.0.0.1:8000/`.
5. Confirm where the private export will be stored. Prefer encrypted storage or
   a location outside the workspace. `review/private/` is ignored but is not an
   encrypted access boundary.

## Choose the holdout set

Choose the holdout as a small, representative evaluation sample—not as a list of
the easiest, hardest, or most memorable examples.

For this benchmark, use about **10% of eligible pairs** as the default target.
Treat 5% as a lower bound only. With roughly 100 pairs, five holdouts are usually
too few to cover several KGs and query patterns; eight to ten is a more useful
target while leaving most data available for development. If the benchmark
grows substantially, revisit the percentage and report uncertainty around any
holdout result. A set of eight to ten pairs is useful as a protected qualitative
check, but it is not large enough for a precise aggregate performance claim.

Select across:

- the represented KGs, roughly in proportion to their benchmark presence while
  ensuring that smaller KGs are not absent;
- different query shapes and complexity levels;
- different evidence and formulation origins;
- straightforward, ambiguous, and graph-context-dependent cases; and
- relevant execution outcomes or endpoint requirements.

Make the selection before using its reviewer annotations to tune prompts or
systems. Record the intended size and sampling rationale in a human-only note.
Do not put that note into an agent-readable selector.

## During review

1. Review candidates normally.
2. Mark a selected candidate **Private holdout**.
3. Treat every annotation on that pair as private, including corrected wording,
   comments, ratings, and fields whose names contain the word `public`.
4. Do not resume an agent task while private annotations remain in browser
   storage.
5. Use the **Set → Holdout only** filter and the **Holdout** summary count to
   check the selected set's size and coverage before exporting.

## Export and close the private session

1. Select **Export Non-Holdout**. Move that file to
   `review/public_exports/` if it will be used by agent-facing build tools.
2. Note the **Holdout** count, then select **Export Private Holdout**. The
   workbench reports how many records the download contains.
3. Move the private file outside the workspace or to `review/private/`.
4. Open the private file yourself. Confirm that it opens and that its record
   count matches the workbench's Holdout count and export message. Do not ask an
   agent to verify it.
5. Select **Clear Private State** and confirm the deletion.
6. Close the review browser tab or window.
7. Only now resume agent-assisted work.

## Continue the public workflow

Agent-facing benchmark tools may receive only a browser export whose kind is
`non_holdout_review_export`, normally from `review/public_exports/`.

Under the identity-visible policy, provide a separate selector-only JSON or
JSONL file when filtering is required. Each record may contain only:

```json
{
  "kg_id": "example-kg",
  "query_id": "example-query",
  "sparql_version": 1,
  "sparql_hash": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
}
```

The version and hash may both be omitted. Never add review decisions, comments,
wording, ratings, timestamps, or provenance to the selector.

Under the identity-private policy, do not create an agent-visible selector.
Perform filtering in the human-only environment before agent-facing processing.

## Before committing or publishing

1. Confirm that repository hooks are enabled:

   ```bash
   git config --get core.hooksPath
   ```

   The expected value is `.githooks`.

2. Run the committed/staged boundary checks through the normal commit and push
   workflow. Do not bypass hooks with `--no-verify`.
3. Build releases with `benchmark/build_public_release.py`; do not publish a
   working snapshot or working-directory copy.
4. Inspect only the allowlisted release inventory. Never use an agent to inspect
   a private directory as part of a publication audit.

## If something goes wrong

- **Private file staged but not committed:** stop. In a human-only session,
  unstage it and verify that it was never pushed.
- **Private file committed:** freeze publication. Audit refs, stashes, reflogs,
  CI artifacts, and remotes in a human-only incident process.
- **Annotation shown to an agent/model:** retire and replace that holdout.
- **Selected pair published:** treat the pair as public and replace it.
- **Private export missing or unreadable:** do not clear browser state. Recover
  or repeat the export during the same human-only session.

History rewriting can reduce further distribution, but it cannot restore a
secrecy guarantee after material has reached another system or person.
