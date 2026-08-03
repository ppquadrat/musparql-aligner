# LinkedMusic Source vs Edited SPARQL Comparison

Date: 2026-08-03

## Question

Do the twenty Musparql-edited LinkedMusic queries execute more successfully
than the corresponding official public examples, and can both outcomes be
retained without replacing source-authored SPARQL?

## Implementation

- Implementation commit: `61a4c94`
- Endpoint: `https://virtuoso.simssa.ca/sparql/`
- Official public queries: retained version `0`
- Musparql-edited working copy: retained version `1`
- Command:

  ```bash
  .venv/bin/python run_queries.py \
    --kg-id linkedmusic \
    --source-id linkedmusic-corrected-examples-working-copy \
    --sparql-version all
  ```

The first sandboxed attempt could not reach the endpoint and recorded forty
`skipped_endpoint_unavailable` observations. The command was then rerun with
network access. Both attempts remain in `kg_queries.jsonl` execution history;
the skipped observations did not overwrite earlier successful executions.

## Results

Forty live jobs were executed: twenty queries at each version.

| Outcome | Version 0: official | Version 1: edited |
| --- | ---: | ---: |
| `ok` | 14 | 14 |
| `http_error` | 5 | 5 |
| `request_error` | 1 | 1 |
| `empty` | 0 | 0 |

All fourteen successful pairs returned the same result count in both versions.
The same six query labels failed at both versions:

- `linkedmusic-0061` through `linkedmusic-0065`: HTTP errors. Version `0` of
  `linkedmusic-0061` returned HTTP 400 while its version `1` returned HTTP 500;
  the other HTTP failures returned 500.
- `linkedmusic-0068`: read timeout at both versions.

Thus the edited text did not improve the coarse executable/non-executable
outcome in this endpoint snapshot. It remains valuable as a separately
attributed retained version rather than a replacement for the official source:
fifteen edits make the examples self-contained, while the five substantive
federated-query rewrites remain unvalidated by successful execution.

## Review and decision

Three independent code-review passes covered execution integrity, schema and
benchmark propagation, migration safety, provenance, and legacy compatibility.
All reported findings were fixed and the final suite passed 74 tests before the
live rerun.

Adopt the versioned representation and retain both query texts and their
execution histories. The experiment itself did not update the benchmark. A
subsequent human-adjudicated migration selected the edited version 1 queries
and canonical LinkedMusic identities for benchmark v8; its manifest points back
to the execution ledger and records the observation cutoff and outcome counts.
