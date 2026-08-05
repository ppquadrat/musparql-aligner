# SPARQL editing policy

## The short version

Keep the query found in the source unchanged. If it needs correction, add a new
version, record why it changed, and require a human to approve it. A query that
has ever received a retained SPARQL edit cannot be used as a holdout pair.

## What happens automatically

When the query pipeline runs, it records queries that could not be executed.
This includes endpoint rejections, unresolved placeholders, unavailable
endpoints, specialised-runtime requirements, and less specific failures. These
records are automatically placed in the correction queue with the execution
observation and available source evidence.

Failure does not prevent a query from appearing in correction review. The queue
is for diagnosis: some entries need a SPARQL edit, while others need parameters,
different infrastructure, or no action at all.

## What must remain unchanged

The source SPARQL is version `0` and is never overwritten. Existing edited
versions are never overwritten either. An approved correction is appended as
version `1`, `2`, and so on.

This lets us answer both questions later: “What did the source contain?” and
“What query did Musparql actually use?”

## What a human approves

Before approving, the reviewer should compare the proposed query with the
source query and its evidence. Approval records:

- the complete proposed SPARQL;
- the kind of edit;
- a brief explanation;
- the evidence supporting the intended meaning;
- whether the proposal came from a human, an agent, or a source artifact; and
- the agent/model name when an agent made the proposal.

The system records technical provenance such as query identity, versions,
hashes, execution failure, timestamps, and review-export digest automatically.

The edit types mean:

- **Syntax correction:** repairs malformed SPARQL without changing the question.
- **Endpoint adaptation:** expresses the same question in the endpoint's dialect.
- **Parameter instantiation:** replaces a placeholder with a concrete value.
- **Benchmark specialisation:** deliberately narrows or fixes the benchmark query.
- **Federation rewrite:** changes how remote graphs or services are queried.
- **Performance optimisation:** seeks the same answer more efficiently.
- **Other:** none of the above; explain the choice in the rationale.

Execution success alone is not enough to prove that a correction preserves the
intended meaning. The human review of the query and evidence is therefore the
approval step.

## What happens after approval

Approval retains the new version immediately. The current UI does not execute
it. The pipeline must subsequently run the latest version against the relevant
endpoint or local dataset.

If execution fails, the correction remains retained and may appear in the queue
again. It is still available for correction review. It is simply not passed to
NL generation or a benchmark until the same version and hash has an `ok` or
`empty` execution record. This prevents an untested edit from silently becoming
the benchmark query.

## Holdout rule

Once a query identity has a retained SPARQL edit, it is permanently ineligible
for holdout inclusion—even if somebody later selects the original version.

Correction review must never expose a selected holdout pair. Commands that read
the canonical query collection therefore require the annotation-free holdout
selector file and remove those identities before creating correction data. The
apply command also refuses any reviewed pair found in the selector file.

## Data-handling rule

Correction queues and exports are local working data. Keep exports under the
ignored `review/public_exports/` directory and do not commit them. Never put raw
holdout annotations into a correction queue, browser bundle, test fixture, or
agent prompt. Tests must use clearly synthetic examples.

For the commands to follow, use
[SPARQL_CORRECTION_RUNBOOK.md](SPARQL_CORRECTION_RUNBOOK.md).
