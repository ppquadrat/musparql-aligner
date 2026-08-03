# Ontology And Graph Shape Enrichment

Date: 2026-05-19

## Question

Would adding explicit ontology term context and observed graph-shape context to
LLM inputs improve generated natural-language questions?

## References

- Implementation commit: `94d966a` (`Add ontology and graph shape enrichment`)
- Preserved run/review/benchmark commit: `2c404b5` (`Record ontology shape enrichment review run`)
- Rollback commit: `b71956f` (`Revert ontology shape enrichment implementation`)
- Baseline run: `runs/2026-04-28-full-review-gpt5`
- Experiment run: `runs/2026-05-19-ontology-shape-gpt5`
- Evaluation report: `evals/reports/ontology-shape-2026-05-19`
- Comparison review export: `review/exports/musparql-review-compare-66270aafdb0e60f8-2026-05-20_00-09-27.json`
- Baseline benchmark: `benchmark/v3`
- Dataset review update: `benchmark/v4`

## Method

The experiment added opt-in evidence enrichment:

- `ontology_term_context`: query-scoped labels, comments, domains, ranges, and
  class/property facts from explicit ontology sources.
- `graph_shape_context`: observed subject/object shapes and common predicates
  from local RDF dumps.

The run used the same baseline generation model path (`gpt-5`) and compared
against the previous full review run.

## Results

Generation completed successfully:

- Outputs: 160/160
- Errors: 0

Autoeval against `benchmark/v3`:

- Baseline mean semantic score: 4.433
- Experiment mean semantic score: 4.066
- Script-classified regressions: 8
- Script-classified improvements: 1
- Raw score deltas: 40 unchanged, 4 improved, 16 worsened

Reviewer inspection produced useful dataset updates, but did not support adopting
the enrichment as a default method.

The review decisions were applied separately to create `benchmark/v4`:

- Pipeline assessment accepted: 46
- Pipeline improvement recommended: 15
- Excluded: 4
- Applied comparative-review decisions: 31

## Observations

The added context sometimes improved terminology or made a question more precise.
However, it also introduced noise:

- Some questions became too close to ontology/data-model language.
- Some answer-shape details were dropped, including evidence text, coordinates,
  distinctness, or selected output columns.
- Some outputs added extra specificity not required by the SPARQL.
- The model sometimes over-compressed report-style queries when ontology context
  made the main entity relation look obvious.

The evidence/provenance-text issue remains ambiguous. Selected provenance fields
such as `mtp:hasEvidenceText` may be semantically significant in some queries,
while in others they may be routinely returned for provenance because they were
already retrieved. We should document and review these cases rather than mutate
SPARQL automatically.

## Decision

Do not adopt ontology/graph-shape enrichment as a default pipeline step.

Keep the generated run, comparative review, evaluation report, and `benchmark/v4`
as history. Revisit the idea only with more constrained prompt guidance or a
smaller, query-shape-specific diagnostic that does not rewrite SPARQL broadly.
