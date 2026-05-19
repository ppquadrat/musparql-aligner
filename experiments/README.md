# Experiment History

This directory records pipeline experiments whose outputs, reviews, or evaluation
reports are worth preserving independently of benchmark updates.

Use an experiment note when a run changes extraction, enrichment, prompting,
model choice, or evaluation procedure. The note should make the adoption decision
explicit even when the generated run is useful for dataset review.

Recommended fields:

- date
- implementation commit
- rollback/superseding commit, if any
- generation run
- comparison review export
- evaluation report
- benchmark baseline
- resulting benchmark update, if applicable
- outcome and adoption decision
- observations for future work

The benchmark answers which NL-SPARQL pairs are valid dataset examples. Experiment
notes answer which pipeline variants were tried and what happened.
