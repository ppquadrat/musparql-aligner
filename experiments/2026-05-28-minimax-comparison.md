# GPT-5 (`gpt-5-2025-08-07`) Vs MiniMax-M2.5 Generation Review

Date: 2026-05-28

## Question

How does MiniMax-M2.5 compare with `gpt-5-2025-08-07` for generating natural-language
questions from SPARQL, evidence, and KG metadata?

## References

- Baseline run: `runs/2026-05-19-ontology-shape-gpt5`
- Experiment run: `runs/2026-05-22-graphia-minimax-m25`
- Comparison review export: historical raw artifact, intentionally not retained in the public tree
- Baseline benchmark: `benchmark/v4`
- Dataset review update: `benchmark/v5`
- Reviewer writeup: `/Users/polina/Downloads/Review Musparql MiniMax 2.5.docx`

## Model provenance

The baseline run requested the OpenAI model alias `gpt-5`. Its run manifest and
the response metadata stored with all 160 generation records identify the
resolved response model as `gpt-5-2025-08-07`. The comparison run requested
`MiniMax-M2.5` through Graphia's OpenAI-compatible endpoint, and both its run
manifest and response metadata identify the returned model as `MiniMax-M2.5`.
The exact identifiers are recorded in the `model_provenance` sections of the two
run manifests and in each record's `response_metadata`.

## Method

The MiniMax run used Graphia's OpenAI-compatible chat completions endpoint with
`MiniMax-M2.5`. Inputs were rebuilt from `kg_queries.jsonl`, excluding dismissed
and private-holdout records from `benchmark/v4`.

The comparative review evaluated changed benchmark pairs between the GPT-5
ontology/shape run and the MiniMax run. The review was then applied to produce
`benchmark/v5`.

## Results

Generation completed successfully:

- Outputs: 156/156
- Errors: 0

The comparative review covered 61 changed benchmark pairs:

- Pipeline assessment accepted: 47
- Prompt improvement recommended: 13
- Excluded: 1
- Private holdout: 0

Status movement relative to `benchmark/v4`:

- 38 stayed accepted
- 9 moved from an improvement recommendation to accepted
- 7 moved from accepted to prompt improvement recommended
- 6 retained the prompt improvement recommendation
- 1 changed from included to excluded

The review update produced `benchmark/v5`:

- Benchmark records: 60
- Included: 60
- Pipeline assessment accepted: 47
- Prompt improvement recommended: 13
- Excluded: 5
- Holdout: 0
- Alternative-formulation records: 45

## Observations

MiniMax is usually more pragmatic and natural than GPT-5. It often produces
more fluent NL questions, and in some cases its phrasing is better than the
reviewer's own first wording. It also preserves the intent of the source
evidence well, especially when important signals are only present in
natural-language evidence rather than SPARQL or ontology terms.

Examples from Jazz Ontology:

- MiniMax used "timestamps" for solo timing because that term was present in the
  Jazz Ontology supplement evidence.
- MiniMax used the "same audio" / duplication interpretation for fingerprints
  because that intent was present in the CQ evidence, even though it was not
  explicit in the SPARQL or ontology.

This is especially notable because MiniMax-M2.5 is a much smaller, open-source,
free model in this setup.

GPT-5 tended to trim evidence more aggressively. This sometimes made its
questions cleaner, but could drop important context. MiniMax's evidence reliance
is a tradeoff: it can preserve strong evidence signals, but it may also preserve
awkward or incorrect source grammar.

MiniMax sometimes omitted important SPARQL constraints. The main observed
example was Jazz Ontology, where band information could be dropped.

## Evidence ID Drift

MiniMax has a systematic evidence citation problem. It often confuses CQ numbers
with `evidence_id` values. In Jazz Ontology, CQ evidence starts at `e2`, so CQ5
is `e6`, CQ6 is `e7`, and so on. MiniMax often cited `e5` for CQ5, `e6` for CQ6,
etc.

Automated citation checks found:

- MiniMax retained evidence phrases: 197
- MiniMax likely wrong evidence IDs: 12
- GPT-5 retained evidence phrases in the previous run: 238
- GPT-5 likely wrong evidence IDs by the same check: 0

Examples:

- `jazzontology-0020`: cited `e5`, should be `e6`
- `jazzontology-0021`: cited `e6`, should be `e7`; cited `e7`, should be `e8`
- `jazzontology-0023`: cited `e8`, should be `e9`; cited `e6`, should be `e7`
- `jazzontology-0024`: cited `e9`, should be `e10`; cited `e6`, should be `e7`; cited `e7`, should be `e8`
- `organs-0006`: cited `e5` for CQ5 wording, but the actual evidence is `e6`

The generated questions can still be useful, but MiniMax evidence IDs should be
validated before using them downstream.

This guardrail has been implemented in `run_llm_generation.py@v4`. The runner
checks each retained evidence phrase against the evidence snippets in the same
prompt input, repairs clear `evidence_id` drift before schema validation, and
records ambiguous or weak cases in `citation_validation` warnings.

## Reviewer Variation

Some changes reflect reviewer learning rather than only model behavior. Repeated
review exposed mistakes in earlier decisions, especially as familiarity with a
KG or ontology improved. This creates expected intra-rater variation and can
contaminate direct model comparisons: a changed decision may reflect a corrected
review judgement rather than a changed generated phrase.

Examples:

- `meetups-0013`: the previous wording missed several returned values and was
  biased by the earlier generated NL. The MiniMax generation was more accurate.
- `meetups-0006`: the reviewer initially preferred "types", then reconsidered
  because "purposes" is semantically plausible. The code comment says "all the
  meetups types", so both phrasings were kept as alternatives.
- `organs-0006`: the IRI contains `Part`, suggesting an organ part, but the
  `includesWhole` pattern and inverse property `isWholeIncludedIn` suggest the
  query refers to a whole physical organ. The previous preferred wording allowed
  for a part and was probably wrong.
- `organs-0004`: both models generated "descriptions", while the variable name
  `external_uri` suggests website URLs. The ontology did not clearly support the
  URL interpretation. The record was approved despite the ambiguity.

## Alternative Formulations

The comparative review produced the records now stored in
`benchmark/v5/alternatives.jsonl`. They preserve:

- previous accepted gold questions,
- accepted model outputs when different from preferred wording,
- reviewer-provided literal SPARQL wordings from `literal_wording` or `Literal:`
  note lines.

Literal SPARQL wordings are stored separately under `literal_formulations` with
`source_type: "literal_sparql_wording"`. They are intended to capture exact
query semantics when a more natural preferred wording is less literal.

## Decision

Move generation to MiniMax-M2.5, with an evidence-citation guardrail.

MiniMax is strong enough to adopt as the generation model because it often
preserves source evidence and produces more natural, pragmatic questions than
GPT-5 in this review. However, it needs safeguards:

- validate retained evidence IDs,
- review cases where SPARQL constraints may be omitted,
- keep ambiguity records so literal, preferred, previous, and model-generated
  wordings can coexist.

Keep the MiniMax run, comparative-review export, and `benchmark/v5` as the
preserved output of this experiment.
