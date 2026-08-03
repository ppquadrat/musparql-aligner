# Linguistic Dimension Ratings Need Pairwise Framing

Date: 2026-07-14

## Context

During review of the new MiniMax Musow bundle, the reviewer sampled Musow
records whose generated questions were aligned with public musoW competency
questions from the source README. The review UI exposes optional linguistic
dimension sliders:

- naturalness
- pragmatism
- room for interpretation

## Finding

The current standalone rating design is underspecified. These dimensions are
more meaningful when applied comparatively, for example:

- literal source wording vs preferred reviewer wording
- evidence wording vs generated wording
- generated wording vs corrected canonical wording

When the reviewer assigns a single absolute score, it is not always clear what
the score is being compared against. This makes the resulting annotations hard
to interpret as an experiment.

`room_for_interpretation` is especially ambiguous because its direction is less
obvious than naturalness or pragmatism. It can mean that a question is usefully
broad, dangerously underspecified, or semantically open in a way that depends
on the SPARQL/query context.

## Decision

Do not rely on the current standalone linguistic-dimension ratings as a clean
experimental result. Continue the current benchmark review without assigning
these dimensions systematically.

## Follow-Up

Revisit the linguistic dimensions as a separate annotation design task. A better
design should specify:

- the comparison target for each rating
- the intended direction of each scale
- whether `room_for_interpretation` is a quality dimension, an ambiguity flag,
  or a separate semantic-risk annotation
- whether ratings belong on single benchmark records or on pairs of alternative
  formulations
