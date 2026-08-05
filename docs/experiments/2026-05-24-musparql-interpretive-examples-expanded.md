# Interpretive Mediation Between Formal Graph Semantics and Human Communicative Intent

Expanded notes from discussions around Musparql, NL↔SPARQL evaluation, interpretive mediation, communicative adequacy, and longitudinal benchmark evolution.

## Examples

### Example 1 — Release vs Album

**Underlying ontology/query structure:** The Music Ontology distinguishes an album-level grouping (`mo:SignalGroup`) from a particular publication of it (`mo:Release`). Publication dates and places can be represented through a related `mo:ReleaseEvent`. In many dataset records, however, only one release of an album is represented, and ordinary language collapses these distinctions.

**Observed graph pattern:** One benchmark query selects the `mo:Release` titled “On Broadway, Vol. 1,” follows its medium and tracks, and retrieves the recording place, date, and performing band.

**Possible literal rendering:** “Where, when, and by whom were the tracks on the release *On Broadway, Vol. 1* recorded?”

**Possible mediated rendering:** “Where, when, and by whom were the tracks on the album *On Broadway, Vol. 1* recorded?”

**Interpretive issue:** Here, “album” is not a direct verbalisation of the queried RDF class: the album concept is represented separately as `mo:SignalGroup`. It is a context-dependent description of the particular release. The substitution is natural where the release corresponds to the single album publication relevant to the question, but it cannot be applied mechanically because an album may have multiple releases and a release may contain multiple media.

**Relevant dimensions:** naturalness, conceptual adequacy, ontological granularity, room for interpretation.

### Example 2 — Aggregation and Salience

**Underlying query logic:** The SPARQL query computes counts of encounters per place, sorts them, and returns the top-ranked places together with counts.

**Possible literal rendering:** “What are the two places where most encounters took place, and how many encounters took place at each of them?”

**Possible mediated rendering:** “What are the two places where most encounters took place?”

**Interpretive issue:** The counts are operationally necessary for ranking but may not be communicatively central. Literal inclusion of all returned values can reduce readability and obscure primary intent.

**Relevant dimensions:** pragmatism, communicative salience, completeness.

### Example 3 — Evidence Text Ambiguity

**Observed generated rendering:** “Which meetups took place in Vienna, and who participated, when and where did they occur, what was their purpose, and what is the supporting evidence text?”

**Interpretive issue:** The communicative role of “supporting evidence text” is ambiguous. In some graphs, evidence text is merely operational provenance used to connect entities; in others, evidence itself may be semantically central.

**Key problem:** Communicative salience cannot always be inferred from SPARQL structure alone. Interpretation may require graph-specific knowledge.

**Proposed annotation flag:** requires graph/context knowledge.

### Example 4 — Operational vs Conceptual Semantics

**Underlying operational mechanism:** Audio tracks are compared via short audio fingerprints or fingerprint similarity metrics.

**Possible literal rendering:** “Which pairs of audio signals have the same short audio fingerprint?”

**Possible conceptual rendering:** “Are there duplicate tracks in the dataset?”

**Interpretive issue:** The conceptual rendering hides implementation details and expresses likely user intent. The literal rendering preserves operational semantics but may fail communicatively for domain users.

**Emerging distinction:** operational semantics vs conceptual semantics vs communicative intention.

## Broader Conceptual Framing

The discussion increasingly suggests that NL↔SPARQL evaluation should not be reduced to binary semantic equivalence. Instead, it may require modeling multiple interacting layers:

| Layer | Question |
| --- | --- |
| Formal adequacy | Does the rendering preserve graph semantics? |
| Communicative adequacy | Would a human naturally ask or understand this phrasing? |
| Conceptual adequacy | Does the rendering capture the domain-level concept the user likely intends? |

## Proposed Interpretive Dimensions

| Dimension | Scale |
| --- | --- |
| Naturalness | awkward/formal ↔ fluent/human |
| Pragmatism | exhaustive/literal ↔ concise/salient |
| Room for Interpretation | objective/constrained ↔ subjective/open |

## Relevant Linguistic and Semantic-Web Areas

| Field | Relevance |
| --- | --- |
| Pragmatics | communicative intent, salience, relevance, implicature |
| Semantics–Pragmatics Interface | literal meaning vs intended meaning |
| Controlled Natural Language | ontology verbalization and graph-to-text |
| Translation Studies | fidelity vs communicative adequacy |
| Human-Centered NLG | semantic mediation and explainable generation |

## Emerging Research Question

**How do humans negotiate between formal graph semantics and communicative intent when interpreting and verbalizing knowledge-graph queries?**
