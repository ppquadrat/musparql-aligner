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

#### Follow-up: album titles and `jazzontology-0002`

Inspection of the Jazz Ontology paper, supplement, data-construction code, and local graph showed that the apparent Release/Album substitution is real but dataset-dependent. The ontology itself keeps the concepts distinct: an album is an `mo:SignalGroup`, while an `mo:Release` is a publication of that album. In the Jazz Encyclopedia (JE) importer, each of the five Encyclopedia parts is represented by both a SignalGroup and a Release, the same part title is attached to both resources, and the SignalGroup is linked to its publication with `mo:published_as`. The ILL importer follows a different pattern: it puts the source `album` value on `mo:Release` and does not create the corresponding SignalGroup. A code comment explicitly questions whether album and release are the same in that source. Thus ordinary album titles generally live on Release resources in the ILL portion, whereas the five JE part titles are also available on SignalGroup resources.

This explains the source semantics of `jazzontology-0002`. Version 0 is a parameterised lookup for a titled `mo:SignalGroup`; it was written specifically to locate one of the five JE parts, not as a general album-title query. Version 1 instantiated the placeholder with *Bix - A Tribute To Bix Beiderbecke* and changed the class to `mo:Release`. Although that query executes and the Release-to-album wording is defensible for other examples such as `jazzontology-0016` and `jazzontology-0024`, the class change altered 0002's source information need to fit the more common ILL data shape. Version 2 therefore restores `mo:SignalGroup` and instantiates the placeholder with the first JE part title, *The Encyclopedia of Jazz, Part 1: Classic Jazz - From New Orleans to Harlem*. It executes successfully and returns the intended JE SignalGroup.

Execution did not make the pair suitable for NL-SPARQL evaluation. The fact that the query is intended to find the five specially modelled JE parts cannot be inferred from its graph pattern alone; a fluent question such as “What album has this title?” conceals the importer-specific assumption that makes the lookup meaningful. In the v10 review, `jazzontology-0002` was therefore dismissed as a technical query whose real information need depends on graph-construction knowledge unavailable from the SPARQL. `linkedmusic-0009` was accepted as its replacement so that the public benchmark remains at 100 pairs. Separately, `jazzontology-0016` was retained and updated because it is a valid case where an `mo:Release` may naturally be described as an album; its revised wording also restores the performing band previously omitted by the model. The episode distinguishes three decisions that must not be conflated: correcting a query to preserve its source semantics, deciding whether ontology terminology may be mediated in natural language, and deciding whether the corrected pair belongs in the benchmark at all.

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
