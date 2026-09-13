Provenance-Aware Curation of NL-SPARQL Evaluation Data for Domain Knowledge Graphs

Polina Proutskova<sup>\*\[0000-0002-7514-2711\]</sup>

Industry Commons Foundation

polina.proutskova@industrycommons.net

**Abstract.** Natural-language interfaces could open specialist knowledge graphs to researchers who do not write SPARQL, but trustworthy evaluation data for these systems remain labour-intensive to curate. Relevant queries and the language that explains them are scattered across project artifacts, while fluent generated questions can conceal omissions, misleading terminology, or unsupported interpretations. This paper presents Musparql, an open-source workflow and review workbench that combines human-authored queries with source evidence, execution observations, provenance, and provisional natural-language formulations. A study of 100 reviewed natural-language/SPARQL pairs from five music-domain knowledge graphs shows that human review remains necessary where graph operations meet domain meaning and communicative intent. Musparql assembles and presents the evidence needed for those decisions, allowing the expert to adjudicate traceable candidates rather than construct every pair end to end. Each intervention remains visible from source material to published benchmark. The resulting evaluation data are easier to inspect and more trustworthy to reuse, while correctness remains under expert control.

**Keywords:** metadata curation; knowledge graph question answering; benchmark construction; human-in-the-loop evaluation; music knowledge graphs

1.  Introduction

SPARQL gives precise access to structured knowledge, yet it remains difficult to use for researchers who are specialists in a subject rather than in Semantic Web technologies \[9\]. The difficulty is not only syntactic. A user must also discover a graph's classes, properties, modelling choices, and sometimes its endpoint-specific conventions. Natural-language interfaces promise to mediate this gap, but meaningful evaluation requires question-query pairs that are both formally faithful and plausible within the domain.

Established knowledge-graph question answering benchmarks largely concern general-purpose resources such as DBpedia and Wikidata \[6, 14, 23\]. Smaller research knowledge graphs present a different data-curation problem. Their examples are dispersed across repositories, papers, guides, endpoint interfaces, XML or JSON backing files, and presentation material. A query may be human-authored and useful even when its natural-language description is elsewhere. Conversely, an executable query may be an administrative operation, a synthetic fixture, or an intermediate analysis rather than a plausible information need. Turning these materials into evaluation data therefore requires source criticism, alignment, versioning, and expert interpretation.

Fully manual benchmark construction asks an expert to rediscover evidence, author wording, execute queries, compare revisions, and maintain provenance as well as decide meaning. Musparql changes that division of labour. It does not automate semantic judgment; it automates the assembly, alignment, execution, comparison, and provenance work needed to make that judgment focused, inspectable, and reusable. For the reported benchmark v10 snapshot, KGs with their SPARQL and natural language phrases sources were seeded manually[^1]. From the seeds, the system aligns and packages traceable candidate pairs; the expert determines benchmark membership and canonical wording.

Musparql is an open-source workflow and review workbench for building natural-language/SPARQL evaluation data from existing domain knowledge-graph artifacts. It brings together source and query acquisition, evidence retrieval and alignment, LLM-assisted provisional formulation, execution records, expert review, and versioned publication. The software, documentation, and runbooks are available in the [GitHub repository[^2]](https://github.com/ppquadrat/musparql-aligner), and the reviewed v10 benchmark can be downloaded from the [benchmark directory[^3]](https://github.com/ppquadrat/musparql-aligner/releases/download/benchmark-v10/musparql-benchmark-v10.zip).

The paper addresses two research questions. RQ1 asks: How can dispersed SPARQL and natural-language evidence be transformed into traceable, reusable evaluation data for heterogeneous domain knowledge graphs? RQ2 asks: How often, and for what semantic or pragmatic reasons, does expert review materially change the canonical natural-language formulation? The empirical basis is an audit of 100 reviewed pairs from five heterogeneous music knowledge graphs.

**Contribution.** Musparql contributes a provenance-aware curation workflow, an item-level artifact model, an audit of formulation origins and review outcomes, and an implemented workbench that supports comparison, append-only SPARQL correction, private holdout separation, and linguistic annotation. Music is the testbed; the central contribution is a reusable account of how machine assistance and expert adjudication combine in evaluation-data curation.

2.  Related Work and Curation Perspective

QALD, LC-QuAD 2.0, and later shared tasks established evaluation collections for natural-language access to linked data \[2, 6, 14, 21, 23\]. Surveys and dataset analyses show persistent limitations in coverage, transferability, and generalisability \[10, 11\]. These resources make comparison possible, but their question-query pairs are usually consumed as finished records rather than as outputs of a documented curation process.

Recent datasets begin closer to real information needs. SPINACH curates difficult requests associated with Wikidata activity, query-log work derives language/query evidence from public use, and bioinformatics collections pair expert questions with federated queries \[3, 13, 24\]. Competency-question research likewise connects domain requirements to formal queries \[8, 16, 20, 26\]. Human mediation is especially important in scientific and cultural-heritage graphs, where ontology terms and modelling conventions embody disciplinary choices \[4, 5, 16-18, 22\].

Musparql connects these strands with research-data curation. FAIR, PROV-O, datasheets for datasets, DCAT, and Croissant emphasise reusable objects, explicit provenance, documented creation decisions, and machine-readable publication metadata \[1, 7, 12, 15, 25\]. Here, provenance is item-level: source evidence, query versions, model proposals, execution observations, reviewer decisions, and publication views remain distinguishable. The gap addressed is therefore a process and metadata model for turning heterogeneous domain evidence into auditable evaluation data.

3.  Provenance-Aware Curation Method

The workflow separates acquisition, evidence alignment and provisional formulation, expert adjudication, and publication (Fig. 1). The separation prevents three common category errors: treating source-adjacent wording as query-matched evidence, treating a model proposal as approved data, and treating successful execution as semantic validation.

<img src="media/image1.png" title="Figure 1" style="width:4.8in;height:1.87569in" alt="Musparql workflow from manually curated source and query acquisition through evidence alignment, expert review, and publication, preserving provenance at each stage." />

**Fig. 1.** Musparql workflow. Source evidence, model proposals, execution observations, and reviewer decisions remain separate but linked.

1.  Stage 1: Source and Query Acquisition

The v10 benchmark was seeded with five music knowledge graphs, selected to vary in schema, documentation, access mode, and intended use, from a specialised organ-building project, through a large collection of jazz recording metadata, to a federation of heterogeneous music resources. Curated sources covered official repositories, publications, interfaces, notebooks, and example files. Human-authored SPARQL was extracted automatically with source location, revision, and hash; nearby comments, prompts, competency questions, and descriptions were retained as language evidence.

Collection was deliberately inclusive. User-facing examples sat beside administrative, parameterised, and application-internal queries because deciding usefulness was part of review. Of 106 acquired candidates, six were excluded before release because they were structurally incomplete or outside the benchmark's SELECT-query scope.

2.  Stage 2: Evidence Alignment and Provisional Formulation

For each acquired query, the workflow first retrieves candidate language written by source authors. Evidence may be directly paired with the query, appear in nearby comments or prose, or occur elsewhere in the project's competency questions, papers, or documentation. The LLM ranks the retrieved evidence for relevance to the query; a reviewer-visible selection records which phrases were retained and why. A provisional formulation may preserve source wording, paraphrase selected evidence, or be generated when no suitable phrase is found. The record stores cited evidence, retained phrases, the alignment decision, prompt, model, rationale, and proposed question. Every LLM-proposed formulation cites any retained evidence and remains a candidate until review. Execution against configured endpoints or local material adds a dated status, result count, timing, errors, and query hash; these are observations, not acceptance criteria.

3.  Stage 3: Expert Review

Reviewers see the SPARQL, selected and ranked source evidence, execution observation, model rationale, and proposed candidate pair. They can retain, rewrite, or dismiss the formulation; record acceptable alternatives and a reviewer-validated literal rendering; and identify source-data or pipeline problems. The preferred question becomes canonical only when a reviewer confirms it for scoring.

Comparative review is used after a change to evidence, prompting, models, or extraction. Previous and current LLM-proposed formulations appear side by side, allowing the reviewer to reuse the prior decision, accept a new question, or write better wording while retaining the earlier record. This append-only comparison supported targeted reconsideration across ten benchmark versions without rebuilding gold data after every pipeline experiment.

<img src="media/image2.jpeg" title="Figure 2" style="width:4.8in;height:2.94375in" alt="Comparative-review workbench showing a source SPARQL query with previous and current natural-language formulations and reviewer controls." />

**Fig. 2.** Comparative-review workbench for jazzontology-0016. The earlier formulation omits the performing band; the current formulation surfaces all salient answer dimensions.

4.  Stage 4: Publication

Publication derives a compact scoring view with one reviewer-confirmed question and selected SPARQL version per item, a public alternatives file, and a manifest summarising the build and execution snapshot. Richer records retain evidence ranking, generation activity, query corrections, review history, and accepted alternatives. The benchmark is therefore usable as a conventional scoring resource without flattening the curation chain that produced it.

4.  Artifact Model and Publication Views

A benchmark item is represented as a chain of related records (Table 1). The model distinguishes what existed before Musparql processing, what the system inferred or observed, and what a reviewer approved.

**Table 1.** Logical layers in the Musparql artifact; the public benchmark is a derived view.

| **Layer** | **Primary content** | **Curation function** |
|----|----|----|
| Source and query history | Location, revision, immutable SPARQL v0, append-only corrections | Separates source authorship from operational repair |
| Evidence alignment | Ranked phrases, retained text, citations, alignment decision | Makes query-specific grounding inspectable |
| Candidate and generation | Selected query, proposed wording, prompt, model, rationale | Keeps machine proposals provisional |
| Execution | Dated status, count, error, query hash | Records infrastructure behaviour as observation |
| Review | Retain/rewrite/dismiss, notes, alternatives | Establishes the canonical pair through expert judgment |
| Publication | Canonical question, selected SPARQL, release metadata | Provides a stable scoring view |

Musparql serialises this record in a project-specific JSONL schema but the correspondence to PROV-O classes is explicit: source snapshots, query and formulation versions, evidence bundles, execution observations, review-decision records, benchmark items, and releases are prov:Entity instances; acquisition, alignment, generation, execution, review, and publication are prov:Activity instances; and reviewers, software components, and language models are prov:Agent instances \[12\]. While derivation, generation, use, revision, and responsibility map to PROV-O relationships, reviewer approval remains a Musparql-specific relation. RDF export and validation of the mapping remain future work.

The public v10 benchmark snapshot comprises three JSON files: benchmark.jsonl, containing one reviewer-confirmed canonical question and selected SPARQL query per item; alternatives.jsonl, containing accepted non-canonical and literal formulations; and manifest.json, recording snapshot metadata, file inventory and counts. The benchmark database is licensed under CC BY 4.0, while the Musparql software is licensed separately under the MIT License; rights in the underlying source materials remain with their original providers.

Versioning is explicit at both query and benchmark levels. A source query is never overwritten; a tested correction is another version whose selection is recorded in the published item. Generation runs freeze their input and configuration so that later comparisons can attribute a wording change to a particular intervention. Benchmark releases likewise record the predecessor and build metadata. This supports reproducibility even when live endpoints, federated services, or upstream repositories change.

Private holdout stays outside public artifacts. The holdout does not conceal the source SPARQL or an earlier model proposal: it withholds the reviewer's gold decision and any reviewer-authored wording from LLM agent access. This implemented separation prevents direct tuning against private judgments while keeping public benchmark construction reproducible; the operational policy and failure procedure remain documented in the repository.

5.  Curation Audit of the v10 Snapshot

    1.  Corpus and Formulation Origins

The public v10 snapshot was built on 6 August 2026 and contains 100 reviewed SELECT queries. The five sources differ in modelling style and availability, making the snapshot a test of curation across heterogeneous domain artifacts rather than of one benchmark-generation recipe. Musical Meetups contains documentary evidence of historical encounters \[16\]; Organs represents instruments, components, locations, and builders \[5\]; DTL1000 (from Dig That Lick project) draws on the Jazz Ontology \[18\]; MusOW surveys music resources on the Web \[4\]; and LinkedMusic integrates heterogeneous music repositories \[17\] (Table 2).

**Table 2.** Benchmark coverage and origin of the initial natural-language candidate.

| **Knowledge graph**      | **Pairs** | **Source-grounded** | **Generated** |
|--------------------------|-----------|---------------------|---------------|
| Musical Meetups          | 30        | 20                  | 10            |
| Organs                   | 9         | 4                   | 5             |
| DTL1000 (JazzOntology)   | 20        | 9                   | 11            |
| Music On the Web (MusOW) | 20        | 18                  | 2             |
| LinkedMusic              | 21        | 20                  | 1             |
| Total                    | 100       | 71                  | 29            |

Seventy-one candidates were grounded in human-authored source language: 28 used wording already associated with a query and 43 paraphrased selected evidence. Twenty-nine were generated because no suitable source formulation was selected. Source grounding therefore reduced unsupported invention but did not by itself establish canonical wording.

The queries also vary substantially in formal structure. All 100 are SELECT queries, which makes the snapshot directly usable with conventional result-set execution, but many go well beyond a basic graph pattern. Table 3 reports the presence of selected SPARQL features; categories overlap because one query may use several features.

Table 3. SPARQL features in the 100 selected v10 queries.

| **SPARQL feature** | **Queries** | **SPARQL feature** | **Queries** |
|--------------------|:-----------:|--------------------|:-----------:|
| FILTER             |     43      | OPTIONAL           |     14      |
| Aggregate function |     42      | UNION              |     10      |
| SELECT DISTINCT    |     43      | Subquery           |      9      |
| ORDER BY           |     34      | BIND               |      6      |
| GROUP BY           |     33      | SERVICE            |      5      |
| VALUES             |     21      | HAVING             |      2      |

2.  Effect of Expert Review

The reviewer retained 52 initial formulations and rewrote 48 (Table 3). Rewrites affected three of 28 direct source formulations, 25 of 43 source-based paraphrases, and 20 of 29 generated formulations. Source grounding therefore reduces neither the need nor the responsibility for expert judgment. The 48% rewrite rate answers the quantitative part of RQ2 for this snapshot; it is a curation-history measure, not a universal rate or a model error estimate.

**Table 3.** Effect of expert review on the initial candidate.

| **Initial candidate**   | **Retained** | **Reviewer rewrite** | **Total** |
|-------------------------|--------------|----------------------|-----------|
| Direct source wording   | 25           | 3                    | 28        |
| Source-based paraphrase | 18           | 25                   | 43        |
| Generated               | 9            | 20                   | 29        |
| Total                   | 52           | 48                   | 100       |

For the selected query versions, 47 executions returned a non-empty result, 29 succeeded with an empty result, five produced HTTP errors, one a request error, eight required a skipped local runtime, and ten were unsupported in the configured runtime. The distribution is operational context, not a quality score: a meaningful query may be empty for a dated snapshot, and an endpoint failure says nothing about whether the question matches the SPARQL.

6.  Where Review Changes Meaning

Qualitative inspection of rewritten and disputed cases identified four recurring reasons for intervention. They are not an exhaustive taxonomy; they show why expert adjudication remains necessary after evidence has been assembled.

1.  Ontology Terminology and Answer-Variable Salience

Fig. 2 provides the central example because one pair exposes two independent review problems. The SPARQL selects the mo:Release titled *On Broadway, Vol. 1* and returns its tracks, performing band, recording date, and place. The reviewer flagged the earlier formulation, which called the release an 'album' and omitted the performers; a later run produced the current formulation, which the reviewer accepted.

While the missing band information was a semantic error, the album/release terminology decision was not straightforward. The Music Ontology distinguishes an album-level SignalGroup from its publication as a Release \[19\]. DTL1000 combines two source subgraphs with different modelling practice: in the Illinois dataset, Release and SignalGroup correspond one-to-one; in The Jazz Encyclopedia, SignalGroup is also used to encode a subgroup within a series. A user may therefore use 'album' naturally in one context without making the classes equivalent across the merged graph. The reviewer must determine whether the wording preserves the intended referent in the selected data as well as whether it surfaces the answer dimensions that matter.

2.  Computational Operation Versus Communicative Salience

Aggregation creates a second recurring ambiguity. A query may count encounters by place, order the groups, and return the two highest-ranked places together with their counts. The counts may be the requested answer - 'How many encounters occurred at each of the two most frequent places?' - or merely the mechanism used to identify the places - 'Where did most encounters occur?' The SPARQL structure permits both verbal descriptions, but the source context and intended use determine which should be canonical.

3.  Provenance Fields as Answers or Support

A Musical Meetups query may return an evidence excerpt alongside participants, time, place, and purpose. When the excerpt is routinely attached to graph statements for traceability, mentioning 'supporting evidence text' can overload the question with an implementation detail. When documentary support is itself the research interest, the same field becomes central. The reviewer therefore interprets the role of provenance in the graph rather than applying a rule that every projected variable must appear mechanically in the question.

4.  Implementation Mechanism Versus Domain Concept

A query for pairs of audio signals with the same short fingerprint can be verbalised literally as a graph-pattern relation or pragmatically as 'Are there duplicate tracks in the dataset?' The latter is more useful but introduces an inference: equal fingerprints are treated as evidence of duplication. Review determines whether that inference is justified by the data and intended application.

Across these cases, three concerns recur: preservation of formal graph semantics, expression of the relevant domain concept, and suitability as a communicative question. After semantic eligibility is established, differences among a literal reference, accepted alternatives, and the preferred formulation can be studied along naturalness, pragmatism or communicative salience, and room for interpretation. More openness is not automatically better: it may represent productive breadth or harmful underspecification. A separate reviewer workbench was implemented to measure the difference between accepted formulations along these linguistic dimensions.

7.  Discussion and Conclusion

The audit answers RQ1 by showing that dispersed evidence becomes reusable when source, query history, alignment, generation, execution, review, and publication are linked but not collapsed. It answers RQ2 by locating expert intervention at semantic boundaries that automated checks cannot settle. The practical benefit is a change in what the expert does: adjudication over traceable candidates instead of end-to-end authorship. Musparql prepares and presents aligned evidence, execution observations, and version comparisons that would otherwise have to be assembled manually. The 52 retained and 48 rewritten formulations show both sides of that division.[^4]

The claims are bounded by one domain and the author as one principal reviewer. The author’s research expertise includes ethnomusicology, musical analysis and music cognition; they developed the Jazz Ontology and its dataset, bringing direct domain and modelling expertise to the jazz cases. The review was not independent, and equivalent source-creator expertise was not available for every graph. The pipeline has generated candidate pairs in other domains outside music, but expert review of those candidates remains outstanding. Holdout separation is implemented but not evaluated as a secrecy or leakage study, and the linguistic-dimensions workbench has not yet produced a multi-reviewer analysis.

A broader research agenda should compare curation workflows across scientific and cultural knowledge graphs, quantify reviewer time and disagreement, and test whether shared provenance representations allow evidence, correction histories, and adjudication decisions to move between pipelines. The newer semi-automated source-discovery pipeline can be evaluated separately against v10's manual acquisition baseline; it did not supply items to this audit.

For metadata research, the main result is that evaluation pairs become more trustworthy when they are published as views over a provenance-rich curation record. Automation makes the process and the record manageable; expert review makes the semantic commitment.

**Acknowledgments.** The Musparql workflow grew out of Quagga, a predecessor initiative that collected question-query pairs for knowledge graphs in the social sciences and humanities. The author thanks the Industry Commons Foundation for institutional support, and Matteo Romanello and Harshdeep Singh of Odoma for initiating Quagga, providing its infrastructure, and offering valuable insights and encouragement. This work was conducted as part of the GRAPHIA project, funded by the European Union's Horizon Europe research and innovation programme under grant agreement No. 101188018. Views and opinions expressed are those of the author only and do not necessarily reflect those of the European Union or the European Commission; neither the European Union nor the granting authority can be held responsible for them.

**Disclosure of Interests.** The author has no competing interests to declare that are relevant to the content of this article.

**Declaration on Generative AI.** During preparation of this manuscript, the author used OpenAI Codex for assistance with software development, code review, drafting, and summarisation of project artifacts. The author reviewed and edited all outputs and takes responsibility for the content of the manuscript.

References

1.  Albertoni, R., Browning, D., Cox, S.J.D., Gonzalez Beltran, A., Perego, A., Winstanley, P. (eds.): Data Catalog Vocabulary (DCAT) - Version 3. W3C Recommendation (2024). https://www.w3.org/TR/vocab-dcat-3/

2.  Banerjee, D., Usbeck, R., Mihindukulasooriya, N., Jaradeh, M.Y., Auer, S., Singh, G., et al. (eds.): Joint Proceedings of Scholarly QALD 2023 and SemREC 2023. CEUR Workshop Proceedings, vol. 3592 (2023). https://ceur-ws.org/Vol-3592/

3.  Bolleman, J., Emonet, V., Altenhoff, A., Bairoch, A., Blatter, M.-C., Bridge, A., et al.: A large collection of bioinformatics question–query pairs over federated knowledge graphs: methodology and applications. GigaScience 14, giaf045 (2025). https://doi.org/10.1093/gigascience/giaf045

4.  Daquino, M., Daga, E., d’Aquin, M., Gangemi, A., Holland, S., Laney, R., et al.: Characterizing the landscape of musical data on the Web: state of the art and challenges. In: WHiSe II. CEUR Workshop Proceedings, vol. 2014, pp. 57–68 (2017). https://ceur-ws.org/Vol-2014/paper-07.pdf

5.  de Berardinis, J., Carriero, V.A., Jain, N., Lazzari, N., Meroño-Peñuela, A., Poltronieri, A., et al.: The Polifonia Ontology Network: building a semantic backbone for musical heritage. In: ISWC 2023, pp. 302–322. Springer (2023). https://doi.org/10.1007/978-3-031-47243-5_17

6.  Dubey, M., Banerjee, D., Abdelkawi, A., Lehmann, J.: LC-QuAD 2.0: A large dataset for complex question answering over Wikidata and DBpedia. In: ISWC 2019. LNCS, vol. 11779, pp. 69–78. Springer (2019). https://doi.org/10.1007/978-3-030-30796-7_5

7.  Gebru, T., Morgenstern, J., Vecchione, B., Vaughan, J.W., Wallach, H., Daumé III, H., et al.: Datasheets for datasets. Commun. ACM 64(12), 86–92 (2021). https://doi.org/10.1145/3458723

8.  Grüninger, M., Fox, M.S.: The role of competency questions in enterprise engineering. In: Benchmarking—Theory and Practice, pp. 22–31. Springer (1995). https://doi.org/10.1007/978-0-387-34847-6_3

9.  Harris, S., Seaborne, A. (eds.): SPARQL 1.1 Query Language. W3C Recommendation (2013). https://www.w3.org/TR/sparql11-query/

10. Höffner, K., Walter, S., Marx, E., Usbeck, R., Lehmann, J., Ngonga Ngomo, A.-C.: Survey on challenges of question answering in the Semantic Web. Semant. Web 8, 895–920 (2017). https://doi.org/10.3233/SW-160247

11. Jiang, L., Usbeck, R.: Knowledge graph question answering datasets and their generalizability: are they enough for future research? In: Proceedings of SIGIR 2022, pp. 3209–3218. ACM (2022). https://doi.org/10.1145/3477495.3531751

12. Lebo, T., Sahoo, S., McGuinness, D., et al.: PROV-O: The PROV Ontology. W3C Recommendation (2013). https://www.w3.org/TR/prov-o/

13. Liu, S., Semnani, S., Triedman, H., Xu, J., Zhao, I.D., Lam, M.: SPINACH: SPARQL-based information navigation for challenging real-world questions. In: Findings of EMNLP 2024, pp. 15977–16001. ACL (2024). https://doi.org/10.18653/v1/2024.findings-emnlp.938

14. Lopez, V., Unger, C., Cimiano, P., Motta, E.: Evaluating question answering over linked data. J. Web Semant. 21, 3–13 (2013). https://doi.org/10.1016/j.websem.2013.05.006

15. MLCommons: Croissant Format Specification 1.1 (2026). https://docs.mlcommons.org/croissant/docs/croissant-spec-1.1.html

16. Morales Tirado, A., Carvalho, J., Ratta, M., Uwasomba, C., Mulholland, P., Barlow, H., et al.: Musical Meetups Knowledge Graph (MMKG): a collection of evidence for historical social network analysis. In: ESWC 2024, pp. 110–127. Springer (2024). https://doi.org/10.1007/978-3-031-60635-9_7

17. Pond, L., Kirby, L., Meng, S., Ngassam, S., Chow, S., Hillerbrand, D., et al.: SESEMMI for LinkedMusic: democratizing access to musical archives via large language models. In: LLM4Music at ISMIR 2025 (2025). https://openreview.net/forum?id=hSDeNeUzcy

18. Proutskova, P., Wolff, D., Fazekas, G., Frieler, K., Höger, F., Velichkina, O., et al.: The Jazz Ontology: a semantic model and large-scale RDF repositories for jazz. J. Web Semant. 74, 100735 (2022). https://doi.org/10.1016/j.websem.2022.100735

19. Raimond, Y., Abdallah, S.A., Sandler, M.B., Giasson, F.: The Music Ontology. In: Proceedings of ISMIR 2007, pp. 417–422. Austrian Computer Society (2007). https://ismir2007.ismir.net/proceedings/ISMIR2007_p417_raimond.pdf

20. Taghzouti, Y., Michel, F., Jiang, T., Nothias, L.-F., Gandon, F.: Q²Forge: minting competency questions and SPARQL queries for question-answering over knowledge graphs. In: Proceedings of K-CAP 2025. ACM (2025). https://doi.org/10.1145/3731443.3771350

21. TEXT2SPARQL: Second International TEXT2SPARQL Challenge: Dataset Participation (2026). https://text2sparql.aksw.org/2026/participation/datasets/, accessed 2 Aug 2026

22. Tsaneva, S., Dessì, D., Osborne, F., Sabou, M.: Enhancing scientific knowledge graph generation pipelines with LLMs and human-in-the-loop. In: Sci-K 2024. CEUR Workshop Proceedings, vol. 3780 (2024). https://ceur-ws.org/Vol-3780/paper1.pdf

23. Usbeck, R., Yan, X., Perevalov, A., Jiang, L., Schulz, J., Kraft, A., et al.: QALD-10—The 10th challenge on question answering over linked data. Semant. Web 15, 277–289 (2024). https://doi.org/10.3233/SW-233471

24. Walter, S., Bast, H.: The Wikidata Query Logs Dataset. In: Proceedings of SIGIR 2026 (2026). https://doi.org/10.1145/3805712.3808612

25. Wilkinson, M.D., Dumontier, M., Aalbersberg, I.J., Appleton, G., Axton, M., Baak, A., et al.: The FAIR Guiding Principles for scientific data management and stewardship. Sci. Data 3, 160018 (2016). https://doi.org/10.1038/sdata.2016.18

26. Wiśniewski, D., Potoniec, J., Ławrynowicz, A., Keet, C.M.: Analysis of ontology competency questions and their formalizations in SPARQL-OWL. J. Web Semant. 59, 100534 (2019). https://doi.org/10.1016/j.websem.2019.100534

[^1]: An LLM assisted source discovery and acquisition process has since been developed and tested and is available in the Musparql GitHub repository. It is not described in the paper because it was not used in v10 benchmark construction

[^2]: Musparql repository: <https://github.com/ppquadrat/musparql-aligner>

[^3]: Musparql benchmark v10 snapshot: https://github.com/ppquadrat/musparql-aligner/tree/main/benchmark/v10

[^4]: The study does not measure time or cost against a fully manual baseline.
