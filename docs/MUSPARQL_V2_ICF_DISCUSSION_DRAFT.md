# Musparql v2: a note for discussion with ICF

**Draft for comments — 19 August 2026**

I would like to check the plans for the next version of Musparql with you before I start collecting information from other people. This note explains what I am planning, why I need external annotators, what information I would collect about them, and the few points on which I need ICF's view.

Musparql is being developed within GRAPHIA T4.1/T6.1

## What Musparql does

Musparql is a research workflow for building evaluation data for natural-language access to knowledge graphs. It starts with SPARQL queries already published by knowledge-graph projects, gathers the available documentation and other evidence, and develops a natural-language question that expresses the query's intended information need.

Language models can help propose or align the wording, but they do not make the final decision. A person checks whether the SPARQL query is meaningful, whether the natural-language question accurately represents it, and whether the pair should be accepted, rewritten, or rejected.

The current public benchmark contains 100 reviewed natural-language/SPARQL pairs from five music-domain knowledge graphs. So far, I have carried out the review myself.

## Why I need other annotators

The immediate practical aims are to:

- produce more reviewed SPARQL–natural-language pairs overall;
- extend the work beyond musicology into other GRAPHIA/Lumen subject areas;
- test whether the review method works when it is used by people other than me; and
- understand where different experts agree or disagree about the meaning and wording of a query.

For the IPL, I would like to run an annotation exercise in domains other than musicology. There are some underrepresented domains in Quagga, it would be nice to cover them - if we get anyone with the relevant expertise. Even if not, I can collect annotations using my pipeline from other Graphia/Lumen domains.

At ICCCM I may be able to recruit additional expert annotators for music-related material. This is an opportunity rather than a confirmed annotation event.

The first rounds would be small and invitation-only. The service is being designed for roughly ten people annotating during the same period, not for public registration or large-scale crowdsourcing.

## What reviewers would do

Reviewers would look at a SPARQL query, its evidence, and a proposed natural-language question. Depending on the task, they would:

- decide whether the query expresses a meaningful information need rather than only an administrative or intermediate operation;
- assess whether the question covers the query's variables, constraints, aggregation, ordering, and graph assumptions;
- accept, rewrite, or reject the proposed question;
- record alternative acceptable wording where that is useful; and
- distinguish a language problem from a problem in the query, source data, evidence, model output, or execution.

Literal accuracy is not always enough. A formulation can be technically correct but pragmatically misleading—for example, when a field returned by a query is only implementation provenance and not part of the real research question. This is one reason expert human judgement is needed. More details in the Musparql paper.

## Research questions this would support

### More and broader benchmark data

The annotations would add reviewed SPARQL–natural-language pairs to the benchmark, including pairs from domains beyond musicology where expertise is available in Graphia/Lumen.

### Longitudinal changes

Reviewing a knowledge graph changes a person's familiarity with its data, model, and terminology. Before each assignment or review round, Musparql v2 would ask reviewers to confirm or update their subject expertise and their familiarity with the particular knowledge graph. The system would keep timestamped responses rather than silently replacing the earlier answer.

This would make it possible to study whether judgements change as reviewers become more familiar with a resource. It would also avoid asking people to complete the full background form again each time.

### Inter-rater and intra-rater comparison

Where assignments overlap, I could compare independent judgements from different reviewers. This would help identify which benchmark decisions are stable and which depend on domain knowledge, knowledge-graph familiarity, linguistic perspective, or task framing.

If a reviewer sees the same or a revised item in a later round, it may also be possible to examine consistency within the same reviewer over time. The purpose is not to rank or assess individual people.

### Linguistic dimensions

A separate annotation task may compare alternative formulations pairwise. The provisional dimensions are:

- naturalness;
- pragmatism or communicative salience; and
- room for interpretation or ambiguity.

Pairwise comparison is preferable to asking for isolated scores because a score has no clear comparison target, and greater openness is not necessarily better: it can represent useful breadth or harmful underspecification. These observations would remain separate from the basic decision about whether a question correctly represents a query.

## Information I would collect

Participation would be limited to invited adults. I currently expect to collect:

| Information | Examples | Why it is needed |
| --- | --- | --- |
| Identity and contact details | Name, email address, optional affiliation | Invitations, login, communication, and responding to privacy requests |
| Languages | Languages and self-described proficiency | Interpreting linguistic judgements and supporting possible multilingual work |
| General expertise | Musicology, other subject areas, digital humanities, knowledge graphs/ontologies, SPARQL, NLP or LLM experience | Understanding which perspectives are represented in the reviewer group |
| Knowledge-graph-specific expertise | Subject expertise relevant to an assigned graph | Interpreting annotations in relation to the graph's research domain |
| Familiarity with the resource | Previous experience with its data, data model, or graph | Distinguishing subject knowledge from familiarity with a particular implementation |
| Annotation records | Decisions, rewrites, alternatives, comments, and timestamps | Building and auditing the benchmark and comparing review rounds |
| Basic security records | Invitation status and login/session metadata | Operating and protecting the invitation-only service |

Each reviewer would receive a randomly allocated pseudonymous identifier such as `reviewer-0001`. Names, email addresses, language and expertise information, familiarity records, authentication data, and privacy-request records would remain in confidential storage. Only the pseudonymous identifier would be used in review provenance or research data. Published reporting would be aggregated and would avoid singling out identifiable reviewers. Reviewer expertise and familiarity would be used to interpret the research results.

## Retention and access

My current proposal is:

- delete identity, profile, expertise, and familiarity records after a valid withdrawal/deletion outcome, or two years after the project has formally closed;
- retain pseudonymous scholarly annotations with the research dataset where this is lawful and clearly explained to participants; and
- keep encrypted backup versions for up to 90 days, with deletions replayed if a backup has to be restored.

## Proposed service and safeguards

The annotation service will be invitation-only and use sensible authentication and access controls appropriate to the limited personal data it holds.

## Questions for ICF

### 1. Are you happy with the proposed collection and research use?

Are you happy for me to collect the information described above for these research purposes? Do you see any objections, fields I should remove, or additional ethical, grant, participant-information, or research approvals I should obtain?

### 2. Who is responsible for the personal-data processing?

Do you consider ICF to be the controller for this processing, with me carrying it out as part of my work for ICF? If not, what arrangement do you think applies?

I would also like your view on:

- the appropriate lawful basis for the different purposes;
- who should approve the participant/privacy notice;
- who should answer a participant rights request;
- who should take the lead if there is a personal-data incident; and
- which contact address should appear in the notice.

Do the GRAPHIA grant agreement, consortium agreement, data-management plan, or existing ICF policies already settle any of these points or impose additional requirements?

### 3. Would ICF like to provide the hosting?

Would you be able to provide or arrange a small Linux server with approximately 1 vCPU, 1–2 GB RAM, 5–10 GB of persistent disk, and HTTPS access?

If not, I can run Musparql in a dedicated, access-controlled environment on my UK home server. I would like to know whether ICF is comfortable with that and whether you want any particular security conditions to apply.

### 4. Could ICF provide a domain name and sending address?

Could ICF provide a project subdomain and an email alias or approved sending address for the login messages—for example, musparql@industrycommons.net?

The alternative is to pay for a domain (about 20 Euros a year) or to use a free Musparql Gmail address for login codes and use an available service URL until a domain is justified.

### 5. Could ICF provide backup space?

Could ICF provide an approved destination with an initial quota of 5–10 GB for encrypted backups?

If not, are you happy for me to use a dedicated Google Drive destination with client-side encryption and a separately held encryption key?

### 6. When should we involve ODOMA?

ODOMA currently receives no Musparql reviewer personal data. Their possible IPL contribution would be to help provide suitable knowledge-graph sources from other domains.

Do you think they should be consulted now, or only if we move towards shared authentication, contributor information, personal-data access, a jointly maintained benchmark, or joint analysis?
