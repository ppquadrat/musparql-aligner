# Reviewer privacy-notice requirements

This document records minimum product requirements for the reviewer form; it
does not determine the project's legal basis.

The current factual assessment, proposed rights and incident procedures, and
questions awaiting ICF approval are in
[`REVIEWER_DATA_GOVERNANCE_DRAFT.md`](REVIEWER_DATA_GOVERNANCE_DRAFT.md). That
draft is not itself an approved reviewer-facing notice. No real reviewer data
may be collected until the controller and final notice are confirmed.

Before collecting a profile, the form must identify the controller and explain:

- why identity, contact, expertise, language, and KG-familiarity data are used;
- which fields are collected and which pseudonymous ID appears in public
  provenance;
- where the confidential registry is stored and backed up;
- the retention and deletion policy;
- who may receive or process the data, including authorised AI-assisted coding
  tools when a maintenance task requires access;
- applicable international transfers and safeguards;
- the controller's chosen lawful basis and the reviewer's applicable rights;
- how to contact the controller or data-protection contact.

The profile stores `privacy_notice_version` and
`privacy_notice_acknowledged_at`. Acknowledgment proves which notice was shown;
it must not be described as consent unless the controller has selected consent
as the lawful basis and implemented freely given, specific, informed,
unambiguous, and withdrawable consent.

The v2 form should collect only the fields in
`schemas/reviewer_profile_v2.schema.json`. That profile is a current-state
projection; its general-domain history must use the append-only
`schemas/reviewer_domain_expertise_assertion.schema.json` contract. Repeated
pre-review and profile-page assessments must use
`schemas/reviewer_kg_domain_assessment.schema.json` and
`schemas/reviewer_resource_familiarity_assessment.schema.json`. Superseded
assertions remain confidential personal data and follow the same access,
retention, correction, and deletion policy as the current profile. The legacy
`schemas/reviewer.schema.json` remains valid only for the pre-migration local
registry. Tests and examples must use synthetic people.
