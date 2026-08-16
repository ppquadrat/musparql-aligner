# Reviewer privacy-notice requirements

This document records minimum product requirements for the reviewer form; it
does not determine the project's legal basis.

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

The form should collect only the fields in `schemas/reviewer.schema.json`.
Tests and examples must use synthetic people.
