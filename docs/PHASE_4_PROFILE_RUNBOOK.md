# Musparql v2 Phase 4 onboarding and profile runbook

This runbook covers synthetic development and verification of reviewer
onboarding and profile administration. It does not approve a privacy notice,
authorise real reviewer profiles, or permit deployment. The gates in
`MUSPARQL_V2_PLAN.md`, `REVIEWER_DATA_GOVERNANCE_DRAFT.md`, and
`PHASE_3_AUTH_RUNBOOK.md` still apply.

## Implemented workflow

- A signed-in non-owner whose profile is incomplete is redirected to `/profile`.
- Completeness requires acknowledgement of the configured current notice,
  values for all three technical-experience fields, at least one language, and
  at least one research-domain assertion.
- Name and optional affiliation begin with the owner's invitation values and
  remain editable. The verified email is displayed but cannot be changed in
  Phase 4; a later email-change flow must reverify the new address.
- The form records knowledge-graph/ontology, SPARQL, and NLP/language-model
  experience using the Phase 1 four-value contract.
- Languages use the constrained language tags and levels in
  `schemas/reviewer_profile_v2.schema.json`. Current language rows retain their
  first-asserted and last-updated timestamps.
- General expertise supports multiple domains. Three blank entry rows are shown
  at a time and reviewers may save again to add more, up to the server-side
  limit of twenty. Suggestions come only from the configured, versioned local
  snapshot; any non-matching label is accepted as free text. The stored entered
  label preserves the reviewer's wording, while a separate normalized label is
  used for duplicate detection.
- Changing a domain's expertise level appends a new assertion that supersedes
  the previous head. Earlier assertions are not overwritten. The first release
  does not silently merge or relabel an existing domain: a reviewer adds the
  corrected wording as a new domain and can set an obsolete entry to `0 — None`.
- The owner account page reports `Complete` or `Awaiting onboarding` by
  pseudonymous reviewer ID. Owner access to identity/contact columns remains
  part of the existing sole-owner administration boundary.

All profile writes are one database transaction. A stale or foreign domain ID,
duplicate domain, invalid language, incomplete paired row, missing current
notice acknowledgement, or unsupported level rejects the entire update.

## Notice and suggestion configuration

Outside tests, the application refuses to start unless both of these are set:

```bash
export MUSPARQL_PRIVACY_NOTICE_VERSION="replace-with-controller-approved-version"
export MUSPARQL_PRIVACY_NOTICE_BODY="replace-with-controller-approved-notice"
```

Changing the configured version makes every existing profile incomplete until
the reviewer acknowledges the new notice. Operational policy must assign a new
version whenever the approved body changes.

The suggestion file defaults to
`catalog/expertise_domain_suggestions.yaml`. An alternate immutable snapshot may
be selected with:

```bash
export MUSPARQL_EXPERTISE_SUGGESTIONS_PATH="/absolute/path/to/approved-snapshot.yaml"
```

The repository snapshot contains six owner-reviewed specialist terms.
EuroSciVoc is recorded as a reference-only source: no entry may claim a
EuroSciVoc mapping until its stable concept URI and vocabulary release have
been checked by the owner and added in a new snapshot.

For synthetic local development only, the application can install its explicit
test notice with:

```bash
export MUSPARQL_ALLOW_SYNTHETIC_PRIVACY_NOTICE=1
```

That switch and the synthetic notice must never be used for real people. It is
independent of `MUSPARQL_ALLOW_SYNTHETIC_EMAIL`; enabling one does not authorise
the other.

## Verification

```bash
.venv/bin/python -m pytest -q tests/test_v2_phase4_profiles.py
.venv/bin/python -m pytest -q
.venv/bin/pip check
```

The Phase 4 tests cover incomplete-profile redirects, notice display and
acknowledgement, current-notice invalidation, local suggestions and free text,
technical experience, languages, append-only domain corrections, atomic
rejection of stale form data, owner completion state, and fail-closed notice
configuration.

## Real-data gate

Do not invite or collect data from a real reviewer until all of the following
are satisfied:

1. ICF or the confirmed controller has approved the controller allocation,
   lawful basis, final notice, contact route, rights and incident procedures,
   and relevant provider/infrastructure arrangements.
2. Phase 2b backup, monitoring, and isolated restore tests have passed for the
   confidential and irreplaceable operational state.
3. The selected real email sender and its crash/retry behaviour have passed the
   Phase 3 handoff gate.
4. Hosting, tunnelling, resource boundaries, paths, and monitoring have received
   the required Phase 10 approval.
5. A synthetic or trusted usability pilot has checked the reviewer-facing
   wording and desktop/mobile completion time.

Until then, use synthetic identities and `example.invalid` email addresses only.
