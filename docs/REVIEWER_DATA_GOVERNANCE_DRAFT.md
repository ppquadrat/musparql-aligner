# Musparql reviewer-data governance draft

Status: **draft for ICF review; not yet an approved privacy notice**

Date: 2026-08-18

This document records the project's current factual understanding, proposed
decisions, and the questions that ICF must confirm before Musparql collects real
reviewer data. It is an operational draft, not legal advice and not a substitute
for ICF's data-protection or research-governance process.

## 1. Current factual picture

- Musparql is Polina Proutskova's research project.
- The work is paid by ICF, a Swedish research foundation, through an EU
  research grant.
- Polina is on ICF's payroll under an A2 arrangement and works from the UK.
- The application server is physically located at Polina's home in the UK.
- Encrypted backups are stored in a dedicated Google Drive destination. The
  backup encryption key remains outside Google and is held by the owner in a
  password manager.
- The invited reviewer pool is small. The application collects identity,
  contact, affiliation, language, technical-experience, general-expertise,
  KG-specific expertise, and resource/data-model/KG-familiarity data.
- Public or research-facing review provenance uses only a pseudonymous
  `reviewer-NNNN` identifier.
- ODOMA is a partner in the same EU project and performs related benchmark work,
  but no ODOMA application, code, personal data, authentication data, or
  contributor data is currently part of Musparql.

## 2. Working allocation of responsibilities

The working assumption is that **ICF is the data controller** if ICF has
commissioned or approved this research as part of Polina's work and determines
or authorises its purposes and essential means. Under that model, Polina acts
as an authorised person working under ICF's instructions rather than as a
separate controller merely because the server is in her home.

The A2/HMRC classification does not by itself determine the GDPR role. ICF must
confirm whether Polina is treated for this processing as staff acting under
ICF's authority, an independent processor requiring an Article 28 agreement,
an independent controller, or a joint controller. The factual allocation of
decisions—not the label used in a payroll or services agreement—governs.

If ICF only funds the work while Polina independently determines why reviewer
personal data is collected, which people are recruited, what fields are used,
how long data is retained, and how rights requests are answered, the controller
analysis may instead identify Polina as a controller or ICF and Polina as joint
controllers. Real-data collection must not start until ICF resolves this.

### ODOMA boundary

ODOMA is not presently a controller, joint controller, or processor for
Musparql reviewer data because there is no shared Musparql processing. Before
ODOMA code, authentication, contributor records, reviewer records, or benchmark
data containing personal data enters Musparql—or before Musparql data is shared
with ODOMA—the parties must document the new processing and decide whether they
are:

- separate controllers using data for separate purposes;
- joint controllers jointly determining purposes and essential means; or
- controller and processor, where one acts only on documented instructions.

No future partnership or technical integration may silently broaden the privacy
notice or the authorised recipient list.

## 3. Proposed processing decisions

Subject to ICF confirmation:

- **Purposes:** invited-reviewer authentication and administration; assignment
  delivery; collection and analysis of reviewer expertise and familiarity;
  longitudinal research; pseudonymous scholarly provenance; application
  security; backup and recovery; and handling reviewer rights requests.
- **Excluded purposes:** marketing, public identity attribution, unrelated
  profiling, automated decisions about reviewers, and reuse by project partners
  without a separately documented basis and notice.
- **Lawful basis:** ICF should select and document the basis for each purpose.
  As a private research foundation, legitimate interests is the current
  candidate for the core research and administration processing, supported by
  a legitimate-interests assessment. If ICF can identify a Swedish or EU legal
  basis for performing a task in the public interest, it should say whether
  public task applies instead. Participation agreement is separate from the
  GDPR lawful basis.
- **Notice acknowledgement:** the application records that the reviewer was
  shown a versioned notice. It is not labelled GDPR consent unless ICF expressly
  selects consent and accepts all resulting withdrawal requirements.
- **Special-category and offence data:** Musparql does not intentionally collect
  them. Free-text controls instruct reviewers not to provide them. If a future
  research design needs such data, collection stops pending ICF review and any
  required Swedish ethical approval.
- **Children:** the first release is for invited adult expert reviewers only.
- **Automated decision-making:** none. Model output cannot approve a benchmark
  decision or make a consequential decision about a reviewer.

## 4. Data locations and recipients/processors

The privacy notice and ICF processing record should identify, at minimum:

- the Musparql application on the owner-operated home server in the UK;
- the selected email-delivery account/provider;
- Tailscale/Funnel when enabled for deployment;
- Google Drive for client-side encrypted backup storage;
- the selected external monitoring/dead-man service;
- infrastructure and software providers whose support or subprocessors may
  access personal data;
- authorised maintainers, including tightly scoped AI-assisted maintenance only
  when the task expressly requires reviewer administration; and
- ODOMA only if a later, separately approved integration or data-sharing
  arrangement makes it a recipient or participant in the processing.

ICF should expressly approve the home-server location, physical and account
access controls, encrypted UK-to-Google backup design, restore procedure, and
incident-reporting path. The EU renewed the UK's GDPR adequacy status in
December 2025, but ICF must still review each service provider's own locations,
subprocessors, contractual terms, and any onward transfers.

For email, the preferred no-new-domain route is an ICF-managed project address
or alias and ICF-approved sending service, if ICF can provide one. The fallback
is a dedicated free `@gmail.com` Musparql mailbox using the Gmail API's
send-only OAuth scope. Reviewer accounts never authorise Google; only the
dedicated sender account does. Sent authentication messages are removed from
the mailbox within 30 days, and resolved replies or bounce messages within 90
days, unless ICF requires a different records schedule. The application itself
does not receive inbox-reading permission.

## 5. Retention schedule

- Canonical non-holdout submissions, sanitized exports, and scholarly decision
  history: for the documented lifetime of the Musparql dataset.
- Reviewer identity, contact, profile, expertise history, and familiarity
  assessments: delete on a valid deletion/withdrawal outcome, or two years after
  a formally recorded project-closure date.
- Login codes: purge within 24 hours after use or expiry.
- Expired or revoked sessions: 30 days.
- Redacted application logs and email delivery identifiers/status: 30 days.
- Sent authentication emails in the dedicated sender mailbox: 30 days;
  resolved replies and bounce messages: 90 days, unless transferred into an
  ICF-approved rights or incident case with its own retention rule.
- Never-accepted invitations: 90 days after invitation expiry.
- Failed and temporary processing jobs: 30 days.
- Owner invitation, disable, restoration, and deletion audit events: one year.
- Complete encrypted backup generations: 90 days, subject to the verified-newer-
  generation and healthy-monitoring safeguards in the Phase 2b plan.
- Minimal deletion tombstone/ledger entry: for as long as retained data or a
  recoverable copy could otherwise reintroduce the deleted identity link.

Withdrawal immediately stops future participation, disables login, revokes all
sessions, and cancels future assignments. Subject to the controller's approved
lawful basis and research safeguards, existing scholarly decisions may remain
under `reviewer-NNNN` after identity, profile, and the confidential identity
link are removed. Because a small cohort can remain indirectly identifiable,
pseudonymous material continues to be treated conservatively as personal data
unless the controller documents effective anonymisation.

## 6. Proposed rights-request procedure

1. Publish one controller-approved contact address in the privacy notice. Until
   ICF confirms the controller, no provisional personal address is published as
   the final rights contact.
2. Route every access, correction, restriction, objection, withdrawal, or
   erasure request promptly to ICF's privacy contact or DPO if ICF is controller.
3. Acknowledge receipt within three working days. The controller owns the legal
   response deadline and any permitted extension; the project should plan to
   complete ordinary requests within one month.
4. Verify identity proportionately, normally through the already verified email
   account. Do not request identity documents unless genuinely necessary.
5. Record only a minimal request ID, type, received date, deadline, status, and
   outcome. Keep request contents in the controller-approved case channel, not
   application logs or Git.
6. For access, produce a comprehensible export of identity/profile data,
   append-only expertise and familiarity history, assignment/submission
   provenance linked to the reviewer, relevant session/admin records, purposes,
   recipients, sources, and retention information. Do not disclose another
   person's data.
7. For correction, update the current projection and append a correction or
   supersession event. Preserve scholarly integrity without continuing to
   present an acknowledged error as current fact. Published artifacts receive a
   correction record rather than silent historical rewriting. Append-only
   profile history is an ordinary-operation integrity rule, not an exception to
   an approved erasure: after an account is marked withdrawn, technical
   experience, languages, domain-expertise assertions, and unreferenced
   reviewer-entered domain labels are deleted.
8. For restriction or an objection, immediately pause new assignments and
   non-essential processing while the controller assesses the request. The
   database must be able to mark the account restricted rather than relying on
   an informal note.
9. For withdrawal or an approved erasure request, revoke access immediately and
   complete the canonical identity/profile deletion within 30 days. Reapply the
   deletion ledger after any restore; normal backups age out within 90 days.
10. Explain clearly when a requested scholarly record is retained, the lawful
    reason, the continuing safeguards, and the person's right to complain to
    the relevant supervisory authority.

## 7. Proposed incident-response procedure

An incident includes loss, unauthorised access or disclosure, unauthorised
change, destruction, inability to access personal data, loss of a device or
credential, accidental restoration of deleted data, misdirected email, or a
backup/monitoring failure that puts confidentiality, integrity, or availability
at risk.

1. **Contain immediately:** disable affected access, revoke sessions/tokens and
   credentials, preserve the last known-good state, and take the portal offline
   if continued operation could worsen exposure. Do not destroy evidence.
2. **Notify internally:** tell ICF's named privacy/security contact without
   undue delay and target no later than four hours after discovery. If Polina is
   classified as a processor, this is processor-to-controller notification, not
   a decision by Polina about regulatory reporting.
3. **Record facts:** discovery time, affected systems, categories and estimated
   number of people/records, likely consequences, containment, recovery, and
   outstanding uncertainty. Keep personal data out of ordinary logs and public
   issue trackers.
4. **Assess risk:** the controller decides whether the incident is unlikely to
   risk people's rights and freedoms. The decision and reasons are documented
   even when no authority notification is made.
5. **Regulatory notification:** if notification is required, the controller
   reports to the competent authority within 72 hours of awareness and supplies
   later information if the investigation is incomplete.
6. **Reviewer communication:** where the breach is likely to create a high risk,
   the controller informs affected reviewers without undue delay in clear
   language, describing what happened, likely effects, mitigation, and a contact
   route.
7. **Partner notification:** notify ODOMA only if its systems/data are affected
   or an approved agreement requires it. Do not disclose unrelated reviewer
   information merely because ODOMA is an EU-project partner.
8. **Recover and learn:** restore only into isolation, replay deletions, rotate
   affected secrets, test the correction, document the decision trail, and add
   a preventive control or test before reopening.

The runbook must contain an offline copy of ICF's incident contact details so a
server or account outage cannot block escalation.

## 8. Questions for ICF to answer or approve

Please review the factual picture and proposed decisions above and answer:

1. Is Musparql research performed under ICF's authority and grant obligations,
   and is ICF the data controller for its reviewer personal data?
2. If not, does ICF consider Polina an independent controller, joint controller,
   or processor for this activity, and what written agreement is required?
3. Who is ICF's privacy/DPO contact and which address should reviewers use for
   rights requests?
4. Which lawful basis does ICF approve for each stated purpose? If legitimate
   interests, will ICF approve or provide the legitimate-interests assessment?
5. Does ICF require research-ethics, grant, security, records-management, or
   works-council review before inviting reviewers?
   Does the grant agreement, consortium agreement, or project data-management
   plan already allocate controller responsibilities or impose a repository,
   retention, or hosting requirement?
6. Does ICF approve the listed fields, the exclusion of special-category data,
   adult-only participation, and the distinction between participation and GDPR
   consent?
7. Does ICF approve the retention schedule and the proposed treatment of
   pseudonymous scholarly decisions after identity/profile deletion?
8. Does ICF approve operation from a private home server in the UK? What
   physical security, patching, access, encryption, audit, or asset-registration
   requirements apply?
9. Does ICF approve the encrypted Google Drive backup design and the eventual
   email, tunnelling, and monitoring providers? Which processor agreements and
   transfer assessments must be in place?
   Can ICF provide an approved project email alias and sending route so that a
   separate public email domain is unnecessary?
10. Which supervisory authority and establishment should the notice identify,
    and does any additional UK registration or ICO-facing obligation apply?
11. Does ICF approve the rights-request and incident procedures, including the
    three-working-day acknowledgement, four-hour internal incident escalation,
    and named out-of-band contacts?
12. Must ODOMA review the current arrangement even though it receives no
    Musparql personal data? What approval is required before any future shared
    authentication, contributor data, reviewer data, or benchmark integration?
13. Who is authorised to sign off the final privacy notice and processing
    record before the one-reviewer real pilot?

## 9. Short approval request to send to ICF

> I am preparing Musparql, a small invite-only expert-review application within
> my EU-grant-funded research work for ICF. It will collect reviewers' name,
> email, optional affiliation, languages, technical experience, research-domain
> expertise, and assignment-specific knowledge-graph expertise/familiarity. The
> public/research provenance uses only a pseudonymous reviewer ID.
>
> The application would run on a dedicated, access-controlled server at my home
> in the UK. Client-side encrypted, versioned backups would be stored in Google
> Drive; the decryption key is held separately. The reviewer pool is small and
> invited, no special-category data is intended, and there is no automated
> decision-making about reviewers. ODOMA currently receives no Musparql personal
> data and has no code or authentication integration with it.
>
> My working assumption is that ICF is the data controller because this is paid
> research under ICF and the EU grant, while I act under ICF's authority. Please
> confirm or correct that allocation, including whether my A2/UK working
> arrangement requires a processor, joint-controller, or other agreement.
> Please also review the attached purposes, proposed lawful basis, retention
> schedule, UK home-server and encrypted-backup arrangement, processor/transfer
> list, rights procedure, and incident procedure. I will not collect real
> reviewer data until the controller, contact route, lawful basis, infrastructure
> approval, and final privacy notice have been confirmed.

## 10. Authoritative reference points

- [Swedish Authority for Privacy Protection (IMY): processing personal data for
  research](https://www.imy.se/en/organisations/data-protection/data-protection-within-different-areas/processing-of-personal-data--for-researchers/)
- [IMY: data controllers and data
  processors](https://www.imy.se/en/organisations/data-protection/this-applies-accordning-to-gdpr/data-controllers-and-data-processors/)
- [IMY: lawful grounds for personal-data
  processing](https://www.imy.se/en/organisations/data-protection/this-applies-accordning-to-gdpr/lawful-grounds-for-personal-data-processing/)
- [IMY: rights of data
  subjects](https://www.imy.se/en/individuals/data-protection/your-rights-as-a-data-subject/)
- [IMY: personal-data breach
  notification](https://www.imy.se/en/organisations/forms-and-e-services/notification-of-a-personal-data-breach/)
- [EDPB Guidelines 07/2020: controller and processor
  concepts](https://www.edpb.europa.eu/documents/guideline/guidelines-072020-on-the-concepts-of-controller-and-processor-in-the-gdpr_en)
- [European Commission: adequacy decisions, including the December 2025 UK
  renewal](https://commission.europa.eu/law/law-topic/data-protection/international-dimension-data-protection/adequacy-decisions_en)
