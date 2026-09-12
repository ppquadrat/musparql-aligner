# Musparql expert-review study: participant information and privacy notice

Status: **draft for ICF review — not approved for use**

Draft date: 10 September 2026

Proposed notice version: `musparql-participant-notice-2026-09-v1`

ICF review copy: [`Musparql_Participant_Notice_ICF_Review.docx`](Musparql_Participant_Notice_ICF_Review.docx).
This Markdown file remains the editable source of truth.

This draft implements the ICF decision recorded in the 29 August 2026 hosting
handover. It is not legal advice and must be approved by ICF before the first
invitation or collection of real reviewer data.

## Approval and implementation points — remove before publication

1. Confirm that the controller's full legal name and address below are the
   details ICF wants published. The address is taken from ICF's public website.
2. Confirm that `musparql@industrycommons.net` is active and monitored before
   publishing the notice.
3. Insert the selected transactional email provider, its processing locations,
   and any relevant international-transfer wording.
4. Confirm the lawful basis for the agreed withdrawal model. On withdrawal,
   Musparql will delete the contact registry (name, email, optional affiliation,
   account access, and the identity link) but retain the reviewer's existing
   annotations and the language proficiency, expertise, and KG-familiarity
   values needed to analyse them in the private pseudonymous research dataset.
   Pseudonymisation alone does not make these data anonymous, particularly in a
   small expert cohort. ICF must identify and document the separate lawful
   ground or applicable research provision that permits this retention after
   consent to participate has been withdrawn. The marked text under **Lawful
   basis and consent** must then name that basis.
5. Confirm whether consent is the lawful basis for every listed operation, or
   whether minimal service-security, rights-request, and incident records use a
   different lawful basis. The final notice must map each purpose to the basis
   ICF has actually recorded.
6. Confirm whether ICF has a data-protection officer or separate privacy contact
   that must be named in addition to the Musparql address.
7. Confirm that the Swedish Authority for Privacy Protection (IMY) is the
   supervisory authority ICF wants identified.
8. Approve the proposed project-closure rule: the project lead recommends
   closure and ICF formally records it when no recruitment, annotation rounds,
   or planned analyses requiring pseudonymous reviewer data remain active and
   no approved successor arrangement exists. Leaving ICF triggers a transition
   review rather than automatic closure or data transfer.
9. Complete ICF's OPERAS/data-management-plan registration check.
10. Make this notice available with the invitation and from the login page.
    Replace the application's current acknowledgement-only checkbox with the
    approved affirmative consent wording. Consent must not be inferred merely
    from requesting a login code or continuing to use the site.
11. Update the v2 public-release projection so new external-reviewer releases
    do not publish stable reviewer IDs, reviewer-level expertise/familiarity, or
    linked individual annotations. Existing v1–v10 single-reviewer releases
    remain unchanged.

---

## Application presentation and consent flow — implementation specification

This section is for the Musparql application team and should not appear as part
of the published participant notice.

The first-time journey is:

1. The invitation links to the complete participant notice.
2. The login page offers the complete notice before an email address is entered.
3. After successful code verification, and before profile or research data are
   collected, a dedicated consent screen appears.
4. The screen shows the essential information in a short summary, followed by a
   prominent way to expand or open the complete notice. Essential or surprising
   information must not exist only behind the expandable section.
5. An unticked, required checkbox records affirmative consent. The profile form
   is shown only after it is selected and submitted.
6. The complete notice remains available later from the profile or site footer.
   Returning reviewers do not see the gate again unless a material change
   requires a revised notice and fresh consent.

Recommended consent-screen summary:

> **Before you take part**
>
> Musparql is an ICF research study about reviewing natural-language questions
> and SPARQL queries. Taking part is voluntary. We collect your profile,
> expertise, language and review data to run the study and analyse the benchmark.
> We publish benchmark questions and ambiguity/alternative data, but not your
> name, contact details, reviewer profile, or reviewer-linked annotations.
> For the IPL workshop, you may work alone or in a self-selected reviewing
> group. A group's joint submission is attributed to the pseudonymous reviewer
> identifiers of the people who contributed to it.
>
> You may stop or withdraw at any time. If you withdraw, we delete your contact
> details and the link between your identity and reviewer ID. Existing
> annotations and their linked language, expertise and KG-familiarity data may
> remain in ICF's private pseudonymous research dataset under the lawful basis
> stated in the complete notice. Contact musparql@industrycommons.net about the
> study or your data rights.
>
> **Read the complete participant information and privacy notice**

The checkbox beside that summary should say:

> I confirm that I am aged 18 or over, that I have read the Musparql participant
> information and privacy notice, and that I voluntarily agree to take part and
> to Industry Commons Foundation using my personal data as described.

Do not preselect the checkbox. Do not infer consent from an invitation, code
request, successful login, or continued use. Store the notice version, consent
statement version, and timestamp. The final screen must use the wording approved
by ICF after it confirms the lawful basis and withdrawal model.

## Participant-facing notice

### Invitation and purpose

You are invited to take part in Musparql, a small expert-review study conducted
within the GRAPHIA research project. Please read this notice before deciding
whether to participate.

Musparql develops evaluation data for systems that translate natural-language
questions into SPARQL queries over knowledge graphs. It starts from queries and
supporting material published by knowledge-graph projects. Human reviewers
assess what the queries mean and whether proposed natural-language questions
represent them accurately.

The study will help us:

- build a larger and more diverse collection of reviewed question–query pairs;
- test whether the review method works across reviewers and subject areas;
- understand where reviewers agree or disagree;
- study how relevant subject expertise and familiarity with a knowledge graph
  relate to review judgements; and
- evaluate linguistic qualities such as naturalness, communicative focus, and
  room for interpretation.

The purpose is to study the review method and the resulting benchmark data. It
is not to rank, evaluate, or make decisions about individual reviewers. The
public benchmark is separate from the private reviewer research dataset used
to study expertise, familiarity, learning, and reviewer agreement.

### Who is responsible for your data?

The data controller is:

**Industry Commons Foundation (insamlingsstiftelse)**

Organisation number: 802481-2599

7A Centralen, Vasagatan 7

111 20 Stockholm, Sweden

The Musparql researcher operates the service on ICF's behalf. For questions,
withdrawal, or requests concerning your personal data, contact:
`musparql@industrycommons.net`.

### Taking part is voluntary

Participation is voluntary. You may decline the invitation without giving a
reason and without disadvantage. If you participate, you may stop an assignment
at any time and may withdraw your consent by emailing
`musparql@industrycommons.net`.

Withdrawing consent does not affect the lawfulness of processing carried out
before your withdrawal. What happens to information already supplied is
explained under **Withdrawal and retention** below.

Musparql's first release is intended only for invited adults aged 18 or over.

### What you will be asked to do

You will normally sign in using a short code sent to your invited email address.
If email login is unavailable at the IPL workshop, the facilitator may instead
give you a shared workshop entry code. Each use of that code creates a separate
participant account and session. You will then complete a short profile and,
before each assignment, confirm your relevant subject expertise and familiarity
with the assigned knowledge graph.

For the IPL workshop, you may work alone or in a self-selected reviewing group.
Each participant consents and completes their profile individually.
A group makes one joint submission, attributed to the pseudonymous reviewer
identifiers of the members who contributed to it.

Depending on the assignment, you may be asked to:

- assess whether a SPARQL query expresses a meaningful information need;
- accept, rewrite, or reject a proposed natural-language question;
- record alternative wording or a comment;
- identify a problem in a query, its source material, or a proposed question;
  or
- compare and rate alternative formulations against a fixed reference.

Your invitation will describe the expected size and timing of the particular
assignment. You may pause and return later while the assignment remains open.

### What personal data we collect

We collect only the information needed to operate the invited review service
and conduct the study:

- **Operational identity and contact:** your name, optional affiliation,
  account status, and the confidential link to your pseudonymous reviewer
  identifier. If you use email sign-in, we also collect your email address.
  These details are used for invitations, login, follow-up questions about your
  annotations, and future annotation invitations; they are not research
  variables.
- **Languages:** the languages you select and your self-described proficiency.
- **General expertise:** research domains you enter and your selected expertise
  level in each domain.
- **Technical experience:** your self-described experience with knowledge
  graphs and ontologies, SPARQL, and natural-language processing or language
  models.
- **Assignment-specific assessments:** your subject expertise relevant to an
  assigned knowledge graph and your familiarity with its resource, data, data
  model, or graph representation.
- **Review contributions:** decisions, rewrites, alternative formulations,
  ratings, comments, problem reports, submission timestamps, and, for joint
  submissions, the reviewing group's pseudonymous identifier and the
  pseudonymous reviewer identifiers of its contributors.
- **Service and security records:** login-code and session records, assignment
  status, submission receipts, processing status, email-delivery status, and
  limited security/audit records needed to protect and operate the service.

Please do not enter sensitive personal information—such as health information,
political opinions, religious beliefs, sexual orientation, trade-union
membership, biometric/genetic information, or information about criminal
offences—in any free-text field.

### How and why we use the data

ICF uses the data to:

- invite and authenticate reviewers and administer their accounts;
- provide assignments and preserve submitted review work;
- understand which forms of expertise and knowledge-graph familiarity are
  represented in the reviewer cohort;
- compare review judgements across reviewers and, where relevant, across time;
- construct, verify, document, and publish the Musparql benchmark and public
  alternatives/provenance file;
- conduct private reviewer-level analyses of expertise, familiarity, learning,
  and agreement, and publish only appropriate aggregate findings unless ICF
  separately approves another disclosure;
- secure, back up, restore, maintain, and audit the service; and
- answer participant questions and data-protection requests.

We do not use your data for marketing or unrelated profiling. Musparql does not
make automated decisions about you. Language models cannot approve a benchmark
decision or make a consequential decision about a reviewer.

### Lawful basis and consent

ICF relies on your consent to participate and to collect and use your personal
data for the Musparql activities described in this notice while you take part.
Consent must be freely given, informed, specific, and expressed through a clear
affirmative action. You can refuse or later withdraw without disadvantage.

**[ICF to insert and explain the lawful basis for retaining existing
pseudonymous annotations and their associated language, expertise, and
KG-familiarity research variables after withdrawal. ICF must also confirm
whether a different lawful basis applies to minimal security, rights-request,
incident, or legal-accountability records.]**

### Pseudonymisation and research outputs

The system assigns you a random identifier such as `reviewer-0001`. Musparql
keeps two separate private records:

- an operational contact registry containing your name, email address,
  optional affiliation, account information, and the identity-to-reviewer link;
  and
- a pseudonymous research dataset containing your annotations and the language,
  expertise, and KG-familiarity information needed to interpret them.

The public Musparql release has two data components: a concise benchmark file
containing canonical question–SPARQL pairs and an alternatives/provenance file
containing accepted formulations and methodological provenance. New releases
using external reviewers will not publish their stable reviewer IDs,
reviewer-level language, expertise or familiarity, or linked individual
annotation histories.

Public reporting about reviewer characteristics will use appropriate aggregate
results and avoid singling out an identifiable reviewer. The private research
dataset remains personal data even after the direct identity link is removed if
a reviewer could still reasonably be identified from their contribution or
combination of characteristics.

### Who can access the data and where it is stored

Access to identifiable and confidential information is limited to the
authorised Musparql researcher and authorised ICF administrators when access is
necessary for the research, continuity, security, recovery, or incident
response.

The application is hosted for ICF by Hetzner on a dedicated server in Germany.
Encrypted backups are stored in ICF-controlled off-server storage and retained
for up to 90 days. The authorised researcher may securely access the system
from the United Kingdom; the European Commission currently recognises the UK as
providing adequate protection for GDPR transfers.

**[Insert the selected transactional email provider.]** The email provider
receives your email address and the limited message content needed to deliver
invitations and login codes. Provider credentials do not give Musparql access
to your personal mailbox.

ODOMA does not receive Musparql reviewer personal data. Any future shared
authentication, joint analysis, or sharing that changes this position would
require a documented decision and an updated notice before it begins.

Individual-level reviewer research data is not published or routinely shared.
If another researcher asks for access in future, ICF will consider that request
separately. No access will be provided merely because it was requested. Any
approved access must have a defined purpose, appropriate safeguards and
agreements, and any further notice or consent required at that time.

If the Musparql researcher's relationship with ICF changes, the private data do
not automatically transfer to the researcher or a new institution. ICF
remains responsible for the data unless it formally approves and documents a
lawful successor or continued-access arrangement.

**[ICF to confirm the selected email provider's processing countries,
subprocessors, and whether any additional international-transfer safeguards
must be described.]**

### Security

Musparql uses controlled access, short-lived single-use login codes, a
time-limited and capped workshop entry code when needed, revocable sessions,
HTTPS, access controls, restricted service accounts, redacted operational logs,
encrypted off-server backups, security updates, and tested recovery procedures.
No online service can eliminate every risk, but ICF and the researcher take
measures proportionate to the limited scale and nature of the study.

If a personal-data incident occurs, ICF leads the response and will inform
affected participants when required.

### Withdrawal and retention

You may stop participating or withdraw your consent at any time by emailing
`musparql@industrycommons.net`. We will stop assigning new work, disable your
login, revoke active sessions, and stop contacting you about further
annotations.

On withdrawal, the live operational contact registry containing your name,
email address, optional affiliation, account data, and identity-to-reviewer link
will be deleted within 30 days. Encrypted backup copies expire through the
normal backup cycle within 90 days, and recorded deletions are reapplied if an
older backup is restored.

Existing annotations and the language proficiency, expertise, and
KG-familiarity information needed to analyse them will remain in the private
pseudonymous research dataset. They will not be added to the two public
benchmark files as reviewer-level data. **[ICF must insert the confirmed lawful
basis or research provision for this retention and explain any applicable
right to object or request erasure.]**

If you do not withdraw, operational identity/contact information is retained
until no later than two years after formal project closure. The project lead
recommends closure and ICF records the closure date when no reviewer
recruitment, annotation rounds, or planned analyses requiring the private
pseudonymous dataset remain active and no approved successor arrangement has
been established. Project status will be reviewed periodically rather than
left open indefinitely.

Pseudonymous annotations and the minimum research variables needed to interpret
them are retained for the documented lifetime of the Musparql research dataset,
subject to ICF's confirmed lawful basis and storage-limitation review. Expired
login codes, sessions, delivery records, logs, temporary processing records,
and administrative audit records have shorter operational retention periods.

### Your data-protection rights

Depending on the circumstances, you may ask ICF to:

- give you information about and access to your personal data;
- correct inaccurate or incomplete information;
- erase your personal data;
- restrict how your data is used; or
- provide personal data you supplied in a portable format where that right
  applies.

You may withdraw consent at any time. Some rights can be limited in particular
circumstances, but ICF will explain any decision not to fulfil a request.
Contact `musparql@industrycommons.net` to exercise a right. ICF may ask for
proportionate information to verify your identity, normally through your
verified email address.

You also have the right to lodge a complaint with the Swedish Authority for
Privacy Protection (Integritetsskyddsmyndigheten, IMY):
<https://www.imy.se/en/individuals/data-protection/your-rights-as-a-data-subject/>.

### Changes to this notice

The system records which version of this notice you accepted. If a material
change affects the purposes of the study or how your personal data is used, we
will provide the revised notice and, where required, ask for fresh consent
before the new processing begins.

## Proposed affirmative consent wording

The following should appear beside an unticked, required checkbox after the
participant has seen the essential summary above and had an opportunity to read
or download the complete notice:

> I confirm that I am aged 18 or over, that I have read the Musparql participant
> information and privacy notice, and that I voluntarily agree to take part and
> to Industry Commons Foundation using my personal data as described.

Store the notice version, consent statement version, and consent timestamp. Do
not preselect the checkbox. A separate invitation or assignment should not be
treated as consent by itself. The withdrawal consequences, including any
approved retention of pseudonymous research data, must be stated immediately
above the checkbox rather than compressed into its label.

## Drafting references

- [European Commission — information for
  individuals](https://commission.europa.eu/law/law-topic/data-protection/information-individuals_en)
- [European Commission — consent and withdrawing consent, including scientific
  research](https://commission.europa.eu/law/law-topic/data-protection/information-business-and-organisations/legal-grounds-processing-data_en)
- [European Commission — adequacy decisions, including the renewed UK
  decision](https://commission.europa.eu/law/law-topic/data-protection/international-dimension-data-protection/adequacy-decisions_en)
- [Swedish Authority for Privacy Protection — processing personal data for
  research](https://www.imy.se/en/organisations/data-protection/data-protection-within-different-areas/processing-of-personal-data--for-researchers/)
- [Swedish Authority for Privacy Protection — data-subject
  rights](https://www.imy.se/en/individuals/data-protection/your-rights-as-a-data-subject/)
- [Industry Commons Foundation public contact
  details](https://industrycommons.net/)
