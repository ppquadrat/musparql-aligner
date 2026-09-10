# Musparql expert-review study: participant information and privacy notice

Status: **draft for ICF review — not approved for use**

Draft date: 10 September 2026

Proposed notice version: `musparql-participant-notice-2026-09-v1`

This draft implements the ICF decision recorded in the 29 August 2026 hosting
handover. It is not legal advice and must be approved by ICF before the first
invitation or collection of real reviewer data.

## Approval and implementation points — remove before publication

1. Confirm that the controller's full legal name and address below are the
   details ICF wants published. The address is taken from ICF's public website.
2. Confirm that `musparql@industrycommons.net` is active and monitored before
   publishing the notice.
3. Insert the selected transactional email provider, its processing locations,
   and any relevant international-transfer wording. Brevo/Mailjet is not a
   sufficiently definite disclosure.
4. Resolve the withdrawal/retained-annotations issue. ICF selected consent as
   the lawful basis but also approved retaining pseudonymous annotations with
   the dataset. Pseudonymisation alone does not make data anonymous. ICF must
   decide and document whether, after withdrawal, earlier annotations will:

   - be irreversibly anonymised so the reviewer is no longer identifiable;
   - remain pseudonymous under a separate lawful ground or applicable research
     safeguard/exception identified by ICF; or
   - be deleted.

   The bracketed paragraph under **Withdrawal and retention** must be replaced
   with the approved outcome.
5. Confirm whether consent is the lawful basis for every listed operation, or
   whether minimal service-security, rights-request, and incident records use a
   different lawful basis. The final notice must map each purpose to the basis
   ICF has actually recorded.
6. Confirm whether ICF has a data-protection officer or separate privacy contact
   that must be named in addition to the Musparql address.
7. Confirm that the Swedish Authority for Privacy Protection (IMY) is the
   supervisory authority ICF wants identified.
8. Define how and when the project is formally declared closed so the two-year
   retention period has a determinate starting point.
9. Complete ICF's OPERAS/data-management-plan registration check.
10. Make this notice available with the invitation and from the login page.
    Replace the application's current acknowledgement-only checkbox with the
    approved affirmative consent wording. Consent must not be inferred merely
    from requesting a login code or continuing to use the site.

---

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
is not to rank, evaluate, or make decisions about individual reviewers.

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

You will sign in using a short code sent to your invited email address. You will
complete a short profile and, before each assignment, confirm your relevant
subject expertise and familiarity with the assigned knowledge graph.

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

- **Identity and contact:** your name, email address, and optional affiliation.
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
  ratings, comments, problem reports, and submission timestamps.
- **Service and security records:** invitation and account status, login-code
  and session records, assignment status, submission receipts, processing
  status, email-delivery status, and limited security/audit records needed to
  protect and operate the service.

Please do not enter sensitive personal information—such as health information,
political opinions, religious beliefs, sexual orientation, trade-union
membership, biometric/genetic information, or information about criminal
offences—in any free-text field.

### How and why we use the data

ICF uses the data to:

- invite and authenticate reviewers and administer their accounts;
- provide assignments and preserve submitted review work;
- understand which forms of expertise and knowledge-graph familiarity are
  represented in the reviewer group;
- compare review judgements across reviewers and, where relevant, across time;
- construct, verify, document, and publish Musparql research datasets and
  related analyses;
- secure, back up, restore, maintain, and audit the service; and
- answer participant questions and data-protection requests.

We do not use your data for marketing or unrelated profiling. Musparql does not
make automated decisions about you. Language models cannot approve a benchmark
decision or make a consequential decision about a reviewer.

### Lawful basis and consent

ICF relies on your consent for the research processing described in this
notice. Consent must be freely given, informed, specific, and expressed through
a clear affirmative action. You can refuse or later withdraw without
disadvantage.

**[ICF to confirm whether a different lawful basis applies to minimal security,
rights-request, incident, or legal-accountability records and insert it here if
so.]**

### Pseudonymisation and research outputs

The system assigns you a random identifier such as `reviewer-0001`. Your name,
email address, affiliation, language profile, expertise information,
knowledge-graph familiarity, authentication data, and rights-request records
are kept in confidential storage and are not placed in ordinary review bundles,
published benchmark files, model prompts, or public reports.

Research-facing review provenance uses the pseudonymous reviewer identifier.
Published reporting will normally be aggregated and will avoid singling out an
identifiable reviewer. Pseudonymised information is still treated as personal
data while ICF retains a way to connect it to you or you remain reasonably
identifiable from the circumstances.

### Who can access the data and where it is stored

Access to identifiable and confidential information is limited to the
authorised Musparql researcher and authorised ICF administrators when access is
necessary for continuity, security, recovery, or incident response.

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

**[ICF to confirm the selected email provider's processing countries,
subprocessors, and whether any additional international-transfer safeguards
must be described.]**

### Security

Musparql uses invitation-only access, short-lived single-use login codes,
revocable sessions, HTTPS, access controls, restricted service accounts,
redacted operational logs, encrypted off-server backups, security updates, and
tested recovery procedures. No online service can eliminate every risk, but ICF
and the researcher take measures proportionate to the limited scale and nature
of the study.

If a personal-data incident occurs, ICF leads the response and will inform
affected participants when required.

### Withdrawal and retention

You may stop participating or withdraw your consent at any time by emailing
`musparql@industrycommons.net`. We will stop assigning new work, disable your
login, and revoke active sessions.

Unless you withdraw earlier, identity, contact, profile, expertise, and
familiarity information is retained until two years after ICF formally records
the end of the project. Expired login codes, sessions, delivery records, logs,
temporary processing records, and administrative audit records have shorter
operational retention periods.

If you withdraw, your identity and profile information will be removed from the
live system within 30 days. Encrypted backup copies expire through the normal
backup cycle within 90 days, and recorded deletions are reapplied if an older
backup is restored.

**[ICF MUST REPLACE THIS PARAGRAPH after resolving the lawful basis: Review
contributions already supplied may be retained with the research dataset under
your pseudonymous reviewer identifier. Explain here whether they are
irreversibly anonymised, retained under another identified lawful ground or
research provision, or deleted following withdrawal.]**

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
participant has had an opportunity to read or download the complete notice:

> I confirm that I am aged 18 or over, that I have read the Musparql participant
> information and privacy notice, and that I voluntarily consent to Industry
> Commons Foundation processing my personal data for the Musparql research and
> service purposes described there. I understand that I may stop participating
> and withdraw my consent at any time by contacting
> musparql@industrycommons.net, without disadvantage.

Store the notice version, consent statement version, and consent timestamp. Do
not preselect the checkbox. A separate invitation or assignment should not be
treated as consent by itself.

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
