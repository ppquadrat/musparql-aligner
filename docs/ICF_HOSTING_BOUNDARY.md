# Musparql v2 ICF hosting boundary

Status: **current production infrastructure source of truth**

Source decision: ICF hosting decision and handover prepared 29 August 2026.
The verbatim handover is retained locally under
`confidential/infrastructure/` and is not a repository artifact.

This document is a public-safe operational summary. It supersedes the
home-server, WSL, Google Drive, and Tailscale-specific deployment assumptions
in `MUSPARQL_V2_PLAN.md`. The application's privacy, holdout, backup-consistency,
testing, and real-reviewer release gates remain in force unless explicitly
revised here or in a later approved decision.

## 1. Hosting and ownership

- ICF is the data controller and the infrastructure owner.
- Musparql v2 runs on a dedicated ICF-owned Ubuntu 24.04 LTS VPS hosted by
  Hetzner in Falkenstein, Germany.
- The public service name is `musparql.industrycommons.net`.
- The project owner has a named sudo-capable account authenticated by an SSH
  public key. ICF retains an escrow administrative route for continuity.
- The approved public firewall surface is SSH, HTTP, and HTTPS only.

The existing firewall, fail2ban, automatic security updates, key-only SSH,
disabled password/root login, and ICF escrow access are infrastructure controls
approved by ICF. Do not weaken, replace, or reconfigure them without explicit
approval. Ask ICF before opening any additional port.

## 2. Application boundary

- Run the Flask application behind Caddy or another explicitly approved HTTPS
  reverse proxy.
- Only the reverse proxy may listen publicly. Bind the application server to
  loopback or a Unix socket.
- Run the web application and background worker as dedicated unprivileged
  services with automatic restart.
- Keep secrets outside Git and outside process arguments. Restrict their files
  to the service account.
- Store application code and all durable reviewer data only below the backed-up
  path families `/home/polina`, `/opt`, or `/srv`. Confirm the final directory
  layout before deployment; `/opt/musparql` for installed code and
  `/srv/musparql` for durable state is the working recommendation.
- Do not store durable or personal data in `/tmp`, `/var/tmp`, an unlisted
  mount, or a repository-local disposable `var/` directory.

Reviewer personal data remains on the ICF server. Only pseudonymised research
exports expressly allowed by the Musparql data model and governance decision
may leave it. Private holdout material remains outside ordinary application and
agent workflows under the repository's existing holdout boundary.

## 3. Backup and recovery boundary

ICF provides a daily, client-side-encrypted restic backup to ICF-controlled
off-server storage, with 90-day retention and integrity checking. It covers
`/etc`, `/home`, `/opt`, and `/srv`. The encryption key and provider-level
credentials are escrowed by ICF.

Agents must not disclose, commit, copy, rotate, or reconfigure the backup
repository credentials or encryption material. Coordinate restores and any
backup-policy change with ICF.

Infrastructure backup does not by itself prove application-consistent recovery.
Before real reviewer data is admitted, Musparql must demonstrate that:

- the WAL-mode SQLite database is captured consistently;
- database-referenced assignments, submissions, exports, and processing files
  are recovered as one coherent generation;
- an isolated restore passes database integrity, migration-version, file
  existence, containment, and digest checks;
- deletions made after the restored snapshot are reapplied before reopening;
- backup failure and service unavailability reach an owner-visible alert; and
- the accepted recovery-point objective is recorded. The earlier v2 plan's
  submission-triggered 15-minute target is not satisfied by a daily backup and
  must either be implemented through application snapshots or explicitly
  revised by the owner and ICF.

Do not test a restore over the live data directory.

## 4. Email and public contact

- `musparql@industrycommons.net` is the approved participant-notice and rights-
  request contact address.
- Login-code delivery will use an ICF-coordinated transactional SMTP service
  scoped to the Musparql subdomain. Its DNS and sender verification are a joint
  ICF/owner deployment task.
- The root domain's existing mail service is outside scope and must not be
  changed.
- SMTP credentials belong in the approved server-side secret store. They must
  not enter Git, logs, exception messages, screenshots, or test fixtures.
- Real invitations remain disabled until production email delivery and its
  failure/retry behaviour pass an end-to-end test.

## 5. Data-protection decision

The ICF decision records:

- ICF as controller, with the project owner operating Musparql as ICF's
  researcher;
- informed consent with an information sheet as the approved lawful basis and
  participation mechanism;
- a participant notice naming ICF as controller and the Musparql address as the
  contact, approved by ICF before the first invitation;
- rights requests answered by the project owner in ICF's name;
- incidents led by ICF and reported to the named ICF contacts the same day;
- deletion of identity/profile data on withdrawal or two years after project
  close, retention of pseudonymous annotations with the dataset, and 90-day
  backup retention;
- no reviewer personal data transfer to ODOMA unless later shared
  authentication or joint analysis is separately approved; and
- no DPIA at the present scale, with ICF recording that assessment.

The participant notice and application acknowledgement must use consent
language consistently with this decision. The final approved notice—not this
summary—is the reviewer-facing authority.

## 6. Real-reviewer release gate

Do not send the first real invitation until all of the following are complete:

1. SSH access and host identity have been verified.
2. The application and worker run as unprivileged, automatically recovering
   services behind HTTPS.
3. Production secrets, host/origin validation, secure cookies, logs, and file
   permissions have been checked.
4. Production SMTP delivery, failure handling, and credential rotation have
   been tested.
5. ICF has approved the final participant notice and acknowledgement wording.
6. ICF has completed its required data-management-plan registration check.
7. A coherent backup has been restored into isolation and validated.
8. Backup/service alerts and a deliberate restart or reboot have been tested.
9. An external browser has completed the full synthetic workflow, including
   submission, processing, and owner review.
10. The Phase 9 human mobile-browser observation has passed or been explicitly
    re-scoped before any workshop flow that depends on mobile use.

## 7. Superseded deployment assumptions

For ICF-hosted Musparql v2 production, do not implement or operate:

- the `MusparqlReview` WSL production design;
- Windows keepalive or watchdog tasks;
- Tailscale Funnel;
- router port forwarding; or
- the proposed Google Drive production backup destination.

The old home-server documents remain historical and retain their strict safety
boundary for any explicitly requested legacy work. They are not instructions
for the ICF VPS.
