# ICF production deployment

This runbook applies only to the dedicated ICF-owned server documented in
[`docs/ICF_HOSTING_BOUNDARY.md`](../../docs/ICF_HOSTING_BOUNDARY.md). Initial
account and workflow checks use synthetic data, but the service will not start
until approved file-backed notice and consent copy is installed. Keep the
application private until the service, recovery, privacy-notice, and email gates
have passed.

## Layout

- `/opt/musparql/current`: read-only application checkout;
- `/opt/musparql/venv`: production Python environment;
- `/srv/musparql`: durable database, bundles, submissions, and candidates;
- `/etc/musparql/musparql.env`: service configuration and application secret;
- `/etc/musparql/participant-notice.txt`: final ICF-approved notice;
- `/etc/musparql/consent-summary.txt`: final ICF-approved consent summary;
- `/etc/musparql/consent-statement.txt`: final ICF-approved checkbox wording; and
- loopback port `8000`: Gunicorn, reachable publicly only through Caddy.

The application and worker run as the unprivileged `musparql` system account.
Code updates remain an explicit sudo-operated action. Do not place real
reviewer data in the checkout.

## Reset a synthetic workshop rehearsal

Only while the workshop contains synthetic test data, build a clean replacement
database with `scripts/reset_ipl_workshop_test_state.py`. The command preserves
the owner, frozen KG seed/package configuration, workshop round, active entry
code, and owner session; it resets the entry-code redemption count and excludes
all participant profiles, teams, forms, assignments, submissions, processing
jobs, and participant sessions. It refuses to overwrite either database.

Stop the web and worker first. Move the stopped database (including any WAL/SHM
files) and the exact `review/submissions` and `review/candidates` directories
into a timestamped directory below `/srv/musparql/deploy-snapshots`; do not use
wildcards. Install the validated clean database and recreate those two durable
directories with owner/group `musparql` and mode `0700`. Restart both services,
then verify SQLite integrity and foreign keys, one owner, zero operational
participant rows, four packages, one active entry code with zero redemptions,
service health, and public HTTPS. Retain the snapshot until the rehearsal reset
has been accepted.

## First installation

Before cloning, verify whether the repository is anonymously readable:

```bash
git ls-remote https://github.com/ppquadrat/musparql-aligner.git HEAD
```

If that fails because the repository is private, stop and configure a distinct
read-only GitHub deploy key. Do not copy a personal SSH private key to the
server.

Install the operating-system packages, create the service account and durable
directories, then clone the repository:

```bash
sudo apt-get update
sudo apt-get install --yes python3-venv caddy
sudo adduser --system --group --home /nonexistent --no-create-home musparql
sudo install -d -o root -g root -m 0755 /opt/musparql
sudo install -d -o musparql -g musparql -m 0700 /srv/musparql
sudo install -d -o musparql -g musparql -m 0700 /srv/musparql/database
sudo install -d -o musparql -g musparql -m 0700 /srv/musparql/review
sudo install -d -o musparql -g musparql -m 0700 /srv/musparql/review/bundles
sudo install -d -o musparql -g musparql -m 0700 /srv/musparql/review/submissions
sudo install -d -o musparql -g musparql -m 0700 /srv/musparql/review/candidates
sudo git clone https://github.com/ppquadrat/musparql-aligner.git /opt/musparql/current
sudo python3 -m venv /opt/musparql/venv
sudo /opt/musparql/venv/bin/pip install --upgrade pip
sudo /opt/musparql/venv/bin/pip install '/opt/musparql/current[production]'
```

Create the restricted configuration directory and generate the application
secret directly into its restricted file, without printing it or placing it in
a process argument:

```bash
sudo install -d -o root -g musparql -m 0750 /etc/musparql
sudo cp /opt/musparql/current/deploy/icf/musparql.env.example /etc/musparql/musparql.env
sudo chown root:musparql /etc/musparql/musparql.env
sudo chmod 0640 /etc/musparql/musparql.env
sudo install -o root -g musparql -m 0640 /dev/null /etc/musparql/app-secret
sudo openssl rand -hex -out /etc/musparql/app-secret 32
```

Before starting the service, set both blank version values to the exact
approved versions, install the three restricted text files, and configure the
real email sender. Synthetic notice generation is restricted to automated test
mode and is rejected by every non-testing process. The application refuses to
start in production unless both versions and all three approved texts are
configured.
Participant profile, workshop, assignment, workbench, and submission access
fails closed when either recorded version or acknowledgement timestamp is
absent or obsolete.

For the 16 September 2026 IPL workshop, the approved source DOCX is retained at
`docs/participant-notices/2026-09-16_Musparql_workshop_participant_notice_APPROVED.docx`.
Install its checked-in, application-ready copy with:

```bash
sudo install -o root -g musparql -m 0640 /opt/musparql/current/deploy/icf/approved-copy/musparql-workshop-2026-09-16-v1/participant-notice.txt /etc/musparql/participant-notice.txt
sudo install -o root -g musparql -m 0640 /opt/musparql/current/deploy/icf/approved-copy/musparql-workshop-2026-09-16-v1/consent-summary.txt /etc/musparql/consent-summary.txt
sudo install -o root -g musparql -m 0640 /opt/musparql/current/deploy/icf/approved-copy/musparql-workshop-2026-09-16-v1/consent-statement.txt /etc/musparql/consent-statement.txt
```

Both configured version values must be
`musparql-workshop-2026-09-16-v1`. Restarting the application with the new
version deliberately returns any existing participant to the affirmative
consent screen before profile or workshop access.

Create the database and the first owner. The prompts collect the owner's name
and email directly in the terminal; do not paste either into an issue, log, or
agent conversation. For this initial gate, use an obviously synthetic name and
an `example.invalid` address. A real owner record is created only when the
real-data conversion gate is complete.

```bash
sudo -u musparql /opt/musparql/venv/bin/musparql-db upgrade --database /srv/musparql/database/musparql.sqlite3
sudo -u musparql /opt/musparql/venv/bin/musparql-web bootstrap-owner --database /srv/musparql/database/musparql.sqlite3 --reviewer-id reviewer-0001
```

Install and start only the loopback services first:

```bash
sudo cp /opt/musparql/current/deploy/icf/musparql-web.service /etc/systemd/system/
sudo cp /opt/musparql/current/deploy/icf/musparql-worker.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now musparql-web musparql-worker
sudo systemctl status musparql-web musparql-worker --no-pager
curl --fail --show-error --silent -H 'Host: musparql.industrycommons.net' http://127.0.0.1:8000/ >/dev/null
```

Do not activate Caddy until the loopback checks pass. Then install its narrowly
scoped configuration and validate it before reloading:

```bash
sudo cp /opt/musparql/current/deploy/icf/Caddyfile /etc/caddy/Caddyfile
sudo caddy validate --config /etc/caddy/Caddyfile
sudo systemctl reload caddy
sudo systemctl status caddy --no-pager
```

## Real-data conversion gate

Do not invite or enrol a real participant until every item in section 6 of
`docs/ICF_HOSTING_BOUNDARY.md` passes. In particular:

- verify the installed file-backed notice and consent versions match the exact
  ICF-approved copy;
- configure and test the production email sender or an approved alternative;
- validate an isolated coherent restore of SQLite and linked files;
- configure owner-visible service and backup failure alerts;
- run the Phase 8 concurrency check on the deployed revision;
- complete an external synthetic browser journey; and
- deliberately reboot and confirm that all three services recover.

Run the installed Phase 8 verifier with
`--project-root /opt/musparql/current` so its version-matched catalogue,
workbench, and schema resources come from the pinned checkout rather than the
Python environment.
