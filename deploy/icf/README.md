# ICF production deployment

This runbook applies only to the dedicated ICF-owned server documented in
[`docs/ICF_HOSTING_BOUNDARY.md`](../../docs/ICF_HOSTING_BOUNDARY.md). It starts
with synthetic data and keeps the application private until the service,
recovery, privacy-notice, and email gates have passed.

## Layout

- `/opt/musparql/current`: read-only application checkout;
- `/opt/musparql/venv`: production Python environment;
- `/srv/musparql`: durable database, bundles, submissions, and candidates;
- `/etc/musparql/musparql.env`: service configuration and application secret;
- `/etc/musparql/participant-notice.txt`: final ICF-approved notice; and
- loopback port `8000`: Gunicorn, reachable publicly only through Caddy.

The application and worker run as the unprivileged `musparql` system account.
Code updates remain an explicit sudo-operated action. Do not place real
reviewer data in the checkout.

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
secret without printing it:

```bash
sudo install -d -o root -g musparql -m 0750 /etc/musparql
sudo cp /opt/musparql/current/deploy/icf/musparql.env.example /etc/musparql/musparql.env
sudo chown root:musparql /etc/musparql/musparql.env
sudo chmod 0640 /etc/musparql/musparql.env
sudo sed -i "s|replace-with-at-least-32-random-bytes|$(openssl rand -hex 32)|" /etc/musparql/musparql.env
```

The initial configuration is explicitly synthetic. It must not receive real
names, addresses, profiles, or reviews. Before real use, replace the synthetic
notice switches with the approved notice file/version and configure the real
email sender.

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

- replace the synthetic notice flags with the exact ICF-approved notice;
- configure and test the production email sender or an approved alternative;
- validate an isolated coherent restore of SQLite and linked files;
- configure owner-visible service and backup failure alerts;
- run the Phase 8 concurrency check on the deployed revision;
- complete an external synthetic browser journey; and
- deliberately reboot and confirm that all three services recover.
