# Agent boundary

## ICF production hosting: read this before any server work

Musparql v2 production is hosted by ICF at
`musparql.industrycommons.net` on a dedicated ICF-owned Ubuntu 24.04 VPS.
Before any production-server work, read
[`docs/ICF_HOSTING_BOUNDARY.md`](docs/ICF_HOSTING_BOUNDARY.md). It is the
deployment source of truth and supersedes the home-server and Tailscale
deployment assumptions in the older v2 plan.

The ICF-configured firewall, fail2ban, automatic security updates, key-only
SSH, disabled root login, escrow administration, and encrypted backup system
are approved infrastructure controls. Do not weaken, replace, or reconfigure
them without the owner's and, where applicable, ICF's approval. Do not open
additional ports. Application services must run unprivileged, and only the
HTTPS reverse proxy may listen publicly.

All application code and reviewer data must remain under the ICF-backed-up
paths `/home/polina`, `/opt`, or `/srv`. Reviewer personal data must not leave
the server. Only the explicitly pseudonymised research exports allowed by the
project's data model may be exported. Never commit credentials, private SSH
keys, SMTP secrets, backup repository identifiers, or backup encryption
material.

The verbatim ICF handover is confidential local operational material under
`confidential/infrastructure/` and is ignored by Git. Agents should normally
use the public-safe boundary document instead. Access the verbatim handover
only when the user's task explicitly requires infrastructure administration.

## Legacy home-server boundary: read this before any home-server work

The home-server WSL environment is no longer the Musparql v2 production target.
Operate it only when the owner explicitly requests legacy home-server work.
Any such work is allowed only in the dedicated environment owned by the Windows
account `musparql` on `192.168.1.147`: its own WSL distribution, its own Linux
user, its own project and data directories, and only the systemd units and
Windows scheduled tasks created specifically for Musparql. The `codex` Windows
account is VocalLanes-only and is outside Musparql's scope.

VocalLanes is the priority production application and contains irreplaceable
recordings of named people. Agents must never read, list, search, modify,
restart, stop, disable, delete, copy, back up, or otherwise operate:

- the `Ubuntu-24.04` distro under the `codex` Windows account;
- the Linux user `multichannel`;
- `/srv/multichannel-new`, `/srv/multichannel*`, or `/data` in that distro;
- `multichannel-new` or any `vocallanes-*` systemd unit;
- the Windows tasks `\Multichannel WSL Keepalive` and
  `\Multichannel WSL Keepalive Watchdog`;
- the `codex` Windows account, its password, credentials, backup configuration,
  backup destination, or backup passphrase.

Never run `wsl --unregister`. Never perform machine-wide cleanup such as
`docker system prune` or `apt-get autoremove` without the owner's prior
approval. Before any deletion, list the exact proposed paths for the owner,
wait for approval, and then delete only those explicit paths; never use a
wildcard.

Musparql has two Windows tasks under the `musparql` account:
`\Musparql WSL Keepalive` and `\Musparql WSL Keepalive Watchdog`. These are the
only Windows tasks agents may inspect or operate for Musparql. Musparql must
also have its own systemd units, its own tunnel started inside its own distro,
and its own encrypted backup destination and passphrase. Do not bind a
Musparql service to a Windows port. Separate WSL distros have separate
localhost namespaces, so Musparql services may use a Linux-distro-local
loopback port.

The operational source of truth for legacy home-server work is
[`docs/HOME_SERVER_BOUNDARY.md`](docs/HOME_SERVER_BOUNDARY.md). Stop and ask the
owner if a requested server action falls outside the allowlist in that runbook.

Real holdout annotations are human-only data. Agents must never read, search,
summarize, transform, audit, migrate, or run commands against any of these paths:

- `review/exports/`
- `review/private/`
- `benchmark/v*/holdout.jsonl`
- any file named `musparql-holdout-private-*`

Do not use recursive filesystem commands that bypass repository ignore rules to
discover those files. If work appears to require private holdout data, stop and
ask for a sanitized public export or a synthetic fixture. Agent-authored code and
tests may use obviously synthetic holdout examples only.

Browser-sanitized `non_holdout_review_export` files may be placed in ignored
`var/review/exports/`; agents may use only that directory for review input.

Selector-only files containing no reviewer fields may be used only when the
human owner has explicitly chosen the identity-visible holdout policy described
in `docs/HOLDOUT_SECURITY.md`.

Reviewer profiles under `confidential/reviewers/` are not holdout data, but
they contain personal information. Agents may read them only when the user's
task explicitly requires reviewer administration. Never copy profile or
familiarity fields into prompts, generated bundles, review exports, benchmark
artifacts, logs, or tests; only pseudonymous `reviewer-NNNN` IDs may cross that
boundary. Use synthetic reviewer records in code and tests.
