# Musparql home-server boundary and deployment runbook

Status: mandatory operational policy

Server: `192.168.1.147`

Read this document before every Musparql home-server session. It overrides any
older note that suggests inspecting or reusing VocalLanes resources.

## 1. Ownership boundary

Musparql work is allowed only in resources owned by the Windows account
`musparql`:

- the dedicated WSL2 distro `MusparqlReview`;
- the dedicated, unprivileged Linux user `musparql` in that distro;
- the Musparql project directory and Musparql-only state directories;
- systemd units whose names begin `musparql-` and were created for this project;
- Windows scheduled tasks `\Musparql WSL Keepalive` and
  `\Musparql WSL Keepalive Watchdog`, running as `DANIEL-PC\musparql`;
- a tunnel configured and started inside `MusparqlReview`; and
- a Musparql-only encrypted backup, remote destination, configuration, and
  passphrase.

The `codex` Windows account is VocalLanes-only. It is not a fallback Musparql
account and must not be used for Musparql deployment or administration.

## 2. VocalLanes exclusion zone

VocalLanes is a priority production application containing irreplaceable audio
recordings of named individuals. Never read, list, search, modify, copy, stop,
restart, disable, delete, back up, or otherwise operate any of the following:

- the `Ubuntu-24.04` distro under the Windows account `codex`;
- the Linux account `multichannel`;
- `/srv/multichannel-new`, `/srv/multichannel*`, or `/data` in that distro;
- the `multichannel-new` or `vocallanes-*` systemd units;
- `\Multichannel WSL Keepalive`;
- `\Multichannel WSL Keepalive Watchdog`;
- the `codex` Windows account or its password;
- any VocalLanes credential, backup configuration, destination, passphrase,
  monitoring check, port, or tunnel.

There is no read-only exception. Do not inspect VocalLanes to copy its setup.

## 3. Host-wide prohibitions

- Never run `wsl --unregister` for any distro.
- Do not run machine-wide cleanup, including `docker system prune` or
  `apt-get autoremove`, without explicit owner approval.
- Do not change global Windows networking, firewall, port-proxy, WSL, Docker,
  account, update, power, or scheduled-task configuration unless the owner has
  separately approved the exact change.
- Do not bind Musparql to a Windows-host port or create Windows port forwarding.
- Do not alter either existing Multichannel scheduled task. They start the
  whole server; a prior change caused a five-day outage.

## 4. Safe deletion protocol

Before deleting anything on the server:

1. Resolve and list every exact target path without reading an excluded path.
2. Show that list to the owner and wait for explicit approval.
3. Delete only the approved explicit paths.
4. Never use a wildcard, recursive glob, unresolved variable, or broad parent
   directory as a deletion target.
5. Report exactly what was deleted and whether recovery is possible.

This applies even to Musparql-owned files. Prefer recoverable moves where
practical.

## 5. Required deployment shape

```text
Windows account: musparql
  scheduled task: \Musparql WSL Keepalive (at boot)
  scheduled task: \Musparql WSL Keepalive Watchdog (every 5 minutes)
    keep alive: WSL distro MusparqlReview as Linux user musparql
      Linux user: musparql
        project: Musparql-only directory
        app: musparql-web.service -> Gunicorn/Flask on 127.0.0.1:<distro-local-port>
        worker: musparql-worker.service
        tunnel: musparql-tunnel.service inside the distro
        backup: musparql-backup.service + musparql-backup.timer
```

The Flask/Gunicorn listener must bind only to the distro's `127.0.0.1`. The
tunnel originates inside the same distro and targets that loopback listener.
WSL distros have separate localhost namespaces, so a distro-local port number
does not conflict with a listener inside VocalLanes' distro. Do not solve
connectivity by exposing the service on Windows.

Use resource limits appropriate for a low-traffic research service so
Musparql cannot starve the priority application. If a required limit or host
change is uncertain, stop and ask before deployment.

## 6. Windows keepalive tasks

The two allowed Windows tasks are:

- `\Musparql WSL Keepalive`, triggered at Windows startup; and
- `\Musparql WSL Keepalive Watchdog`, triggered every five minutes.

Both run under the stored-password principal `DANIEL-PC\musparql`, at limited
privilege, and execute only:

```text
C:\Windows\System32\wsl.exe -d MusparqlReview -u musparql --exec /usr/bin/sleep infinity
```

Both have `MultipleInstances=IgnoreNew` and `ExecutionTimeLimit=PT0S`. A healthy
long-running task normally has state `Running` and result `267009`. If a second
manual start is attempted while it is already running, `IgnoreNew` can leave
the task `Running` while the most recent result becomes `2147946720`
(`0x800710E0`, “The operator or administrator has refused the request”). That
combination is expected and must not be “fixed” by stopping the healthy task.
The Windows account requires the `SeBatchLogonRight` user right; grant it only to
`DANIEL-PC\musparql` and do not replace unrelated user-right assignments.

On this host, `Register-ScheduledTask` with S4U failed with access denied, and
`Get-Credential` hung behind an invisible GUI when invoked over SSH. The proven
procedure is `schtasks /create` from an elevated, TTY-enabled SSH session, with
`/ru "DANIEL-PC\musparql"` and `/rp *` so the password is entered through an
obscured prompt and never appears in command history. Password-backed tasks
must receive `-User` and `-Password` again when `Set-ScheduledTask` updates
their settings; use `Read-Host -AsSecureString`, not `Get-Credential`.

Before creation or diagnosis, query only these exact task names. Never enumerate
or modify the Multichannel tasks. The action must identify `MusparqlReview` and
Linux user `musparql` explicitly. Do not use the default distro, `root`, the
`codex` account, or `Ubuntu-24.04`. WSL registrations and running-distro lists
are per Windows user; a check from `Polina1` cannot establish whether the
`musparql` account's distro is running.

## 7. Backup boundary

Musparql uses a wholly independent backup system:

- a Musparql-only source allowlist;
- a Musparql-only destination and remote name;
- a Musparql-only encryption passphrase generated for this project;
- a Musparql-only configuration file readable only by the Linux user
  `musparql`;
- `musparql-backup.service` and `musparql-backup.timer`; and
- an isolated restore test that never writes over the live application.

The owner stores the new Musparql passphrase in their password manager. Agents
must never request, read, reuse, derive, or hold the VocalLanes backup
passphrase. Do not inspect VocalLanes backup scripts or configuration. Do not
claim success until an encrypted off-host generation and an isolated restore
have both been verified.

## 8. Session preflight and postflight

At the start of a server session, verify only within the permitted scope:

- the remote Windows identity is `musparql`;
- commands target `MusparqlReview` explicitly;
- the Linux identity is `musparql`;
- the working directory is the Musparql project directory;
- service names begin `musparql-`; and
- proposed paths do not match an exclusion in section 2.

Abort on any mismatch. Do not investigate the other account or distro.

At the end, record:

- Musparql files and units changed;
- the status of Musparql-only services, tunnel, and backup;
- verification performed; and
- any owner approval used.

## 9. Current provisioning checklist

- [x] `musparql` Windows login and key-based remote access verified.
- [x] `MusparqlReview` created without unregistering or modifying another distro.
- [x] unprivileged Linux user `musparql` created.
- [x] systemd enabled in `MusparqlReview`.
- [ ] read-only GitHub deploy key verified from inside `MusparqlReview`.
- [ ] project installed in a Musparql-only directory.
- [ ] Musparql systemd units installed and enabled.
- [ ] Flask/Gunicorn verified on distro-local loopback only.
- [ ] Musparql tunnel installed inside the distro and verified.
- [x] Musparql keepalive and watchdog tasks created under `musparql`, configured
  for password logon, unlimited runtime, and `IgnoreNew`.
- [x] keepalive manually verified `Running` with result `267009`.
- [x] watchdog manually verified `Running`; a redundant manual start was
  correctly refused with `0x800710E0` because `IgnoreNew` was active.
- [ ] both tasks verified by an owner-observed Windows reboot.
- [ ] independent encrypted backup destination and passphrase configured.
- [ ] backup and isolated restore verified.
- [x] no VocalLanes resource was accessed or changed during this provisioning.

Do not mark an item complete without direct evidence from the Musparql-owned
environment.

The evidence and outstanding gates from each provisioning session are recorded
in [`HOME_SERVER_PROVISIONING_LOG.md`](HOME_SERVER_PROVISIONING_LOG.md).
