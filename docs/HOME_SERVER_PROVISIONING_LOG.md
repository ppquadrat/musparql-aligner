# Musparql home-server provisioning log

This log covers only Musparql-owned resources. It must never contain
VocalLanes observations, credentials, configuration, paths, or backup details.

## 2026-08-18/19 — isolation bootstrap

Verified and completed:

- SSH connected as Windows `DANIEL-PC\musparql` at `192.168.1.147`.
- That Windows account initially had no registered WSL distros.
- WSL version `2.7.3.0` was confirmed.
- `wsl --install` was attempted but made no change because Windows required an
  interactive elevated session.
- Canonical's Ubuntu 24.04 WSL root filesystem was downloaded to
  `C:\Users\musparql\WSL\downloads\ubuntu-noble-wsl-amd64-24.04lts.rootfs.tar.gz`.
- Its SHA-256 was verified as
  `2a790896740b14d637dbdc583cce1ba081ac53b9e9cdb46dc09a2f73abbd9934`,
  matching Canonical's published manifest.
- The verified image was imported as WSL2 distro `MusparqlReview` at
  `C:\Users\musparql\WSL\MusparqlReview`.
- An unprivileged Linux user `musparql` was created and set as the distro's
  default user.
- `systemd` was directly verified as PID 1.
- Ubuntu package indexes were updated inside this distro only.
- The minimal Python 3.12, venv, pip, Git, curl, CA-certificate, compiler, and
  OpenSSH-client runtime was installed inside this distro only.
- A dedicated Ed25519 key was generated for Linux `musparql`. The private key
  remains at `/home/musparql/.ssh/id_ed25519`, mode `600`, owned by
  `musparql:musparql`.
- GitHub's officially published Ed25519 host key was installed as
  `/home/musparql/.ssh/known_hosts`, mode `600`.
- The owner added the Linux public key to `ppquadrat/musparql-aligner` as a
  read-only GitHub deploy key; an in-distro `git ls-remote` verification remains
  outstanding.
- Two Musparql-only Windows tasks were created from an elevated `Polina1` SSH
  session and configured to run as `DANIEL-PC\musparql` with password logon:
  `\Musparql WSL Keepalive` at boot and
  `\Musparql WSL Keepalive Watchdog` every five minutes.
- Both tasks explicitly target `MusparqlReview`, run Linux
  `/usr/bin/sleep infinity` as unprivileged Linux user `musparql`, use limited
  Windows privilege, have `MultipleInstances=IgnoreNew`, and have unlimited
  execution time (`PT0S`).
- The owner granted `SeBatchLogonRight` to `DANIEL-PC\musparql` through a local
  security-policy INF after Task Scheduler warned that the right was missing.
- `\Musparql WSL Keepalive` was manually verified `Running` at
  `2026-08-19 01:07:01` with result `267009`.
- `\Musparql WSL Keepalive Watchdog` was manually verified `Running` at
  `2026-08-19 01:18:29`. A redundant manual start was refused with
  `2147946720` (`0x800710E0`) because `MultipleInstances=IgnoreNew`; the running
  state is healthy and the task was not stopped merely to restore result
  `267009`.

Host-specific lessons recorded for repeat provisioning:

- Omitting `/rp` created a password-logon task without prompting on this host;
  use an explicit `/rp *` with `schtasks` to force an obscured terminal prompt.
- S4U registration failed with `0x80070005`; use a stored password for the
  dedicated `musparql` account.
- `Get-Credential` opened an invisible prompt over SSH and could not be
  cancelled normally. Use `Read-Host -AsSecureString`; terminate a stuck SSH
  session with a new-line followed by `~.`.
- `Set-ScheduledTask` returned `0x8007052e` until the password-backed task's
  `-User` and `-Password` were supplied again.
- Result `267011` means the task has never run; result `267009` means the
  long-running task is currently healthy.
- A task that is already `Running` can show `0x800710E0` after `IgnoreNew`
  refuses a redundant start. Check state before treating that result as a
  failure.

Outstanding gates:

- Verify the read-only deploy key from inside `MusparqlReview`, then clone into
  a Musparql-only project directory.
- Perform an owner-approved Windows reboot and verify both Musparql tasks and
  Musparql systemd recovery. The reboot remains a deliberate maintenance event
  because VocalLanes has priority.
- The repository now contains the Flask application, worker, and completed
  local synthetic hardening implementation. It has not yet been cloned or
  installed inside `MusparqlReview`, and no web, worker, or tunnel unit has been
  installed or enabled there.
- A backup cannot be called complete until the owner authorizes a new
  Musparql-only remote destination, stores a new Musparql-only encryption
  passphrase, and an encrypted backup plus isolated restore are verified. No
  VocalLanes backup secret or configuration may be consulted.

Safety statement:

- No `wsl --unregister`, WSL-wide shutdown, cleanup, port forwarding, or
  Windows-host listener was used.
- No VocalLanes account, distro, path, systemd unit, scheduled task, backup,
  credential, or configuration was read or changed.
