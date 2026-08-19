# MusparqlReview WSL deployment assets

These files apply only to the `MusparqlReview` distro registered under the
Windows account `DANIEL-PC\musparql`. Read
[`../../docs/HOME_SERVER_BOUNDARY.md`](../../docs/HOME_SERVER_BOUNDARY.md)
before using them.

## GitHub host key

`github_known_hosts` contains GitHub's published Ed25519 host key. Install it
as `/home/musparql/.ssh/known_hosts`, owned by `musparql:musparql` with mode
`600`, before the first SSH clone. If GitHub publishes a rotation, verify the
replacement against GitHub's official SSH-fingerprint documentation before
changing this file.

## Windows keepalive and watchdog

The proven host procedure uses two password-backed, limited-privilege tasks:

- `\Musparql WSL Keepalive`, scheduled `ONSTART`; and
- `\Musparql WSL Keepalive Watchdog`, scheduled every five minutes.

Both execute only:

```text
C:\Windows\System32\wsl.exe -d MusparqlReview -u musparql --exec /usr/bin/sleep infinity
```

`Musparql-WSL-Startup.xml` is a reference definition for the boot keepalive,
not the preferred installer on this host. S4U and XML registration failed here.
Use `schtasks` in an elevated `Polina1` SSH session with a TTY, after confirming
that neither exact task name exists:

```cmd
schtasks /create /tn "\Musparql WSL Keepalive" /sc onstart /rl LIMITED /ru "DANIEL-PC\musparql" /rp * /tr "C:\Windows\System32\wsl.exe -d MusparqlReview -u musparql --exec /usr/bin/sleep infinity"
schtasks /create /tn "\Musparql WSL Keepalive Watchdog" /sc minute /mo 5 /rl LIMITED /ru "DANIEL-PC\musparql" /rp * /tr "C:\Windows\System32\wsl.exe -d MusparqlReview -u musparql --exec /usr/bin/sleep infinity"
```

The literal `/rp *` is required to force the obscured terminal password prompt
on this host. Never place the password in the command. Ensure only
`DANIEL-PC\musparql` receives `SeBatchLogonRight`; do not replace other
user-right assignments.

Configure both tasks with `ExecutionTimeLimit=PT0S` and
`MultipleInstances=IgnoreNew`. Because they are password-backed,
`Set-ScheduledTask` must receive `-User "DANIEL-PC\musparql"` and `-Password`
again. Obtain the password using `Read-Host -AsSecureString`; `Get-Credential`
hangs behind an invisible dialog over SSH on this host.

A healthy task is `Running` with result `267009`. Result `267011` means it has
never run. If `IgnoreNew` refuses a redundant start, a still-`Running` task may
show `0x800710E0`; do not stop it just to change that result. WSL state must be
checked from the Windows `musparql` account because
WSL registration and `wsl --list --running` are per Windows user. A final reboot
test requires an owner-approved maintenance window.

Do not enumerate, modify, disable, or replace either Multichannel task.

## Application units

Do not install placeholder application, worker, tunnel, or backup units. Add
and enable `musparql-*` units only when their real executables, allowlisted
paths, configuration, and health checks exist. The web listener must bind to a
loopback address inside the distro, and the tunnel must also originate inside
the distro.
