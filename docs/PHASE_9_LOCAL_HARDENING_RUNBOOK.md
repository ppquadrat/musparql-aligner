# Musparql v2 Phase 9 local hardening and synthetic pilot

Phase 9 is a synthetic-only release gate for realistic local failures and
reviewer friction. It uses the real authenticated Flask routes, profile and
assignment services, immutable submission path, processing queue, and SQLite
database. It never reads reviewer profiles, real review exports, or private
holdout annotations.

## Passing scope

A passing run verifies:

- first-time code login, onboarding, pre-review assessment, hosted workbench,
  authenticated submission, and repeat assessment with prior answers;
- a persistent remembered cookie for an explicitly private browser, a
  session-only cookie by default on a shared browser, isolated logout, and
  denial to an unauthenticated browser;
- mobile-shaped code entry with the responsive viewport and one-time-code
  input contract;
- recovery and completion of a processing job that was running when the
  application stopped;
- an owner-facing, pseudonymous safe processing summary;
- a deliberately triggered integrity diagnostic whose application log contains
  no synthetic name, email, code, session token, or receipt ID; and
- an online SQLite backup restored into a separate path, checked with
  `PRAGMA integrity_check`, compared by durable row counts, and reconnected to
  the immutable submission file.

This local database exercise is not the Phase 2b durable-backup activation
gate. It does not claim encrypted off-host backup, retention, monitoring, key
custody, or a complete file-set recovery. Those remain governed by
[`PHASE_2B_BACKUP_RECOVERY_PLAN.md`](PHASE_2B_BACKUP_RECOVERY_PLAN.md).

## Friction targets

The Phase 9 pilot makes the previously qualitative friction goal operational:

- first login through saved onboarding: at most five minutes; and
- a returning reviewer's pre-review confirmation: at most one minute.

Automation records HTTP behavior but cannot measure whether a human finds the
flow understandable. Before running the gate, a human operator should complete
the flow with an obviously synthetic identity on a desktop or private browser,
repeat the assessment, and complete the code flow in a mobile browser. Record
elapsed wall time and one concise feedback observation in an ignored JSON file:

### Start the interactive synthetic pilot

No real reviewer address or outbound email account is needed. Start the
loopback-only pilot from the Phase 9 branch:

```bash
.venv/bin/python -m musparql.web.local_hardening --interactive
```

The command prints a local URL and the fictional invited address
`phase9-reviewer@example.invalid`. Open the URL, submit that address, and copy
the six-digit synthetic code printed in the terminal. The code is neither
emailed nor written to the application log. The server binds only to
`127.0.0.1`; it is not reachable from another computer or phone.

Time these two paths:

1. first code request through successful profile save; and
2. opening the second assignment through confirmed repeat assessment.

Use the first assignment to inspect the real hosted workbench, record a
synthetic decision, and submit it. For the mobile-login check, use the browser's
responsive-design/device mode at a narrow phone-sized viewport and repeat only
the login-code flow. Conducting the review itself on a phone is not a Phase 9
requirement. A third, explicitly synthetic assignment opens the linguistic-
dimensions workbench; complete its short pre-assignment assessment to inspect
that interface. Press `Ctrl-C` in the terminal when finished. Unless `--workspace`
is supplied, the fictional database and submissions are deleted when the
command stops.

To retain the fictional state for local diagnosis, provide a new or empty
workspace:

```bash
.venv/bin/python -m musparql.web.local_hardening \
  --interactive \
  --workspace var/verification/phase9-interactive-workspace
```

Then record the measured observations:

```json
{
  "observer": "synthetic-operator",
  "onboarding_seconds": 180,
  "repeat_assessment_seconds": 30,
  "mobile_login_success": true,
  "feedback": "Controls were clear and no assistance was required."
}
```

Do not include a real name, email address, profile answer, login code, or review
content in this observation. The command rejects missing feedback, failed
mobile login, non-positive timings, and timings outside the targets.

## Run the gate

From a clean checkout with test dependencies installed:

```bash
.venv/bin/python -m musparql.web.local_hardening \
  --usability-observation var/verification/phase9-usability.json \
  --output var/verification/phase9-local-hardening.json
```

The default workspace is a temporary mode-`0700` directory removed after the
report is printed. To retain the synthetic database, submissions, candidates,
and isolated restore for diagnosis, pass a new or empty directory:

```bash
.venv/bin/python -m musparql.web.local_hardening \
  --usability-observation var/verification/phase9-usability.json \
  --workspace var/verification/phase9-workspace \
  --output var/verification/phase9-local-hardening.json
```

Never point `--workspace` at an application, submission, review, holdout, or
benchmark directory. Reports conform to
`schemas/local_hardening.schema.json`. Retained workspaces, observations, and
reports are local operational evidence and must not be committed.

## Verification tests

```bash
.venv/bin/python -m pytest -q tests/test_v2_phase9_local_hardening.py
.venv/bin/python -m pytest -q
.venv/bin/pip check
```

Phase 9 blocks deployment if any invariant fails, the human timing observation
is missing, or privacy inspection finds a forbidden value. Phase 10 still owns
isolated server installation, encrypted backup activation, external network
behavior, and reboot recovery.
