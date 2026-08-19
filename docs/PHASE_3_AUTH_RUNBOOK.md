# Musparql v2 Phase 3 authentication runbook

This runbook covers local development of the invitation-only Flask application.
It does not authorize deployment, a public endpoint, real reviewer accounts, or
real email. Those remain subject to the Phase 2b, privacy, email-provider, and
Phase 10 gates in `MUSPARQL_V2_PLAN.md`.

## Implemented boundary

- Six-digit, single-use login codes expire after 15 minutes and are stored only
  as keyed hashes.
- Login requests have per-address and per-request-context limits. Persistent
  records enforce the invited-address limit across process restarts; the
  bounded in-process keyed-digest limiter also covers unknown addresses without
  storing their email addresses. Login lookup and delivery use a bounded
  in-process background queue so provider latency cannot disclose account
  membership or create an unbounded request backlog. Provider failures roll
  back the unusable challenge and release its in-process rate-limit reservation.
- Successful login creates a random, server-side, revocable session. The browser
  receives only an opaque `HttpOnly` cookie; authentication data is never put in
  local storage.
- Ordinary, remembered, and owner sessions use the idle and absolute lifetimes
  recorded in the v2 plan. Owner sessions are never remembered.
- The configured sole owner can invite, disable, restore, or erase a reviewer's
  identity data after recent authentication. Disable and erasure immediately
  revoke the affected sessions. Restore returns a never-accepted invitation to
  `invited`, rather than activating it without verification. Failed invitation
  delivery rolls back the reviewer and audit rows so the owner can retry. The
  configured owner cannot be changed through the web UI.
- Owner actions are recorded in an append-only table using pseudonymous IDs,
  without names or email addresses.
- Phase 3 contains only the `SyntheticEmailSender`. A real sender must implement
  the small adapter in `src/musparql/web/email.py` and be explicitly injected.

Identity erasure retains the pseudonymous reviewer row and changes its status to
`withdrawn`, because later scholarly records may refer to that ID. It removes
the name, affiliation, original email, privacy acknowledgement, outstanding
login codes, and active sessions. This matches the governance draft's proposed
separation between identity data and pseudonymous scholarly decisions; the
controller must still approve the final policy before real data is collected.

## Local synthetic setup

Install the package and migrate a disposable database:

```bash
.venv/bin/pip install --no-build-isolation -e '.[test]'
.venv/bin/musparql-db upgrade --database var/phase3-synthetic.sqlite3
```

Create the sole owner. The command prompts for name and email so those values do
not appear in the process list or command history:

```bash
.venv/bin/musparql-web bootstrap-owner \
  --database var/phase3-synthetic.sqlite3 \
  --reviewer-id reviewer-0001
```

Set configuration only for the current synthetic shell. Use a newly generated
secret of at least 32 bytes and do not commit it:

```bash
export MUSPARQL_DATABASE_PATH="$PWD/var/phase3-synthetic.sqlite3"
export MUSPARQL_OWNER_REVIEWER_ID="reviewer-0001"
export MUSPARQL_APP_SECRET="replace-with-a-new-random-development-secret-of-32-bytes-or-more"
export MUSPARQL_ALLOW_SYNTHETIC_EMAIL="1"
.venv/bin/flask --app musparql.web:create_app run
```

This development server does not display the synthetic outbox, so normal manual
email-code login is intentionally unavailable from a separate browser process.
Use the automated tests to exercise the complete flow. A future local-only
outbox viewer must be explicitly development-gated and must never be enabled on
a real-data instance.

## Validation

```bash
.venv/bin/python -m pytest -q tests/test_v2_phase3_auth.py
.venv/bin/python -m pytest -q
.venv/bin/pip check
```

The Phase 3 tests cover membership-neutral login responses, hashed and
single-use codes, replacement and expiry, request and attempt limits, session
rotation, ordinary shared-browser defaults, logout-all, idle and absolute
expiry, CSRF, security headers, recent-owner authentication, immediate disable,
state-preserving restore, retryable delivery failures, identity erasure, audit
immutability, migration from the original Phase 3 schema, and non-owner
isolation.

## Real-sender handoff

Do not weaken the adapter by passing mailbox credentials, OAuth refresh tokens,
message bodies, or login codes through route logs. The selected implementation
must:

1. use the ICF-approved route, or the separately approved send-only Gmail API
   fallback;
2. load credentials outside Git;
3. request no inbox-reading permission;
4. raise a generic delivery failure without embedding recipient or code data in
   the exception text;
5. support credential revocation and rotation; and
6. be tested with synthetic recipients before the real-data gate is opened.

The Phase 3 background queue is process-local and deliberately non-durable.
Before real invitations are enabled, the selected production delivery design
must define crash recovery and retry semantics; a durable outbox or equivalent
provider-supported idempotency is preferred.

The Flask development server is not a deployment server. Remote exposure,
service units, tunnelling, and server operations remain outside this phase and
must follow `HOME_SERVER_BOUNDARY.md`.
