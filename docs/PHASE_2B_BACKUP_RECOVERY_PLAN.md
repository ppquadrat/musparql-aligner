# Phase 2b — durable backup and recovery plan

Status: **on hold** as of 2026-08-18.

Implementation must not begin until the VocalLanes backup dead-man's-switch
failure has been explained and the reference design has passed both a real
success-ping test and a deliberately missed-window alert test. Planning and
synthetic-only development in later Musparql phases may continue subject to the
gate in [Working on later phases](#working-on-later-phases).

## 1. Purpose

Phase 2b must make Musparql recoverable as a coherent system. The recovery unit
is not just the SQLite database: database rows refer to file-backed assignments,
review submissions, exports, generation runs, query provenance, and staged
processing results. A database restored without the matching files, or files
restored without the matching database state, is not a successful recovery.

This phase therefore separates reproducible working data from durable research
state, creates coordinated database-and-file snapshots, writes an encrypted
off-site copy to Google Drive, monitors the process from outside its failure
domain, and proves restoration into an isolated location.

## 2. Non-negotiable boundaries

1. VocalLanes has operational priority. Musparql must not modify or depend on
   the VocalLanes repository, WSL distribution, `/data` tree, rclone
   configuration, remotes, credentials, timers, services, ports, Windows
   startup tasks, monitoring checks, or alert configuration.
2. Musparql must use its own WSL distribution, Linux account, paths, service
   names, rclone configuration, Google Drive remote, crypt remote, encryption
   passphrase, timers, alerts, and monitoring checks.
3. Private or holdout-bearing review material remains in the existing separate
   private repository. It is outside the application backup and every agent
   workflow. Phase 2b neither reads it nor migrates it.
4. Only sanitized non-holdout review material may enter the durable application
   store described here.
5. No real reviewer profile or review submission may be collected before all
   Phase 2b exit criteria pass.

## 3. Decisions recorded on 2026-08-18

- Canonical non-holdout review submissions, sanitized exports, and review
  decisions are retained for the lifetime of the Musparql dataset. This includes
  exclusions, dismissals, deferrals, `no_edit` decisions, and reviews that never
  produce a benchmark record.
- Reviewer personal information is deleted on a valid deletion or withdrawal
  request. Otherwise, it is retained until two years after the project ends.
- A canonical deletion may remain in rolling disaster-recovery backups for no
  more than 90 days. Restoration procedures must reapply deletions made after
  the restored snapshot.
- Successful complete backup generations are retained for 90 days. Pruning must
  not begin unless a newer verified Google Drive generation exists.
- Google Drive is the sole backup destination for Phase 2b. The owner accepted
  the absence of a physically separate on-site copy on 2026-08-18; adding one is
  deferred future hardening rather than a release gate for this phase.
- The encryption passphrase is held by the owner in a password manager. The
  passphrase must not be committed, logged, embedded in tests, or recoverable
  only from the encrypted backup it unlocks.
- During active reviewing, the recovery-point target is 15 minutes: an accepted
  submission queues an asynchronous backup, with a nightly reconciliation run
  as a safety net.
- Recovery of the database and durable file state should be achievable within
  four hours under the documented server-loss procedure.
- An isolated restore test runs monthly against Google Drive.
- The implementation remains on hold until the VocalLanes validation gate in
  section 12 is satisfied.

## 4. Accepted single-destination risk

Phase 2b will use Google Drive as its only backup destination. There will be no
physically separate on-site copy for now. The live Musparql data and the
encrypted, versioned Google Drive backup remain in different failure domains,
but this is less resilient than keeping a third copy on separate local media.

The accepted consequences are:

- recovery depends on Google Drive availability, the Musparql rclone/OAuth
  configuration being reproducible, and the crypt passphrase remaining
  available in the owner's password manager;
- there is no fast local-media restore if both the server copy and network
  access are unavailable; and
- account loss or a provider-side problem must be addressed through Google
  account recovery and retained backup versioning rather than another stored
  copy.

Adding an encrypted external SSD or independent NAS copy remains a future
hardening option. It must use Musparql-only paths and configuration and must not
be shared with or mounted into VocalLanes.

## 5. Data classification

### 5.1 Git-tracked and recoverable from Git

Code, schemas, migrations, prompts, public-safe catalogues, curated source
material, experiment conclusions, and published benchmark snapshots remain in
Git. The application backup may include a commit identifier in its manifest but
does not need to duplicate the repository as authoritative state.

### 5.2 Disposable working state under `var/`

After the storage separation, deleting `var/` may cost compute or network time
but must not erase a human decision, an exact retained model response, or a
database-referenced artifact. Expected disposable families include:

- downloaded KG dumps and repository caches;
- temporary and partial files;
- lock files and service logs;
- regenerated browser assets;
- evaluation reports reproducible from retained runs; and
- derived outputs whose complete inputs and recipe are retained elsewhere.

Every writer under `var/` must have a documented reconstruction source and
command. If it does not, its output is not disposable and must move to the
durable store.

### 5.3 Durable non-holdout application state

Development uses a repository-local ignored `data/` root. Production uses a
Musparql-owned root provisionally `/data/musparql/` inside the dedicated
Musparql WSL distribution. Both resolve through `MUSPARQL_DATA_DIR`; database
rows store normalized paths relative to that root rather than machine-specific
absolute paths.

Proposed layout:

```text
data/
  db/
    musparql.sqlite3
  assignments/
  submissions/
  sanitized-review-exports/
  decisions/
  query-catalogue/
  generation-runs/
  unfrozen-model-outputs/
  processing-outputs/
```

The durable set includes:

- the confidential and operational SQLite database;
- every issued assignment bundle referenced by the database;
- every accepted non-holdout submission revision;
- sanitized non-holdout review exports;
- the canonical append-only decision history;
- the working query catalogue and its execution/correction provenance;
- frozen generation runs;
- unfrozen model outputs explicitly marked for retention; and
- processing outputs referenced by database jobs.

### 5.4 Human-only private state

Private or holdout-bearing review material remains in the existing private
repository under its owner-controlled backup procedure. Its repository,
credentials, retention, and recovery are not inputs to this phase. Application
backup code must use an allowlisted durable root and must fail if a source path
resolves outside it.

## 6. Durable review and decision model

Git publication is not proof that a review outcome is retained. A pair may be
excluded, dismissed, deferred, or superseded without appearing in a benchmark.
Phase 2b therefore protects three linked records:

1. **Immutable sanitized submission/export** — the accepted source evidence.
2. **Canonical decision event** — the normalized append-only operational and
   scholarly record.
3. **Public-safe Git projection where permitted** — enough non-holdout state to
   restore safe decisions after a clean checkout, without publishing private
   reviewer or holdout information.

Each decision event must identify at least:

- the query identity and reviewed SPARQL version/hash;
- the decision type and reason or note where supplied;
- the pseudonymous reviewer ID and globally distinct review-event ID;
- the reviewed timestamp;
- the source submission ID and digest;
- predecessor/supersession links; and
- enough source/bundle provenance to reject a stale decision after a query
  revision.

The original sanitized submission remains durable even when its normalized
decisions are all excluded from a benchmark.

## 7. File lifecycle and consistency rules

1. Any file named by `bundle_path`, `export_path`, or
   `candidate_output_path` must live below `MUSPARQL_DATA_DIR`.
2. The server derives final paths; a browser or request payload cannot select
   them.
3. Durable filenames include an immutable identity, revision, or digest. A
   registered durable file is never silently overwritten.
4. Writes use a temporary file in the destination directory, flush data,
   atomically rename, calculate SHA-256, and then register the path and digest
   in a short database transaction.
5. A failed database registration leaves no apparently accepted final file, or
   queues a deterministic orphan cleanup. A registered database record must
   never point to a missing file.
6. Mutable working catalogues are copied into each backup generation. Frozen
   runs and immutable submissions may be linked or copied into the staging
   generation, but the snapshot must not depend on their remaining unchanged
   after the manifest is finalized.

## 8. Snapshot format and construction

Each run creates a new immutable generation identified by a UTC timestamp and a
random suffix. It is assembled in a Musparql-owned staging directory that is
not itself part of the source set.

Construction order:

1. Acquire a single-instance backup lock.
2. Check the durable root, destination mounts, required executables, free space,
   and expected database location.
3. Snapshot the live WAL-mode SQLite database with SQLite's backup API; never
   copy the live database file directly.
4. Run `PRAGMA integrity_check` and verify the expected Alembic revision.
5. Enumerate the durable allowlist and every file path referenced by the
   database.
6. Reject traversal, symlinks escaping the root, missing referenced files,
   unexpected absolute paths, digest mismatches, and temporary/partial files.
7. Copy the mutable catalogue and other selected files into the generation.
8. Write a manifest containing format version, generation ID, creation time,
   source Git commit, database revision and digest, and for every file its
   relative path, size, SHA-256 digest, and durability class.
9. Verify the completed local generation against its manifest.
10. Transfer it to the Musparql Google Drive crypt remote.
11. Verify remote file listings/digests and write a small completion marker
    last.
12. Only after the Google Drive generation completes, update the local success
    heartbeat and send the off-box success ping.

No remote completion marker, heartbeat, or success ping is written by a dry
run. A partially uploaded generation is ignored by restore selection and may be
cleaned only after a newer complete generation is verified.

Because the durable text artifacts are currently small compared with the
multi-gigabyte cache, generation-based copies are preferred over synchronizing
the live root. This avoids propagating partial local deletion and prevents a
database snapshot from being paired ambiguously with files from another point
in time.

## 9. Encryption and destination isolation

Use the VocalLanes operational pattern—rclone plus a `crypt` remote—but create a
Musparql-owned implementation and configuration.

Required separation:

- distinct rclone configuration file;
- distinct Google Drive backend and root folder;
- distinct crypt remote name and passphrase;
- a private OAuth client where practical, so shared public-client quota cannot
  stop scheduled backups;
- restrictive ownership and file permissions; and
- no imports, symlinks, calls, or deployment steps referencing the VocalLanes
  repository or server paths.

No on-site backup remote is required in Phase 2b. If one is added later, it must
be independently configured and must not change the Google Drive backup's
correctness or monitoring.

The password-manager record must contain the crypt passphrase, remote/account
identity, setup date, and recovery instructions. The rclone `obscure` value is
reversible and is not a substitute for password-manager custody.

## 10. Scheduling and concurrency

- An accepted submission marks backup work pending and asynchronously starts or
  wakes a debounced backup job. The HTTP response does not wait for remote
  upload.
- During an active review period, pending work is backed up within 15 minutes.
- A persistent nightly timer reconciles all durable state even when no trigger
  was recorded.
- One lock prevents overlapping backup generations.
- A missed timer runs after the Musparql environment returns, but off-box
  monitoring must still report that the expected window was missed.
- Absolute executable paths and an explicit minimal `PATH` are used in systemd
  services. A manually successful shell run is not evidence that the scheduled
  unit works.

## 11. Retention and deletion

### 11.1 Canonical records

- Non-holdout review submissions, sanitized exports, and decision history are
  retained for the lifetime of the Musparql dataset, including decisions that
  never enter a benchmark.
- Public-safe Git projections follow normal repository history.
- Reviewer identity and profile data are deleted on a valid deletion or
  withdrawal request, or two years after the project ends if no earlier request
  applies.
- Authentication codes, expired sessions, temporary jobs, and scrubbed logs
  receive separate short operational retention periods in their implementing
  phases; they are not scholarly provenance.

### 11.2 Backup generations

- Retain successful complete Google Drive generations for 90 days.
- Pruning is suspended if Google Drive lacks a newer verified generation, the
  most recent restore test failed, or monitoring is unhealthy.
- Never infer eligibility for deletion from age alone; require a valid manifest
  and completion marker on the replacement generation.
- Record pruning counts and generation IDs without logging reviewer fields,
  submission contents, secrets, or private paths.

### 11.3 Deletion propagation

A canonical personal-data deletion is recorded in a deletion ledger containing
only the minimum operational information needed to reapply it after an older
restore. The recovery runbook must replay deletions later than the chosen
snapshot before the restored application is exposed. Rolling backup expiry
means deleted personal data ages out of all normal recovery copies within 90
days.

Deletion of scholarly review provenance is a separate governance decision from
deletion of identity/contact information. Where lawful and appropriate, retain
the pseudonymous decision while removing the confidential identity link.

## 12. Monitoring and the VocalLanes activation gate

The VocalLanes implementation demonstrated that unit tests, on-box heartbeats,
and manually successful runs are insufficient. Musparql must have:

1. Immediate notification when a backup command returns failure.
2. A watchdog for a missing or stale successful-generation heartbeat.
3. A count of failed runs since the last success, not merely heartbeat age.
4. An off-box dead-man's switch that alarms when a run never starts or dies with
   the Musparql WSL environment.
5. A watchdog for failures to contact the dead-man service itself.
6. External availability monitoring once the portal is deployed.
7. Restore-test failure alerts independent of ordinary backup success.

The phase remains on hold until VocalLanes has:

- explained the missing 2026-08-18 healthcheck ping;
- demonstrated a real successful scheduled ping;
- deliberately missed a window and delivered the expected owner alert; and
- documented any resulting correction so Musparql can incorporate the proven
  design rather than the unverified branch.

Musparql's own monitoring is not accepted until the same end-to-end tests are
performed with Musparql-only checks and credentials. Tests must scrub inherited
alert and healthcheck environment variables so they cannot contact real
services.

## 13. Restore procedure and drills

Restore always targets a newly created isolated directory or disposable test
environment. It must never accept the live data directory as its destination.

For each restore:

1. Select the newest generation with a valid completion marker, or an explicit
   older generation for point-in-time recovery.
2. Download and decrypt the entire generation.
3. Verify the manifest and every file digest.
4. Run SQLite integrity and Alembic revision checks.
5. Verify every database-referenced durable file exists, remains inside the
   restored root, and matches its registered digest.
6. Verify non-sensitive plausibility counts and append-only chain invariants.
7. Reapply canonical deletions recorded after the snapshot.
8. Start an isolated read-only application check against the restored root when
   the application exists.
9. Record a scrubbed PASS/FAIL result and securely remove the temporary restore
   workspace.

Monthly automated drills restore from Google Drive. Before admitting real
reviewer data, perform one attended full Google Drive restore and retain the
scrubbed evidence.

## 14. Implementation work packages

Implementation resumes only after the activation gate in section 12.

### Package A — inventory and storage boundary

- Inventory every writer to ignored state without accessing private holdout
  material.
- Classify each output as Git-tracked, disposable, durable non-holdout, or
  human-only private.
- Introduce `MUSPARQL_DATA_DIR` and the durable directory contract.
- Change the database default from `var/` to the durable root.
- Add tests proving that deleting synthetic `var/` state does not lose durable
  synthetic decisions.

### Package B — durable decisions and path contracts

- Implement the append-only non-holdout decision ledger.
- Persist exclusion/dismissal/defer/no-edit decisions even when they do not
  produce benchmark records.
- Move database-referenced files into immutable durable paths.
- Enforce relative-path containment, immutable revisions, and digest checks.
- Add a public-safe restoration projection where policy permits.

### Package C — local generation builder

- Implement the SQLite backup-API snapshot.
- Build and validate versioned manifests.
- Add dry-run behaviour that cannot forge success state.
- Test corruption, WAL activity, missing files, stale digests, unsafe paths,
  partial staging, concurrency, and low-disk refusal with synthetic fixtures.

### Package D — encrypted Google Drive destination

- Configure the Musparql-only Google Drive and crypt remotes.
- Transfer generations and write completion markers only after verification.
- Add retry/backoff suitable for Drive quota and network failures.
- Ensure remote failure cannot be hidden by a completed local staging
  generation.

### Package E — scheduling and monitoring

- Add submission-triggered debounced backup and nightly reconciliation units.
- Add failure, stale, failed-runs-since-success, and dead-man monitoring.
- Verify the scheduled environment using the real systemd units.
- Perform real success, failure, stale, and deliberately missed-window alerts.

### Package F — restoration and retention

- Implement isolated full restore and validation.
- Add monthly Google Drive restore drills.
- Implement guarded 90-day pruning.
- Implement deletion-ledger replay and verify the 90-day backup expiry rule.
- Complete and record attended restore exercises.

## 15. Test and acceptance matrix

Phase 2b is complete only when all of the following are demonstrated:

- A live WAL-mode SQLite database snapshots consistently while synthetic writes
  occur.
- Database corruption prevents publication of a backup generation.
- A missing or digest-mismatched database-referenced file prevents completion.
- Unsafe paths cannot escape the durable root.
- A dry run writes no remote data, completion marker, heartbeat, or monitoring
  success ping.
- Loss of all or part of the source root cannot delete an earlier generation.
- A completed local staging generation does not count as backup success until
  the Google Drive transfer and remote verification succeed.
- The real scheduled service succeeds under its production environment.
- Real failure and stale alerts arrive.
- A deliberately missed dead-man window alerts from outside the Musparql WSL
  failure domain.
- A complete generation restores from Google Drive into isolation.
- Restored database/file references and SHA-256 digests all verify.
- A synthetic personal-data deletion is replayed after restoring an older
  snapshot.
- Generations older than 90 days prune only after a newer verified Google Drive
  replacement exists.
- No application backup or agent workflow reads or copies private holdout
  material.

## 16. Working on later phases

Later phases may proceed before Phase 2b is implemented, with these limits:

- Phase 3 and later application code, authentication, assignment, submission,
  processing, and deployment automation may be developed and tested with
  obviously synthetic reviewer and review data.
- Database migrations and durable path interfaces may be designed so they are
  compatible with this plan.
- No real reviewer profile, expertise/familiarity assessment, review draft, or
  submission may be collected.
- Do not invite reviewers, run a real workshop/pilot, or expose a production
  review portal that can accept irreplaceable data.
- Do not treat browser local storage, a manual export on one machine, Git, or a
  second directory on the same disk as an interim backup.
- Remote deployment work that could affect VocalLanes or shared Windows-host
  infrastructure remains subject to the explicit later deployment approvals in
  the v2 plan.

In short: implementation can advance with synthetic data, but the real-data
gate remains closed until Phase 2b has passed its full backup, alert, and restore
acceptance tests.
