# Open issues

This is the maintained index of known implementation and governance work. Open
items are listed first. A short resolved reference is retained where it explains
the provenance of the currently deployed workshop packages; other completed
findings belong in the implementation, tests, and durable runbooks.

## Reviewer administration and privacy

### Reviewer privacy approval and real-data gate

Reviewer profiles, repeatable general-domain expertise, KG-specific subject
expertise, and resource/data-model/KG familiarity have versioned Phase 1 schemas,
append-only history contracts, chain and seed-snapshot validation, synthetic
examples, a privacy notice, and a confidential storage boundary. Phase 4 now
provides reviewer onboarding and correction, versioned notice acknowledgement,
the versioned local suggestion set with free-text fallback, and owner-visible
pseudonymous completion state. The documented legacy registry was never
populated, so Phase 2 creates the v2 database directly and deliberately does not
add legacy-value tables or infer v2 assertions from legacy scales.

The proposed decisions, working ICF-controller assessment, rights/incident
procedures, and ICF/ODOMA questions are now recorded in
[`REVIEWER_DATA_GOVERNANCE_DRAFT.md`](REVIEWER_DATA_GOVERNANCE_DRAFT.md).
ICF has confirmed the controller allocation, consent basis, participant contact
route, ICF-owned hosting arrangement, retention position, and allocation of
rights and incident responsibilities; these decisions are recorded in
[`ICF_HOSTING_BOUNDARY.md`](ICF_HOSTING_BOUNDARY.md). Before collecting real
reviewer data, the remaining governance dependency is ICF's confirmation of the
final participant notice and acknowledgement wording. Production SMTP and the
other technical real-reviewer gates in that boundary must also pass.

Retention periods, access/correction/deletion procedures, and the proposed
consequences of withdrawal are decided for implementation but remain subject to
that controller approval.

The form must never place names, email addresses, affiliation, experience, or
KG-familiarity fields in review bundles, exports, benchmarks, logs, or tests.

### Durable backup and recovery

ICF now supplies daily client-side-encrypted restic backup of `/etc`, `/home`,
`/opt`, and `/srv`, with 90-day retention and integrity checking. Application
state is kept under those backed-up paths. Backup design must still protect more
than the database: review outcomes remain irreplaceable before benchmark
publication, and substantial provenance is intentionally Git-ignored.

The detailed Phase 2b plan is in
[`PHASE_2B_BACKUP_RECOVERY_PLAN.md`](PHASE_2B_BACKUP_RECOVERY_PLAN.md), but its
home-server/Google Drive topology is historical. The current work is to prove a
coherent application restore using ICF's backup: WAL-safe database capture,
matching referenced files, isolated validation, deletion replay, owner-visible
failure alerts, and an accepted recovery-point objective. No real reviewer data
may be collected until those gates pass.

Define, implement, and test an encrypted, authenticated, versioned backup of:

- the confidential and operational SQLite database;
- server-received non-holdout review submissions and sanitized exports;
- the working query catalogue and its local execution/correction provenance;
- frozen generation runs and any model output not already frozen into a run; and
- separately, through a human-only process, any private or holdout-bearing
  review material that application and agent workflows must never access.

ICF controls the off-server repository, encryption material, retention, and
infrastructure-level integrity checks. The application must not access or
reconfigure those secrets. A second directory on the same disk must not be
represented as a backup. Hosted durable submission protects accepted reviews
from browser-local loss, but those receipts and submission files remain unique
state that must be recovered coherently with the database.

### Linguistic workbench introduction and correction policy

The Phase 6b rating screen and navigation have received an initial reviewer UI
pass, but the starting instruction/calibration page still needs a dedicated
content and usability review before a pilot or real collection. Revisit its
information hierarchy, amount of calibration text, examples, and transition
into the first trial; validate the result with representative reviewers rather
than treating the current development copy as final.

Completed linguistic observations are currently locked, which protects the
primary cognition-study design from hindsight and recalibration after later
stimuli. If reviewer operations require amendments, design an explicit
append-only correction mechanism that preserves the original observation,
revised value, reason, actor, and timestamp. Do not add an ordinary overwrite
or silently requeue completed observations.

Phase 7 may omit an unusable linguistic observation or request an objective,
append-only correction. A genuine re-rating after later stimuli must be a new
observation or separately designated round rather than a replacement. The
implementation and reviewer-facing wording remain open.

## SPARQL correction backlog

The detailed, prioritized correction-workbench backlog remains in
[`SPARQL_CORRECTION_FOLLOW_UP.md`](SPARQL_CORRECTION_FOLLOW_UP.md). Its largest
open items are durable non-approval decisions, explicit benchmark exclusion,
source/target execution modelling, contextual evidence, bounded parameter-value
discovery, clearer saved-decision feedback, and clean service shutdown.

The stale agent-metadata defect described there is fixed: changing an agent
proposal now clears the suggestion, edit type, rationale, and evidence IDs and
requires the human to enter fresh edit metadata.

## Resolved: NL provenance and query-comment handling

Resolved in `db9a8b2` and hardened after review in `c5e8d8b`.

### Historical problem: authored NL was dropped at the generation-input boundary

The IPL rehearsal first exposed a concrete case in `nfdi4culture-0006`, but an
audit of all 112 workshop pairs shows that the problem is systematic. The
packages contain 106 `generated`, five `paraphrased`, and one `verbatim`
formulation. All 52 Europeana pairs, all six NFDI4Culture pairs, and all 45
Camera dei Deputati pairs are labelled `generated`; only CDEC has any other
origins.

The working query catalogue contains a source-authored structured
`nl_question` for 82 of those pairs: 52 Europeana, six NFDI4Culture, and 24
Camera dei Deputati records. All 82 also have only a `curated_query` evidence
item. `extract_queries.py` gained support for storing curated prompts in August
2026, but the older `build_llm_inputs.py` boundary had not been extended with
it. That builder excluded `curated_query` as SPARQL-block evidence by default
and did not pass the catalogue's structured `nl_question` to generation. All 82
therefore reached the model with an empty evidence list, and all 82 were
classified as `generated`. This was not an ID-alignment or workshop-package
join failure: every workshop query ID occurred in the expected ledger, input,
output, and package records.

`nfdi4culture-0006` is the clearest symptom. Its authored prompt also appears
verbatim as a leading comment inside the SPARQL. Although the structured prompt
and evidence were absent from the model input, the model copied the comment
word for word and described the result as `generated` with no evidence IDs.
It is the only exact whole-question copy of a SPARQL comment found in this
workshop run, but the other 81 curated questions were still needlessly replaced
by newly generated wording.

The remaining 30 pairs reached the model with one `web_query_desc` item. Of
these, 24 were classified as `generated`, five as `paraphrased`, and one as
`verbatim`. Seven `generated` outputs nevertheless cited retained evidence. The
Quagga candidate builder emitted an empty `sources` array for every `generated`
output, so those seven citations and their retained phrases were also lost
before packaging. One CDEC output additionally had the inconsistent combination
`mode=generated` with a non-null `primary_evidence_id`; the schema and citation
validator allowed it at the time of the audit.

The implementation now retains a structured question as
`curated_nl_question` evidence only when it has the explicit source-authored
shape (`generator: null`, `generated_at: null`) and its source resolves in the
record. Question-like SPARQL comments are extracted without duplicating a
question already contained in existing `query_comment` evidence and inherit
metadata from the selected SPARQL version rather than an unrelated source.
Origin validation rejects inconsistent primary evidence, generated wording
cannot exactly copy retained authored-question evidence, generated outputs keep
their cited evidence in Quagga candidates, and schema validation fails closed
when the `jsonschema` dependency is unavailable.

Historical generation outputs and workshop packages are immutable records of
the earlier behavior. Where corrected candidate or package artifacts are still
needed, rerun generation and rebuild them with new immutable digests; do not
rewrite the historical artifacts in place.

The corrected IPL artifacts were rebuilt locally on 12 September 2026 from
generation run `2026-09-12-014200-minimax-m2-5`. The historical August run and
package directory remain untouched. The corrected frozen package set is
`30a5695ba92519fe`; its manifest is under the ignored operational path
`var/review/bundles/ipl-2026-20260912-provenance-fix/`. That package set is
registered and deployed in production.

## Dependency maintenance

The test suite currently emits deprecation warnings from `rdflib` using legacy
`pyparsing` APIs. They do not affect correctness today, but dependency upgrades
should remove or re-evaluate the warnings before they become runtime failures.

## Production readiness

The application and corrected four-package IPL release are deployed on the
ICF-owned VPS at `musparql.industrycommons.net`. The unprivileged web and worker
services run behind Caddy, and repeated external synthetic rehearsals have
covered admission, required names, batch-first team formation, outstanding
forms, durable team-attributed submission, and return to in-progress work.

The database was returned to a clean synthetic rehearsal baseline on
13 September 2026 after taking a recoverable server snapshot. Remaining work
before real-reviewer invitations is governed by
[`ICF_HOSTING_BOUNDARY.md`](ICF_HOSTING_BOUNDARY.md):

- obtain ICF confirmation of the final participant notice and acknowledgement
  wording;
- complete and verify production SMTP delivery and its failure/retry behaviour;
- validate an isolated coherent database-plus-files restore, deletion replay,
  and owner-visible backup/service alerts;
- record the accepted recovery-point objective and complete the required
  restart/reboot check; and
- pass or explicitly re-scope the outstanding human mobile-browser observation.

The linguistic-dimensions instruction-page redesign is a pre-pilot usability
item rather than an infrastructure dependency. Human mobile-browser validation
was deliberately deferred; the automated narrow-viewport contract passes, but
the Phase 9 operational gate is not fully recorded as passed unless its current
human-observation requirement is either completed or explicitly re-scoped.
