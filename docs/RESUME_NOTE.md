# Resume note

Short, dated handover notes for picking the project back up after a break.
Newest first. This file records state and next moves only; the durable
technical and governance backlog stays in
[`OPEN_ISSUES.md`](OPEN_ISSUES.md).

## 7 November 2026

### Where the code stands

No code has changed since 16 September 2026. The last code commit is
`69916fe` ("Add IPL workshop slides"). Everything between then and now was
paper work, and it is all committed as of today.

- `main` is in sync with `origin/main`.
- All `codex/*` branches are fully merged into `main`; none holds unmerged
  work. They can be deleted whenever you want the branch list tidy.
- Working tree is clean.
- **Nothing was deployed today, and nothing needs deploying.** Production
  still matches the code it was running in September.

### Where production stands

The application and the corrected four-package IPL release remain deployed on
the ICF-owned VPS at `musparql.industrycommons.net`, behind Caddy, with the
database on the clean synthetic rehearsal baseline taken on 13 September 2026.
No real reviewer data has been collected.

### Paper work committed today

- Removed six superseded MTSR 2026 CCIS drafts plus the anonymised and
  reference-checked copies. The tracked September submission
  (`mtsr-2026-Proutskova-musparql-submitted.docx` / `.pdf`) is unchanged.
- Added the revised MTSR submission `1776-Proutskova-MTSR2026.docx` / `.pdf`,
  prepared after the October review round.
- Added the final ICCCM 2026 A0 poster.

### Kept deliberately out of Git

This repository is **public** (`ppquadrat/musparql-aligner`). Three files are
on disk but gitignored, by decision on 7 November 2026:

- `docs/papers/MTSR-2026/MTSR paper reviews.docx` — venue-confidential
  reviewer comments;
- `docs/papers/MTSR-2026/ResponseToReviewers.docx` — the response letter;
- `docs/papers/musparql-venues-2027.xlsx` — 2027 venue planning.

These exist only in your local checkout and in whatever personal backup you
keep. They are **not** covered by the repository or by the ICF server backup.
If they matter, make sure they are backed up somewhere.

### Open paper threads to check first

- MTSR 2026: revision 1776 was submitted on or around 23 October 2026. Check
  for a camera-ready request, a copyright/consent form, and the registration
  deadline — CCIS volumes usually chase all three.
- ICCCM 2026: poster is final. Nothing outstanding that the repository shows.
- 2027 venues: the planning spreadsheet is the live document; deadlines in it
  are the thing most likely to have moved while you were away.

### Where to pick the code back up

Nothing is half-finished or broken. The blockers are the same gates recorded
in [`OPEN_ISSUES.md`](OPEN_ISSUES.md) and
[`ICF_HOSTING_BOUNDARY.md`](ICF_HOSTING_BOUNDARY.md), unchanged since
September. In rough order of what stops real reviewers being invited:

1. **Admission throttling redesign** — the 16 September incident. Shared-NAT
   participants exhaust a ten-attempt-per-address window. Unfixed. Do not run
   another cohort until the redesign and the 30-browser-behind-one-address
   tests are done. This is the single largest open item.
2. **Participant notice deployment** — deploy and verify the ICF-approved
   `musparql-workshop-2026-09-16-v1` notice and consent wording in production.
3. **SMTP** — complete and verify production delivery, failure, and retry
   behaviour. Note that email login carries the same shared-address throttle
   problem as item 1.
4. **Backup restore proof** — validate a coherent database-plus-files restore,
   deletion replay, and owner-visible alerts; record the accepted RPO.
5. **ICF data-management-plan registration check** — needs an explicit
   readiness record.
6. **Linguistic workbench instruction page** — pre-pilot usability rework;
   not an infrastructure gate.

Smaller items: the SPARQL correction backlog in
[`SPARQL_CORRECTION_FOLLOW_UP.md`](SPARQL_CORRECTION_FOLLOW_UP.md), and
`rdflib`/`pyparsing` deprecation warnings in the test suite.
