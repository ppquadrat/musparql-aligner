# IPL workshop package preparation

This runbook covers Track A item 6: preparing, validating, and registering the
seven reviewer-neutral IPL knowledge-graph packages. It does not authorise access
to real holdout annotations or deployment to the ICF server.

The fixed package order and membership policy are:

1. Europeana Knowledge Graph (`europeana`, core): deduplicated candidates only;
2. NFDI4Culture Culture Knowledge Graph (`nfdi4culture`, core): current package unchanged;
3. Camera dei Deputati Knowledge Graph (`camera-dei-deputati`, specialist): deduplicated candidates only;
4. CDEC Knowledge Graph (`cdec`, specialist): current package unchanged;
5. Musical Meetups (`meetups`, specialist): public v10 pairs for reinspection;
6. Music On the Web (`musow`, specialist): new, non-holdout pairs only; and
7. Organs Knowledge Graph (`organs`, specialist): public v10 pairs for reinspection.

Each package retains the two-pass contract. Deduplicated/new candidates are
presented first, followed by any existing/all-pairs records. Europeana and
Camera dei Deputati are shuffled independently and reproducibly for each
assignment to increase coverage when groups complete only part of a package.
CKG, CDEC, Musical Meetups, MusOW, and Organs use ascending pair identity
(`query_id`) within each pass. Item position is retained for later analysis.

## Safety boundary

Holdout pairs are excluded from the IPL package set completely. Build the source
with `scripts.build_review_bundle` in reviewer-neutral mode, using either the
owner-approved annotation-free selector or an identity-private process that has
already removed every holdout. The package command rejects the `no_holdout`
assertion: it accepts only reviewer-neutral, initial-review data whose policy
records actual holdout filtering and whose contents contain no holdout markers.

The final selection is an annotation-free JSON array or JSONL file covering all
seven package members. The source bundle marks the deduplicated/new subset with
`workshop_pass: deduplicated`; remaining records use `workshop_pass: all_pairs`.
Every row contains exactly these immutable identity fields:

```json
{
  "kg_id": "europeana",
  "query_id": "europeana__sha256:...",
  "sparql_version": 0,
  "sparql_hash": "sha256:..."
}
```

Do not put reviewer fields, decisions, comments, or private holdout annotations
in this selection. The builder rejects unknown fields, duplicate identities,
missing records, and stale SPARQL version/hash pins.

## Build a provisional rehearsal set

A provisional build uses every eligible record in the source bundle. It creates
all seven packages but marks them `provisional` and disabled. It cannot be
registered in the database, so its digests cannot be mistaken for the workshop
freeze.

```bash
.venv/bin/python -m scripts.prepare_ipl_workshop_packages build \
  --source-bundle var/review/bundles/ipl/source.json \
  --seed-snapshots catalog/kg_seed_snapshots.yaml \
  --bundle-root var/review/bundles \
  --out-dir var/review/bundles/ipl/provisional \
  --round-id ipl-2026 \
  --provisional
```

## Build the final frozen set

First convert the received Quagga reports into the reviewer-neutral source
bundle and its complete pinned selection:

```bash
.venv/bin/python scripts/prepare_ipl_workshop_packages.py prepare-source \
  --all-candidates var/workshop/quagga-filter-candidates.json \
  --deduplicated-candidates var/workshop/quagga-filter-candidates-deduplicated.json \
  --source-bundle-out var/workshop/ipl-quagga-source-bundle.json \
  --selection-out var/workshop/ipl-quagga-selection.json
```

Then build the final set:

```bash
.venv/bin/python -m scripts.prepare_ipl_workshop_packages build \
  --source-bundle var/workshop/ipl-quagga-source-bundle.json \
  --seed-snapshots catalog/kg_seed_snapshots.yaml \
  --bundle-root var/review/bundles \
  --out-dir var/review/bundles/ipl-2026 \
  --round-id ipl-2026 \
  --selection var/workshop/ipl-quagga-selection.json
```

The command atomically replaces seven canonical JSON bundles and `manifest.json`,
then immediately validates them. Identical inputs produce identical package-set
and bundle digests. The manifest embeds the canonical annotation-free selection
pins and pins the source bundle, canonical selection digest, seed version/digest,
record count, package path, and package digest. Final packages are enabled;
provisional packages never are.

Revalidate files after copying them to their operational location:

```bash
.venv/bin/python -m scripts.prepare_ipl_workshop_packages validate \
  --manifest var/review/bundles/ipl-2026/manifest.json \
  --bundle-root var/review/bundles
```

Validation requires exactly the seven fixed packages in the fixed order, at
least one item per package, one KG per bundle, canonical record and path order,
unique query identities, exact agreement with the embedded SPARQL pins,
reviewer-neutral content, approved holdout handling, and independently derived
selection, package-set, file, and provenance digests.

## Register the final set

Create the 30-person workshop round in `draft` state before registration. Then
register the validated set against a migrated database:

```bash
.venv/bin/python -m scripts.prepare_ipl_workshop_packages register \
  --manifest var/review/bundles/ipl-2026/manifest.json \
  --bundle-root var/review/bundles \
  --database var/database/musparql.sqlite3 \
  --seed-snapshots catalog/kg_seed_snapshots.yaml
```

Registration imports missing immutable KG seed snapshots and package metadata in
one transaction, is idempotent, and can insert or replace package metadata only
while the round remains a draft.
It refuses provisional sets, unexpected extra packages, missing seed snapshots,
and replacement after a package has been claimed. Open the round only after
recording and independently checking the seven manifest digests.

## Verification

```bash
.venv/bin/python -m pytest -q tests/test_ipl_workshop_packages.py
.venv/bin/python -m pytest -q
.venv/bin/pip check
```

Before the workshop, follow the release plan's run sheet: revalidate the copied
manifest, compare its seven digests with the recorded freeze, and run one
synthetic claim and submission. Package files and the operational database stay
under ICF-backed-up paths in production.
