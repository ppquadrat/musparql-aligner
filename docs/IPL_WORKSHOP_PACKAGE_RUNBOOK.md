# IPL workshop package preparation

This runbook covers Track A item 6: preparing, validating, and registering the
five reviewer-neutral IPL knowledge-graph packages. It does not authorise access
to real holdout annotations or deployment to the ICF server.

The fixed package order is:

1. Archaic Lyric Poetry Ontology (`alyra`)
2. Camera dei Deputati Knowledge Graph (`camera-dei-deputati`)
3. Europeana Knowledge Graph (`europeana`)
4. NFDI4Culture Culture Knowledge Graph (`nfdi4culture`)
5. CDEC Knowledge Graph (`cdec`)

## Safety boundary

Holdout pairs are excluded from the IPL package set completely. Build the source
with `scripts.build_review_bundle` in reviewer-neutral mode, using either the
owner-approved annotation-free selector or an identity-private process that has
already removed every holdout. The package command rejects the `no_holdout`
assertion: it accepts only reviewer-neutral, initial-review data whose policy
records actual holdout filtering and whose contents contain no holdout markers.

The final deduplicated selection is an annotation-free JSON array or JSONL file.
Every row contains exactly these immutable identity fields:

```json
{
  "kg_id": "alyra",
  "query_id": "alyra__sha256:...",
  "sparql_version": 0,
  "sparql_hash": "sha256:..."
}
```

Do not put reviewer fields, decisions, comments, or private holdout annotations
in this selection. The builder rejects unknown fields, duplicate identities,
missing records, and stale SPARQL version/hash pins.

## Build a provisional rehearsal set

A provisional build uses every eligible record in the source bundle. It creates
all five packages but marks them `provisional` and disabled. It cannot be
registered in the database, so its digests cannot be mistaken for the workshop
freeze.

```bash
.venv/bin/python -m scripts.prepare_ipl_workshop_packages build \
  --source-bundle var/review/bundles/ipl/source.json \
  --seed-snapshots catalog/kg_seed_snapshots.yaml \
  --bundle-root var/review/bundles \
  --out-dir var/review/bundles/ipl/provisional \
  --round-id ipl-2026-09-16 \
  --provisional
```

## Build the final frozen set

After the owner has approved the deduplicated selection, build the final set:

```bash
.venv/bin/python -m scripts.prepare_ipl_workshop_packages build \
  --source-bundle var/review/bundles/ipl/source.json \
  --seed-snapshots catalog/kg_seed_snapshots.yaml \
  --bundle-root var/review/bundles \
  --out-dir var/review/bundles/ipl/final \
  --round-id ipl-2026-09-16 \
  --selection var/review/ipl-deduplicated-selection.json
```

The command writes five canonical JSON bundles and `manifest.json`, then
immediately validates them. Identical inputs produce identical package-set and
bundle digests. The manifest pins the source bundle, selection file, seed
version/digest, record count, package path, and package digest. Final packages
are enabled; provisional packages never are.

Revalidate files after copying them to their operational location:

```bash
.venv/bin/python -m scripts.prepare_ipl_workshop_packages validate \
  --manifest var/review/bundles/ipl/final/manifest.json \
  --bundle-root var/review/bundles
```

Validation requires exactly the five fixed packages in the fixed order, at
least one item per package, one KG per bundle, unique query identities, current
SPARQL pins, reviewer-neutral content, approved holdout handling, and matching
file/provenance digests.

## Register the final set

Create the 30-person workshop round in `draft` state before registration. Then
register the validated set against a migrated database:

```bash
.venv/bin/python -m scripts.prepare_ipl_workshop_packages register \
  --manifest var/review/bundles/ipl/final/manifest.json \
  --bundle-root var/review/bundles \
  --database var/database/musparql.sqlite3 \
  --seed-snapshots catalog/kg_seed_snapshots.yaml
```

Registration imports missing immutable KG seed snapshots, is idempotent, and
can insert or replace package metadata only while the round remains a draft.
It refuses provisional sets, unexpected extra packages, missing seed snapshots,
and replacement after a package has been claimed. Open the round only after
recording and independently checking the five manifest digests.

## Verification

```bash
.venv/bin/python -m pytest -q tests/test_ipl_workshop_packages.py
.venv/bin/python -m pytest -q
.venv/bin/pip check
```

Before the workshop, follow the release plan's run sheet: revalidate the copied
manifest, compare its five digests with the recorded freeze, and run one
synthetic claim and submission. Package files and the operational database stay
under ICF-backed-up paths in production.
