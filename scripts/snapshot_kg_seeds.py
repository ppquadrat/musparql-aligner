#!/usr/bin/env python3
"""Append current KG seed versions to the immutable snapshot archive."""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any, Dict, List

import yaml

from musparql.source_catalog import (
    KG_SEED_SNAPSHOTS_SCHEMA,
    kg_seed_digest,
    validate_current_kg_seed_snapshots,
    validate_kg_seed_snapshots,
    validate_kg_seeds,
)


def update_snapshot_archive(
    seeds_payload: Any, snapshots_payload: Any | None
) -> tuple[Dict[str, Any], int]:
    seeds = validate_kg_seeds(seeds_payload)
    if snapshots_payload is None:
        archive: Dict[str, Any] = {
            "schema": KG_SEED_SNAPSHOTS_SCHEMA,
            "snapshots": [],
        }
        snapshots: List[Dict[str, Any]] = []
    else:
        snapshots = validate_kg_seed_snapshots(snapshots_payload)
        archive = dict(snapshots_payload)
        archive["snapshots"] = list(snapshots)

    by_key = {
        (str(snapshot["kg_id"]), str(snapshot["seed_version"])): snapshot
        for snapshot in snapshots
    }
    referenced = {
        str(snapshot["previous_seed_digest"])
        for snapshot in snapshots
        if snapshot.get("previous_seed_digest") is not None
    }
    heads = {
        str(snapshot["kg_id"]): str(snapshot["seed_digest"])
        for snapshot in snapshots
        if snapshot["seed_digest"] not in referenced
    }

    added = 0
    for seed in seeds:
        kg_id = str(seed["kg_id"])
        seed_version = str(seed["seed_version"])
        digest = kg_seed_digest(seed)
        existing = by_key.get((kg_id, seed_version))
        if existing is not None:
            if existing.get("seed_digest") != digest:
                raise ValueError(
                    f"KG seed version was reused with changed content: {kg_id}/{seed_version}"
                )
            continue
        snapshot = {
            "kg_id": kg_id,
            "seed_version": seed_version,
            "seed_digest": digest,
            "previous_seed_digest": heads.get(kg_id),
            "seed": seed,
        }
        archive["snapshots"].append(snapshot)
        by_key[(kg_id, seed_version)] = snapshot
        heads[kg_id] = digest
        added += 1

    validated = validate_kg_seed_snapshots(archive)
    validate_current_kg_seed_snapshots(seeds, validated)
    return archive, added


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seeds", default="catalog/seeds.yaml")
    parser.add_argument("--snapshots", default="catalog/kg_seed_snapshots.yaml")
    args = parser.parse_args()

    seeds_path = Path(args.seeds)
    snapshots_path = Path(args.snapshots)
    seeds_payload = yaml.safe_load(seeds_path.read_text(encoding="utf-8-sig"))
    snapshots_payload = (
        yaml.safe_load(snapshots_path.read_text(encoding="utf-8-sig"))
        if snapshots_path.exists()
        else None
    )
    archive, added = update_snapshot_archive(seeds_payload, snapshots_payload)
    snapshots_path.parent.mkdir(parents=True, exist_ok=True)
    snapshots_path.write_text(
        yaml.safe_dump(archive, sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )
    print(f"{snapshots_path}: appended {added} immutable KG seed snapshots")


if __name__ == "__main__":
    main()
