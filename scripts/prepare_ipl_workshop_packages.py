#!/usr/bin/env python3
"""Build, validate, and register the five reviewer-neutral IPL packages."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import yaml

from musparql.database.engine import create_database_engine, session_factory
from musparql.database.services import SeedSnapshotService
from musparql.workshop_packages import (
    build_package_set,
    register_package_set,
    validate_package_set,
)


def _mapping(path: Path, *, yaml_input: bool = False) -> dict[str, Any]:
    payload = (
        yaml.safe_load(path.read_text(encoding="utf-8-sig"))
        if yaml_input
        else json.loads(path.read_text(encoding="utf-8-sig"))
    )
    if not isinstance(payload, dict):
        raise ValueError(f"Expected an object in {path}")
    return payload


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    commands = result.add_subparsers(dest="command", required=True)

    build = commands.add_parser("build", help="Create and immediately validate all five packages.")
    build.add_argument("--source-bundle", type=Path, required=True)
    build.add_argument("--seed-snapshots", type=Path, default=Path("catalog/kg_seed_snapshots.yaml"))
    build.add_argument("--bundle-root", type=Path, required=True)
    build.add_argument("--out-dir", type=Path, required=True)
    build.add_argument("--round-id", required=True)
    choice = build.add_mutually_exclusive_group(required=True)
    choice.add_argument(
        "--selection",
        type=Path,
        help="Final annotation-free JSON/JSONL selection with immutable SPARQL pins.",
    )
    choice.add_argument(
        "--provisional",
        action="store_true",
        help="Use every eligible source-bundle record and keep every package disabled.",
    )

    for name in ("validate", "register"):
        command = commands.add_parser(name)
        command.add_argument("--manifest", type=Path, required=True)
        command.add_argument("--bundle-root", type=Path, required=True)
        if name == "register":
            command.add_argument("--database", type=Path, required=True)
            command.add_argument(
                "--seed-snapshots",
                type=Path,
                default=Path("catalog/kg_seed_snapshots.yaml"),
            )
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    if args.command == "build":
        manifest = build_package_set(
            source_bundle=args.source_bundle,
            seed_archive=_mapping(args.seed_snapshots, yaml_input=True),
            bundle_root=args.bundle_root,
            output_dir=args.out_dir,
            round_id=args.round_id,
            selection_path=args.selection,
        )
        manifest_path = args.out_dir.resolve() / "manifest.json"
        print(
            f"Built and validated {len(manifest['packages'])} {manifest['status']} "
            f"IPL packages at {manifest_path}."
        )
        return 0

    manifest = _mapping(args.manifest)
    validate_package_set(manifest, bundle_root=args.bundle_root)
    if args.command == "validate":
        print(
            f"Validated {len(manifest['packages'])} {manifest['status']} IPL packages "
            f"for {manifest['workshop_round_id']}."
        )
        return 0

    engine = create_database_engine(args.database)
    sessions = session_factory(engine)
    try:
        imported = SeedSnapshotService(sessions).import_archive(
            _mapping(args.seed_snapshots, yaml_input=True)
        )
        inserted, updated = register_package_set(
            manifest, sessions=sessions, bundle_root=args.bundle_root
        )
    finally:
        engine.dispose()
    print(
        f"Registered IPL package set: {inserted} inserted, {updated} updated; "
        f"{imported} seed snapshots imported."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
