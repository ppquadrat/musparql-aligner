from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml
from sqlalchemy import select

from musparql.database.engine import create_database_engine, session_factory
from musparql.database.migrations import upgrade_database
from musparql.database.models import WorkshopRound, WorkshopWorkPackage
from musparql.database.services import SeedSnapshotService
from musparql.workshop_packages import (
    IPL_PACKAGES,
    build_package_set,
    register_package_set,
    validate_package_set,
)


ROOT = Path(__file__).resolve().parents[1]
SEEDS = ROOT / "catalog" / "kg_seed_snapshots.yaml"


def _source_bundle(path: Path, *, holdout_policy: str = "identity_private_filtered_upstream") -> Path:
    records = []
    for index, package in enumerate(IPL_PACKAGES, start=1):
        digest = f"sha256:{index:064x}"
        records.append(
            {
                "review_id": f"{package.kg_id}::query-{index}::synthetic",
                "kg_id": package.kg_id,
                "query_id": f"{package.kg_id}__synthetic-{index}",
                "query_label": f"{package.kg_id}-{index:04d}",
                "input": {
                    "sparql_clean": "SELECT * WHERE { ?s ?p ?o } LIMIT 1",
                    "sparql_version": 0,
                    "sparql_hash": digest,
                    "evidence": [],
                },
                "output": {"nl_question": "What is the synthetic result?"},
            }
        )
    payload = {
        "schema": "musparql.review-bundle.v2",
        "dataset_id": "synthetic-five-kg-source",
        "mode": "initial",
        "holdout_input_policy": holdout_policy,
        "record_count": len(records),
        "records": records,
    }
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _seed_archive() -> dict:
    return yaml.safe_load(SEEDS.read_text(encoding="utf-8-sig"))


def _selection(path: Path, *, stale: bool = False) -> Path:
    records = []
    for index, package in enumerate(IPL_PACKAGES, start=1):
        records.append(
            {
                "kg_id": package.kg_id,
                "query_id": f"{package.kg_id}__synthetic-{index}",
                "sparql_version": 0,
                "sparql_hash": f"sha256:{(99 if stale and index == 1 else index):064x}",
            }
        )
    path.write_text(json.dumps(records), encoding="utf-8")
    return path


def test_provisional_package_set_is_complete_deterministic_and_disabled(tmp_path: Path) -> None:
    root = tmp_path / "bundles"
    kwargs = {
        "source_bundle": _source_bundle(tmp_path / "source.json"),
        "seed_archive": _seed_archive(),
        "bundle_root": root,
        "round_id": "ipl-2026",
        "selection_path": None,
    }
    first = build_package_set(output_dir=root / "first", **kwargs)
    second = build_package_set(output_dir=root / "second", **kwargs)

    assert first["status"] == "provisional"
    assert first["package_set_id"] == second["package_set_id"]
    assert [row["kg_id"] for row in first["packages"]] == [
        package.kg_id for package in IPL_PACKAGES
    ]
    assert all(row["record_count"] == 1 and row["enabled"] is False for row in first["packages"])
    assert [row["bundle_digest"] for row in first["packages"]] == [
        row["bundle_digest"] for row in second["packages"]
    ]
    validate_package_set(first, bundle_root=root)


def test_final_selection_is_pinned_and_packages_are_enabled(tmp_path: Path) -> None:
    root = tmp_path / "bundles"
    manifest = build_package_set(
        source_bundle=_source_bundle(tmp_path / "source.json"),
        seed_archive=_seed_archive(),
        bundle_root=root,
        output_dir=root / "final",
        round_id="ipl-2026",
        selection_path=_selection(tmp_path / "selection.json"),
    )

    assert manifest["status"] == "frozen"
    assert manifest["selection_digest"].startswith("sha256:")
    assert all(row["enabled"] is True for row in manifest["packages"])

    first_path = root / manifest["packages"][0]["bundle_path"]
    first = json.loads(first_path.read_text(encoding="utf-8"))
    assert first["workshop_package"]["status"] == "frozen"
    assert first["records"][0]["kg_id"] == "alyra"


def test_final_selection_rejects_stale_sparql_pin(tmp_path: Path) -> None:
    root = tmp_path / "bundles"
    with pytest.raises(ValueError, match="Stale package selection SPARQL pin"):
        build_package_set(
            source_bundle=_source_bundle(tmp_path / "source.json"),
            seed_archive=_seed_archive(),
            bundle_root=root,
            output_dir=root / "final",
            round_id="ipl-2026",
            selection_path=_selection(tmp_path / "selection.json", stale=True),
        )


def test_package_set_requires_explicit_holdout_filtering(tmp_path: Path) -> None:
    root = tmp_path / "bundles"
    with pytest.raises(ValueError, match="holdouts to be explicitly filtered"):
        build_package_set(
            source_bundle=_source_bundle(
                tmp_path / "source.json", holdout_policy="no_holdout"
            ),
            seed_archive=_seed_archive(),
            bundle_root=root,
            output_dir=root / "provisional",
            round_id="ipl-2026",
            selection_path=None,
        )


def test_validation_detects_bundle_tampering(tmp_path: Path) -> None:
    root = tmp_path / "bundles"
    manifest = build_package_set(
        source_bundle=_source_bundle(tmp_path / "source.json"),
        seed_archive=_seed_archive(),
        bundle_root=root,
        output_dir=root / "final",
        round_id="ipl-2026",
        selection_path=_selection(tmp_path / "selection.json"),
    )
    path = root / manifest["packages"][0]["bundle_path"]
    path.write_text("{}\n", encoding="utf-8")

    with pytest.raises(ValueError, match="unsupported assignment contract"):
        validate_package_set(manifest, bundle_root=root)


def test_validation_rejects_manifest_bundle_path_escape(tmp_path: Path) -> None:
    root = tmp_path / "bundles"
    manifest = build_package_set(
        source_bundle=_source_bundle(tmp_path / "source.json"),
        seed_archive=_seed_archive(),
        bundle_root=root,
        output_dir=root / "final",
        round_id="ipl-2026",
        selection_path=_selection(tmp_path / "selection.json"),
    )
    manifest["packages"][0]["bundle_path"] = "../source.json"

    with pytest.raises(ValueError, match="escapes the configured root"):
        validate_package_set(manifest, bundle_root=root)


def test_frozen_package_set_registers_idempotently_on_draft_round(tmp_path: Path) -> None:
    root = tmp_path / "bundles"
    archive = _seed_archive()
    manifest = build_package_set(
        source_bundle=_source_bundle(tmp_path / "source.json"),
        seed_archive=archive,
        bundle_root=root,
        output_dir=root / "final",
        round_id="ipl-2026",
        selection_path=_selection(tmp_path / "selection.json"),
    )
    database = tmp_path / "musparql.sqlite3"
    upgrade_database(database)
    engine = create_database_engine(database)
    sessions = session_factory(engine)
    try:
        SeedSnapshotService(sessions).import_archive(archive)
        with sessions.begin() as session:
            session.add(
                WorkshopRound(
                    id="ipl-2026",
                    name="Synthetic IPL workshop",
                    status="draft",
                    opens_at="2026-09-16T08:00:00Z",
                    closes_at="2026-09-16T18:00:00Z",
                    max_participants=30,
                    allow_additional_assignments=False,
                    created_at="2026-09-11T12:00:00Z",
                )
            )

        assert register_package_set(manifest, sessions=sessions, bundle_root=root) == (5, 0)
        assert register_package_set(manifest, sessions=sessions, bundle_root=root) == (0, 0)
        with sessions() as session:
            rows = list(
                session.scalars(
                    select(WorkshopWorkPackage).order_by(WorkshopWorkPackage.display_order)
                )
            )
            assert [row.kg_id for row in rows] == [package.kg_id for package in IPL_PACKAGES]
            assert all(row.enabled for row in rows)
    finally:
        engine.dispose()


def test_provisional_package_set_cannot_be_registered(tmp_path: Path) -> None:
    root = tmp_path / "bundles"
    manifest = build_package_set(
        source_bundle=_source_bundle(tmp_path / "source.json"),
        seed_archive=_seed_archive(),
        bundle_root=root,
        output_dir=root / "provisional",
        round_id="ipl-2026",
        selection_path=None,
    )
    database = tmp_path / "musparql.sqlite3"
    upgrade_database(database)
    engine = create_database_engine(database)
    sessions = session_factory(engine)
    try:
        with pytest.raises(ValueError, match="Provisional IPL packages cannot be registered"):
            register_package_set(manifest, sessions=sessions, bundle_root=root)
    finally:
        engine.dispose()
