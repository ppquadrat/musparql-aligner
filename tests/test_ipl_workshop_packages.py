from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path

import pytest
import yaml
from sqlalchemy import select

from musparql.database.engine import create_database_engine, session_factory
from musparql.database.migrations import upgrade_database
from musparql.database.models import KgSeedSnapshot, WorkshopRound, WorkshopWorkPackage
from musparql.database.services import SeedSnapshotService
from musparql.web.ipl_workshop_pilot import parser as pilot_parser
from musparql.workshop_packages import (
    IPL_PACKAGES,
    build_package_set,
    canonical_json,
    digest_bytes,
    prepare_expanded_workshop_source,
    prepare_quagga_workshop_source,
    register_package_set,
    validate_package_set,
)


ROOT = Path(__file__).resolve().parents[1]
SEEDS = ROOT / "catalog" / "kg_seed_snapshots.yaml"


def test_local_pilot_defaults_to_provenance_corrected_package_set() -> None:
    args = pilot_parser().parse_args([])

    assert args.packages == Path(
        "var/review/bundles/ipl-2026-20260912-provenance-fix"
    )


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
        "dataset_id": "synthetic-four-kg-source",
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


def _candidate_report(*, deduplicated: bool = False) -> dict:
    graphs = []
    packages = (*IPL_PACKAGES, type("Dropped", (), {"kg_id": "alyra"})())
    for index, package in enumerate(packages, start=1):
        records = []
        for record_index in range(1, 2 if deduplicated else 3):
            query_id = f"{package.kg_id}__sha256:{index:062x}{record_index:02x}"
            records.append(
                {
                    "kg_id": package.kg_id,
                    "query_id": query_id,
                    "query_label": f"{package.kg_id}-{record_index:04d}",
                    "sparql": "SELECT * WHERE { ?s ?p ?o }",
                    "sparql_hash": f"sha256:{index:062x}{record_index:02x}",
                    "nl": {
                        "text": f"Synthetic question {index}-{record_index}?",
                        "origin": "generated",
                        "model": "synthetic-model",
                        "sources": [],
                    },
                }
            )
        graphs.append({"kg_id": package.kg_id, "records": records})
    return {
        "schema": "musparql.quagga-filter-candidates.v1",
        "source_run_id": "synthetic-run",
        "graphs": graphs,
    }


def test_quagga_reports_prepare_all_package_kgs_two_pass_source() -> None:
    bundle, selection = prepare_quagga_workshop_source(
        all_candidates=_candidate_report(),
        deduplicated_candidates=_candidate_report(deduplicated=True),
    )

    assert bundle["record_count"] == 2 * len(IPL_PACKAGES)
    assert len(selection) == 2 * len(IPL_PACKAGES)
    assert {record["kg_id"] for record in bundle["records"]} == {
        package.kg_id for package in IPL_PACKAGES
    }
    assert sum(record["workshop_pass"] == "deduplicated" for record in bundle["records"]) == len(IPL_PACKAGES)
    assert sum(record["workshop_pass"] == "all_pairs" for record in bundle["records"]) == len(IPL_PACKAGES)
    assert bundle["workshop_pass_order"] == ["deduplicated", "all_pairs"]


def test_expanded_source_trims_large_packages_and_adds_polifonia_packages(
    tmp_path: Path,
) -> None:
    quagga, _selection = prepare_quagga_workshop_source(
        all_candidates=_candidate_report(),
        deduplicated_candidates=_candidate_report(deduplicated=True),
    )
    quagga["records"] = [
        record for record in quagga["records"] if record["kg_id"] in {
            "europeana", "nfdi4culture", "camera-dei-deputati", "cdec"
        }
    ]
    quagga["record_count"] = len(quagga["records"])
    musow = _source_bundle(tmp_path / "expanded-source.json")
    musow_payload = json.loads(musow.read_text(encoding="utf-8"))
    musow_payload["records"] = [
        {
            **next(record for record in musow_payload["records"] if record["kg_id"] == "musow"),
            "review_scope": "new",
        }
    ]
    benchmark = []
    for kg_id in ("meetups", "organs"):
        benchmark.append(
            {
                "kg_id": kg_id,
                "query_id": f"{kg_id}__synthetic-public",
                "query_label": f"{kg_id}-0001",
                "benchmark_id": f"{kg_id}::synthetic-public",
                "gold_question": "What is the synthetic public question?",
                "gold_question_source": "reviewer_rewrite",
                "sparql": "SELECT * WHERE { ?s ?p ?o }",
                "sparql_version": 0,
                "sparql_hash": "sha256:" + ("a" if kg_id == "meetups" else "b") * 64,
            }
        )

    bundle, selection = prepare_expanded_workshop_source(
        quagga_source=quagga,
        musow_source=musow_payload,
        public_benchmark_records=benchmark,
    )

    selected = {(item["kg_id"], item["query_id"]) for item in selection}
    for kg_id in ("europeana", "camera-dei-deputati"):
        assert all(
            record["workshop_pass"] == "deduplicated"
            for record in bundle["records"]
            if (record["kg_id"], record["query_id"]) in selected and record["kg_id"] == kg_id
        )
    assert {record["kg_id"] for record in bundle["records"]} == {
        package.kg_id for package in IPL_PACKAGES
    }
    assert {
        record["kg_id"]
        for record in bundle["records"]
        if record.get("review_scope") == "previously_reviewed"
    } == {"meetups", "organs"}
    assert bundle["review_scope_policy"]["default_scope"] == "all"


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
    assert manifest["selection"]
    assert manifest["selection_digest"].startswith("sha256:")
    assert all(row["enabled"] is True for row in manifest["packages"])

    first_path = root / manifest["packages"][0]["bundle_path"]
    first = json.loads(first_path.read_text(encoding="utf-8"))
    assert first["workshop_package"]["status"] == "frozen"
    assert first["records"][0]["kg_id"] == "europeana"


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


def test_validation_rejects_noncanonical_manifest_bundle_path(tmp_path: Path) -> None:
    root = tmp_path / "bundles"
    manifest = build_package_set(
        source_bundle=_source_bundle(tmp_path / "source.json"),
        seed_archive=_seed_archive(),
        bundle_root=root,
        output_dir=root / "final",
        round_id="ipl-2026",
        selection_path=_selection(tmp_path / "selection.json"),
    )
    manifest["packages"][0]["bundle_path"] = "final/../final/01-europeana.json"

    with pytest.raises(ValueError, match="bundle path is not canonical"):
        validate_package_set(manifest, bundle_root=root)


def test_validation_derives_frozen_selection_digest_and_package_set_id(
    tmp_path: Path,
) -> None:
    root = tmp_path / "bundles"
    provisional = build_package_set(
        source_bundle=_source_bundle(tmp_path / "source.json"),
        seed_archive=_seed_archive(),
        bundle_root=root,
        output_dir=root / "provisional",
        round_id="ipl-2026",
        selection_path=None,
    )
    forged = deepcopy(provisional)
    forged["status"] = "frozen"
    forged["selection_digest"] = f"sha256:{99:064x}"
    forged["package_set_id"] = "a" * 16

    with pytest.raises(ValueError, match="selection provenance"):
        validate_package_set(forged, bundle_root=root)

    manifest = build_package_set(
        source_bundle=_source_bundle(tmp_path / "source-final.json"),
        seed_archive=_seed_archive(),
        bundle_root=root,
        output_dir=root / "final",
        round_id="ipl-2026",
        selection_path=_selection(tmp_path / "selection.json"),
    )
    manifest["selection_digest"] = f"sha256:{98:064x}"
    with pytest.raises(ValueError, match="selection digest mismatch"):
        validate_package_set(manifest, bundle_root=root)

    manifest = build_package_set(
        source_bundle=tmp_path / "source-final.json",
        seed_archive=_seed_archive(),
        bundle_root=root,
        output_dir=root / "final",
        round_id="ipl-2026",
        selection_path=tmp_path / "selection.json",
    )
    invented_id = "b" * 16
    manifest["package_set_id"] = invented_id
    for row in manifest["packages"]:
        path = root / row["bundle_path"]
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload["dataset_id"] = f"ipl-ipl-2026-{row['kg_id']}-{invented_id}"
        payload["workshop_package"]["package_set_id"] = invented_id
        raw = canonical_json(payload)
        path.write_bytes(raw)
        row["bundle_digest"] = digest_bytes(raw)
    with pytest.raises(ValueError, match="package-set ID"):
        validate_package_set(manifest, bundle_root=root)


def test_validation_rejects_noncanonical_bundle_record_order(tmp_path: Path) -> None:
    root = tmp_path / "bundles"
    source_path = _source_bundle(tmp_path / "source.json")
    source = json.loads(source_path.read_text(encoding="utf-8"))
    extra = deepcopy(source["records"][0])
    extra["query_id"] = "europeana__synthetic-0"
    extra["review_id"] = "europeana::query-0::synthetic"
    source["records"].append(extra)
    source["record_count"] += 1
    source_path.write_bytes(canonical_json(source))
    selection_path = _selection(tmp_path / "selection.json")
    selection = json.loads(selection_path.read_text(encoding="utf-8"))
    selection.append(
        {
            "kg_id": "europeana",
            "query_id": "europeana__synthetic-0",
            "sparql_version": extra["input"]["sparql_version"],
            "sparql_hash": extra["input"]["sparql_hash"],
        }
    )
    selection_path.write_bytes(canonical_json(selection))
    manifest = build_package_set(
        source_bundle=source_path,
        seed_archive=_seed_archive(),
        bundle_root=root,
        output_dir=root / "final",
        round_id="ipl-2026",
        selection_path=selection_path,
    )
    row = manifest["packages"][0]
    path = root / row["bundle_path"]
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["records"].reverse()
    raw = canonical_json(payload)
    path.write_bytes(raw)
    row["bundle_digest"] = digest_bytes(raw)

    with pytest.raises(ValueError, match="records are not in canonical order"):
        validate_package_set(manifest, bundle_root=root)


def test_build_atomically_replaces_output_symlinks(tmp_path: Path) -> None:
    root = tmp_path / "bundles"
    destination = root / "final"
    destination.mkdir(parents=True)
    external_package = tmp_path / "external-package.json"
    external_manifest = tmp_path / "external-manifest.json"
    external_package.write_text("package sentinel", encoding="utf-8")
    external_manifest.write_text("manifest sentinel", encoding="utf-8")
    (destination / "01-europeana.json").symlink_to(external_package)
    (destination / "manifest.json").symlink_to(external_manifest)

    build_package_set(
        source_bundle=_source_bundle(tmp_path / "source.json"),
        seed_archive=_seed_archive(),
        bundle_root=root,
        output_dir=destination,
        round_id="ipl-2026",
        selection_path=_selection(tmp_path / "selection.json"),
    )

    assert external_package.read_text(encoding="utf-8") == "package sentinel"
    assert external_manifest.read_text(encoding="utf-8") == "manifest sentinel"
    assert not (destination / "01-europeana.json").is_symlink()
    assert not (destination / "manifest.json").is_symlink()


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

        assert register_package_set(manifest, sessions=sessions, bundle_root=root) == (len(IPL_PACKAGES), 0)
        assert register_package_set(manifest, sessions=sessions, bundle_root=root) == (0, 0)
        with sessions.begin() as session:
            session.get(WorkshopRound, "ipl-2026").status = "open"
        with pytest.raises(ValueError, match="only on a draft round"):
            register_package_set(manifest, sessions=sessions, bundle_root=root)
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


def test_seed_import_rolls_back_when_registration_fails(tmp_path: Path) -> None:
    root = tmp_path / "bundles"
    archive = _seed_archive()
    manifest = build_package_set(
        source_bundle=_source_bundle(tmp_path / "source.json"),
        seed_archive=archive,
        bundle_root=root,
        output_dir=root / "final",
        round_id="unknown-round",
        selection_path=_selection(tmp_path / "selection.json"),
    )
    database = tmp_path / "musparql.sqlite3"
    upgrade_database(database)
    engine = create_database_engine(database)
    sessions = session_factory(engine)
    try:
        with pytest.raises(ValueError, match="Unknown workshop round"):
            with sessions.begin() as session:
                SeedSnapshotService.import_archive_in_session(session, archive)
                register_package_set(manifest, session=session, bundle_root=root)
        with sessions() as session:
            assert list(session.scalars(select(KgSeedSnapshot))) == []
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
