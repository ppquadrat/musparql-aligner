"""Deterministic preparation and registration of the five IPL work packages."""
from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import re
from typing import Any, Mapping, Sequence, cast

from sqlalchemy import func, select
from sqlalchemy.orm import Session, sessionmaker

from musparql.database.models import (
    KgSeedSnapshot,
    ReviewAssignment,
    WorkshopRound,
    WorkshopWorkPackage,
)
from musparql.source_catalog import validate_kg_seed_snapshots
from musparql.web.assignments import load_neutral_bundle_file
from musparql.web.auth import timestamp, utc_now


MANIFEST_SCHEMA = "musparql.ipl-workshop-package-set.v1"
SELECTION_FIELDS = frozenset({"kg_id", "query_id", "sparql_version", "sparql_hash"})
SHA256_RE = re.compile(r"sha256:[0-9a-f]{64}")


@dataclass(frozen=True)
class PackageDefinition:
    kg_id: str
    display_name: str
    short_description: str


IPL_PACKAGES = (
    PackageDefinition(
        "alyra",
        "Archaic Lyric Poetry Ontology (ALyrA)",
        "Questions about archaic Greek lyric poetry, people, works, places, and terminology.",
    ),
    PackageDefinition(
        "camera-dei-deputati",
        "Camera dei Deputati Knowledge Graph",
        "Questions about the Italian Chamber of Deputies and its parliamentary record.",
    ),
    PackageDefinition(
        "europeana",
        "Europeana Knowledge Graph",
        "Questions about cultural-heritage objects and their Europeana metadata.",
    ),
    PackageDefinition(
        "nfdi4culture",
        "NFDI4Culture Culture Knowledge Graph (CKG)",
        "Questions about cultural research data, resources, organisations, and standards.",
    ),
    PackageDefinition(
        "cdec",
        "CDEC Knowledge Graph",
        "Questions about CDEC's linked documentary record of Jews in twentieth-century Italy.",
    ),
)
IPL_KG_IDS = frozenset(item.kg_id for item in IPL_PACKAGES)


def canonical_json(value: Any) -> bytes:
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode("utf-8")


def digest_bytes(value: bytes) -> str:
    return "sha256:" + hashlib.sha256(value).hexdigest()


def _load_neutral_bundle(path: Path) -> tuple[dict[str, Any], str]:
    # Bundle validation is deliberately shared with the hosted assignment path.
    payload, _relative, digest = load_neutral_bundle_file(path.parent, path.name)
    if str(payload.get("mode") or "initial") != "initial":
        raise ValueError("IPL packages require an initial-review bundle")
    if payload.get("holdout_input_policy") not in {
        "identity_visible_selectors",
        "identity_private_filtered_upstream",
    }:
        raise ValueError("IPL packages require holdouts to be explicitly filtered")
    return payload, digest


def _load_json_records(path: Path) -> tuple[list[dict[str, Any]], bytes]:
    raw = path.read_bytes()
    text = raw.decode("utf-8-sig").strip()
    if not text:
        raise ValueError("Package selection is empty")
    if text.startswith("["):
        payload = json.loads(text)
        if not isinstance(payload, list):
            raise ValueError("Package selection must be a JSON array or JSONL")
        records = payload
    else:
        records = [json.loads(line) for line in text.splitlines() if line.strip()]
    if not all(isinstance(item, dict) for item in records):
        raise ValueError("Package selection records must be JSON objects")
    return records, raw


def _record_pin(record: Mapping[str, Any]) -> tuple[int, str]:
    source = record.get("input")
    if not isinstance(source, Mapping):
        raise ValueError("Review bundle record is missing its frozen input")
    version = source.get("sparql_version")
    digest = source.get("sparql_hash")
    if isinstance(version, bool) or not isinstance(version, int) or version < 0:
        raise ValueError("Review bundle record has an invalid SPARQL version")
    if not isinstance(digest, str) or SHA256_RE.fullmatch(digest) is None:
        raise ValueError("Review bundle record has an invalid SPARQL digest")
    return version, digest


def _selected_records(
    source_records: Sequence[Mapping[str, Any]], selection_path: Path | None
) -> tuple[dict[str, list[dict[str, Any]]], str, str | None]:
    index: dict[tuple[str, str], Mapping[str, Any]] = {}
    for record in source_records:
        kg_id = str(record.get("kg_id") or "")
        query_id = str(record.get("query_id") or "")
        if kg_id not in IPL_KG_IDS:
            continue
        key = (kg_id, query_id)
        if not query_id or key in index:
            raise ValueError(f"Duplicate or empty review identity: {kg_id}/{query_id}")
        _record_pin(record)
        index[key] = record

    if selection_path is None:
        selected_keys = set(index)
        status = "provisional"
        selection_digest = None
    else:
        selections, raw = _load_json_records(selection_path)
        selected_keys: set[tuple[str, str]] = set()
        for selection in selections:
            if set(selection) != SELECTION_FIELDS:
                raise ValueError(
                    "Each final package selection requires only kg_id, query_id, "
                    "sparql_version, and sparql_hash"
                )
            kg_id = selection.get("kg_id")
            query_id = selection.get("query_id")
            if not isinstance(kg_id, str) or kg_id not in IPL_KG_IDS:
                raise ValueError(f"Unknown IPL package KG: {kg_id}")
            if not isinstance(query_id, str) or not query_id:
                raise ValueError("Package selection query_id must be nonempty")
            key = (kg_id, query_id)
            if key in selected_keys:
                raise ValueError(f"Duplicate package selection: {kg_id}/{query_id}")
            source = index.get(key)
            if source is None:
                raise ValueError(f"Package selection is absent from the source bundle: {kg_id}/{query_id}")
            if (selection.get("sparql_version"), selection.get("sparql_hash")) != _record_pin(source):
                raise ValueError(f"Stale package selection SPARQL pin: {kg_id}/{query_id}")
            selected_keys.add(key)
        status = "frozen"
        selection_digest = digest_bytes(raw)

    grouped = {kg_id: [] for kg_id in IPL_KG_IDS}
    for key in sorted(selected_keys):
        grouped[key[0]].append(deepcopy(dict(index[key])))
    empty = sorted(kg_id for kg_id, records in grouped.items() if not records)
    if empty:
        raise ValueError("Every IPL package must contain at least one record: " + ", ".join(empty))
    return grouped, status, selection_digest


def _head_seeds(seed_archive: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    snapshots = validate_kg_seed_snapshots(seed_archive)
    previous = {
        (str(item["kg_id"]), str(item["previous_seed_digest"]))
        for item in snapshots
        if item.get("previous_seed_digest") is not None
    }
    heads = {
        str(item["kg_id"]): item
        for item in snapshots
        if (str(item["kg_id"]), str(item["seed_digest"])) not in previous
        and str(item["kg_id"]) in IPL_KG_IDS
    }
    if set(heads) != IPL_KG_IDS:
        raise ValueError("The seed archive does not contain one current seed for every IPL KG")
    for kg_id, snapshot in heads.items():
        seed = snapshot["seed"]
        if not seed.get("review_domains") and not seed.get("familiarity_scopes"):
            raise ValueError(f"IPL seed has no assessment prompts: {kg_id}")
    return heads


def build_package_set(
    *,
    source_bundle: Path,
    seed_archive: Mapping[str, Any],
    bundle_root: Path,
    output_dir: Path,
    round_id: str,
    selection_path: Path | None,
) -> dict[str, Any]:
    if not round_id.strip():
        raise ValueError("Workshop round ID is required")
    root = bundle_root.resolve()
    destination = output_dir.resolve()
    try:
        destination.relative_to(root)
    except ValueError as exc:
        raise ValueError("Package output directory must be inside the assignment bundle root") from exc

    source, source_digest = _load_neutral_bundle(source_bundle.resolve())
    grouped, status, selection_digest = _selected_records(source["records"], selection_path)
    heads = _head_seeds(seed_archive)
    package_set_id = digest_bytes(
        canonical_json(
            {
                "round_id": round_id,
                "source_bundle_digest": source_digest,
                "selection_digest": selection_digest,
                "records": {
                    kg_id: [str(item["query_id"]) for item in grouped[kg_id]]
                    for kg_id in sorted(grouped)
                },
            }
        )
    )[7:23]

    destination.mkdir(parents=True, exist_ok=True)
    package_rows: list[dict[str, Any]] = []
    for order, definition in enumerate(IPL_PACKAGES, start=1):
        records = grouped[definition.kg_id]
        package_payload = deepcopy(source)
        package_payload["dataset_id"] = f"ipl-{round_id}-{definition.kg_id}-{package_set_id}"
        package_payload["record_count"] = len(records)
        package_payload["records"] = records
        review_scope_policy = package_payload.get("review_scope_policy")
        if isinstance(review_scope_policy, dict):
            source_counts = review_scope_policy.get("counts")
            if isinstance(source_counts, dict):
                review_scope_policy["source_bundle_counts"] = deepcopy(source_counts)
            review_scope_policy["counts"] = {
                "new_records": sum(record.get("review_scope") != "previously_reviewed" for record in records),
                "previously_reviewed_records": sum(
                    record.get("review_scope") == "previously_reviewed" for record in records
                ),
                "previously_reviewed_excluded": 0,
                "holdout_excluded": 0,
            }
        package_payload["workshop_package"] = {
            "kg_id": definition.kg_id,
            "package_set_id": package_set_id,
            "status": status,
            "source_bundle_digest": source_digest,
            "selection_digest": selection_digest,
        }
        raw = canonical_json(package_payload)
        filename = f"{order:02d}-{definition.kg_id}.json"
        path = destination / filename
        path.write_bytes(raw)
        snapshot = heads[definition.kg_id]
        package_rows.append(
            {
                "id": f"{round_id}-{definition.kg_id}",
                "workshop_round_id": round_id,
                "kg_id": definition.kg_id,
                "seed_version": snapshot["seed_version"],
                "seed_digest": snapshot["seed_digest"],
                "display_name": definition.display_name,
                "short_description": definition.short_description,
                "display_order": order,
                "bundle_path": path.relative_to(root).as_posix(),
                "bundle_digest": digest_bytes(raw),
                "record_count": len(records),
                "processing_recipe": "validate_initial_review",
                "enabled": status == "frozen",
            }
        )

    manifest = {
        "schema": MANIFEST_SCHEMA,
        "package_set_id": package_set_id,
        "status": status,
        "source_bundle_digest": source_digest,
        "selection_digest": selection_digest,
        "workshop_round_id": round_id,
        "packages": package_rows,
    }
    (destination / "manifest.json").write_bytes(canonical_json(manifest))
    validate_package_set(manifest, bundle_root=root)
    return manifest


def validate_package_set(manifest: Mapping[str, Any], *, bundle_root: Path) -> None:
    if manifest.get("schema") != MANIFEST_SCHEMA:
        raise ValueError("Unsupported IPL package manifest schema")
    status = manifest.get("status")
    if status not in {"provisional", "frozen"}:
        raise ValueError("IPL package manifest has an invalid status")
    if not isinstance(manifest.get("package_set_id"), str) or re.fullmatch(
        r"[0-9a-f]{16}", str(manifest.get("package_set_id"))
    ) is None:
        raise ValueError("IPL package manifest has an invalid package-set ID")
    if not isinstance(manifest.get("source_bundle_digest"), str) or SHA256_RE.fullmatch(
        str(manifest.get("source_bundle_digest"))
    ) is None:
        raise ValueError("IPL package manifest has an invalid source-bundle digest")
    selection_digest = manifest.get("selection_digest")
    if (status == "provisional" and selection_digest is not None) or (
        status == "frozen"
        and (
            not isinstance(selection_digest, str)
            or SHA256_RE.fullmatch(selection_digest) is None
        )
    ):
        raise ValueError("IPL package manifest selection provenance does not match its status")
    packages = manifest.get("packages")
    if not isinstance(packages, list) or len(packages) != len(IPL_PACKAGES):
        raise ValueError("IPL package manifest must contain exactly five packages")
    expected_round = manifest.get("workshop_round_id")
    for order, (row, definition) in enumerate(zip(packages, IPL_PACKAGES), start=1):
        if not isinstance(row, Mapping):
            raise ValueError("IPL package manifest rows must be objects")
        if (
            row.get("workshop_round_id") != expected_round
            or row.get("id") != f"{expected_round}-{definition.kg_id}"
            or row.get("kg_id") != definition.kg_id
            or row.get("display_name") != definition.display_name
            or row.get("short_description") != definition.short_description
            or row.get("display_order") != order
            or row.get("enabled") is not (status == "frozen")
            or row.get("processing_recipe") != "validate_initial_review"
        ):
            raise ValueError(f"IPL package metadata mismatch at display position {order}")
        if not isinstance(row.get("seed_version"), str) or not row["seed_version"]:
            raise ValueError(f"IPL package has no seed version: {definition.kg_id}")
        if not isinstance(row.get("seed_digest"), str) or SHA256_RE.fullmatch(
            str(row.get("seed_digest"))
        ) is None:
            raise ValueError(f"IPL package has an invalid seed digest: {definition.kg_id}")
        if not isinstance(row.get("record_count"), int) or row["record_count"] <= 0:
            raise ValueError(f"IPL package is empty: {definition.kg_id}")
        payload, _relative, digest = load_neutral_bundle_file(
            bundle_root, str(row.get("bundle_path") or "")
        )
        if digest != row.get("bundle_digest"):
            raise ValueError(f"IPL package digest mismatch: {definition.kg_id}")
        if payload.get("record_count") != row["record_count"]:
            raise ValueError(f"IPL package record count mismatch: {definition.kg_id}")
        if payload.get("dataset_id") != (
            f"ipl-{expected_round}-{definition.kg_id}-{manifest['package_set_id']}"
        ):
            raise ValueError(f"IPL package dataset ID mismatch: {definition.kg_id}")
        kg_ids = {str(record.get("kg_id") or "") for record in payload["records"]}
        if kg_ids != {definition.kg_id}:
            raise ValueError(f"IPL package contains records from another KG: {definition.kg_id}")
        query_ids: set[str] = set()
        for record in payload["records"]:
            query_id = str(record.get("query_id") or "")
            if not query_id or query_id in query_ids:
                raise ValueError(f"IPL package has duplicate or empty query IDs: {definition.kg_id}")
            query_ids.add(query_id)
            _record_pin(record)
        metadata = payload.get("workshop_package")
        if not isinstance(metadata, Mapping) or (
            metadata.get("kg_id") != definition.kg_id
            or metadata.get("package_set_id") != manifest.get("package_set_id")
            or metadata.get("status") != status
            or metadata.get("source_bundle_digest") != manifest.get("source_bundle_digest")
            or metadata.get("selection_digest") != manifest.get("selection_digest")
        ):
            raise ValueError(f"IPL package provenance mismatch: {definition.kg_id}")


def register_package_set(
    manifest: Mapping[str, Any], *, sessions: sessionmaker[Session], bundle_root: Path
) -> tuple[int, int]:
    """Register or safely replace an unclaimed package set for a draft round."""
    validate_package_set(manifest, bundle_root=bundle_root)
    if manifest.get("status") != "frozen":
        raise ValueError("Provisional IPL packages cannot be registered")
    rows = cast(list[Mapping[str, Any]], manifest["packages"])
    round_id = str(manifest["workshop_round_id"])
    inserted = updated = 0
    with sessions.begin() as session:
        workshop_round = session.get(WorkshopRound, round_id)
        if workshop_round is None:
            raise ValueError(f"Unknown workshop round: {round_id}")
        existing_rows = list(
            session.scalars(
                select(WorkshopWorkPackage).where(
                    WorkshopWorkPackage.workshop_round_id == round_id
                )
            )
        )
        unexpected = [row for row in existing_rows if row.kg_id not in IPL_KG_IDS]
        if unexpected:
            raise ValueError("Workshop round contains packages outside the IPL five")
        by_kg = {row.kg_id: row for row in existing_rows}
        for values in rows:
            seed = session.get(
                KgSeedSnapshot, (str(values["kg_id"]), str(values["seed_version"]))
            )
            if seed is None or seed.seed_digest != values["seed_digest"]:
                raise ValueError(
                    f"Frozen KG seed is not imported: {values['kg_id']}/{values['seed_version']}"
                )
            fields = {
                key: values[key]
                for key in (
                    "id", "workshop_round_id", "kg_id", "seed_version", "seed_digest",
                    "display_name", "short_description", "display_order", "bundle_path",
                    "bundle_digest", "processing_recipe", "enabled",
                )
            }
            existing = by_kg.get(str(values["kg_id"]))
            if existing is None:
                if workshop_round.status != "draft":
                    raise ValueError("New workshop packages may be registered only on a draft round")
                session.add(WorkshopWorkPackage(**fields, created_at=timestamp(utc_now())))
                inserted += 1
                continue
            changed = any(getattr(existing, key) != value for key, value in fields.items())
            if not changed:
                continue
            claims = int(
                session.scalar(
                    select(func.count())
                    .select_from(ReviewAssignment)
                    .where(ReviewAssignment.work_package_id == existing.id)
                )
                or 0
            )
            if workshop_round.status != "draft" or claims:
                raise ValueError("Claimed or non-draft workshop packages cannot be replaced")
            if existing.id != fields["id"]:
                raise ValueError("Existing IPL package uses an unexpected stable ID")
            for key, value in fields.items():
                setattr(existing, key, value)
            updated += 1
    return inserted, updated
