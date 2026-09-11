"""Deterministic preparation and registration of the four IPL work packages."""
from __future__ import annotations

from contextlib import nullcontext
from copy import deepcopy
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import re
import tempfile
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


MANIFEST_SCHEMA = "musparql.ipl-workshop-package-set.v2"
SELECTION_FIELDS = frozenset({"kg_id", "query_id", "sparql_version", "sparql_hash"})
SHA256_RE = re.compile(r"sha256:[0-9a-f]{64}")


@dataclass(frozen=True)
class PackageDefinition:
    kg_id: str
    display_name: str
    short_description: str
    priority_tier: str


IPL_PACKAGES = (
    PackageDefinition(
        "europeana",
        "Europeana Knowledge Graph",
        "Core workshop work on cultural-heritage objects and their Europeana metadata.",
        "core",
    ),
    PackageDefinition(
        "nfdi4culture",
        "NFDI4Culture Culture Knowledge Graph (CKG)",
        "Core workshop work on cultural research data, resources, organisations, and standards.",
        "core",
    ),
    PackageDefinition(
        "camera-dei-deputati",
        "Camera dei Deputati Knowledge Graph",
        "Specialist work on the Italian Chamber of Deputies and its parliamentary record.",
        "specialist",
    ),
    PackageDefinition(
        "cdec",
        "CDEC Knowledge Graph",
        "Specialist work on CDEC's linked documentary record of Jews in twentieth-century Italy.",
        "specialist",
    ),
)
IPL_KG_IDS = frozenset(item.kg_id for item in IPL_PACKAGES)
WORKSHOP_PASS_ORDER = ("deduplicated", "all_pairs")


def _candidate_records(payload: Mapping[str, Any], *, label: str) -> list[dict[str, Any]]:
    graphs = payload.get("graphs")
    if not isinstance(graphs, list):
        raise ValueError(f"{label} candidate file has no graph list")
    records: list[dict[str, Any]] = []
    for graph in graphs:
        if not isinstance(graph, Mapping) or not isinstance(graph.get("records"), list):
            raise ValueError(f"{label} candidate file contains an invalid graph")
        for record in graph["records"]:
            if not isinstance(record, dict):
                raise ValueError(f"{label} candidate file contains a non-object record")
            records.append(record)
    return records


def prepare_quagga_workshop_source(
    *,
    all_candidates: Mapping[str, Any],
    deduplicated_candidates: Mapping[str, Any],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Convert Quagga candidate reports into a frozen, reviewer-neutral source bundle."""
    all_records = _candidate_records(all_candidates, label="All-pairs")
    deduplicated_records = _candidate_records(
        deduplicated_candidates, label="Deduplicated"
    )
    all_index: dict[str, Mapping[str, Any]] = {}
    for record in all_records:
        query_id = str(record.get("query_id") or "")
        if not query_id or query_id in all_index:
            raise ValueError(f"All-pairs candidate file has duplicate or empty query ID: {query_id}")
        all_index[query_id] = record

    deduplicated_ids: set[str] = set()
    for record in deduplicated_records:
        query_id = str(record.get("query_id") or "")
        source = all_index.get(query_id)
        if source is None:
            raise ValueError(f"Deduplicated candidate is absent from all pairs: {query_id}")
        if query_id in deduplicated_ids:
            raise ValueError(f"Deduplicated candidate file repeats query ID: {query_id}")
        if record.get("sparql_hash") != source.get("sparql_hash"):
            raise ValueError(f"Deduplicated candidate has a stale SPARQL hash: {query_id}")
        deduplicated_ids.add(query_id)

    source_run_id = str(all_candidates.get("source_run_id") or "")
    if not source_run_id:
        raise ValueError("All-pairs candidate file has no source run ID")
    records: list[dict[str, Any]] = []
    selection: list[dict[str, Any]] = []
    counts = {kg_id: {name: 0 for name in WORKSHOP_PASS_ORDER} for kg_id in IPL_KG_IDS}
    for query_id, candidate in sorted(all_index.items()):
        kg_id = query_id.split("__", 1)[0]
        if kg_id not in IPL_KG_IDS:
            continue
        query_label = str(candidate.get("query_label") or "")
        sparql = candidate.get("sparql")
        sparql_hash = candidate.get("sparql_hash")
        nl = candidate.get("nl")
        if (
            not query_label
            or not isinstance(sparql, str)
            or not isinstance(sparql_hash, str)
            or SHA256_RE.fullmatch(sparql_hash) is None
            or not isinstance(nl, Mapping)
            or not isinstance(nl.get("text"), str)
        ):
            raise ValueError(f"Candidate is incomplete: {query_id}")
        evidence: list[dict[str, Any]] = []
        evidence_ids: list[str] = []
        for index, item in enumerate(nl.get("sources") or (), start=1):
            if not isinstance(item, Mapping):
                continue
            evidence_id = f"e{index}"
            evidence_ids.append(evidence_id)
            evidence.append(
                {
                    "evidence_id": evidence_id,
                    "type": str(item.get("evidence_type") or "source"),
                    "snippet": str(item.get("source_text") or ""),
                    "source_url": item.get("source_url"),
                    "source_path": item.get("source_path"),
                }
            )
        workshop_pass = (
            "deduplicated" if query_id in deduplicated_ids else "all_pairs"
        )
        counts[kg_id][workshop_pass] += 1
        records.append(
            {
                "review_id": f"{kg_id}::{query_label}::{source_run_id}",
                "prior_review_ids": [],
                "review_scope": "new",
                "has_prior_pair_review": False,
                "run_id": source_run_id,
                "generation_run_id": source_run_id,
                "run_label": source_run_id,
                "kg_id": kg_id,
                "query_id": query_id,
                "query_label": query_label,
                "workshop_pass": workshop_pass,
                "input": {
                    "sparql_clean": sparql,
                    "sparql_version": 0,
                    "sparql_hash": sparql_hash,
                    "evidence": evidence,
                },
                "output": {
                    "nl_question": nl["text"],
                    "nl_question_origin": {
                        "mode": str(nl.get("origin") or "unknown"),
                        "evidence_ids": evidence_ids,
                    },
                },
                "output_meta": {"model": nl.get("model")},
            }
        )
        selection.append(
            {
                "kg_id": kg_id,
                "query_id": query_id,
                "sparql_version": 0,
                "sparql_hash": sparql_hash,
            }
        )

    empty = sorted(kg_id for kg_id, value in counts.items() if not sum(value.values()))
    if empty:
        raise ValueError("Candidate files do not cover every workshop KG: " + ", ".join(empty))
    dataset_id = digest_bytes(
        canonical_json(
            {
                "source_run_id": source_run_id,
                "records": [(item["query_id"], item["workshop_pass"]) for item in records],
            }
        )
    )[7:23]
    bundle = {
        "schema": "musparql.review-bundle.v2",
        "mode": "initial",
        "dataset_id": f"ipl-quagga-{dataset_id}",
        "source_run_id": source_run_id,
        "holdout_input_policy": "identity_private_filtered_upstream",
        "holdout_review_provenance_complete": False,
        "record_count": len(records),
        "review_scope_policy": {
            "include_reviewed": True,
            "reveal_previous_decision": False,
            "default_scope": "all",
            "counts": {
                "new_records": len(records),
                "previously_reviewed_records": 0,
                "previously_reviewed_excluded": 0,
                "holdout_excluded": 0,
            },
        },
        "workshop_pass_order": list(WORKSHOP_PASS_ORDER),
        "workshop_pass_counts": counts,
        "records": records,
    }
    return bundle, sorted(selection, key=lambda item: (item["kg_id"], item["query_id"]))


def canonical_json(value: Any) -> bytes:
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode("utf-8")


def digest_bytes(value: bytes) -> str:
    return "sha256:" + hashlib.sha256(value).hexdigest()


def _atomic_write(path: Path, value: bytes) -> None:
    """Replace a directory entry without ever following an existing file symlink."""
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            dir=path.parent, prefix=f".{path.name}.", delete=False
        ) as output:
            temporary_path = Path(output.name)
            output.write(value)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary_path, path)
        temporary_path = None
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)


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
) -> tuple[dict[str, list[dict[str, Any]]], str, list[dict[str, Any]] | None, str | None]:
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
        pinned_selection = None
        selection_digest = None
    else:
        selections, _raw = _load_json_records(selection_path)
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
        pinned_selection = [
            {
                "kg_id": kg_id,
                "query_id": query_id,
                "sparql_version": _record_pin(index[(kg_id, query_id)])[0],
                "sparql_hash": _record_pin(index[(kg_id, query_id)])[1],
            }
            for kg_id, query_id in sorted(selected_keys)
        ]
        selection_digest = digest_bytes(canonical_json(pinned_selection))

    grouped = {kg_id: [] for kg_id in IPL_KG_IDS}
    for key in sorted(selected_keys):
        grouped[key[0]].append(deepcopy(dict(index[key])))
    empty = sorted(kg_id for kg_id, records in grouped.items() if not records)
    if empty:
        raise ValueError("Every IPL package must contain at least one record: " + ", ".join(empty))
    return grouped, status, pinned_selection, selection_digest


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
    grouped, status, pinned_selection, selection_digest = _selected_records(
        source["records"], selection_path
    )
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
        pass_counts = {
            name: sum(record.get("workshop_pass", "deduplicated") == name for record in records)
            for name in WORKSHOP_PASS_ORDER
        }
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
            "priority_tier": definition.priority_tier,
            "pass_order": list(WORKSHOP_PASS_ORDER),
            "pass_counts": pass_counts,
            "package_set_id": package_set_id,
            "status": status,
            "source_bundle_digest": source_digest,
            "selection_digest": selection_digest,
        }
        raw = canonical_json(package_payload)
        filename = f"{order:02d}-{definition.kg_id}.json"
        path = destination / filename
        _atomic_write(path, raw)
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
        "selection": pinned_selection,
        "selection_digest": selection_digest,
        "workshop_round_id": round_id,
        "packages": package_rows,
    }
    _atomic_write(destination / "manifest.json", canonical_json(manifest))
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
    selection = manifest.get("selection")
    if (
        status == "provisional"
        and (selection is not None or selection_digest is not None)
    ) or (
        status == "frozen"
        and (
            not isinstance(selection, list)
            or not isinstance(selection_digest, str)
            or SHA256_RE.fullmatch(selection_digest) is None
        )
    ):
        raise ValueError("IPL package manifest selection provenance does not match its status")
    selected_pins: dict[tuple[str, str], tuple[int, str]] = {}
    if status == "frozen":
        assert isinstance(selection, list)
        for item in selection:
            if not isinstance(item, Mapping) or set(item) != SELECTION_FIELDS:
                raise ValueError("Frozen IPL selection contains invalid fields")
            kg_id = item.get("kg_id")
            query_id = item.get("query_id")
            version = item.get("sparql_version")
            digest = item.get("sparql_hash")
            if (
                not isinstance(kg_id, str)
                or kg_id not in IPL_KG_IDS
                or not isinstance(query_id, str)
                or not query_id
                or isinstance(version, bool)
                or not isinstance(version, int)
                or version < 0
                or not isinstance(digest, str)
                or SHA256_RE.fullmatch(digest) is None
            ):
                raise ValueError("Frozen IPL selection contains an invalid record pin")
            key = (kg_id, query_id)
            if key in selected_pins:
                raise ValueError(f"Duplicate frozen package selection: {kg_id}/{query_id}")
            selected_pins[key] = (version, digest)
        canonical_selection = [
            {
                "kg_id": kg_id,
                "query_id": query_id,
                "sparql_version": selected_pins[(kg_id, query_id)][0],
                "sparql_hash": selected_pins[(kg_id, query_id)][1],
            }
            for kg_id, query_id in sorted(selected_pins)
        ]
        if selection != canonical_selection:
            raise ValueError("Frozen IPL selection is not in canonical record order")
        if digest_bytes(canonical_json(canonical_selection)) != selection_digest:
            raise ValueError("Frozen IPL selection digest mismatch")
    packages = manifest.get("packages")
    if not isinstance(packages, list) or len(packages) != len(IPL_PACKAGES):
        raise ValueError("IPL package manifest must contain exactly four packages")
    expected_round = manifest.get("workshop_round_id")
    record_membership: dict[str, list[str]] = {}
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
        payload, relative, digest = load_neutral_bundle_file(
            bundle_root, str(row.get("bundle_path") or "")
        )
        if relative != row.get("bundle_path"):
            raise ValueError(f"IPL package bundle path is not canonical: {definition.kg_id}")
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
        ordered_query_ids: list[str] = []
        for record in payload["records"]:
            query_id = str(record.get("query_id") or "")
            workshop_pass = record.get("workshop_pass", "deduplicated")
            if not query_id or query_id in query_ids:
                raise ValueError(f"IPL package has duplicate or empty query IDs: {definition.kg_id}")
            if workshop_pass not in WORKSHOP_PASS_ORDER:
                raise ValueError(
                    f"IPL package has an invalid workshop pass: {definition.kg_id}/{query_id}"
                )
            query_ids.add(query_id)
            ordered_query_ids.append(query_id)
            pin = _record_pin(record)
            if (
                status == "frozen"
                and selected_pins.get((definition.kg_id, query_id)) != pin
            ):
                raise ValueError(
                    "IPL package record is absent from its frozen selection: "
                    f"{definition.kg_id}/{query_id}"
                )
        if ordered_query_ids != sorted(ordered_query_ids):
            raise ValueError(f"IPL package records are not in canonical order: {definition.kg_id}")
        record_membership[definition.kg_id] = ordered_query_ids
        metadata = payload.get("workshop_package")
        expected_pass_counts = {
            name: sum(
                record.get("workshop_pass", "deduplicated") == name
                for record in payload["records"]
            )
            for name in WORKSHOP_PASS_ORDER
        }
        if not isinstance(metadata, Mapping) or (
            metadata.get("kg_id") != definition.kg_id
            or metadata.get("priority_tier") != definition.priority_tier
            or metadata.get("pass_order") != list(WORKSHOP_PASS_ORDER)
            or metadata.get("pass_counts") != expected_pass_counts
            or metadata.get("package_set_id") != manifest.get("package_set_id")
            or metadata.get("status") != status
            or metadata.get("source_bundle_digest") != manifest.get("source_bundle_digest")
            or metadata.get("selection_digest") != manifest.get("selection_digest")
        ):
            raise ValueError(f"IPL package provenance mismatch: {definition.kg_id}")
    if status == "frozen" and set(selected_pins) != {
        (kg_id, query_id)
        for kg_id, query_ids in record_membership.items()
        for query_id in query_ids
    }:
        raise ValueError("Frozen IPL selection membership does not match its packages")
    expected_package_set_id = digest_bytes(
        canonical_json(
            {
                "round_id": expected_round,
                "source_bundle_digest": manifest["source_bundle_digest"],
                "selection_digest": selection_digest,
                "records": {
                    kg_id: record_membership[kg_id] for kg_id in sorted(record_membership)
                },
            }
        )
    )[7:23]
    if manifest.get("package_set_id") != expected_package_set_id:
        raise ValueError("IPL package-set ID does not match its package membership")


def register_package_set(
    manifest: Mapping[str, Any],
    *,
    bundle_root: Path,
    sessions: sessionmaker[Session] | None = None,
    session: Session | None = None,
) -> tuple[int, int]:
    """Register or safely replace an unclaimed package set for a draft round."""
    validate_package_set(manifest, bundle_root=bundle_root)
    if manifest.get("status") != "frozen":
        raise ValueError("Provisional IPL packages cannot be registered")
    rows = cast(list[Mapping[str, Any]], manifest["packages"])
    round_id = str(manifest["workshop_round_id"])
    inserted = updated = 0
    if (sessions is None) == (session is None):
        raise ValueError("Provide exactly one session or session factory")
    transaction = sessions.begin() if sessions is not None else nullcontext(session)
    with transaction as session:
        assert session is not None
        workshop_round = session.get(WorkshopRound, round_id)
        if workshop_round is None:
            raise ValueError(f"Unknown workshop round: {round_id}")
        if workshop_round.status != "draft":
            raise ValueError("Workshop packages may be registered only on a draft round")
        existing_rows = list(
            session.scalars(
                select(WorkshopWorkPackage).where(
                    WorkshopWorkPackage.workshop_round_id == round_id
                )
            )
        )
        unexpected = [row for row in existing_rows if row.kg_id not in IPL_KG_IDS]
        if unexpected:
            raise ValueError("Workshop round contains packages outside the IPL four")
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
            if claims:
                raise ValueError("Claimed or non-draft workshop packages cannot be replaced")
            if existing.id != fields["id"]:
                raise ValueError("Existing IPL package uses an unexpected stable ID")
            for key, value in fields.items():
                setattr(existing, key, value)
            updated += 1
    return inserted, updated
