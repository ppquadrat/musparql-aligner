#!/usr/bin/env python3
"""Build a sanitized, deterministic public benchmark release from a working snapshot."""
from __future__ import annotations

import argparse
import hashlib
import json
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List

from musparql.reviewer_provenance import validate_public_reviewer_ids

from scripts.benchmark.audit_snapshot import audit_snapshot
from scripts.benchmark.build_benchmark import (
    ALTERNATIVES_FILE,
    PUBLIC_RELEASE_FILES,
    public_sparql_provenance,
    read_json,
    read_jsonl,
    write_json,
    write_jsonl,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
RELEASE_DOCUMENTS = {
    "LICENSE": REPO_ROOT / "benchmark" / "LICENSE.md",
    "THIRD_PARTY_NOTICES.md": REPO_ROOT / "THIRD_PARTY_NOTICES.md",
}

BENCHMARK_FIELDS = (
    "benchmark_version",
    "benchmark_id",
    "kg_id",
    "query_id",
    "query_label",
    "sparql",
    "sparql_version",
    "sparql_hash",
    "sparql_provenance",
    "gold_question",
    "gold_question_source",
)
ALTERNATIVE_FIELDS = (
    "benchmark_version",
    "benchmark_id",
    "kg_id",
    "query_id",
    "query_label",
    "sparql",
    "sparql_version",
    "sparql_hash",
    "sparql_provenance",
    "canonical_question",
    "canonical_question_source",
)
FORMULATION_FIELDS = (
    "formulation_id", "text", "source_type", "acceptance",
    "authored_by_reviewer_id", "approval_review_ids", "approval_reviewer_ids",
    "generation_run_id", "model",
)
EVIDENCE_FIELDS = (
    "evidence_count",
    "evidence_types",
    "has_source_evidence",
    "has_query_specific_evidence",
)
PROVENANCE_FIELDS = (
    "question_source",
    "source_type",
    "prompt_source",
    "challenge",
    "cq",
    "dataset",
    "reported_result_rows",
    "reviewer_comment",
    "reviewer_id",
    "approval_review_id",
    "formulation_id",
    "authored_by_reviewer_id",
)
FORBIDDEN_KEYS = {
    "note",
    "internal_comment",
    "review_export",
    "request_config",
    "response_metadata",
    "response_id",
    "run_manifest",
    "source_file",
    "interpretive",
    "interpretive_annotations",
    "api_key_env",
    "api_key",
    "base_url",
    "authorization",
    "token",
}
PRIVATE_PATH = re.compile(
    r"(?:/Users/|/home/|/(?:private/)?(?:tmp|var)/|(?:^|\s)~/|[A-Za-z]:\\\\Users\\\\)"
)
EMAIL = re.compile(r"(?<![\w.-])[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}(?![\w.-])")


def selected(record: Dict[str, Any], fields: Iterable[str]) -> Dict[str, Any]:
    return {field: record[field] for field in fields if record.get(field) not in (None, "", [], {})}


def public_benchmark_record(record: Dict[str, Any]) -> Dict[str, Any]:
    result = selected(record, BENCHMARK_FIELDS)
    provenance_pin = public_sparql_provenance(record.get("sparql_provenance"))
    if provenance_pin:
        result["sparql_provenance"] = provenance_pin
    else:
        result.pop("sparql_provenance", None)
    evidence = record.get("evidence_summary")
    if isinstance(evidence, dict):
        result["evidence_summary"] = selected(evidence, EVIDENCE_FIELDS)
    provenance = record.get("provenance")
    if isinstance(provenance, dict):
        result["provenance"] = selected(provenance, PROVENANCE_FIELDS)
    if not str(result.get("gold_question") or "").strip():
        raise ValueError(f"Public record has no canonical question: {record.get('benchmark_id')}")
    return result


def public_alternative_record(record: Dict[str, Any]) -> Dict[str, Any]:
    result = selected(record, ALTERNATIVE_FIELDS)
    provenance_pin = public_sparql_provenance(record.get("sparql_provenance"))
    if provenance_pin:
        result["sparql_provenance"] = provenance_pin
    else:
        result.pop("sparql_provenance", None)
    for field in ("accepted_alternatives", "literal_formulations"):
        formulations = [
            selected(item, FORMULATION_FIELDS)
            for item in record.get(field, [])
            if isinstance(item, dict) and str(item.get("text") or "").strip()
        ]
        if formulations:
            result[field] = formulations
    return result


def assert_public_safe(value: Any, location: str = "root") -> None:
    validate_public_reviewer_ids(value, location)
    if isinstance(value, dict):
        for key, item in value.items():
            if key in FORBIDDEN_KEYS:
                raise ValueError(f"Forbidden public key {key!r} at {location}")
            assert_public_safe(item, f"{location}.{key}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            assert_public_safe(item, f"{location}[{index}]")
    elif isinstance(value, str):
        if PRIVATE_PATH.search(value):
            raise ValueError(f"Private filesystem path at {location}")
        if EMAIL.search(value):
            raise ValueError(f"Email address at {location}")


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def copy_release_document(source: Path, destination: Path) -> None:
    if not source.is_file():
        raise ValueError(f"Required public release document is missing: {source}")
    text = source.read_text(encoding="utf-8")
    if not text.strip():
        raise ValueError(f"Required public release document is empty: {source}")
    if PRIVATE_PATH.search(text):
        raise ValueError(f"Private filesystem path in public release document: {source}")
    destination.write_text(text, encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", required=True, help="Working benchmark/vN snapshot directory")
    parser.add_argument("--outdir", required=True, help="New, empty public release directory")
    args = parser.parse_args()

    snapshot = Path(args.snapshot)
    outdir = Path(args.outdir)
    snapshot_errors = audit_snapshot(snapshot)
    if snapshot_errors:
        raise ValueError("Snapshot audit failed before release:\n- " + "\n- ".join(snapshot_errors))
    if outdir.exists() and any(outdir.iterdir()):
        raise ValueError(f"Public release directory must be empty: {outdir}")
    outdir.mkdir(parents=True, exist_ok=True)

    source_manifest = read_json(snapshot / "manifest.json")
    benchmark = [public_benchmark_record(row) for row in read_jsonl(snapshot / "benchmark.jsonl")]
    alternatives = [
        public_alternative_record(row) for row in read_jsonl(snapshot / ALTERNATIVES_FILE)
    ]
    alternatives = [
        row for row in alternatives if row.get("accepted_alternatives") or row.get("literal_formulations")
    ]
    benchmark_keys = {(row.get("kg_id"), row.get("query_id")) for row in benchmark}
    for row in alternatives:
        if (row.get("kg_id"), row.get("query_id")) not in benchmark_keys:
            raise ValueError(f"Public alternative has no benchmark referent: {row.get('benchmark_id')}")
    assert_public_safe(benchmark, "benchmark")
    assert_public_safe(alternatives, "alternatives")

    benchmark_path = outdir / "benchmark.jsonl"
    alternatives_path = outdir / ALTERNATIVES_FILE
    write_jsonl(benchmark_path, benchmark)
    write_jsonl(alternatives_path, alternatives)
    for filename, source in RELEASE_DOCUMENTS.items():
        copy_release_document(source, outdir / filename)
    payload_files = ["benchmark.jsonl", ALTERNATIVES_FILE, *RELEASE_DOCUMENTS]
    expected_files = ["manifest.json", *payload_files]
    if expected_files != list(PUBLIC_RELEASE_FILES):
        raise ValueError("Public release file declarations are inconsistent")
    manifest = {
        "benchmark_version": source_manifest.get("benchmark_version") or snapshot.name,
        "release_schema_version": "1.2",
        "counts": {"benchmark": len(benchmark), "alternatives": len(alternatives)},
        "files": {filename: {"sha256": sha256(outdir / filename)} for filename in payload_files},
        "licensing": {
            "benchmark_spdx": "CC-BY-4.0",
            "license_file": "LICENSE",
            "third_party_notices_file": "THIRD_PARTY_NOTICES.md",
        },
        "release_policy": {
            "all_benchmark_pairs_are_human_confirmed": True,
            "pipeline_assessments_are_internal": True,
            "linguistic_annotations_are_internal": True,
            "generated_improvement_candidates_are_not_accepted_alternatives": True,
        },
    }
    assert_public_safe(manifest, "manifest")
    write_json(outdir / "manifest.json", manifest)
    print(f"Wrote sanitized public release with {len(benchmark)} pairs to {outdir}")


if __name__ == "__main__":
    main()
