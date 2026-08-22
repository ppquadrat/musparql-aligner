from __future__ import annotations

from pathlib import Path
import pytest

from scripts.migrations.migrate_linkedmusic_versions import (
    EDIT_SOURCE_ID,
    OFFICIAL_SOURCE_ID,
    migrate_records,
    parse_corrected_markdown,
    read_jsonl,
    record_source_ids,
)
from musparql.sparql_versions import execution_resolves, resolve_sparql_version


def canonical_records_or_skip(root: Path):
    path = root / "var/queries/kg_queries.jsonl"
    if not path.exists():
        pytest.skip("local generated kg_queries.jsonl is intentionally absent from a clean checkout")
    return read_jsonl(path)


def test_current_linkedmusic_records_migrate_one_to_one():
    root = Path(__file__).resolve().parents[1]
    records = canonical_records_or_skip(root)
    official = read_jsonl(root / "catalog/curated/LinkedMusic_Queries_Official.jsonl")
    corrected = parse_corrected_markdown(root / "catalog/curated/LinkedMusic_Queries_Corrected.md")
    migrated, mapping = migrate_records(
        records,
        official,
        corrected,
        extracted_at="2026-08-03T00:00:00+00:00",
    )

    linkedmusic = [record for record in migrated if record.get("kg_id") == "linkedmusic"]
    canonical = [record for record in linkedmusic if OFFICIAL_SOURCE_ID in record_source_ids(record)]
    assert len(linkedmusic) == sum(1 for record in records if record.get("kg_id") == "linkedmusic")
    assert len(canonical) == 20
    assert len(mapping) == 20
    assert not any(EDIT_SOURCE_ID in record_source_ids(record) for record in linkedmusic)
    for record in canonical:
        assert record["nl_question"]["text"]
        assert resolve_sparql_version(record, 0)["sparql"] == record["sparql_clean"]
        assert resolve_sparql_version(record, 1)["sparql"] == record["sparql_edits"][0]["sparql"]
        assert record["sparql_edits"][0]["source_id"] == EDIT_SOURCE_ID
        assert all(execution_resolves(record, item) for item in record["execution_history"])


def test_migration_is_idempotent():
    root = Path(__file__).resolve().parents[1]
    records = canonical_records_or_skip(root)
    official = read_jsonl(root / "catalog/curated/LinkedMusic_Queries_Official.jsonl")
    corrected = parse_corrected_markdown(root / "catalog/curated/LinkedMusic_Queries_Corrected.md")
    first, _ = migrate_records(records, official, corrected, extracted_at="2026-08-03T00:00:00+00:00")
    canonical = next(record for record in first if OFFICIAL_SOURCE_ID in record_source_ids(record))
    canonical["sparql_edits"].append(
        {"version": 2, "sparql": canonical["sparql_edits"][0]["sparql"] + "\n# v2", "note": "Later edit."}
    )
    version_zero = resolve_sparql_version(canonical, 0)
    version_two = resolve_sparql_version(canonical, 2)
    canonical["execution_history"] = [
        {
            "status": "ok",
            "sparql_version": 0,
            "sparql_hash": version_zero["sparql_hash"],
        },
        {
            "status": "empty",
            "sparql_version": 2,
            "sparql_hash": version_two["sparql_hash"],
        },
    ]
    canonical["run_history"] = list(canonical["execution_history"])
    canonical["comments"] = "Keep this annotation."
    canonical["verification"] = {"status": "verified", "notes": "Reviewed."}
    canonical["cq_items"] = [{"text": "Retain me"}]
    canonical["justification"] = "Curator decision."
    second, mapping = migrate_records(first, official, corrected, extracted_at="2027-01-01T00:00:00+00:00")
    assert second == first
    assert len(mapping) == 20


def test_first_migration_preserves_legacy_curator_annotations():
    root = Path(__file__).resolve().parents[1]
    current = canonical_records_or_skip(root)
    official = read_jsonl(root / "catalog/curated/LinkedMusic_Queries_Official.jsonl")
    corrected = parse_corrected_markdown(root / "catalog/curated/LinkedMusic_Queries_Corrected.md")
    legacy = []
    for record in current:
        if OFFICIAL_SOURCE_ID not in record_source_ids(record):
            legacy.append(record)
            continue
        old = dict(record)
        edit = record["sparql_edits"][0]
        old["query_id"] = f"legacy-{record['query_id']}"
        old["sparql_clean"] = edit["sparql"]
        old["sparql_raw"] = edit["sparql"]
        old["sparql_hash"] = resolve_sparql_version(record, 1)["sparql_hash"]
        old["sparql_edits"] = []
        old["evidence"] = [{"source_id": EDIT_SOURCE_ID}]
        old["comments"] = "Keep legacy annotation."
        old["verification"] = {"status": "verified", "notes": "Legacy review."}
        old["cq_items"] = [{"text": "Legacy CQ"}]
        old["justification"] = "Legacy curator decision."
        legacy.append(old)
    migrated, _ = migrate_records(
        legacy, official, corrected, extracted_at="2026-08-03T00:00:00+00:00"
    )
    migrated_canonical = [r for r in migrated if OFFICIAL_SOURCE_ID in record_source_ids(r)]
    assert len(migrated_canonical) == 20
    for record in migrated_canonical:
        assert record["comments"] == "Keep legacy annotation."
        assert record["verification"]["status"] == "verified"
        assert record["cq_items"] == [{"text": "Legacy CQ"}]
        assert record["justification"] == "Legacy curator decision."
