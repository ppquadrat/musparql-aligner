#!/usr/bin/env python3
"""Deterministically convert LinkedMusic's public XLSX query sheet to JSONL."""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import xml.etree.ElementTree as ET
import zipfile
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List


SOURCE_ID = "linkedmusic-public-query-database"
SOURCE_URL = (
    "https://docs.google.com/spreadsheets/d/"
    "1Bsb1fZXPUGgqAMfNQeCf4J5l0p5rLhTQMCwRc0ZVrFs/edit?gid=1479578801#gid=1479578801"
)
SHEET_NAME = "Queries per Challenge"
SOURCE_EXPORT_FILENAME = "Public LinkedMusic Query Database(1).xlsx"
SOURCE_SNAPSHOT_PATH = "catalog/curated/LinkedMusic_Public_Query_Database_2026-08-03.xlsx"
NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"


def file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def cell_column(reference: str) -> str:
    match = re.match(r"[A-Z]+", reference)
    return match.group(0) if match else ""


def shared_strings(archive: zipfile.ZipFile) -> List[str]:
    root = ET.fromstring(archive.read("xl/sharedStrings.xml"))
    return ["".join(node.text or "" for node in item.iter(NS + "t")) for item in root.findall(NS + "si")]


def sheet_path(archive: zipfile.ZipFile, sheet_name: str) -> str:
    office_rel = "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id"
    workbook = ET.fromstring(archive.read("xl/workbook.xml"))
    relation_id = next(
        sheet.attrib[office_rel]
        for sheet in workbook.iter(NS + "sheet")
        if sheet.attrib.get("name") == sheet_name
    )
    package_ns = "{http://schemas.openxmlformats.org/package/2006/relationships}"
    relations = ET.fromstring(archive.read("xl/_rels/workbook.xml.rels"))
    target = next(rel.attrib["Target"] for rel in relations.findall(package_ns + "Relationship") if rel.attrib["Id"] == relation_id)
    return "xl/" + target.lstrip("/")


def extract_records(path: Path) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    cq_by_challenge: Dict[int, int] = defaultdict(int)
    with zipfile.ZipFile(path) as archive:
        strings = shared_strings(archive)
        root = ET.fromstring(archive.read(sheet_path(archive, SHEET_NAME)))
        for row in root.iter(NS + "row"):
            values: Dict[str, str] = {}
            for cell in row.findall(NS + "c"):
                value_node = cell.find(NS + "v")
                value = "" if value_node is None else (value_node.text or "")
                if cell.attrib.get("t") == "s" and value:
                    value = strings[int(value)]
                values[cell_column(cell.attrib.get("r", ""))] = value
            challenge_match = re.fullmatch(r"Challenge\s+(\d+)", values.get("A", "").strip())
            if not challenge_match or not values.get("B") or not values.get("D"):
                continue
            challenge = int(challenge_match.group(1))
            cq_by_challenge[challenge] += 1
            reported_rows = values.get("E", "").strip()
            if re.fullmatch(r"\d+\.0", reported_rows):
                reported_rows = reported_rows[:-2]
            records.append(
                {
                    "challenge": challenge,
                    "cq": cq_by_challenge[challenge],
                    "prompt": values["B"].strip(),
                    "databases": values.get("C", "").strip(),
                    "sparql": values["D"].strip(),
                    "reported_result_rows": reported_rows,
                    "source": {"source_id": SOURCE_ID, "url": SOURCE_URL, "sheet": SHEET_NAME},
                }
            )
    if len(records) != 20:
        raise ValueError(f"Expected 20 LinkedMusic queries, found {len(records)}")
    return records


def serialize_jsonl(records: List[Dict[str, Any]]) -> str:
    return "".join(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n" for record in records)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("xlsx", type=Path)
    parser.add_argument("--output", type=Path, default=Path("catalog/curated/LinkedMusic_Queries_Official.jsonl"))
    parser.add_argument("--metadata-output", type=Path, default=Path("catalog/curated/LinkedMusic_Queries_Official.meta.json"))
    parser.add_argument("--captured-at", default="2026-08-03")
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    records = extract_records(args.xlsx)
    rendered = serialize_jsonl(records)
    metadata = {
        "source_url": SOURCE_URL,
        "worksheet": SHEET_NAME,
        "captured_at": args.captured_at,
        "source_export_filename": SOURCE_EXPORT_FILENAME,
        "source_snapshot_path": SOURCE_SNAPSHOT_PATH,
        "source_export_sha256": file_sha256(args.xlsx),
        "source_export_size_bytes": args.xlsx.stat().st_size,
        "record_count": len(records),
        "conversion_script": "extract_linkedmusic_official.py@v1",
    }
    rendered_metadata = json.dumps(metadata, ensure_ascii=False, indent=2) + "\n"
    if args.check:
        if args.output.read_text(encoding="utf-8") != rendered:
            raise SystemExit(f"Generated records differ from {args.output}")
        expected = json.loads(args.metadata_output.read_text(encoding="utf-8"))
        if expected != metadata:
            raise SystemExit(f"Generated metadata differs from {args.metadata_output}")
        print(f"Validated {len(records)} records against {args.output}")
        return
    args.output.write_text(rendered, encoding="utf-8")
    args.metadata_output.write_text(rendered_metadata, encoding="utf-8")
    print(f"Wrote {len(records)} records to {args.output}")


if __name__ == "__main__":
    main()
