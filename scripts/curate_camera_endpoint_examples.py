#!/usr/bin/env python3
"""Convert the captured Camera endpoint examples XML to curated JSONL."""

from __future__ import annotations

import html
import json
import re
from pathlib import Path
from xml.etree import ElementTree as ET


ROOT = Path(__file__).resolve().parents[1]
INPUT = ROOT / "catalog/snapshots/camera-dei-deputati__02__dati-camera-it.txt"
OUTPUT = ROOT / "catalog/curated/camera-dei-deputati-endpoint-examples.jsonl"
SOURCE_URL = "https://dati.camera.it/ocd/dump/custom_endpoint/sparql.xml"


def clean_label(value: str) -> str:
    value = html.unescape(value)
    value = re.sub(r"<br\s*/?>", " ", value, flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", value).strip()


def main() -> None:
    captured = INPUT.read_text(encoding="utf-8-sig")
    _header, xml = captured.split("\n\n", 1)
    root = ET.fromstring(xml)
    records = []
    for number, query in enumerate(root.findall("./qgroup/query"), start=1):
        # xml.etree does not retain parent pointers, so locate the owning group.
        owner = next(group for group in root.findall("qgroup") if query in list(group))
        label = clean_label(query.attrib["label"])
        records.append(
            {
                "cq": f"endpoint-example-{number:02d}",
                "prompt": label,
                "databases": ["Camera dei Deputati Knowledge Graph"],
                "sparql": (query.text or "").strip(),
                "source": {
                    "source_id": "camera-dei-deputati-endpoint-examples",
                    "url": SOURCE_URL,
                    "location": f"{owner.attrib['label']}; query {number}: {label}",
                },
            }
        )
    if len(records) != 22:
        raise ValueError(f"Expected 22 endpoint examples, found {len(records)}")
    OUTPUT.write_text(
        "".join(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n" for record in records),
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
