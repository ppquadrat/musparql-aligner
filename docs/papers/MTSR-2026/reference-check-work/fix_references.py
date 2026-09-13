from __future__ import annotations

import re
import shutil
import sys
import tempfile
import zipfile
from pathlib import Path

from lxml import etree


W_NS = "http://schemas.openxmlformats.org/wordprocessingml/2006/main"
NS = {"w": W_NS}


def paragraph_text(paragraph: etree._Element) -> str:
    return "".join(paragraph.xpath(".//w:t/text()", namespaces=NS))


def replace_in_paragraph(paragraph: etree._Element, old: str, new: str) -> None:
    text_nodes = paragraph.xpath(".//w:t", namespaces=NS)
    for node in text_nodes:
        if node.text and old in node.text:
            node.text = node.text.replace(old, new)
            return
    raise ValueError(f"Could not find {old!r} in paragraph {paragraph_text(paragraph)!r}")


def find_paragraph(paragraphs: list[etree._Element], prefix: str) -> etree._Element:
    matches = [p for p in paragraphs if paragraph_text(p).startswith(prefix)]
    if len(matches) != 1:
        raise ValueError(f"Expected one paragraph beginning {prefix!r}, found {len(matches)}")
    return matches[0]


def main(source: Path, output: Path) -> None:
    with tempfile.TemporaryDirectory(prefix="mtsr-refs-") as temp_dir:
        temp = Path(temp_dir)
        with zipfile.ZipFile(source) as archive:
            archive.extractall(temp)

        document_path = temp / "word" / "document.xml"
        parser = etree.XMLParser(remove_blank_text=False)
        tree = etree.parse(str(document_path), parser)
        root = tree.getroot()
        body = root.find(f"{{{W_NS}}}body")
        if body is None:
            raise ValueError("Word document has no body")
        paragraphs = list(body.xpath("./w:p", namespaces=NS))

        replacements = [
            (
                "SPARQL gives precise access",
                [("[1]", "[9]")],
            ),
            (
                "Established knowledge-graph question answering benchmarks",
                [("[3-5]", "[6, 14, 23]")],
            ),
            (
                "QALD, LC-QuAD 2.0",
                [
                    ("[3-5, 7-9]", "[2, 6, 14, 21, 23]"),
                    ("[5, 6]", "[10, 11]"),
                ],
            ),
            (
                "Recent datasets begin closer to real information needs.",
                [
                    ("[10-12]", "[3, 13, 24]"),
                    ("[13, 14, 18]", "[8, 16, 20, 26]"),
                    ("[15-17, 19-21]", "[4, 5, 16-18, 22]"),
                ],
            ),
            (
                "Musparql connects these strands with research-data curation.",
                [
                    (
                        "FAIR, PROV-O, and datasheets for datasets emphasise reusable objects, explicit provenance, and documented creation decisions [22-24].",
                        "FAIR, PROV-O, datasheets for datasets, DCAT, and Croissant emphasise reusable objects, explicit provenance, documented creation decisions, and machine-readable publication metadata [1, 7, 12, 15, 25].",
                    )
                ],
            ),
            (
                "Musparql serialises this record",
                [("[22]", "[12]")],
            ),
            (
                "The public v10 snapshot was built",
                [
                    ("[19]", "{CIT_MMKG}"),
                    ("[16]", "{CIT_ORGANS}"),
                    ("[20]", "{CIT_JAZZ}"),
                    ("[21]", "{CIT_MUSOW}"),
                    ("[15]", "{CIT_LINKEDMUSIC}"),
                    ("{CIT_MMKG}", "[16]"),
                    ("{CIT_ORGANS}", "[5]"),
                    ("{CIT_JAZZ}", "[18]"),
                    ("{CIT_MUSOW}", "[4]"),
                    ("{CIT_LINKEDMUSIC}", "[17]"),
                ],
            ),
            (
                "While the missing band information",
                [("[2]", "[19]")],
            ),
        ]

        for prefix, paragraph_replacements in replacements:
            paragraph = find_paragraph(paragraphs, prefix)
            for old, new in paragraph_replacements:
                replace_in_paragraph(paragraph, old, new)

        reference_paragraphs = [
            p
            for p in paragraphs
            if p.xpath("./w:pPr/w:pStyle[@w:val='referenceitem']", namespaces=NS)
        ]
        if len(reference_paragraphs) != 26:
            raise ValueError(f"Expected 26 reference paragraphs, found {len(reference_paragraphs)}")

        order = [
            "Albertoni,",
            "Banerjee,",
            "Bolleman,",
            "Daquino,",
            "de Berardinis,",
            "Dubey,",
            "Gebru,",
            "Grüninger,",
            "Harris,",
            "Höffner,",
            "Jiang,",
            "Lebo,",
            "Liu,",
            "Lopez,",
            "MLCommons:",
            "Morales Tirado,",
            "Pond,",
            "Proutskova,",
            "Raimond,",
            "Taghzouti,",
            "TEXT2SPARQL:",
            "Tsaneva,",
            "Usbeck,",
            "Walter,",
            "Wilkinson,",
            "Wiśniewski,",
        ]
        by_prefix: dict[str, etree._Element] = {}
        for paragraph in reference_paragraphs:
            text = paragraph_text(paragraph)
            matches = [prefix for prefix in order if text.startswith(prefix)]
            if len(matches) != 1:
                raise ValueError(f"Could not identify reference: {text!r}")
            by_prefix[matches[0]] = paragraph

        if len(by_prefix) != len(order):
            raise ValueError("Reference prefixes were not unique")

        anchor = reference_paragraphs[0]
        insert_at = body.index(anchor)
        for paragraph in reference_paragraphs:
            body.remove(paragraph)
        for offset, prefix in enumerate(order):
            body.insert(insert_at + offset, by_prefix[prefix])

        tree.write(
            str(document_path),
            xml_declaration=True,
            encoding="UTF-8",
            standalone=True,
        )

        output.parent.mkdir(parents=True, exist_ok=True)
        if output.exists():
            output.unlink()
        with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as archive:
            for path in sorted(temp.rglob("*")):
                if path.is_file():
                    archive.write(path, path.relative_to(temp))

    if not zipfile.is_zipfile(output):
        raise ValueError("Output is not a valid DOCX archive")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        raise SystemExit("Usage: fix_references.py SOURCE.docx OUTPUT.docx")
    main(Path(sys.argv[1]), Path(sys.argv[2]))
