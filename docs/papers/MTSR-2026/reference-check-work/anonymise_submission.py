from __future__ import annotations

import sys
import tempfile
import zipfile
from pathlib import Path

from lxml import etree


W_NS = "http://schemas.openxmlformats.org/wordprocessingml/2006/main"
R_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
PKG_REL_NS = "http://schemas.openxmlformats.org/package/2006/relationships"
NS = {"w": W_NS, "r": R_NS, "pr": PKG_REL_NS}


def paragraph_text(paragraph: etree._Element) -> str:
    return "".join(paragraph.xpath(".//w:t/text()", namespaces=NS))


def find_paragraph(body: etree._Element, prefix: str) -> etree._Element:
    matches = [
        p
        for p in body.xpath("./w:p", namespaces=NS)
        if paragraph_text(p).startswith(prefix)
    ]
    if len(matches) != 1:
        raise ValueError(f"Expected one paragraph beginning {prefix!r}, found {len(matches)}")
    return matches[0]


def replace_paragraph_text(paragraph: etree._Element, text: str) -> None:
    ppr = paragraph.find(f"{{{W_NS}}}pPr")
    for child in list(paragraph):
        if child is not ppr:
            paragraph.remove(child)
    run = etree.SubElement(paragraph, f"{{{W_NS}}}r")
    text_node = etree.SubElement(run, f"{{{W_NS}}}t")
    text_node.text = text


def replace_footnote_text(footnotes_root: etree._Element, footnote_id: str, text: str) -> None:
    matches = footnotes_root.xpath(
        f"./w:footnote[@w:id='{footnote_id}']", namespaces=NS
    )
    if len(matches) != 1:
        raise ValueError(f"Expected one footnote with id {footnote_id}")
    paragraph = matches[0].find(f"{{{W_NS}}}p")
    if paragraph is None:
        raise ValueError(f"Footnote {footnote_id} has no paragraph")
    ppr = paragraph.find(f"{{{W_NS}}}pPr")
    for child in list(paragraph):
        if child is not ppr:
            paragraph.remove(child)
    marker_run = etree.SubElement(paragraph, f"{{{W_NS}}}r")
    marker_props = etree.SubElement(marker_run, f"{{{W_NS}}}rPr")
    marker_style = etree.SubElement(marker_props, f"{{{W_NS}}}rStyle")
    marker_style.set(f"{{{W_NS}}}val", "FootnoteCharacters")
    etree.SubElement(marker_run, f"{{{W_NS}}}footnoteRef")
    text_run = etree.SubElement(paragraph, f"{{{W_NS}}}r")
    etree.SubElement(text_run, f"{{{W_NS}}}tab")
    text_node = etree.SubElement(text_run, f"{{{W_NS}}}t")
    text_node.text = text


def remove_relationships(rel_path: Path, ids: set[str]) -> None:
    parser = etree.XMLParser(remove_blank_text=False)
    tree = etree.parse(str(rel_path), parser)
    root = tree.getroot()
    for relationship in list(root):
        if relationship.get("Id") in ids:
            root.remove(relationship)
    tree.write(str(rel_path), xml_declaration=True, encoding="UTF-8", standalone=True)


def main(source: Path, output: Path) -> None:
    with tempfile.TemporaryDirectory(prefix="mtsr-anon-") as temp_dir:
        temp = Path(temp_dir)
        with zipfile.ZipFile(source) as archive:
            archive.extractall(temp)

        parser = etree.XMLParser(remove_blank_text=False)
        document_path = temp / "word" / "document.xml"
        document_tree = etree.parse(str(document_path), parser)
        body = document_tree.getroot().find(f"{{{W_NS}}}body")
        if body is None:
            raise ValueError("Word document has no body")

        for prefix in (
            "Polina Proutskova",
            "Industry Commons Foundation",
            "polina.proutskova@industrycommons.net",
        ):
            body.remove(find_paragraph(body, prefix))

        repository_paragraph = find_paragraph(body, "Musparql is an open-source workflow")
        replace_paragraph_text(
            repository_paragraph,
            "Musparql is an open-source workflow and review workbench for building natural-language/SPARQL evaluation data from existing domain knowledge-graph artifacts. It brings together source and query acquisition, evidence retrieval and alignment, LLM-assisted provisional formulation, execution records, expert review, and versioned publication. The software, documentation, runbooks, and reviewed v10 benchmark snapshot are available in an anonymised repository; access details are withheld for double-blind review.",
        )

        limitations_paragraph = find_paragraph(body, "The claims are bounded by one domain")
        replace_paragraph_text(
            limitations_paragraph,
            "The claims are bounded by one domain and one principal reviewer. The reviewer had relevant domain and knowledge-graph modelling expertise, but the review was not independent, and equivalent source-creator expertise was not available for every graph. The pipeline has generated candidate pairs in other domains outside music, but expert review of those candidates remains outstanding. Holdout separation is implemented but not evaluated as a secrecy or leakage study, and the linguistic-dimensions workbench has not yet produced a multi-reviewer analysis.",
        )

        body.remove(find_paragraph(body, "Acknowledgments."))

        document_tree.write(
            str(document_path),
            xml_declaration=True,
            encoding="UTF-8",
            standalone=True,
        )

        footnotes_path = temp / "word" / "footnotes.xml"
        footnotes_tree = etree.parse(str(footnotes_path), parser)
        footnotes_root = footnotes_tree.getroot()
        replace_footnote_text(
            footnotes_root,
            "2",
            "An LLM-assisted source-discovery and acquisition process has since been developed and tested. It is not described in the paper because it was not used in v10 benchmark construction. Repository access details are withheld for double-blind review.",
        )
        for footnote_id in ("3", "4"):
            matches = footnotes_root.xpath(
                f"./w:footnote[@w:id='{footnote_id}']", namespaces=NS
            )
            if len(matches) != 1:
                raise ValueError(f"Expected one footnote with id {footnote_id}")
            footnotes_root.remove(matches[0])
        footnotes_tree.write(
            str(footnotes_path),
            xml_declaration=True,
            encoding="UTF-8",
            standalone=True,
        )

        remove_relationships(temp / "word" / "_rels" / "document.xml.rels", {"rId2", "rId3"})
        remove_relationships(temp / "word" / "_rels" / "footnotes.xml.rels", {"rId1"})

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
        raise SystemExit("Usage: anonymise_submission.py SOURCE.docx OUTPUT.docx")
    main(Path(sys.argv[1]), Path(sys.argv[2]))
