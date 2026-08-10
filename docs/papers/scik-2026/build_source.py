#!/usr/bin/env python3
"""Prepare the Sci-K Markdown draft for deterministic Pandoc conversion."""

from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
SOURCE = ROOT / "scik-2026-musparql-draft.md"
OUTPUT = Path(__file__).resolve().parent / "manuscript.md"

CITATIONS = {
    1: "harris2013sparql",
    2: "raimond2007music",
    3: "lopez2013evaluating",
    4: "trivedi2017lcquad",
    5: "dubey2019lcquad2",
    6: "hoffner2017survey",
    7: "diefenbach2018core",
    8: "steinmetz2021kgqa",
    9: "jiang2022generalizability",
    10: "usbeck2023qald10",
    11: "banerjee2023scholarlyqald",
    12: "tramp2025text2sparql",
    13: "text2sparql2026datasets",
    14: "liu2024spinach",
    15: "walter2026wdql",
    16: "bolleman2025bioinformatics",
    17: "wisniewski2019competency",
    18: "taghzouti2025q2forge",
    19: "pond2025sesemmi",
    20: "deberardinis2023polifonia",
    21: "tsaneva2024enhancing",
    22: "gruninger1995competency",
    23: "morales2024mmkg",
    24: "proutskova2022jazz",
    25: "daquino2017musical",
    26: "salatino2025ceurpolicy",
    27: "morales2023meetups",
}


def citation(match: re.Match[str]) -> str:
    value = match.group(1).replace("–", "-")
    numbers: list[int] = []
    for part in re.split(r",\s*", value):
        if "-" in part:
            start, end = map(int, part.split("-", 1))
            numbers.extend(range(start, end + 1))
        else:
            numbers.append(int(part))
    return "[" + "; ".join(f"@{CITATIONS[number]}" for number in numbers) + "]"


def main() -> None:
    text = SOURCE.read_text(encoding="utf-8")
    body = text.split("## 1. Introduction", 1)[1]
    body = "## 1. Introduction" + body.split("## References", 1)[0]

    # Let LaTeX number the numbered sections and subsections. The source starts
    # at Markdown level 2 because it also contains non-paper metadata above the
    # abstract, so promote its paper headings by one level.
    body = re.sub(
        r"^(#{2,3})\s+\d+(?:\.\d+)*\.?\s+",
        lambda match: match.group(1)[1:] + " ",
        body,
        flags=re.MULTILINE,
    )
    body = re.sub(
        r"^(#{2,3})\s+(Acknowledgments|Declaration on Generative AI)$",
        lambda match: match.group(1)[1:] + " " + match.group(2),
        body,
        flags=re.MULTILINE,
    )

    # Convert the draft's stable numeric references to Pandoc citations.
    body = re.sub(r"\[(\d+(?:\s*[–-]\s*\d+)?(?:,\s*\d+(?:\s*[–-]\s*\d+)?)*)\]", citation, body)

    # The draft bibliography includes the CEUR policy as item 26; attach it to
    # the disclosure so the formatted bibliography preserves that source.
    body = body.replace(
        "takes responsibility for the content of the manuscript.",
        "takes responsibility for the content of the manuscript [@salatino2025ceurpolicy].",
    )

    # Fold the draft's explicit figure captions into semantic figures.
    figure_pattern = re.compile(
        r"!\[[^\]]*\]\(([^)]+)\)\n\n\*\*Figure\s+\d+:\*\*\s+([^\n]+)"
    )

    def figure(match: re.Match[str]) -> str:
        path, caption = match.groups()
        if "workflow" in path:
            path = "scik-2026-musparql-workflow.png"
            label, width = "fig:workflow", "95%"
        else:
            label, width = "fig:review-ui", "100%"
        return f"![{caption}]({path}){{#{label} width={width}}}"

    body = figure_pattern.sub(figure, body)

    # Pandoc recognises a colon-led paragraph after a pipe table as its caption.
    body = re.sub(
        r"^\*\*Table\s+\d+:\*\*\s+(.+)$",
        r": \1",
        body,
        flags=re.MULTILINE,
    )

    OUTPUT.write_text(body.strip() + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
