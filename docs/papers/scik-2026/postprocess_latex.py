#!/usr/bin/env python3
"""Apply small CEURART compatibility fixes to Pandoc's LaTeX output."""

from __future__ import annotations

import re
from pathlib import Path


PATH = Path(__file__).resolve().parent / "manuscript.tex"


def main() -> None:
    text = PATH.read_text(encoding="utf-8")
    # Pandoc 3.9 emits an accessibility `alt` graphics key that is newer than
    # the TeX Live bundle used by the reproducible Tectonic compiler.
    text = re.sub(r",alt=\{.*?\}(?=\])", "", text)
    # Keep the acknowledgments and disclosure out of the numbered hierarchy.
    text = text.replace("\\section{Acknowledgments}", "\\section*{Acknowledgments}")
    text = text.replace(
        "\\section{Declaration on Generative AI}",
        "\\section*{Declaration on Generative AI}",
    )
    PATH.write_text(text, encoding="utf-8")


if __name__ == "__main__":
    main()
