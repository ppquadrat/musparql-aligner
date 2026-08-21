"""Build a versioned ISO 639-1 language-name snapshot from Unicode CLDR JSON."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re


TAG = re.compile(r"^[a-z]{2}$")


def build(source: Path, output: Path, version: str) -> None:
    payload = json.loads(source.read_text(encoding="utf-8"))
    languages = payload["main"]["en"]["localeDisplayNames"]["languages"]
    options = [
        {"tag": tag, "name": name}
        for tag, name in languages.items()
        if TAG.fullmatch(tag)
    ]
    options.sort(key=lambda item: (item["name"].casefold(), item["tag"]))
    if len(options) < 180 or len({item["tag"] for item in options}) != len(options):
        raise ValueError("CLDR input did not yield a complete unique ISO 639-1 option set")
    result = {
        "schema": "musparql.language-options.v1",
        "snapshot_id": f"unicode-cldr-{version}-en-iso639-1",
        "source": {
            "name": "Unicode Common Locale Data Repository",
            "version": version,
            "url": f"https://github.com/unicode-org/cldr-json/releases/tag/{version}",
        },
        "options": options,
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    rendered = json.dumps(result, ensure_ascii=False, indent=2) + "\n"
    temporary = output.with_name(f".{output.name}.{os.getpid()}.tmp")
    temporary.write_text(rendered, encoding="utf-8")
    os.replace(temporary, output)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--version", required=True)
    args = parser.parse_args(argv)
    build(args.source, args.output, args.version)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
