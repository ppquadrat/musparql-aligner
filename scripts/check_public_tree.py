#!/usr/bin/env python3
"""Fail when staged or committed blobs cross the holdout boundary."""
from __future__ import annotations

import argparse
import fnmatch
import json
import re
import subprocess
from pathlib import PurePosixPath
from typing import Any, Iterable


FORBIDDEN_PATHS = (
    "benchmark/v*/holdout.jsonl",
    "review/exports/*",
    "review/private/*",
    "review/public_exports/*",
    "benchmark/v*/included.jsonl",
    "benchmark/v*/dismissed.jsonl",
    "benchmark/v*/linguistic_annotations.jsonl",
    "kg_queries.jsonl",
    "llm_inputs.jsonl",
    "llm_outputs*.jsonl",
    "runs/*/*",
    "review/review_data*.js",
    "review/sparql_correction_data*.js",
    "sparql_correction_candidates*.jsonl",
    "**/musparql-sparql-correction-review-*",
    "prompts/llm_nl_generation.inputs.jsonl",
    "var/*",
    "build/*",
    ".vscode/*",
    "dumps/*",
    "repos/*",
    "tmp/*",
)

PRIVATE_NAME_RE = re.compile(r"^musparql-holdout-private-", re.IGNORECASE)
CORRECTION_NAME_RE = re.compile(
    r"^(?:musparql-sparql-correction-review-|sparql_correction_(?:candidates|data)(?:[._-]|$))",
    re.IGNORECASE,
)
CORRECTION_SCHEMAS = {
    "musparql.sparql-correction-candidate.v1",
    "musparql.sparql-correction-bundle.v1",
    "musparql.sparql-correction-review-export.v1",
    "musparql.sparql-correction-ui-execution.v1",
    "musparql.sparql-correction-agent-suggestion.v1",
}
PRIVATE_TEXT_RE = re.compile(
    r'(?:(?:"|\b)(?:split|benchmark_disposition|kind)(?:"|\b)\s*:\s*'
    r'(?:"(?:private_holdout|withheld|private_holdout_export)"|'
    r"'(?:private_holdout|withheld|private_holdout_export)'))",
    re.IGNORECASE,
)
DATA_SUFFIXES = {".json", ".jsonl"}


def git(*args: str) -> str:
    return subprocess.run(
        ["git", *args], check=True, text=True,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    ).stdout


def candidate_paths(*, staged: bool, rev: str | None) -> list[str]:
    if staged:
        output = git("diff", "--cached", "--name-only", "--diff-filter=ACMR", "-z")
    else:
        output = git("ls-tree", "-r", "--name-only", "-z", rev or "HEAD")
    return sorted(path for path in output.split("\0") if path)


def matches(path: str, patterns: Iterable[str]) -> bool:
    return any(fnmatch.fnmatchcase(path, pattern) for pattern in patterns)


def blob_text(path: str, *, staged: bool, rev: str | None) -> str:
    return git("show", f":{path}" if staged else f"{rev or 'HEAD'}:{path}")


def contains_private_marker(value: Any) -> bool:
    if isinstance(value, dict):
        if value.get("kind") == "private_holdout_export":
            return True
        if value.get("split") == "private_holdout":
            return True
        if value.get("benchmark_disposition") == "withheld":
            return True
        return any(contains_private_marker(item) for item in value.values())
    if isinstance(value, list):
        return any(contains_private_marker(item) for item in value)
    return False


def contains_correction_artifact(value: Any) -> bool:
    if isinstance(value, dict):
        schema = value.get("schema")
        return (isinstance(schema, str) and schema in CORRECTION_SCHEMAS) or any(
            contains_correction_artifact(item) for item in value.values()
        )
    if isinstance(value, list):
        return any(contains_correction_artifact(item) for item in value)
    return False


def structured_private_marker(path: str, text: str) -> bool:
    suffix = PurePosixPath(path).suffix.lower()
    if suffix not in DATA_SUFFIXES:
        return False
    try:
        values = (
            [json.loads(line) for line in text.splitlines() if line.strip()]
            if suffix == ".jsonl"
            else [json.loads(text)]
        )
    except (json.JSONDecodeError, UnicodeDecodeError):
        return PRIVATE_TEXT_RE.search(text) is not None
    return any(contains_private_marker(value) for value in values)


def check(*, staged: bool = False, rev: str | None = None) -> list[str]:
    errors: list[str] = []
    for path in candidate_paths(staged=staged, rev=rev):
        name = PurePosixPath(path).name
        if PRIVATE_NAME_RE.match(name) or CORRECTION_NAME_RE.match(name) or matches(path, FORBIDDEN_PATHS):
            if path.startswith("tests/fixtures/") and "synthetic" in name.lower():
                pass
            else:
                errors.append(f"forbidden tracked path: {path}")
                continue
        if PurePosixPath(path).suffix.lower() not in DATA_SUFFIXES:
            continue
        try:
            text = blob_text(path, staged=staged, rev=rev)
        except (UnicodeDecodeError, subprocess.CalledProcessError) as exc:
            errors.append(f"could not inspect data blob {path}: {exc}")
            continue
        if structured_private_marker(path, text):
            errors.append(f"private review marker in data blob: {path}")
        try:
            values = [json.loads(line) for line in text.splitlines() if line.strip()] if path.endswith(".jsonl") else [json.loads(text)]
        except json.JSONDecodeError:
            values = []
        if not path.startswith("tests/fixtures/") and any(contains_correction_artifact(value) for value in values):
            errors.append(f"SPARQL correction artifact in public data blob: {path}")
    return errors


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    scope = parser.add_mutually_exclusive_group()
    scope.add_argument("--staged", action="store_true", help="Inspect staged index blobs.")
    scope.add_argument("--rev", help="Inspect the tree at this commit revision (default: HEAD).")
    args = parser.parse_args()
    errors = check(staged=args.staged, rev=args.rev)
    if errors:
        raise SystemExit("Public-tree boundary check failed:\n- " + "\n- ".join(errors))
    label = "staged blobs" if args.staged else f"tree {args.rev or 'HEAD'}"
    print(f"Public-tree boundary check passed: {label}")


if __name__ == "__main__":
    main()
