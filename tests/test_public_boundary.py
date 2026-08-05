from __future__ import annotations

import importlib.util
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "check_public_tree.py"
SPEC = importlib.util.spec_from_file_location("check_public_tree", SCRIPT)
assert SPEC and SPEC.loader
boundary = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(boundary)


def test_private_markers_detected_independent_of_json_formatting() -> None:
    pretty = '{\n  "split"  :  "private_holdout"\n}\n'
    nested = '{"outer": {"benchmark_disposition": "withheld"}}\n'
    assert boundary.structured_private_marker("artifact.json", pretty)
    assert boundary.structured_private_marker("artifact.json", nested)


def test_private_export_name_is_extension_independent() -> None:
    assert boundary.PRIVATE_NAME_RE.match("musparql-holdout-private-backup.enc")


def test_known_local_work_trees_are_forbidden() -> None:
    assert boundary.matches("var/tmp/example.json", boundary.FORBIDDEN_PATHS)
    assert boundary.matches("var/cache/dumps/example.ttl", boundary.FORBIDDEN_PATHS)
    assert boundary.matches("build/public-releases/v8/benchmark.jsonl", boundary.FORBIDDEN_PATHS)
    assert boundary.matches("review/sparql_correction_data.js", boundary.FORBIDDEN_PATHS)
    assert boundary.CORRECTION_NAME_RE.match("musparql-sparql-correction-review-abc.json")


def test_correction_schemas_are_publication_tripwires() -> None:
    assert boundary.contains_correction_artifact(
        {"schema": "musparql.sparql-correction-review-export.v1"}
    )
    assert not boundary.contains_correction_artifact({"schema": {"type": "object"}})
