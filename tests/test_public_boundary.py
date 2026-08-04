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
    assert boundary.matches("tmp/example.json", boundary.FORBIDDEN_PATHS)
    assert boundary.matches("dumps/example.ttl", boundary.FORBIDDEN_PATHS)
