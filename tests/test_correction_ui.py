from __future__ import annotations

import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_synthetic_browser_workflow_and_persistence() -> None:
    result = subprocess.run(
        ["node", "tests/correction_ui_harness.js"],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "synthetic workflow: passed" in result.stdout
