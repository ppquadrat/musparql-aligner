from __future__ import annotations

import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_correction_state_is_scoped_to_reviewer() -> None:
    app = (ROOT / "review" / "correction_app.js").read_text(encoding="utf-8")
    assert "musparql-sparql-correction:v3:${data.dataset_id}:${data.bundle_digest}:${data.reviewer_id}" in app


def test_manual_change_clears_agent_derived_edit_metadata() -> None:
    script = r'''
const assert = require("node:assert/strict");
const fs = require("node:fs");
const vm = require("node:vm");
const sandbox = {window:{SPARQL_CORRECTION_DATA:null}};
vm.runInNewContext(fs.readFileSync("review/correction_app.js", "utf8"), sandbox);
const update = sandbox.window.MUSPARQL_CORRECTION_SCHEMA.draftAfterInput;
const previous = {
  proposal_origin:"agent", edit_type:"syntax_correction", rationale:"Agent rationale",
  evidence_ids:["e1"], agent_suggestion:{output:{proposed_sparql:"SELECT * WHERE {}"}},
};
const changed = update(previous, {
  proposed_sparql:"SELECT ?s WHERE { ?s ?p ?o }", edit_type:"syntax_correction",
  rationale:"Agent rationale", evidence_ids:["e1"], reviewer_note:"",
});
assert.equal(changed.proposal_origin, "human");
assert.equal(changed.agent_suggestion, null);
assert.equal(changed.edit_type, "");
assert.equal(changed.rationale, "");
assert.deepEqual(JSON.parse(JSON.stringify(changed.evidence_ids)), []);
'''
    subprocess.run(["node", "-e", script], check=True, cwd=ROOT)


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
