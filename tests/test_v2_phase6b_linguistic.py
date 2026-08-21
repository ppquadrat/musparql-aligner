from __future__ import annotations

import json
from pathlib import Path
import subprocess

import pytest
from jsonschema import Draft202012Validator, FormatChecker

from musparql.linguistic_dimensions import (
    build_bundle,
    normalized_trial,
    text_digest,
    validate_bundle,
)


ROOT = Path(__file__).resolve().parents[1]


def test_linguistic_workbench_confirms_submission_inline() -> None:
    html = (ROOT / "review" / "linguistic" / "index.html").read_text(encoding="utf-8")
    app = (ROOT / "review" / "linguistic" / "app.js").read_text(encoding="utf-8")

    assert 'id="submissionStatus"' in html
    assert "Thank you — your annotations were submitted." in html
    assert 'id="backToAssignmentsLink"' in html
    assert "You completed ${percentage}% of this assignment" in app


def stimulus(trial_id: str, stratum: str = "source-vs-llm") -> dict:
    sparql = f"SELECT ?item WHERE {{ ?item a <urn:{trial_id}> }}"
    literal_text = f"Which items have type {trial_id}?"
    first = f"What are the {trial_id} items?"
    second = f"Show me everything classified as {trial_id}."
    return {
        "trial_id": trial_id,
        "kg_id": "synthetic-kg",
        "query_id": f"query-{trial_id}",
        "query_label": f"Synthetic {trial_id}",
        "sparql": sparql,
        "sparql_version": "v1",
        "sparql_digest": text_digest(sparql),
        "literal": {
            "formulation_id": f"literal-{trial_id}", "version": "v1",
            "text": literal_text, "digest": text_digest(literal_text),
            "validated": True, "validation_provenance": {"review_id": "synthetic-validation"},
        },
        "candidates": [
            {"formulation_id": f"candidate-{trial_id}-1", "version": "v1", "text": first, "digest": text_digest(first), "provenance": {"origin": "synthetic-source"}},
            {"formulation_id": f"candidate-{trial_id}-2", "version": "v1", "text": second, "digest": text_digest(second), "provenance": {"origin": "synthetic-llm"}},
        ],
        "eligible": True, "non_holdout": True, "presentation_arity": 3,
        "sampling_stratum": stratum, "contrast_id": "synthetic-contrast",
    }


def test_deterministic_balanced_bundle_and_contract() -> None:
    pool = [stimulus("a1", "a"), stimulus("a2", "a"), stimulus("b1", "b"), stimulus("b2", "b")]
    first = build_bundle(pool, dataset_id="synthetic-linguistic", seed="recorded-seed", target_trials=3)
    second = build_bundle(reversed(pool), dataset_id="synthetic-linguistic", seed="recorded-seed", target_trials=3)
    assert first == second
    assert {item["sampling_stratum"] for item in first["records"]} == {"a", "b"}
    validate_bundle(first)
    schema = json.loads((ROOT / "schemas/linguistic_stimulus_bundle.schema.json").read_text())
    Draft202012Validator(schema, format_checker=FormatChecker()).validate(first)


def test_bundle_validation_requires_reproducibility_metadata_and_unique_trials() -> None:
    item = stimulus("one")
    bundle = build_bundle([item], dataset_id="synthetic-linguistic", seed="recorded-seed")
    for field in ("randomization", "sampling"):
        invalid = dict(bundle)
        invalid.pop(field)
        with pytest.raises(ValueError):
            validate_bundle(invalid)
    invalid = json.loads(json.dumps(bundle))
    invalid["sampling"]["target_trials"] = 2
    with pytest.raises(ValueError):
        validate_bundle(invalid)
    with pytest.raises(ValueError, match="Duplicate linguistic trial_id"):
        build_bundle([item, item], dataset_id="synthetic-linguistic", seed="recorded-seed")


def test_stimulus_rejects_anchor_mutation_holdout_and_two_way() -> None:
    for mutation in ("digest", "holdout", "nested-holdout", "two-way"):
        record = stimulus("unsafe")
        if mutation == "digest": record["literal"]["text"] = "mutated"
        if mutation == "holdout": record["non_holdout"] = False
        if mutation == "nested-holdout": record["candidates"][0]["provenance"]["split"] = "private_holdout"
        if mutation == "two-way": record["presentation_arity"] = 2
        with pytest.raises(ValueError):
            build_bundle([record], dataset_id="synthetic", seed="seed")


def test_normalized_rated_and_non_rating_outcomes() -> None:
    item = stimulus("one")
    ids = [candidate["formulation_id"] for candidate in item["candidates"]]
    ratings = {candidate_id: {"naturalness": 0, "pragmatism": -25, "interpretation_room": 80} for candidate_id in ids}
    rated = normalized_trial(item, assignment_id="assignment-synthetic", dataset_id="synthetic", reviewer_id="reviewer-0042", display_order=list(reversed(ids)), outcome="rated", ratings=ratings, started_at="2026-08-19T10:00:00Z", completed_at="2026-08-19T10:01:00Z")
    assert rated["display_order"] == list(reversed(ids))
    assert rated["ratings"][ids[0]]["naturalness"] == 0
    export = {"schema": "musparql.linguistic-annotation-export.v1", "assignment_id": "assignment-synthetic", "dataset_id": "synthetic", "reviewer_id": "reviewer-0042", "task_design_version": "phase-6b-v1", "exported_at": "2026-08-19T10:01:00Z", "annotations": [rated]}
    export_schema = json.loads((ROOT / "schemas/linguistic_annotation_export.schema.json").read_text())
    Draft202012Validator(export_schema, format_checker=FormatChecker()).validate(export)
    flagged = normalized_trial(item, assignment_id="assignment-synthetic", dataset_id="synthetic", reviewer_id="reviewer-0042", display_order=ids, outcome="literal_inaccurate", proposed_literal="A correction", started_at="2026-08-19T10:00:00Z", completed_at="2026-08-19T10:01:00Z")
    assert "ratings" not in flagged and flagged["proposed_literal"] == "A correction"
    with pytest.raises(ValueError):
        normalized_trial(item, assignment_id="assignment-synthetic", dataset_id="synthetic", reviewer_id="reviewer-0042", display_order=ids, outcome="rated", ratings={ids[0]: ratings[ids[0]]}, started_at="2026-08-19T10:00:00Z", completed_at="2026-08-19T10:01:00Z")


def test_browser_contract_tracks_touch_and_assignment_isolation() -> None:
    script = r'''
const assert = require("node:assert/strict");
const fs = require("node:fs");
const vm = require("node:vm");
const sandbox = {window:{REVIEW_DATA:null, MUSPARQL_HOSTED_CONTEXT:null}, document:{getElementById:()=>null}};
vm.runInNewContext(fs.readFileSync("review/linguistic/app.js", "utf8"), sandbox);
const api = sandbox.window.MUSPARQL_LINGUISTIC;
assert.notEqual(api.stateKey({dataset_id:"d"},{reviewer_id:"reviewer-0042",assignment_id:"a"}), api.stateKey({dataset_id:"d"},{reviewer_id:"reviewer-0042",assignment_id:"b"}));
assert.deepEqual(JSON.parse(JSON.stringify(api.shuffled(["a","b"], () => 0))), ["b","a"]);
const merged = api.mergeState(
 {queue:["t3"],completed:{t1:{outcome:"rated"}},drafts:{t3:{}},skips:{t3:1},finished:false},
 {queue:["t3"],completed:{t2:{outcome:"cannot_assess"}},drafts:{t2:{},t3:{}},skips:{t3:2},finished:true},
 ["t1","t2","t3"]
);
assert.deepEqual(Object.keys(merged.completed).sort(), ["t1","t2"]);
assert.deepEqual(JSON.parse(JSON.stringify(merged.queue)), ["t3"]);
assert.equal(Object.hasOwn(merged.drafts,"t2"), false);
assert.equal(merged.skips.t3, 2);
assert.equal(merged.finished, false);
assert.equal(merged.includeSkipped, false);
const discarded = api.mergeState(
 {queue:["t3"],completed:{},drafts:{},skips:{t3:1},finished:false},
 {queue:["t3"],completed:{},drafts:{t3:{ratings:{}}},skips:{},finished:false},
 ["t3"], ["t3"]
);
assert.equal(Object.hasOwn(discarded.drafts,"t3"), false);
assert.equal(api.hasTouchedRatings({ratings:{c1:{naturalness:{value:25,touched:true}}}}), true);
assert.equal(api.hasTouchedRatings({ratings:{c1:{naturalness:{value:0,touched:false}}}}), false);
const stimulus = {
 trial_id:"t", query_id:"q", sparql_version:"v1", sparql_digest:"sha256:"+"a".repeat(64), presentation_arity:3,
 literal:{formulation_id:"l",version:"v1",digest:"sha256:"+"b".repeat(64)},
 candidates:[{formulation_id:"c1",version:"v1",digest:"sha256:"+"c".repeat(64)},{formulation_id:"c2",version:"v1",digest:"sha256:"+"d".repeat(64)}]
};
const draft = {display_order:["c2","c1"],started_at:"2026-08-19T10:00:00Z",ratings:{
 c1:{naturalness:{value:0,touched:false},pragmatism:{value:1,touched:true},interpretation_room:{value:2,touched:true}},
 c2:{naturalness:{value:0,touched:true},pragmatism:{value:1,touched:true},interpretation_room:{value:2,touched:true}}
}};
assert.throws(() => api.normalizedTrial(stimulus,{assignment_id:"a",reviewer_id:"reviewer-0042"},{dataset_id:"d"},draft,"rated"), /six controls/);
draft.ratings.c1.naturalness.touched = true;
const rated = api.normalizedTrial(stimulus,{assignment_id:"a",reviewer_id:"reviewer-0042"},{dataset_id:"d"},draft,"rated");
assert.equal(rated.ratings.c1.naturalness, 0);
const flagged = api.normalizedTrial(stimulus,{assignment_id:"a",reviewer_id:"reviewer-0042"},{dataset_id:"d"},draft,"literal_inaccurate",{proposed_literal:"fixed"});
assert.equal(Object.hasOwn(flagged,"ratings"), false);
'''
    subprocess.run(["node", "-e", script], cwd=ROOT, check=True)


def test_ui_has_no_provenance_or_manual_selection_controls() -> None:
    html = (ROOT / "review/linguistic/index.html").read_text(encoding="utf-8")
    app = (ROOT / "review/linguistic/app.js").read_text(encoding="utf-8")
    assert "Formulation A" not in html  # labels are assigned only after randomization
    assert "holdout" not in html.casefold()
    assert "manual" not in html.casefold()
    assert "candidate.provenance" not in app
    assert "validation_provenance" not in app
    assert "Compare all three formulations" not in html
    assert "sparql-details" in html
    assert "Discard and skip" in html
    assert "Include skipped" in html
    assert "Review skipped items" in html
