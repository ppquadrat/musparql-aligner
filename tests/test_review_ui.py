from pathlib import Path
import subprocess


ROOT = Path(__file__).resolve().parents[1]


def test_browser_review_state_is_scoped_to_reviewer() -> None:
    app = (ROOT / "review" / "app.js").read_text(encoding="utf-8")
    assert "musparql-review:schema4:${data.dataset_id}:${data.reviewer_id}" in app
    assert "musparql-review-compare:schema4:${data.dataset_id}:${data.reviewer_id}" in app
    assert 'data.reviewer_id === "reviewer-0001"' in app


def test_imports_reject_cross_reviewer_and_non_owner_legacy_state() -> None:
    script = r'''
const fs = require("node:fs");
const vm = require("node:vm");
const sandbox = {window:{REVIEW_DATA:null}, document:{getElementById:()=>null, querySelectorAll:()=>[]}};
vm.runInNewContext(fs.readFileSync("review/app.js", "utf8"), sandbox);
const validate = sandbox.window.MUSPARQL_REVIEW_SCHEMA.validateReviewerImport;
validate({reviewer_id:"reviewer-0002"}, {reviewer_id:"reviewer-0002"});
if (!(() => { try { validate({reviewer_id:"reviewer-0001"}, {reviewer_id:"reviewer-0002"}); return false; } catch (_) { return true; } })()) process.exit(1);
if (!(() => { try { validate({}, {reviewer_id:"reviewer-0002"}); return false; } catch (_) { return true; } })()) process.exit(2);
validate({}, {reviewer_id:"reviewer-0001"});
'''
    subprocess.run(["node", "-e", script], check=True, cwd=ROOT)


def test_initial_review_does_not_collect_linguistic_dimensions() -> None:
    html = (ROOT / "review" / "index.html").read_text(encoding="utf-8")
    app = (ROOT / "review" / "app.js").read_text(encoding="utf-8")

    removed_control_ids = {
        "naturalnessInput",
        "pragmatismInput",
        "roomForInterpretationInput",
        "requiresGraphContextKnowledgeInput",
        "clearInterpretiveBtn",
    }
    for control_id in removed_control_ids:
        assert control_id not in html
        assert control_id not in app


def test_holdout_selector_export_controls_are_wired_for_both_review_modes() -> None:
    html = (ROOT / "review" / "index.html").read_text(encoding="utf-8")
    app = (ROOT / "review" / "app.js").read_text(encoding="utf-8")

    assert 'id="exportHoldoutSelectorsBtn"' in html
    assert 'id="holdoutSelectorsInput"' in html
    assert 'id="holdoutSelectorDialog"' in html
    assert 'id="chooseExistingSelectorsBtn"' in html
    assert 'id="createNewSelectorsBtn"' in html
    assert 'accept=".json,.jsonl,application/json,application/x-ndjson"' in html
    assert app.count("bindHoldoutSelectorExport(() =>") == 2
    assert "holdoutSelectorDialog.showModal()" in app
    assert "els.chooseExistingSelectorsBtn.addEventListener" in app
    assert 'downloadText(content, "selectors.jsonl"' in app
    assert "holdout_selectors.jsonl" not in app
    assert "selectorUpdatesForInitialReview" in app
    assert "selectorUpdatesForCompareReview" in app
    assert 'data.holdout_input_policy !== "identity_private_filtered_upstream"' in app


def test_holdout_selector_parsing_merge_and_validation() -> None:
    script = r'''
const assert = require("node:assert/strict");
const fs = require("node:fs");
const vm = require("node:vm");
const sandbox = {
  window: { REVIEW_DATA: null, alert: () => {} },
  document: { getElementById: () => null, querySelectorAll: () => [] },
};
vm.runInNewContext(fs.readFileSync("review/app.js", "utf8"), sandbox);
const schema = sandbox.window.MUSPARQL_REVIEW_SCHEMA;
const digestA = "a".repeat(64);
const digestB = "b".repeat(64);

const parsedJsonl = schema.parseHoldoutSelectors(
  JSON.stringify({kg_id:"kg-z", query_id:"q-z"}) + "\n" +
  JSON.stringify({kg_id:"kg-a", query_id:"q-a", sparql_version:0, sparql_hash:digestA}) + "\n"
);
assert.equal(parsedJsonl.length, 2);
const parsedArray = schema.parseHoldoutSelectors(JSON.stringify([{kg_id:"kg", query_id:"q"}]));
assert.equal(parsedArray.length, 1);
assert.equal(schema.parseHoldoutSelectors("\n").length, 0);

const addedRecord = {
  kg_id:"kg-b", query_id:"q-b",
  input:{sparql_version:1, sparql_hash:"sha256:" + digestB},
};
const removedRecord = {kg_id:"kg-a", query_id:"q-a", input:{sparql_version:0, sparql_hash:digestA}};
const result = schema.mergeHoldoutSelectors(parsedJsonl, [
  {record:addedRecord, selected:true},
  {record:removedRecord, selected:false},
]);
assert.equal(result.additions, 1);
assert.equal(result.removals, 1);
assert.deepEqual(JSON.parse(JSON.stringify(result.selectors)), [
  {kg_id:"kg-b", query_id:"q-b", sparql_version:1, sparql_hash:digestB},
  {kg_id:"kg-z", query_id:"q-z"},
]);

const unchanged = schema.mergeHoldoutSelectors(parsedJsonl, []);
assert.equal(unchanged.selectors.length, 2, "untouched existing selectors must be preserved");
const existingPinPreserved = schema.mergeHoldoutSelectors(parsedJsonl, [{
  record:{kg_id:"kg-a", query_id:"q-a", input:{sparql_version:2, sparql_hash:digestB}},
  selected:true,
}]);
assert.equal(existingPinPreserved.selectors[0].sparql_version, 0);
assert.equal(existingPinPreserved.selectors[0].sparql_hash, digestA);
assert.equal(existingPinPreserved.additions, 0);

assert.throws(() => schema.parseHoldoutSelectors('{"kg_id":"kg"}\n'), /query_id/);
assert.throws(() => schema.parseHoldoutSelectors('{"kg_id":"kg","query_id":"q","status":"accepted"}\n'), /identity\/version fields only/);
assert.throws(() => schema.parseHoldoutSelectors(
  JSON.stringify({kg_id:"kg", query_id:"q", sparql_version:0, sparql_hash:"sha256:" + digestA})
), /lowercase SHA-256/);
assert.throws(() => schema.parseHoldoutSelectors(
  '{"kg_id":"kg","query_id":"q"}\n{"kg_id":"kg","query_id":"q"}\n'
), /Duplicate/);
assert.throws(() => schema.parseHoldoutSelectors('{broken}\n'), /line 1/);
assert.throws(() => schema.validateHoldoutSelector({kg_id:"kg", query_id:"q", sparql_version:0}), /supplied together/);
assert.throws(() => schema.selectorForRecord({
  kg_id:"kg", query_id:"q", input:{sparql_version:0},
}), /incomplete SPARQL pin/);
assert.throws(() => schema.mergeHoldoutSelectors([], [
  {record:addedRecord, selected:true}, {record:addedRecord, selected:false},
]), /Duplicate current review identity/);

const normalized = schema.normalizeReview({holdout_selection_touched:true});
assert.equal(normalized.holdout_selection_touched, true);
assert.equal(schema.reviewDecisionCount({
  public:{status:"accepted"},
  holdout:{split:"private_holdout", holdout_selector_selected:true},
  untouched:{},
}), 2, "reviewed count must include a holdout decision without a status");
const exported = schema.exportableReview(normalized);
assert.equal(Object.hasOwn(exported, "holdout_selection_touched"), false, "browser-only removal markers must not enter review exports");
assert.deepEqual(JSON.parse(JSON.stringify(schema.selectorUpdateForReview(addedRecord, {}))), []);
assert.deepEqual(JSON.parse(JSON.stringify(schema.selectorUpdateForReview(addedRecord, {split:"private_holdout"}))), []);
assert.equal(schema.selectorUpdateForReview(addedRecord, {
  split:"private_holdout", holdout_selection_touched:true, holdout_selector_selected:true,
})[0].selected, true);
assert.equal(schema.selectorUpdateForReview(addedRecord, {
  split:"private_holdout", holdout_selection_touched:true, holdout_selector_selected:false,
})[0].selected, false);
assert.throws(() => schema.validateImportedReviews({x:{holdout_selection_touched:true}}), /browser-only/);
assert.throws(() => schema.validateImportedReviews({x:{holdout_selector_selected:false}}), /browser-only/);
const retiredPrivate = schema.exportableReview({
  status:"accepted", split:"private_holdout", holdout_selector_selected:false,
});
assert.equal(retiredPrivate.benchmark_disposition, "withheld", "selector removal must not declassify annotations");
assert.equal(retiredPrivate.split, "private_holdout");

const codeUnitOrder = schema.mergeHoldoutSelectors([
  {kg_id:"kg", query_id:"é"}, {kg_id:"kg", query_id:"Z"}, {kg_id:"kg", query_id:"a"},
], []).selectors.map((item) => item.query_id);
assert.deepEqual(JSON.parse(JSON.stringify(codeUnitOrder)), ["Z", "a", "é"]);
'''
    subprocess.run(["node", "-e", script], check=True, cwd=ROOT)
