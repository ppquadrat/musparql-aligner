from pathlib import Path
import subprocess


ROOT = Path(__file__).resolve().parents[1]


def test_browser_review_state_is_scoped_to_reviewer() -> None:
    app = (ROOT / "review" / "app.js").read_text(encoding="utf-8")
    assert "musparql-review:schema4:${data.dataset_id}:${data.reviewer_id}" in app
    assert "musparql-review-compare:schema4:${data.dataset_id}:${data.reviewer_id}" in app
    assert 'data.reviewer_id === "reviewer-0001"' in app


def test_hosted_review_state_is_scoped_to_assignment_and_keeps_local_keys() -> None:
    app = (ROOT / "review" / "app.js").read_text(encoding="utf-8")
    html = (ROOT / "review" / "index.html").read_text(encoding="utf-8")

    assert "musparql-review:schema5:${data.dataset_id}:${data.reviewer_id}:${hosted.assignment_id}" in app
    assert "musparql-review-compare:schema5:${data.dataset_id}:${data.reviewer_id}:${hosted.assignment_id}" in app
    assert "!hosted && data.reviewer_id" in app
    assert "hideHostedHoldoutControls()" in app
    assert "Signed in as ${hosted.reviewer_id}" in app
    assert "Thank you — your review was submitted." in app
    assert "You completed ${percentage}% of this assignment" in app
    assert 'id="continueReviewBtn"' in html
    assert 'id="backToAssignmentsLink"' in html
    assert '<script src="host_context.js?' in html
    assert (ROOT / "review" / "host_context.js").read_text(encoding="utf-8").strip() == (
        "window.MUSPARQL_HOSTED_CONTEXT = null;"
    )


def test_workbench_keeps_columns_until_a_genuinely_narrow_window() -> None:
    css = (ROOT / "review" / "styles.css").read_text(encoding="utf-8")

    assert "@media (max-width: 900px)" in css
    assert "@media (max-width: 760px)" in css
    assert "@media (max-width: 1100px)" not in css


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


def test_hosted_v2_import_contract_rejects_unknown_envelope_and_review_fields() -> None:
    script = r'''
const assert = require("node:assert/strict");
const fs = require("node:fs");
const vm = require("node:vm");
const sandbox = {window:{REVIEW_DATA:null}, document:{getElementById:()=>null, querySelectorAll:()=>[]}};
vm.runInNewContext(fs.readFileSync("review/app.js", "utf8"), sandbox);
const schema = sandbox.window.MUSPARQL_REVIEW_SCHEMA;
const review = {review_id:"event::reviewer-0042", reviewer_id:"reviewer-0042", reviewed_at:"2026-08-20T10:00:00Z", prior_review_ids:[], authored_formulation_ids:[], approved_formulation_ids:["event::reviewer-0042::formulation::candidate"], benchmark_disposition:"included", pipeline_assessment:"accepted", preferred_question:"", literal_wording:"", public_comment:"", internal_comment:"", split:"", interpretive:{naturalness:null, pragmatism:null, room_for_interpretation:null, requires_graph_context_knowledge:false}};
const payload = {schema:"musparql.review-export.v2", kind:"non_holdout_review_export", assignment_id:"assignment-000000000000000000000001", bundle_digest:`sha256:${"a".repeat(64)}`, reviewer_id:"reviewer-0042", dataset_id:"synthetic", run_id:"run", run_ids:["run"], runs:[], exported_at:"2026-08-20T10:01:00Z", reviews:{record:review}};
assert.equal(schema.validateV2Envelope(payload), true);
schema.validateImportedReviews(payload.reviews, true);
assert.throws(() => schema.validateV2Envelope({...payload, debug:true}), /undeclared/);
assert.throws(() => schema.validateImportedReviews({record:{...review, debug:true}}, true), /undeclared/);
assert.throws(() => schema.validateImportedReviews({record:{...review, pipeline_assessment:"unknown"}}, true), /Unknown pipeline/);
assert.equal(schema.validateV2Envelope({schema:"musparql.review-export.v2"}), false, "legacy local v2 remains importable through the transition path");
'''
    subprocess.run(["node", "-e", script], check=True, cwd=ROOT)


def test_compare_semantic_actions_reset_stale_attribution() -> None:
    script = r'''
const assert = require("node:assert/strict");
const fs = require("node:fs");
const vm = require("node:vm");
const sandbox = {window:{REVIEW_DATA:null}, document:{getElementById:()=>null, querySelectorAll:()=>[]}};
vm.runInNewContext(fs.readFileSync("review/app.js", "utf8"), sandbox);
const schema = sandbox.window.MUSPARQL_REVIEW_SCHEMA;
const prior = {
  review_id:"prior::reviewer-0001", reviewer_id:"reviewer-0001",
  status:"accepted", preferred_question:"Prior wording?",
  approved_formulation_ids:["prior::reviewer-0001::formulation::preferred"],
};
const current = {review_id:"current::reviewer-0002", reviewer_id:"reviewer-0002"};
const reused = schema.reusedPreviousReview(prior, current, "wrong-bundle-key");
assert.equal(reused.copied_from_review_id, "prior::reviewer-0001");
let next = {...reused, ...schema.resetFormulationAttribution({status:"accepted", preferred_question:""})};
let exported = schema.exportableReview(next, "current-key");
assert.equal(exported.copied_from_review_id, undefined);
assert.deepEqual(JSON.parse(JSON.stringify(exported.approved_formulation_ids)), [
  "current::reviewer-0002::formulation::candidate"
]);
next = {...reused, ...schema.resetFormulationAttribution({status:"excluded"})};
exported = schema.exportableReview(next, "current-key");
assert.deepEqual(JSON.parse(JSON.stringify(exported.approved_formulation_ids)), []);

const preferredEdited = {
  ...reused,
  preferred_question:"New wording?",
  ...schema.editedFormulationAttribution(reused, "preferred"),
};
exported = schema.exportableReview(preferredEdited, "current-key");
assert.deepEqual(JSON.parse(JSON.stringify(exported.authored_formulation_ids)), [
  "current::reviewer-0002::formulation::preferred"
]);
assert.equal(exported.authored_formulation_ids.includes(
  "current::reviewer-0002::formulation::literal"
), false, "unchanged copied literal must not be attributed to the current reviewer");

const literalEdited = {
  ...reused,
  literal_wording:"New literal wording?",
  ...schema.editedFormulationAttribution(reused, "literal"),
};
exported = schema.exportableReview(literalEdited, "current-key");
assert.deepEqual(JSON.parse(JSON.stringify(exported.authored_formulation_ids)), [
  "current::reviewer-0002::formulation::literal"
]);
assert.equal(exported.approved_formulation_ids.includes(
  "prior::reviewer-0001::formulation::preferred"
), true, "editing literal must preserve copied preferred approval");
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
