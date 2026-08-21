"use strict";

// Dependency-free browser-state harness for the vanilla correction UI. It uses
// synthetic records, an in-memory localStorage, and delayed same-origin API
// responses so the workflow remains testable in environments without Playwright.
const assert = require("node:assert/strict");
const crypto = require("node:crypto").webcrypto;
const fs = require("node:fs");
const vm = require("node:vm");

const APP = fs.readFileSync("review/correction_app.js", "utf8");
const IDS = ["datasetId","recordCount","visibleCount","reviewedCount","searchInput","kgFilter","categoryFilter","decisionFilter","recordList","emptyState","detailView","detailMeta","detailTitle","priorityBadge","categoryBadge","holdoutWarning","baseSparql","baseVersionLabel","latestSparql","latestVersionLabel","proposedSparqlInput","proposalHash","sparqlDiff","triageSummary","pipelineObservation","executionHistory","agentStatus","agentProvenance","generateSuggestionBtn","executeBaseBtn","executeProposalBtn","executeLatestBtn","approveBtn","noEditBtn","deferBtn","prevBtn","nextBtn","editTypeInput","rationaleInput","evidenceIdsInput","reviewerNoteInput","proposalOrigin","validationMessage","evidenceCount","evidenceList","exportCorrectionsBtn","clearCorrectionStateBtn","saveState"];

class FakeElement {
  constructor(document, tag = "div", id = "") {
    this.ownerDocument = document; this.tagName = tag.toUpperCase(); this.id = id;
    this.children = []; this.listeners = {}; this.attributes = {}; this.disabled = false;
    this.value = ""; this.textContent = ""; this._innerHTML = ""; this._classes = new Set();
    this.classList = {
      add: (...names) => names.forEach((name) => this._classes.add(name)),
      remove: (...names) => names.forEach((name) => this._classes.delete(name)),
      toggle: (name, force) => force ? this._classes.add(name) : this._classes.delete(name),
      contains: (name) => this._classes.has(name),
    };
  }
  set className(value) { this._classes = new Set(String(value).split(/\s+/).filter(Boolean)); }
  get className() { return [...this._classes].join(" "); }
  set innerHTML(value) { this._innerHTML = String(value); this.children = []; }
  get innerHTML() { return this._innerHTML; }
  setAttribute(name, value) { this.attributes[name] = String(value); }
  getAttribute(name) { return this.attributes[name]; }
  appendChild(child) { this.children.push(child); if (this.tagName === "SELECT" && this.children.length === 1) this.value = child.value; return child; }
  addEventListener(type, listener) { (this.listeners[type] ||= []).push(listener); }
  async dispatch(type) { if (type === "click" && this.disabled) return; const results = []; if (type === "click") this.clicked = true; if (type === "click" && this.onclick) results.push(this.onclick()); for (const listener of this.listeners[type] || []) results.push(listener({ target: this })); await Promise.all(results); }
  click() { return this.dispatch("click"); }
  focus() { this.ownerDocument.activeElement = this; }
}

class FakeDocument {
  constructor() {
    this.elements = Object.fromEntries(IDS.map((id) => [id, new FakeElement(this, id.includes("Input") || id.endsWith("Filter") ? "input" : "div", id)]));
    this.body = new FakeElement(this, "body", "body"); this.activeElement = null; this.created = [];
  }
  getElementById(id) { return this.elements[id]; }
  createElement(tag) { const element = new FakeElement(this, tag); this.created.push(element); return element; }
  querySelectorAll(selector) { return selector === ".record-item" ? this.elements.recordList.children.filter((item) => item.classList.contains("record-item")) : []; }
}

class MemoryStorage {
  constructor(values = new Map()) { this.values = values; }
  getItem(key) { return this.values.has(key) ? this.values.get(key) : null; }
  setItem(key, value) { this.values.set(key, String(value)); }
}

async function digest(text) {
  const bytes = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(text));
  return "sha256:" + Buffer.from(bytes).toString("hex");
}

async function makeData(bundleDigest) {
  const base = "SELECT * WHERE { ?s ?p ?o }";
  const baseHash = await digest(base);
  const records = ["one", "two"].map((name, index) => ({
    candidate_id: `candidate-${name}`, candidate_digest: `sha256:${String(index + 1).padStart(64, "0")}`,
    kg_id: "synthetic-kg", query_id: `synthetic-${name}`, query_label: `Synthetic ${name}`,
    base_sparql: base, base_sparql_hash: baseHash, base_sparql_version: 0,
    retained_versions: [{ sparql: base, sparql_hash: baseHash, sparql_version: 0 }],
    triage: { reason_code: "not_attempted", category: "needs_observation", priority: "medium", summary: "Synthetic candidate." },
    sparql_provenance: { retained_edit_count: 0 }, execution: { status: "not_attempted" },
    evidence: [{ evidence_id: "e1", type: "synthetic", snippet: "Synthetic public evidence." }],
  }));
  return { mode: "sparql_correction", reviewer_id: "reviewer-0001", dataset_id: "synthetic-ui", bundle_digest: bundleDigest, built_at: "2026-08-05T10:00:00Z", holdout_input_policy: "synthetic", record_count: 2, records };
}

async function boot(bundleDigest, backing) {
  const document = new FakeDocument(); const storage = new MemoryStorage(backing);
  let exportCount = 0; const alerts = []; const exportedBlobs = [];
  const proposal = "SELECT DISTINCT ?s WHERE { ?s ?p ?o }";
  const proposalHash = await digest(proposal);
  const fetch = async (path) => {
    await new Promise((resolve) => setTimeout(resolve, 8));
    if (path === "/api/suggest") return { ok: true, json: async () => ({
      suggestion_id: "suggestion-1", request_id: "request-1", model: "synthetic-agent",
      output: { recommendation: "edit", proposed_sparql: proposal, edit_type: "syntax_correction", rationale: "Synthetic rationale.", evidence_ids: [], uncertainty: "low" },
    }) };
    if (path === "/api/execute") return { ok: true, json: async () => ({
      attempt_id: "attempt-1", target: "proposal", status: "unavailable", sparql_hash: proposalHash,
      effective_sparql_hash: proposalHash, retained_sparql_transformed: false, duration_ms: 1,
      result_count: null, sample_rows: [], endpoint: null, graph: null, error: "Synthetic endpoint unavailable.",
    }) };
    throw new Error(`unexpected path ${path}`);
  };
  const window = { SPARQL_CORRECTION_DATA: await makeData(bundleDigest), crypto, setTimeout: (fn) => { fn(); return 1; } };
  const sandbox = {
    window, document, localStorage: storage, crypto, fetch, TextEncoder, Uint8Array, Blob,
    URL: { createObjectURL: (blob) => { exportedBlobs.push(blob); return `blob:synthetic-${++exportCount}`; }, revokeObjectURL: () => {} },
    confirm: () => true, alert: (message) => alerts.push(message), console,
  };
  vm.runInNewContext(APP, sandbox, { filename: "review/correction_app.js" });
  return { document, storage, alerts, exportedBlobs, get exportCount() { return exportCount; } };
}

(async () => {
  const backing = new Map();
  const app = await boot("sha256:bundle-a", backing); const el = app.document.elements;
  assert.equal(el.reviewedCount.textContent, 0);
  const lockedControls = () => [
    el.executeBaseBtn, el.executeProposalBtn, el.executeLatestBtn, el.generateSuggestionBtn,
    el.approveBtn, el.noEditBtn, el.deferBtn, el.prevBtn, el.nextBtn,
    el.searchInput, el.kgFilter, el.categoryFilter, el.decisionFilter,
    el.proposedSparqlInput, el.editTypeInput, el.rationaleInput, el.evidenceIdsInput,
    el.reviewerNoteInput, ...el.recordList.children,
  ];

  const generating = el.generateSuggestionBtn.click();
  for (const control of lockedControls()) assert.equal(control.disabled, true, `${control.id || control.className} must lock during generation`);
  await el.nextBtn.click(); assert.equal(el.detailTitle.textContent, "Synthetic one", "locked navigation must not change candidates");
  await generating;
  assert.equal(el.proposedSparqlInput.value, "SELECT DISTINCT ?s WHERE { ?s ?p ?o }");
  assert.equal(el.proposalOrigin.textContent, "Agent-authored suggestion");

  const executing = el.executeProposalBtn.click();
  for (const control of lockedControls()) assert.equal(control.disabled, true, `${control.id || control.className} must lock during execution`);
  await executing;
  assert.match(el.executionHistory.children[0].innerHTML, /effective sha256:/);

  await el.approveBtn.click();
  assert.equal(el.detailTitle.textContent, "Synthetic two", "approval must auto-advance");
  assert.equal(app.document.activeElement, el.detailTitle, "focus must follow auto-advance");
  assert.equal(el.recordList.children[1].getAttribute("aria-current"), "true");
  await el.approveBtn.click();
  assert.equal(el.reviewedCount.textContent, 1, "duplicate approval must not affect the next candidate");

  await el.exportCorrectionsBtn.click();
  assert.equal(app.alerts.length, 0); assert.equal(app.exportCount, 1); assert.match(el.saveState.textContent, /Export downloaded/);
  const link = app.document.created.find((item) => item.tagName === "A"); assert.equal(link.clicked, true); assert.match(link.download, /^musparql-sparql-correction-review-/);
  const exported = JSON.parse(await app.exportedBlobs[0].text()); const decision = exported.reviews[0];
  assert.equal(exported.bundle_digest, "sha256:bundle-a"); assert.equal(decision.candidate_id, "candidate-one");
  assert.equal(decision.bundle_digest, "sha256:bundle-a"); assert.equal(decision.candidate_digest, "sha256:" + "1".padStart(64, "0"));
  assert.equal(decision.base_sparql_version, 0); assert.equal(decision.base_sparql_hash, await digest("SELECT * WHERE { ?s ?p ?o }"));
  assert.equal(decision.agent_suggestion.suggestion_id, "suggestion-1"); assert.equal(decision.execution_attempts[0].attempt_id, "attempt-1");
  assert.equal(decision.reviewer_id, "reviewer-0001"); assert.equal(exported.reviewer_id, "reviewer-0001");
  const keys = [...backing.keys()]; assert.equal(keys.length, 1); assert.match(keys[0], /sha256:bundle-a:reviewer-0001$/);

  const restored = await boot("sha256:bundle-a", backing);
  assert.equal(restored.document.elements.reviewedCount.textContent, 1, "same-bundle decisions must restore");
  const isolated = await boot("sha256:bundle-b", backing);
  assert.equal(isolated.document.elements.reviewedCount.textContent, 0, "new bundle digest must isolate stale state");

  const css = fs.readFileSync("review/styles.css", "utf8");
  assert.match(css, /@media\s*\(max-width:\s*900px\)\s*\{\s*\.correction-layout \.sidebar\s*\{\s*min-width:\s*0;\s*\}\s*\}/, "narrow-window sidebar reset regression guard");
  console.log("correction UI synthetic workflow: passed");
})().catch((error) => { console.error(error); process.exitCode = 1; });
