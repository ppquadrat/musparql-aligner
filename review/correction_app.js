(function () {
  "use strict";
  const data = window.SPARQL_CORRECTION_DATA || null;
  const EXPORT_SCHEMA = "musparql.sparql-correction-review-export.v2";
  const EDIT_TYPES = ["syntax_correction", "endpoint_dialect_adaptation", "parameter_instantiation", "benchmark_specialization", "federation_rewrite", "performance_optimization", "other"];
  const busy = { execute: false, suggest: false, approve: false };

  function normalizeReview(value) {
    return {
      review_id: value?.review_id || "",
      decision: value?.decision || "", proposed_sparql: value?.proposed_sparql || "",
      edit_type: value?.edit_type || "", rationale: value?.rationale || "",
      evidence_ids: Array.isArray(value?.evidence_ids) ? value.evidence_ids : [],
      proposal_origin: value?.proposal_origin || "human", reviewer_note: value?.reviewer_note || "",
      reviewer_id: value?.reviewer_id || data?.reviewer_id || "",
      reviewed_at: value?.reviewed_at || "", agent_suggestion: value?.agent_suggestion || null,
      prior_review_ids: Array.isArray(value?.prior_review_ids) ? value.prior_review_ids : [],
      authored_formulation_ids: Array.isArray(value?.authored_formulation_ids) ? value.authored_formulation_ids : [],
      approved_formulation_ids: Array.isArray(value?.approved_formulation_ids) ? value.approved_formulation_ids : [],
      execution_attempts: Array.isArray(value?.execution_attempts) ? value.execution_attempts : [],
    };
  }
  function validateReview(record, review) {
    const errors = [];
    if (!review.decision) return errors;
    if (!/^reviewer-[0-9]{4,}$/.test(review.reviewer_id)) errors.push("A pseudonymous reviewer ID is required.");
    if (review.decision === "approve_edit") {
      if (!review.proposed_sparql.trim()) errors.push("Changed SPARQL is required.");
      if (review.proposed_sparql.trim() === record.base_sparql.trim()) errors.push("Change the SPARQL before approval.");
      if (!EDIT_TYPES.includes(review.edit_type) && !review.rationale.trim()) errors.push("Add an edit type or a short rationale.");
      if (review.proposal_origin === "agent" && !review.agent_suggestion) errors.push("Regenerate the agent suggestion so its provenance can be retained.");
    }
    if (review.decision === "approve_edit") {
      const valid = new Set((record.evidence || []).map((item) => item.evidence_id));
      const unknown = review.evidence_ids.filter((id) => !valid.has(id));
      if (unknown.length) errors.push(`Unknown evidence IDs: ${unknown.join(", ")}.`);
    }
    return errors;
  }
  function sparqlHash(text) {
    if (!window.crypto?.subtle) return Promise.reject(new Error("Secure hashing is unavailable in this browser."));
    return crypto.subtle.digest("SHA-256", new TextEncoder().encode(text)).then((bytes) => "sha256:" + Array.from(new Uint8Array(bytes), (b) => b.toString(16).padStart(2, "0")).join(""));
  }
  function lineDiff(base, proposal) {
    const a = base.split("\n"), b = proposal.split("\n"), table = Array.from({ length: a.length + 1 }, () => Array(b.length + 1).fill(0));
    for (let i = a.length - 1; i >= 0; i--) for (let j = b.length - 1; j >= 0; j--) table[i][j] = a[i] === b[j] ? table[i + 1][j + 1] + 1 : Math.max(table[i + 1][j], table[i][j + 1]);
    const out = []; let i = 0, j = 0;
    while (i < a.length || j < b.length) {
      if (i < a.length && j < b.length && a[i] === b[j]) { out.push({ kind: "same", text: `  ${a[i]}` }); i++; j++; }
      else if (j < b.length && (i === a.length || table[i][j + 1] >= table[i + 1][j])) { out.push({ kind: "add", text: `+ ${b[j++]}` }); }
      else { out.push({ kind: "remove", text: `- ${a[i++]}` }); }
    }
    return out;
  }
  function exportPayloadFor(records, reviews, dataset) {
    const output = [];
    for (const record of records) {
      const review = normalizeReview(reviews[record.candidate_id]);
      if (!review.decision) continue;
      const errors = validateReview(record, review);
      if (errors.length) throw new Error(`${record.query_label}: ${errors.join(" ")}`);
      const reviewId = review.review_id || `${record.candidate_id}::${review.reviewer_id}`;
      const formulationId = `${reviewId}::formulation::sparql`;
      output.push({
        review_id: reviewId,
        candidate_id: record.candidate_id, candidate_digest: record.candidate_digest,
        candidate_reason_code: record.triage?.reason_code, kg_id: record.kg_id, query_id: record.query_id,
        query_label: record.query_label, base_sparql_version: record.base_sparql_version,
        base_sparql_hash: record.base_sparql_hash, bundle_digest: dataset.bundle_digest,
        decision: review.decision, proposed_sparql: review.proposed_sparql,
        edit_type: review.edit_type || null, rationale: review.rationale || null,
        evidence_ids: review.evidence_ids, proposal_origin: review.proposal_origin,
        proposal_model: review.agent_suggestion?.model || null,
        agent_suggestion: review.agent_suggestion, execution_attempts: review.execution_attempts,
        reviewer_note: review.reviewer_note || null, reviewer_id: review.reviewer_id,
        reviewed_at: review.reviewed_at,
        prior_review_ids: review.prior_review_ids,
        authored_formulation_ids: review.decision === "approve_edit" && review.proposal_origin === "human"
          ? [formulationId] : review.authored_formulation_ids,
        approved_formulation_ids: review.decision === "approve_edit"
          ? [formulationId] : review.approved_formulation_ids,
      });
    }
    return { schema: EXPORT_SCHEMA, mode: "sparql_correction", reviewer_id: dataset.reviewer_id, dataset_id: dataset.dataset_id, bundle_digest: dataset.bundle_digest, exported_at: new Date().toISOString(), source_bundle_built_at: dataset.built_at, holdout_input_policy: dataset.holdout_input_policy, reviews: output };
  }
  window.MUSPARQL_CORRECTION_SCHEMA = { normalizeReview, validateReview, exportPayloadFor, lineDiff };
  if (!data || !Array.isArray(data.records) || !data.records.length) return;
  if (!/^reviewer-[0-9]{4,}$/.test(data.reviewer_id || "")) throw new Error("Correction bundle requires a pseudonymous reviewer-NNNN identifier.");

  const ids = ["datasetId","recordCount","visibleCount","reviewedCount","searchInput","kgFilter","categoryFilter","decisionFilter","recordList","emptyState","detailView","detailMeta","detailTitle","priorityBadge","categoryBadge","holdoutWarning","baseSparql","baseVersionLabel","latestSparql","latestVersionLabel","proposedSparqlInput","proposalHash","sparqlDiff","triageSummary","pipelineObservation","executionHistory","agentStatus","agentProvenance","generateSuggestionBtn","executeBaseBtn","executeProposalBtn","executeLatestBtn","approveBtn","noEditBtn","deferBtn","prevBtn","nextBtn","editTypeInput","rationaleInput","evidenceIdsInput","reviewerNoteInput","proposalOrigin","validationMessage","evidenceCount","evidenceList","exportCorrectionsBtn","clearCorrectionStateBtn","saveState"];
  const els = Object.fromEntries(ids.map((id) => [id, document.getElementById(id)]));
  els.detailTitle.setAttribute("tabindex", "-1");
  const storageKey = `musparql-sparql-correction:v3:${data.dataset_id}:${data.bundle_digest}:${data.reviewer_id}`;
  let reviews = loadReviews();
  let hashRenderToken = 0;
  let decisionLockUntil = 0;
  const state = { selected: data.records[0].candidate_id, search: "", kg: "all", category: "all", decision: "all" };
  fillSelect(els.kgFilter, ["all", ...unique(data.records.map((r) => r.kg_id))], "All KGs");
  fillSelect(els.categoryFilter, ["all", ...unique(data.records.map((r) => r.triage?.category))], "All triage classes");
  fillSelect(els.decisionFilter, ["all", "unreviewed", "approve_edit", "no_edit", "defer"], "All decisions");
  fillSelect(els.editTypeInput, ["", ...EDIT_TYPES], "Optional edit type");
  bind(); render();

  function unique(values) { return [...new Set(values.filter(Boolean))].sort(); }
  function escapeHtml(value) { return String(value ?? "").replace(/[&<>'"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", "'": "&#39;", '"': "&quot;" }[c])); }
  function fillSelect(select, values, first) { select.innerHTML = ""; values.forEach((value, i) => { const option = document.createElement("option"); option.value = value; option.textContent = i ? value.replaceAll("_", " ") : first; select.appendChild(option); }); }
  function loadReviews() { try { const legacy = data.reviewer_id === "reviewer-0001" ? localStorage.getItem(`musparql-sparql-correction:v2:${data.dataset_id}:${data.bundle_digest}`) : null; return JSON.parse(localStorage.getItem(storageKey) || legacy || "{}"); } catch (_) { return {}; } }
  function saveReviews() { localStorage.setItem(storageKey, JSON.stringify(reviews)); els.saveState.textContent = `Saved locally ${new Date().toLocaleTimeString()}`; }
  function currentRecord() { return data.records.find((r) => r.candidate_id === state.selected) || null; }
  function currentReview() { return normalizeReview(reviews[state.selected]); }
  function latest(record) { return (record.retained_versions || []).at(-1) || { sparql: record.base_sparql, sparql_hash: record.base_sparql_hash, sparql_version: record.base_sparql_version }; }
  function filtered() { return data.records.filter((record) => { const review = normalizeReview(reviews[record.candidate_id]); const text = [record.query_label, record.kg_id, record.triage?.summary, record.execution?.error, ...(record.evidence || []).map((e) => e.snippet)].join(" ").toLowerCase(); return (!state.search || text.includes(state.search)) && (state.kg === "all" || record.kg_id === state.kg) && (state.category === "all" || record.triage?.category === state.category) && (state.decision === "all" || (review.decision || "unreviewed") === state.decision); }); }
  function bind() {
    els.searchInput.addEventListener("input", () => { state.search = els.searchInput.value.trim().toLowerCase(); render(); });
    [[els.kgFilter,"kg"],[els.categoryFilter,"category"],[els.decisionFilter,"decision"]].forEach(([el,key]) => el.addEventListener("change", () => { state[key] = el.value; render(); }));
    [els.proposedSparqlInput, els.rationaleInput, els.evidenceIdsInput, els.reviewerNoteInput].forEach((el) => el.addEventListener("input", updateDraft));
    els.editTypeInput.addEventListener("change", updateDraft);
    els.generateSuggestionBtn.addEventListener("click", generateSuggestion);
    els.executeBaseBtn.addEventListener("click", () => execute("base"));
    els.executeProposalBtn.addEventListener("click", () => execute("proposal"));
    els.executeLatestBtn.addEventListener("click", () => execute("latest_approved"));
    els.approveBtn.addEventListener("click", () => decide("approve_edit"));
    els.noEditBtn.addEventListener("click", () => decide("no_edit"));
    els.deferBtn.addEventListener("click", () => decide("defer"));
    els.prevBtn.addEventListener("click", () => move(-1)); els.nextBtn.addEventListener("click", () => move(1));
    els.exportCorrectionsBtn.addEventListener("click", exportReviews);
    els.clearCorrectionStateBtn.addEventListener("click", () => { if (confirm("Clear all local decisions for this bundle?")) { reviews = {}; saveReviews(); render(); } });
  }
  function updateDraft() {
    const record = currentRecord(); if (!record) return;
    const previous = currentReview();
    const changedAfterAgent = previous.agent_suggestion && els.proposedSparqlInput.value.trim() !== (previous.agent_suggestion.output?.proposed_sparql || "").trim();
    reviews[record.candidate_id] = { ...previous, decision: "", proposed_sparql: els.proposedSparqlInput.value.trim(), edit_type: els.editTypeInput.value, rationale: els.rationaleInput.value.trim(), evidence_ids: els.evidenceIdsInput.value.split(",").map((v) => v.trim()).filter(Boolean), reviewer_note: els.reviewerNoteInput.value.trim(), proposal_origin: changedAfterAgent ? "human" : previous.proposal_origin, agent_suggestion: changedAfterAgent ? null : previous.agent_suggestion };
    saveReviews(); renderComparison(record, normalizeReview(reviews[record.candidate_id])); showValidation(record, normalizeReview(reviews[record.candidate_id]));
  }
  function setBusy(kind, value) { busy[kind] = value; const anyBusy = busy.execute || busy.suggest || busy.approve; [els.executeBaseBtn,els.executeProposalBtn,els.executeLatestBtn,els.generateSuggestionBtn,els.approveBtn,els.noEditBtn,els.deferBtn,els.prevBtn,els.nextBtn].forEach((button) => button.disabled = anyBusy); [els.searchInput,els.kgFilter,els.categoryFilter,els.decisionFilter,els.proposedSparqlInput,els.editTypeInput,els.rationaleInput,els.evidenceIdsInput,els.reviewerNoteInput].forEach((control) => control.disabled = anyBusy); document.querySelectorAll(".record-item").forEach((button) => button.disabled = anyBusy); }
  async function api(path, payload) { const response = await fetch(path, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload) }); const body = await response.json().catch(() => ({})); if (!response.ok) throw new Error(body.error || `Request failed (${response.status})`); return body; }
  async function execute(target) {
    const record = currentRecord(); if (!record || busy.execute) return; setBusy("execute", true); els.agentStatus.textContent = `Executing ${target.replaceAll("_", " ")}…`;
    try { let text, hash; if (target === "base") { text = record.base_sparql; hash = record.base_sparql_hash; } else if (target === "latest_approved") { const v = latest(record); text = v.sparql; hash = v.sparql_hash; } else { text = els.proposedSparqlInput.value.trim(); hash = await sparqlHash(text); }
      const attempt = await api("/api/execute", { candidate_id: record.candidate_id, candidate_digest: record.candidate_digest, target, sparql: text, sparql_hash: hash });
      const review = normalizeReview(reviews[record.candidate_id]); review.execution_attempts.push(attempt); reviews[record.candidate_id] = review; saveReviews(); if (state.selected === record.candidate_id) { els.agentStatus.textContent = `Execution ${attempt.status}: ${attempt.result_count ?? "—"} rows in ${attempt.duration_ms} ms.`; renderExecutionHistory(review); }
    } catch (error) { els.agentStatus.textContent = `Execution unavailable: ${error.message}`; } finally { setBusy("execute", false); }
  }
  async function generateSuggestion() {
    const record = currentRecord(); if (!record || busy.suggest) return; setBusy("suggest", true); els.agentStatus.textContent = "Generating suggestion…";
    try { const suggestion = await api("/api/suggest", { candidate_id: record.candidate_id, candidate_digest: record.candidate_digest }); const output = suggestion.output; const review = normalizeReview(reviews[record.candidate_id]); review.agent_suggestion = suggestion; review.proposal_origin = "agent"; review.proposed_sparql = output.proposed_sparql || record.base_sparql; review.edit_type = output.edit_type || ""; review.rationale = output.rationale || ""; review.evidence_ids = output.evidence_ids || []; reviews[record.candidate_id] = review; saveReviews(); if (state.selected === record.candidate_id) { renderDetail(record); els.agentStatus.textContent = output.recommendation === "edit" ? "Suggestion loaded for review; it has not been approved." : "Agent recommends no edit; review before deciding."; }
    } catch (error) { els.agentStatus.textContent = `Suggestion unavailable: ${error.message}`; } finally { setBusy("suggest", false); }
  }
  function decide(decision) {
    const record = currentRecord(); if (!record || busy.approve || Date.now() < decisionLockUntil) return; updateDraft(); const review = normalizeReview(reviews[record.candidate_id]); review.decision = decision; review.reviewed_at = new Date().toISOString(); if (decision !== "approve_edit") review.evidence_ids = []; const errors = validateReview(record, review); if (errors.length) { reviews[record.candidate_id] = review; showValidation(record, review); return; }
    decisionLockUntil = Date.now() + 700; setBusy("approve", true); reviews[record.candidate_id] = review; saveReviews(); const rows = filtered(); const current = rows.findIndex((r) => r.candidate_id === record.candidate_id); const next = rows.slice(current + 1).find((r) => !normalizeReview(reviews[r.candidate_id]).decision) || rows.find((r) => !normalizeReview(reviews[r.candidate_id]).decision); if (next) state.selected = next.candidate_id; render(); setBusy("approve", true); els.detailTitle.focus(); window.setTimeout(() => setBusy("approve", false), 700);
  }
  function move(delta) { const rows = filtered(); if (!rows.length) return; const index = rows.findIndex((r) => r.candidate_id === state.selected); state.selected = rows[(index + delta + rows.length) % rows.length].candidate_id; render(); }
  function render() { const rows = filtered(); if (!rows.some((r) => r.candidate_id === state.selected)) state.selected = rows[0]?.candidate_id || null; els.datasetId.textContent = data.dataset_id; els.recordCount.textContent = data.record_count; els.visibleCount.textContent = rows.length; els.reviewedCount.textContent = Object.values(reviews).filter((r) => r?.decision).length; els.recordList.innerHTML = ""; rows.forEach((record) => { const review = normalizeReview(reviews[record.candidate_id]); const button = document.createElement("button"); const selected = record.candidate_id === state.selected; button.className = "record-item" + (selected ? " active" : ""); if (selected) button.setAttribute("aria-current", "true"); button.innerHTML = `<span class="record-title">${escapeHtml(record.query_label)}</span><span class="record-subline">${escapeHtml(record.triage?.reason_code)} · ${escapeHtml(record.triage?.priority)}</span><span class="record-subline">${escapeHtml(review.decision || "unreviewed")}</span>`; button.onclick = () => { state.selected = record.candidate_id; render(); }; els.recordList.appendChild(button); }); renderDetail(currentRecord()); }
  function renderDetail(record) { if (!record) { els.detailView.classList.add("hidden"); els.emptyState.classList.remove("hidden"); return; } els.emptyState.classList.add("hidden"); els.detailView.classList.remove("hidden"); const review = currentReview(), last = latest(record); els.detailMeta.textContent = `${record.kg_id} / ${record.query_id}`; els.detailTitle.textContent = record.query_label; els.priorityBadge.textContent = record.triage?.priority || "—"; els.categoryBadge.textContent = (record.triage?.category || "—").replaceAll("_", " "); els.holdoutWarning.textContent = record.sparql_provenance?.retained_edit_count ? "This identity already has retained edits and is permanently holdout-ineligible." : "Approving any edit permanently makes this identity holdout-ineligible."; els.baseSparql.textContent = record.base_sparql; els.baseVersionLabel.textContent = `v${record.base_sparql_version} · ${record.base_sparql_hash}`; els.latestSparql.textContent = last.sparql; els.latestVersionLabel.textContent = `v${last.sparql_version} · ${last.sparql_hash}`; els.proposedSparqlInput.value = review.proposed_sparql || record.base_sparql; els.editTypeInput.value = review.edit_type; els.rationaleInput.value = review.rationale; els.evidenceIdsInput.value = review.evidence_ids.join(", "); els.reviewerNoteInput.value = review.reviewer_note; els.proposalOrigin.textContent = review.proposal_origin === "agent" ? "Agent-authored suggestion" : "Human-authored proposal"; els.triageSummary.textContent = record.triage?.summary || ""; els.pipelineObservation.textContent = JSON.stringify(record.execution || {}, null, 2); els.agentProvenance.textContent = review.agent_suggestion ? `${review.agent_suggestion.model} · ${review.agent_suggestion.request_id || "no request id"} · ${review.agent_suggestion.suggestion_id}` : "No agent suggestion loaded."; renderComparison(record, review); renderExecutionHistory(review); renderEvidence(record); showValidation(record, review); }
  function renderComparison(record, review) { const proposed = review.proposed_sparql || record.base_sparql; const token = ++hashRenderToken; sparqlHash(proposed).then((hash) => { if (token === hashRenderToken && state.selected === record.candidate_id && els.proposedSparqlInput.value.trim() === proposed.trim()) els.proposalHash.textContent = hash; }).catch(() => { if (token === hashRenderToken) els.proposalHash.textContent = "hash unavailable"; }); els.sparqlDiff.innerHTML = lineDiff(record.base_sparql, proposed).map((line) => `<span class="diff-${line.kind}">${escapeHtml(line.text)}</span>`).join("\n"); }
  function renderExecutionHistory(review) { els.executionHistory.innerHTML = ""; if (!review.execution_attempts.length) { els.executionHistory.textContent = "No UI execution attempts yet."; return; } review.execution_attempts.slice().reverse().forEach((attempt) => { const card = document.createElement("article"); card.className = "attempt-card"; card.innerHTML = `<strong>${escapeHtml(attempt.target)} · ${escapeHtml(attempt.status)}</strong><span>retained ${escapeHtml(attempt.sparql_hash)}${attempt.sparql_version == null ? "" : ` · v${escapeHtml(attempt.sparql_version)}`}</span><span>effective ${escapeHtml(attempt.effective_sparql_hash || attempt.sparql_hash)} · transformed ${escapeHtml(Boolean(attempt.retained_sparql_transformed))}</span><span>${escapeHtml(attempt.duration_ms)} ms · ${escapeHtml(attempt.result_count ?? "—")} rows · pipeline ${escapeHtml(attempt.pipeline_status || "not run")}${attempt.skip_reason ? ` · ${escapeHtml(attempt.skip_reason)}` : ""}</span><span>endpoint ${escapeHtml(attempt.endpoint || "unavailable")} · graph ${escapeHtml(attempt.graph || "default")}</span>${attempt.error ? `<span class="validation-error">${escapeHtml(attempt.error)}</span>` : ""}<pre>${escapeHtml(JSON.stringify(attempt.sample_rows || [], null, 2))}</pre>`; els.executionHistory.appendChild(card); }); }
  function renderEvidence(record) { els.evidenceList.innerHTML = ""; els.evidenceCount.textContent = `${(record.evidence || []).length} items`; (record.evidence || []).forEach((item) => { const card = document.createElement("article"); card.className = "evidence-card"; card.innerHTML = `<div class="evidence-meta"><span class="pill">${escapeHtml(item.evidence_id)}</span><span class="pill">${escapeHtml(item.type || "unknown")}</span></div><p>${escapeHtml(item.snippet || "")}</p>`; els.evidenceList.appendChild(card); }); }
  function showValidation(record, review) { const errors = validateReview(record, review); els.validationMessage.textContent = errors.length ? errors.join(" ") : review.decision ? "Decision saved locally and ready to export." : "Approval requires changed SPARQL plus an edit type or rationale; evidence and notes are optional."; els.validationMessage.classList.toggle("validation-error", Boolean(errors.length)); }
  function exportReviews() { try { const payload = exportPayloadFor(data.records, reviews, data); if (!payload.reviews.length) throw new Error("No decisions to export."); const blob = new Blob([JSON.stringify(payload, null, 2)], { type: "application/json" }); const link = document.createElement("a"); link.href = URL.createObjectURL(blob); link.download = `musparql-sparql-correction-review-${data.dataset_id}-${new Date().toISOString().replaceAll(":", "-")}.json`; link.click(); window.setTimeout(() => URL.revokeObjectURL(link.href), 1000); els.saveState.textContent = "Export downloaded; canonical data is unchanged until apply."; } catch (error) { alert(error.message); } }
})();
