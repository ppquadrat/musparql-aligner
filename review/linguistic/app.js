(() => {
  "use strict";
  const data = window.REVIEW_DATA;
  const hosted = window.MUSPARQL_HOSTED_CONTEXT;
  const dimensions = ["naturalness", "pragmatism", "interpretation_room"];
  const labels = {
    naturalness: "Naturalness",
    pragmatism: "Communicative salience",
    interpretation_room: "Room for interpretation",
  };

  function shuffled(values, random = Math.random) {
    const result = [...values];
    for (let i = result.length - 1; i > 0; i -= 1) {
      const j = Math.floor(random() * (i + 1));
      [result[i], result[j]] = [result[j], result[i]];
    }
    return result;
  }

  function stateKey(bundle, context) {
    return `musparql-linguistic:schema1:${bundle.dataset_id}:${context.reviewer_id}:${context.assignment_id}`;
  }

  function freshState(bundle) {
    return {queue: shuffled(bundle.records.map((item) => item.trial_id)), completed: {}, drafts: {}, skips: {}, includeSkipped: false, finished: false};
  }

  function mergeState(current, stored, validIds = null, discardedDraftIds = []) {
    if (!stored || typeof stored !== "object" || !Array.isArray(stored.queue) || !stored.completed || !stored.drafts || !stored.skips) return current;
    const completed = {...stored.completed, ...current.completed};
    const allowed = validIds ? new Set(validIds) : null;
    const queue = [];
    for (const id of [...current.queue, ...stored.queue]) {
      if ((!allowed || allowed.has(id)) && !completed[id] && !queue.includes(id)) queue.push(id);
    }
    const drafts = {...stored.drafts, ...current.drafts};
    for (const id of Object.keys(completed)) delete drafts[id];
    for (const id of discardedDraftIds) delete drafts[id];
    const skips = {...stored.skips};
    for (const [id, count] of Object.entries(current.skips)) skips[id] = Math.max(Number(skips[id]) || 0, Number(count) || 0);
    return {queue, completed, drafts, skips, includeSkipped: current.includeSkipped === true, finished: current.finished === true};
  }

  function hasTouchedRatings(draft) {
    return Object.values(draft?.ratings || {}).some((ratings) =>
      Object.values(ratings).some((answer) => answer?.touched === true)
    );
  }

  function validateBundle(bundle, context) {
    if (!bundle || bundle.schema !== "musparql.linguistic-stimulus-bundle.v1" || bundle.mode !== "linguistic") throw new Error("Invalid linguistic bundle");
    if (!context || bundle.assignment_id !== context.assignment_id || bundle.reviewer_id !== context.reviewer_id) throw new Error("Assignment attribution mismatch");
    if (!Array.isArray(bundle.records) || bundle.records.length !== bundle.record_count) throw new Error("Invalid linguistic record count");
    for (const record of bundle.records) {
      if (record.presentation_arity !== 3 || record.non_holdout !== true || record.eligible !== true || !Array.isArray(record.candidates) || record.candidates.length !== 2) throw new Error("Invalid linguistic stimulus");
    }
  }

  function normalizedTrial(stimulus, context, bundle, draft, outcome, extra = {}) {
    const identities = stimulus.candidates.map(({formulation_id, version, digest}) => ({formulation_id, version, digest}));
    const now = new Date().toISOString();
    const record = {
      schema: "musparql.linguistic-trial-annotation.v1", assignment_id: context.assignment_id,
      dataset_id: bundle.dataset_id, trial_id: stimulus.trial_id, reviewer_id: context.reviewer_id,
      query_id: stimulus.query_id, sparql_version: stimulus.sparql_version, sparql_digest: stimulus.sparql_digest,
      literal: (({formulation_id, version, digest}) => ({formulation_id, version, digest}))(stimulus.literal),
      display_order: [...draft.display_order], displayed_formulations: identities, presentation_arity: 3,
      outcome, started_at: draft.started_at, completed_at: now, submitted_at: now, task_design_version: "phase-6b-v1",
    };
    if (outcome === "rated") {
      record.ratings = {};
      for (const id of draft.display_order) {
        record.ratings[id] = {};
        for (const dimension of dimensions) {
          const answer = draft.ratings[id][dimension];
          if (!answer || answer.touched !== true || !Number.isInteger(answer.value) || answer.value < -100 || answer.value > 100) throw new Error("All six controls must be deliberately answered");
          record.ratings[id][dimension] = answer.value;
        }
      }
    } else if (outcome === "cannot_assess") {
      if (extra.reason) record.reason = extra.reason;
      if (extra.comment) record.comment = extra.comment;
    } else if (outcome === "literal_inaccurate") {
      if (extra.proposed_literal) record.proposed_literal = extra.proposed_literal;
      if (extra.comment) record.comment = extra.comment;
    } else throw new Error("Invalid outcome");
    return record;
  }

  window.MUSPARQL_LINGUISTIC = {shuffled, stateKey, freshState, mergeState, hasTouchedRatings, validateBundle, normalizedTrial};
  if (!data || !hosted || typeof document === "undefined" || !document.getElementById("instructions")) return;
  try { validateBundle(data, hosted); } catch (error) { document.body.textContent = `Workbench unavailable: ${error.message}`; return; }

  const key = stateKey(data, hosted);
  let state;
  try { state = JSON.parse(localStorage.getItem(key)) || freshState(data); } catch (_) { state = freshState(data); }
  const records = new Map(data.records.map((item) => [item.trial_id, item]));
  state.queue = state.queue.filter((id) => records.has(id) && !state.completed[id]);
  for (const id of records.keys()) if (!state.completed[id] && !state.queue.includes(id)) state.queue.push(id);
  let currentId = null;
  let pendingOutcome = null;

  const byId = (id) => document.getElementById(id);
  const save = (discardedDraftIds = []) => {
    let stored = null;
    try { stored = JSON.parse(localStorage.getItem(key)); } catch (_) { /* replace invalid state */ }
    state = mergeState(state, stored, records.keys(), discardedDraftIds);
    localStorage.setItem(key, JSON.stringify(state));
  };
  const show = (id) => { for (const section of ["instructions", "workbench", "finished"]) byId(section).hidden = section !== id; };
  const skippedPendingIds = () => [...records.keys()].filter((id) => !state.completed[id] && state.skips[id]);
  const unseenPendingIds = () => [...records.keys()].filter((id) => !state.completed[id] && !state.skips[id]);
  const progressText = () => `${Object.keys(state.completed).length} completed · ${skippedPendingIds().length} skipped · ${unseenPendingIds().length} unseen`;
  byId("identity").textContent = `Signed in as ${hosted.reviewer_id}`;
  byId("backToAssignmentsLink").href = hosted.assignments_url || "/";

  function draftFor(stimulus) {
    if (!state.drafts[stimulus.trial_id]) {
      const ids = shuffled(stimulus.candidates.map((item) => item.formulation_id));
      const ratings = Object.fromEntries(ids.map((id) => [id, Object.fromEntries(dimensions.map((dimension) => [dimension, {value: 0, touched: false}]))]));
      state.drafts[stimulus.trial_id] = {display_order: ids, ratings, started_at: new Date().toISOString()};
      save();
    }
    return state.drafts[stimulus.trial_id];
  }

  function appendTicks(row) {
    const ticks = document.createElement("div"); ticks.className = "ticks";
    for (const [tick, position] of [["−100", 0], ["−50", 25], ["0", 50], ["+50", 75], ["+100", 100]]) {
      const span = document.createElement("span"); span.textContent = tick; span.style.left = `${position}%`; ticks.append(span);
    }
    row.append(ticks);
  }

  function slider(candidateId, dimension, draft) {
    const row = document.createElement("div"); row.className = "slider-row";
    const label = document.createElement("label");
    const title = document.createElement("span"); title.textContent = labels[dimension];
    const value = document.createElement("span"); value.className = draft.ratings[candidateId][dimension].touched ? "answered" : "unanswered";
    value.textContent = draft.ratings[candidateId][dimension].touched ? String(draft.ratings[candidateId][dimension].value) : "0 · unanswered";
    label.append(title, value);
    const input = document.createElement("input"); input.type = "range"; input.min = "-100"; input.max = "100"; input.step = "1"; input.value = String(draft.ratings[candidateId][dimension].value);
    input.setAttribute("aria-label", `${labels[dimension]} for formulation ${candidateId}`);
    input.addEventListener("input", () => { draft.ratings[candidateId][dimension] = {value: Number(input.value), touched: true}; value.textContent = input.value; value.className = "answered"; save(); });
    row.append(label, input); appendTicks(row); return row;
  }

  function anchorSlider(dimension) {
    const row = document.createElement("div"); row.className = "slider-row";
    const label = document.createElement("label");
    const title = document.createElement("span"); title.textContent = labels[dimension];
    const value = document.createElement("span"); value.className = "fixed-value"; value.textContent = "0 · fixed";
    label.append(title, value);
    const input = document.createElement("input"); input.type = "range"; input.min = "-100"; input.max = "100"; input.value = "0"; input.disabled = true;
    input.setAttribute("aria-label", `${labels[dimension]} for literal reference, fixed at zero`);
    row.append(label, input); appendTicks(row); return row;
  }

  function render() {
    const nextIndex = state.queue.findIndex((id) => state.includeSkipped || !state.skips[id]);
    if (nextIndex < 0) { finish(); return; }
    if (nextIndex > 0) state.queue = [...state.queue.slice(nextIndex), ...state.queue.slice(0, nextIndex)];
    currentId = state.queue[0]; const stimulus = records.get(currentId); const draft = draftFor(stimulus);
    byId("progress").textContent = progressText();
    byId("queryTitle").textContent = `${stimulus.kg_id} · ${stimulus.query_id}`;
    byId("queryLabel").textContent = stimulus.query_label;
    byId("includeSkipped").checked = state.includeSkipped === true;
    byId("skippedCount").textContent = `(${skippedPendingIds().length})`;
    byId("sparql").textContent = stimulus.sparql;
    const grid = byId("formulationGrid"); grid.replaceChildren();
    const literalCard = document.createElement("section"); literalCard.className = "formulation literal-card";
    const literalHeading = document.createElement("h2"); literalHeading.textContent = "Literal reference";
    const literalWording = document.createElement("p"); literalWording.className = "wording literal"; literalWording.textContent = stimulus.literal.text;
    literalCard.append(literalHeading, literalWording); for (const dimension of dimensions) literalCard.append(anchorSlider(dimension)); grid.append(literalCard);
    draft.display_order.forEach((id, index) => {
      const candidate = stimulus.candidates.find((item) => item.formulation_id === id);
      const card = document.createElement("section"); card.className = "formulation candidate";
      const heading = document.createElement("h2"); heading.textContent = `Formulation ${index === 0 ? "A" : "B"}`;
      const wording = document.createElement("p"); wording.className = "wording"; wording.textContent = candidate.text;
      card.append(heading, wording); for (const dimension of dimensions) card.append(slider(id, dimension, draft)); grid.append(card);
    });
    byId("ratingError").hidden = true; byId("skipWarning").hidden = true; byId("ratingActions").hidden = false;
    byId("outcomePanel").hidden = true; byId("ratingForm").hidden = false; show("workbench");
  }

  function complete(outcome, extra = {}) {
    const stimulus = records.get(currentId); const draft = draftFor(stimulus);
    state.completed[currentId] = normalizedTrial(stimulus, hosted, data, draft, outcome, extra);
    delete state.drafts[currentId]; state.queue = state.queue.filter((id) => id !== currentId); save(); render();
  }
  function finish() {
    state.finished = true; save(); byId("finishedProgress").textContent = progressText();
    byId("resumeBtn").hidden = unseenPendingIds().length === 0;
    byId("reviewSkippedBtn").hidden = skippedPendingIds().length === 0;
    show("finished");
  }

  byId("beginBtn").addEventListener("click", () => { state.finished = false; save(); render(); });
  byId("resumeBtn").addEventListener("click", () => { state.finished = false; state.includeSkipped = false; save(); render(); });
  byId("reviewSkippedBtn").addEventListener("click", () => { state.finished = false; state.includeSkipped = true; save(); render(); });
  byId("includeSkipped").addEventListener("change", () => { state.includeSkipped = byId("includeSkipped").checked; save(); render(); });
  byId("finishBtn").addEventListener("click", finish);
  byId("ratingForm").addEventListener("submit", (event) => { event.preventDefault(); try { complete("rated"); } catch (_) { byId("ratingError").hidden = false; } });
  function skipCurrent() { const skippedId = currentId; delete state.drafts[skippedId]; state.skips[skippedId] = (state.skips[skippedId] || 0) + 1; state.queue.push(state.queue.shift()); state.includeSkipped = false; save([skippedId]); render(); }
  byId("skipBtn").addEventListener("click", () => { if (hasTouchedRatings(draftFor(records.get(currentId)))) { byId("skipWarning").hidden = false; byId("ratingActions").hidden = true; } else skipCurrent(); });
  byId("discardSkipBtn").addEventListener("click", skipCurrent);
  byId("cancelSkipBtn").addEventListener("click", () => { byId("skipWarning").hidden = true; byId("ratingActions").hidden = false; });
  function openOutcome(outcome) { pendingOutcome = outcome; byId("outcomeTitle").textContent = outcome === "cannot_assess" ? "Cannot assess this presentation" : "Report an inaccurate literal"; byId("reasonRow").hidden = outcome !== "cannot_assess"; byId("proposalRow").hidden = outcome !== "literal_inaccurate"; byId("reason").value = ""; byId("proposal").value = ""; byId("comment").value = ""; byId("ratingForm").hidden = true; byId("outcomePanel").hidden = false; }
  byId("cannotBtn").addEventListener("click", () => openOutcome("cannot_assess"));
  byId("literalErrorBtn").addEventListener("click", () => openOutcome("literal_inaccurate"));
  byId("cancelOutcomeBtn").addEventListener("click", () => { pendingOutcome = null; byId("outcomePanel").hidden = true; byId("ratingForm").hidden = false; });
  byId("confirmOutcomeBtn").addEventListener("click", () => { const extra = {reason: byId("reason").value, proposed_literal: byId("proposal").value.trim(), comment: byId("comment").value.trim()}; complete(pendingOutcome, extra); pendingOutcome = null; });
  byId("exportBtn").textContent = "Submit completed annotations";
  byId("exportBtn").addEventListener("click", async () => {
    const payload = {schema: "musparql.linguistic-annotation-export.v1", assignment_id: hosted.assignment_id, dataset_id: data.dataset_id, reviewer_id: hosted.reviewer_id, task_design_version: "phase-6b-v1", exported_at: new Date().toISOString(), annotations: Object.values(state.completed)};
    const button = byId("exportBtn"); button.disabled = true;
    try {
      const response = await fetch(hosted.submission_url, {method: "POST", credentials: "same-origin", headers: {"Content-Type": "application/json", "X-CSRF-Token": hosted.csrf_token}, body: JSON.stringify(payload)});
      const result = await response.json();
      if (!response.ok) throw new Error(result.error || "Submission was not accepted");
      const completed = Object.keys(state.completed).length;
      const percentage = Math.round((completed / data.record_count) * 100);
      byId("submissionProgress").textContent = `You completed ${percentage}% of this assignment (${completed} of ${data.record_count} items).`;
      byId("submissionReceipt").textContent = `Receipt recorded · revision ${result.revision}.`;
      byId("submissionStatus").hidden = false;
    } catch (error) { window.alert(error.message); }
    finally { button.disabled = false; }
  });
  if (state.finished) finish();
})();
