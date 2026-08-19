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
    return {queue: shuffled(bundle.records.map((item) => item.trial_id)), completed: {}, drafts: {}, skips: {}, finished: false};
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

  window.MUSPARQL_LINGUISTIC = {shuffled, stateKey, freshState, validateBundle, normalizedTrial};
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
  const save = () => localStorage.setItem(key, JSON.stringify(state));
  const show = (id) => { for (const section of ["instructions", "workbench", "outcomePanel", "finished"]) byId(section).hidden = section !== id; };
  const progressText = () => `${Object.keys(state.completed).length} completed of up to ${data.record_count}`;
  byId("identity").textContent = `Signed in as ${hosted.reviewer_id}`;

  function draftFor(stimulus) {
    if (!state.drafts[stimulus.trial_id]) {
      const ids = shuffled(stimulus.candidates.map((item) => item.formulation_id));
      const ratings = Object.fromEntries(ids.map((id) => [id, Object.fromEntries(dimensions.map((dimension) => [dimension, {value: 0, touched: false}]))]));
      state.drafts[stimulus.trial_id] = {display_order: ids, ratings, started_at: new Date().toISOString()};
      save();
    }
    return state.drafts[stimulus.trial_id];
  }

  function slider(candidateId, dimension, draft) {
    const row = document.createElement("div"); row.className = "slider-row";
    const label = document.createElement("label");
    const title = document.createElement("span"); title.textContent = labels[dimension];
    const value = document.createElement("span"); value.className = draft.ratings[candidateId][dimension].touched ? "answered" : "unanswered";
    value.textContent = draft.ratings[candidateId][dimension].touched ? String(draft.ratings[candidateId][dimension].value) : "Unanswered";
    label.append(title, value);
    const input = document.createElement("input"); input.type = "range"; input.min = "-100"; input.max = "100"; input.step = "1"; input.value = String(draft.ratings[candidateId][dimension].value);
    input.setAttribute("aria-label", `${labels[dimension]} for formulation ${candidateId}`);
    input.addEventListener("input", () => { draft.ratings[candidateId][dimension] = {value: Number(input.value), touched: true}; value.textContent = input.value; value.className = "answered"; save(); });
    const ticks = document.createElement("div"); ticks.className = "ticks";
    for (const tick of ["−100", "−50", "0", "+25", "+50", "+75", "+100"]) { const span = document.createElement("span"); span.textContent = tick; ticks.append(span); }
    row.append(label, input, ticks); return row;
  }

  function render() {
    if (!state.queue.length) { finish(); return; }
    currentId = state.queue[0]; const stimulus = records.get(currentId); const draft = draftFor(stimulus);
    byId("progress").textContent = progressText(); byId("queryLabel").textContent = stimulus.query_label;
    byId("sparql").textContent = stimulus.sparql; byId("literal").textContent = stimulus.literal.text;
    const grid = byId("candidateGrid"); grid.replaceChildren();
    draft.display_order.forEach((id, index) => {
      const candidate = stimulus.candidates.find((item) => item.formulation_id === id);
      const card = document.createElement("section"); card.className = "candidate";
      const heading = document.createElement("h2"); heading.textContent = `Formulation ${index === 0 ? "A" : "B"}`;
      const wording = document.createElement("p"); wording.className = "wording"; wording.textContent = candidate.text;
      card.append(heading, wording); for (const dimension of dimensions) card.append(slider(id, dimension, draft)); grid.append(card);
    });
    byId("ratingError").hidden = true; show("workbench");
  }

  function complete(outcome, extra = {}) {
    const stimulus = records.get(currentId); const draft = draftFor(stimulus);
    state.completed[currentId] = normalizedTrial(stimulus, hosted, data, draft, outcome, extra);
    delete state.drafts[currentId]; state.queue = state.queue.filter((id) => id !== currentId); save(); render();
  }
  function finish() { state.finished = true; save(); byId("finishedProgress").textContent = progressText(); show("finished"); }

  byId("beginBtn").addEventListener("click", () => { state.finished = false; save(); render(); });
  byId("resumeBtn").addEventListener("click", () => { state.finished = false; save(); render(); });
  byId("finishBtn").addEventListener("click", finish);
  byId("ratingForm").addEventListener("submit", (event) => { event.preventDefault(); try { complete("rated"); } catch (_) { byId("ratingError").hidden = false; } });
  byId("skipBtn").addEventListener("click", () => { delete state.drafts[currentId]; state.skips[currentId] = (state.skips[currentId] || 0) + 1; state.queue.push(state.queue.shift()); save(); render(); });
  function openOutcome(outcome) { pendingOutcome = outcome; byId("outcomeTitle").textContent = outcome === "cannot_assess" ? "Cannot assess this presentation" : "Report an inaccurate literal"; byId("reasonRow").hidden = outcome !== "cannot_assess"; byId("proposalRow").hidden = outcome !== "literal_inaccurate"; byId("reason").value = ""; byId("proposal").value = ""; byId("comment").value = ""; show("outcomePanel"); }
  byId("cannotBtn").addEventListener("click", () => openOutcome("cannot_assess"));
  byId("literalErrorBtn").addEventListener("click", () => openOutcome("literal_inaccurate"));
  byId("cancelOutcomeBtn").addEventListener("click", () => { pendingOutcome = null; show("workbench"); });
  byId("confirmOutcomeBtn").addEventListener("click", () => { const extra = {reason: byId("reason").value, proposed_literal: byId("proposal").value.trim(), comment: byId("comment").value.trim()}; complete(pendingOutcome, extra); pendingOutcome = null; });
  byId("exportBtn").addEventListener("click", () => {
    const payload = {schema: "musparql.linguistic-annotation-export.v1", assignment_id: hosted.assignment_id, dataset_id: data.dataset_id, reviewer_id: hosted.reviewer_id, task_design_version: "phase-6b-v1", exported_at: new Date().toISOString(), annotations: Object.values(state.completed)};
    const blob = new Blob([JSON.stringify(payload, null, 2) + "\n"], {type: "application/json"}); const link = document.createElement("a"); link.href = URL.createObjectURL(blob); link.download = `${hosted.assignment_id}-linguistic-annotations.json`; link.click(); URL.revokeObjectURL(link.href);
  });
  if (state.finished) finish();
})();
