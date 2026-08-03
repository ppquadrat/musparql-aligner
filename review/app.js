(function () {
  const data = window.REVIEW_DATA || null;
  const HOLDOUT_SPLIT = "private_holdout";

  const els = {
    emptyState: document.getElementById("emptyState"),
    detailView: document.getElementById("detailView"),
    datasetId: document.getElementById("datasetId"),
    recordCount: document.getElementById("recordCount"),
    visibleCount: document.getElementById("visibleCount"),
    reviewedCount: document.getElementById("reviewedCount"),
    searchInput: document.getElementById("searchInput"),
    kgFilter: document.getElementById("kgFilter"),
    modeFilter: document.getElementById("modeFilter"),
    statusFilter: document.getElementById("statusFilter"),
    scopeFilter: document.getElementById("scopeFilter"),
    runFilter: document.getElementById("runFilter"),
    recordList: document.getElementById("recordList"),
    prevBtn: document.getElementById("prevBtn"),
    nextBtn: document.getElementById("nextBtn"),
    exportReviewsBtn: document.getElementById("exportReviewsBtn"),
    importReviewsInput: document.getElementById("importReviewsInput"),
    detailMeta: document.getElementById("detailMeta"),
    detailTitle: document.getElementById("detailTitle"),
    modeBadge: document.getElementById("modeBadge"),
    confidenceBadge: document.getElementById("confidenceBadge"),
    reviewBadge: document.getElementById("reviewBadge"),
    detailQuestion: document.getElementById("detailQuestion"),
    detailOrigin: document.getElementById("detailOrigin"),
    detailModel: document.getElementById("detailModel"),
    detailRun: document.getElementById("detailRun"),
    detailElapsed: document.getElementById("detailElapsed"),
    detailRationale: document.getElementById("detailRationale"),
    sparqlBlock: document.getElementById("sparqlBlock"),
    rankedEvidenceList: document.getElementById("rankedEvidenceList"),
    evidenceCount: document.getElementById("evidenceCount"),
    allEvidenceList: document.getElementById("allEvidenceList"),
    preferredQuestionInput: document.getElementById("preferredQuestionInput"),
    literalWordingInput: document.getElementById("literalWordingInput"),
    reviewNoteInput: document.getElementById("reviewNoteInput"),
    holdoutSplitInput: document.getElementById("holdoutSplitInput"),
    naturalnessInput: document.getElementById("naturalnessInput"),
    pragmatismInput: document.getElementById("pragmatismInput"),
    roomForInterpretationInput: document.getElementById("roomForInterpretationInput"),
    naturalnessValue: document.getElementById("naturalnessValue"),
    pragmatismValue: document.getElementById("pragmatismValue"),
    roomForInterpretationValue: document.getElementById("roomForInterpretationValue"),
    clearInterpretiveBtn: document.getElementById("clearInterpretiveBtn"),
    requiresGraphContextKnowledgeInput: document.getElementById("requiresGraphContextKnowledgeInput"),
    decisionButtons: Array.from(document.querySelectorAll(".decision-btn")),
  };

  window.MUSPARQL_REVIEW_SCHEMA = {
    normalizeReview,
    exportableReview,
    internalReviews,
    validateCompareImportPayload,
    validateImportedReviews,
  };

  if (!data || !Array.isArray(data.records) || !data.records.length) {
    return;
  }

  if (data.mode === "compare") {
    initCompareMode();
    return;
  }

  const reviewStorageKey = `musparql-review:schema2:${data.dataset_id}`;
  let reviews = loadReviews();
  const state = {
    selectedReviewId: data.records[0].review_id,
    search: "",
    kg: "all",
    mode: "all",
    status: "all",
    scope: data.review_scope_policy?.default_scope || "all",
    run: "all",
  };

  els.emptyState.classList.add("hidden");
  els.detailView.classList.remove("hidden");
  els.datasetId.textContent = data.dataset_id;
  els.recordCount.textContent = String(data.record_count);

  populateFilters();
  bindEvents();
  render();

  function loadReviews() {
    try {
      const raw = window.localStorage.getItem(reviewStorageKey);
      const previousRaw = window.localStorage.getItem(`musparql-review:${data.dataset_id}`);
      if (!raw && previousRaw) {
        window.alert("Review data from an earlier schema is still preserved in browser storage. Export it with the earlier workbench before clearing it; it cannot be imported automatically.");
      }
      if (!raw) return {};
      const parsed = JSON.parse(raw);
      return parsed && typeof parsed === "object" ? parsed : {};
    } catch (_err) {
      return {};
    }
  }

  function saveReviews() {
    window.localStorage.setItem(reviewStorageKey, JSON.stringify(reviews));
  }

  function populateFilters() {
    fillSelect(els.kgFilter, ["all", ...uniqueValues(data.records.map((r) => r.kg_id))], "All KGs");
    fillSelect(els.modeFilter, ["all", ...uniqueValues(data.records.map((r) => getMode(r)))], "All modes");
    fillSelect(
      els.statusFilter,
      ["all", "unreviewed", "accepted", "excluded", "prompt_improvement_recommended", "input_data_improvement_recommended"],
      "All review states"
    );
    fillSelect(els.scopeFilter, ["all", "new", "previously_reviewed"], "All scopes");
    els.scopeFilter.value = state.scope;
    fillSelect(els.runFilter, ["all", ...uniqueValues(data.records.map((r) => r.run_label))], "All runs");
  }

  function fillSelect(select, values, firstLabel) {
    select.innerHTML = "";
    values.forEach((value, idx) => {
      const option = document.createElement("option");
      option.value = value;
      option.textContent = idx === 0 ? firstLabel : value;
      select.appendChild(option);
    });
  }

  function uniqueValues(values) {
    return [...new Set(values.filter(Boolean))].sort();
  }

  function bindEvents() {
    els.searchInput.addEventListener("input", () => {
      state.search = els.searchInput.value.trim().toLowerCase();
      render();
    });
    els.kgFilter.addEventListener("change", () => {
      state.kg = els.kgFilter.value;
      render();
    });
    els.modeFilter.addEventListener("change", () => {
      state.mode = els.modeFilter.value;
      render();
    });
    els.statusFilter.addEventListener("change", () => {
      state.status = els.statusFilter.value;
      render();
    });
    els.scopeFilter.addEventListener("change", () => {
      state.scope = els.scopeFilter.value;
      render();
    });
    els.runFilter.addEventListener("change", () => {
      state.run = els.runFilter.value;
      render();
    });
    els.prevBtn.addEventListener("click", () => moveSelection(-1));
    els.nextBtn.addEventListener("click", () => moveSelection(1));
    els.preferredQuestionInput.addEventListener("input", () => updateCurrentReview({ rerender: false }));
    els.literalWordingInput.addEventListener("input", () => updateCurrentReview({ rerender: false }));
    els.reviewNoteInput.addEventListener("input", () => updateCurrentReview({ rerender: false }));
    els.holdoutSplitInput.addEventListener("change", () => updateCurrentReview({ rerender: true }));
    [els.naturalnessInput, els.pragmatismInput, els.roomForInterpretationInput].forEach((input) => {
      input.addEventListener("input", () => {
        input.dataset.hasValue = "true";
        syncSliderOutputs();
        updateCurrentReview({ rerender: false });
      });
    });
    els.clearInterpretiveBtn.addEventListener("click", () => {
      clearInterpretiveInputs();
      updateCurrentReview({ rerender: true });
    });
    els.requiresGraphContextKnowledgeInput.addEventListener("change", () => updateCurrentReview({ rerender: false }));
    els.decisionButtons.forEach((btn) => {
      btn.addEventListener("click", () => {
        updateCurrentReview({ forcedStatus: btn.dataset.status || "", rerender: true });
      });
    });
    els.exportReviewsBtn.addEventListener("click", exportReviews);
    els.importReviewsInput.addEventListener("change", importReviews);
    document.addEventListener("keydown", (event) => {
      if (event.target && ["INPUT", "TEXTAREA", "SELECT"].includes(event.target.tagName)) {
        return;
      }
      if (event.key === "ArrowDown") moveSelection(1);
      if (event.key === "ArrowUp") moveSelection(-1);
    });
  }

  function getMode(record) {
    return record.output?.nl_question_origin?.mode || "unknown";
  }

  function getReview(record) {
    return normalizeReview(reviews[record.review_id]);
  }

  function isHoldoutReview(review) {
    return review?.split === HOLDOUT_SPLIT;
  }

  function getFilteredRecords() {
    return data.records.filter((record) => {
      const review = getReview(record);
      const status = review.status || "unreviewed";
      const haystack = [
        record.query_label,
        record.kg_id,
        record.output?.nl_question,
        record.output?.confidence_rationale,
        ...(record.input?.evidence || []).map((ev) => ev.snippet || ""),
      ]
        .join("\n")
        .toLowerCase();
      if (state.search && !haystack.includes(state.search)) return false;
      if (state.kg !== "all" && record.kg_id !== state.kg) return false;
      if (state.mode !== "all" && getMode(record) !== state.mode) return false;
      if (state.status !== "all" && status !== state.status) return false;
      if (state.scope !== "all" && getReviewScope(record) !== state.scope) return false;
      if (state.run !== "all" && record.run_label !== state.run) return false;
      return true;
    });
  }

  function reviewedCount() {
    return Object.values(reviews).filter((review) => review && review.status).length;
  }

  function render() {
    const filtered = getFilteredRecords();
    if (!filtered.some((rec) => rec.review_id === state.selectedReviewId)) {
      state.selectedReviewId = filtered.length ? filtered[0].review_id : null;
    }
    els.visibleCount.textContent = String(filtered.length);
    els.reviewedCount.textContent = String(reviewedCount());
    renderList(filtered);
    renderDetail(filtered.find((rec) => rec.review_id === state.selectedReviewId) || null);
  }

  function renderList(records) {
    els.recordList.innerHTML = "";
    records.forEach((record) => {
      const review = getReview(record);
      const item = document.createElement("div");
      item.className = "record-item" + (record.review_id === state.selectedReviewId ? " active" : "");
      item.tabIndex = 0;
      item.setAttribute("role", "button");
      item.addEventListener("click", () => {
        state.selectedReviewId = record.review_id;
        render();
      });
      item.addEventListener("keydown", (event) => {
        if (event.key === "Enter" || event.key === " ") {
          event.preventDefault();
          state.selectedReviewId = record.review_id;
          render();
        }
      });
      item.innerHTML = `
        <div class="record-item-title">
          <strong>${escapeHtml(record.query_label)}</strong>
          <span class="pill">${escapeHtml(record.kg_id)}</span>
        </div>
        <p>${escapeHtml(record.output?.nl_question || "No model question")}</p>
        <p class="record-subline">${escapeHtml(record.run_label)} · ${escapeHtml(getMode(record))} · confidence ${escapeHtml(formatInlineValue(record.output?.confidence, "-"))}</p>
        <p class="record-subline">${escapeHtml(review.status || "unreviewed")}${isHoldoutReview(review) ? " · holdout" : ""}${scopeSummary(record)}</p>
      `;
      els.recordList.appendChild(item);
    });
  }

  function renderDetail(record) {
    if (!record) {
      els.detailView.classList.add("hidden");
      els.emptyState.classList.remove("hidden");
      return;
    }
    els.emptyState.classList.add("hidden");
    els.detailView.classList.remove("hidden");

    const review = getReview(record);
    const output = record.output || {};
    const mode = getMode(record);
    const confidence = output.confidence ?? "-";
    const evidence = record.input?.evidence || [];
    const usedEvidenceIds = new Set([
      ...(output.nl_question_origin?.evidence_ids || []),
      ...(output.ranked_evidence_phrases || []).map((item) => item.evidence_id),
    ]);

    els.detailMeta.textContent = `${record.kg_id} · ${record.run_label}`;
    els.detailTitle.textContent = record.query_label;
    els.modeBadge.textContent = mode;
    els.confidenceBadge.textContent = `confidence ${confidence}`;
    els.reviewBadge.textContent = review.status || "unreviewed";
    els.detailQuestion.textContent = output.nl_question || "No model question";
    els.detailOrigin.textContent = formatOrigin(output.nl_question_origin);
    els.detailModel.textContent = record.output_meta?.model || "-";
    els.detailRun.textContent = record.run_label;
    els.detailElapsed.textContent = record.output_meta?.elapsed_ms ? `${record.output_meta.elapsed_ms} ms` : "-";
    els.detailRationale.textContent = output.confidence_rationale || "-";
    els.sparqlBlock.textContent = record.input?.sparql_clean || "No SPARQL found.";
    els.evidenceCount.textContent = `${evidence.length} evidence item${evidence.length === 1 ? "" : "s"}`;

    els.preferredQuestionInput.value = review.preferred_question || "";
    els.literalWordingInput.value = review.literal_wording || "";
    els.reviewNoteInput.value = review.note || "";
    els.holdoutSplitInput.checked = isHoldoutReview(review);
    setInterpretiveInputs(review.interpretive);
    els.decisionButtons.forEach((btn) => {
      btn.classList.toggle("active", btn.dataset.status === (review.status || ""));
    });

    els.rankedEvidenceList.innerHTML = "";
    const ranked = output.ranked_evidence_phrases || [];
    if (!ranked.length) {
      els.rankedEvidenceList.innerHTML = '<p class="muted-meta">No retained evidence phrases.</p>';
    } else {
      ranked.forEach((item) => {
        const card = document.createElement("div");
        card.className = "evidence-card used";
        card.innerHTML = `
          <div class="evidence-meta">
            <span class="pill">rank ${escapeHtml(formatInlineValue(item.rank, "-"))}</span>
            <span class="pill">${escapeHtml(item.source_type)}</span>
            <span class="pill">${escapeHtml(item.evidence_id)}</span>
            <span class="pill">${item.verbatim ? "verbatim" : "cleaned"}</span>
          </div>
          <p class="snippet">${escapeHtml(item.text || "")}</p>
        `;
        els.rankedEvidenceList.appendChild(card);
      });
    }

    els.allEvidenceList.innerHTML = "";
    if (!evidence.length) {
      els.allEvidenceList.innerHTML = '<p class="muted-meta">No input evidence on this record.</p>';
    } else {
      evidence.forEach((item) => {
        const card = document.createElement("div");
        card.className = "evidence-card" + (usedEvidenceIds.has(item.evidence_id) ? " used" : "");
        card.innerHTML = `
          <div class="evidence-meta">
            <span class="pill">${escapeHtml(item.evidence_id || "")}</span>
            <span class="pill">${escapeHtml(item.type || "unknown")}</span>
            ${item.source_path ? `<span class="pill">${escapeHtml(item.source_path)}</span>` : ""}
          </div>
          <p class="snippet">${escapeHtml(item.snippet || "")}</p>
          ${
            item.source_url
              ? `<p class="record-subline">${escapeHtml(item.source_url)}</p>`
              : ""
          }
        `;
        els.allEvidenceList.appendChild(card);
      });
    }
  }

  function moveSelection(delta) {
    const filtered = getFilteredRecords();
    if (!filtered.length) return;
    const idx = filtered.findIndex((rec) => rec.review_id === state.selectedReviewId);
    const nextIdx = Math.max(0, Math.min(filtered.length - 1, (idx >= 0 ? idx : 0) + delta));
    state.selectedReviewId = filtered[nextIdx].review_id;
    render();
  }

  function updateCurrentReview(options = {}) {
    const reviewId = state.selectedReviewId;
    if (!reviewId) return;
    const current = getReviewById(reviewId);
    const nextStatus = Object.prototype.hasOwnProperty.call(options, "forcedStatus")
      ? options.forcedStatus
      : current.status;
    const interpretive = readInterpretiveInputs();
    reviews[reviewId] = {
      status: nextStatus,
      preferred_question: els.preferredQuestionInput.value.trim(),
      literal_wording: els.literalWordingInput.value.trim(),
      note: els.reviewNoteInput.value.trim(),
      split: els.holdoutSplitInput.checked ? HOLDOUT_SPLIT : "",
      interpretive,
      updated_at: new Date().toISOString(),
    };
    if (
      !reviews[reviewId].status &&
      !reviews[reviewId].preferred_question &&
      !reviews[reviewId].literal_wording &&
      !reviews[reviewId].note &&
      !reviews[reviewId].split &&
      isEmptyInterpretive(interpretive)
    ) {
      delete reviews[reviewId];
    }
    saveReviews();
    if (options.rerender !== false) {
      render();
    }
  }

  function getReviewById(reviewId) {
    return normalizeReview(reviews[reviewId]);
  }

  function normalizeReview(review) {
    const note = review?.note || "";
    const allowedStatuses = new Set([
      "accepted",
      "prompt_improvement_recommended",
      "input_data_improvement_recommended",
      "not_applicable",
      "excluded",
    ]);
    const rawStatus = review?.status
      || (review?.benchmark_disposition === "excluded" ? "excluded" : review?.pipeline_assessment)
      || "";
    const status = allowedStatuses.has(rawStatus) ? rawStatus : "";
    return {
      status,
      note,
      preferred_question: review?.preferred_question || "",
      literal_wording: review?.literal_wording || extractLiteralWordingFromNote(note),
      split: review?.split || (review?.benchmark_disposition === "withheld" ? HOLDOUT_SPLIT : ""),
      interpretive: normalizeInterpretive(review?.interpretive),
    };
  }

  function exportableReview(review) {
    const normalized = normalizeReview(review);
    const benchmarkDisposition = normalized.split === HOLDOUT_SPLIT
      ? "withheld"
      : normalized.status === "excluded"
        ? "excluded"
        : normalized.status
          ? "included"
          : null;
    return {
      benchmark_disposition: benchmarkDisposition,
      pipeline_assessment: normalized.status && normalized.status !== "excluded" ? normalized.status : null,
      preferred_question: normalized.preferred_question,
      literal_wording: normalized.literal_wording,
      note: normalized.note,
      split: normalized.split,
      interpretive: normalized.interpretive,
      updated_at: review?.updated_at || null,
      ...(review?.copied_from_review_id ? { copied_from_review_id: review.copied_from_review_id } : {}),
    };
  }

  function exportableReviews(reviewMap) {
    return Object.fromEntries(
      Object.entries(reviewMap).map(([reviewId, review]) => [reviewId, exportableReview(review)])
    );
  }

  function internalReviews(reviewMap) {
    return Object.fromEntries(
      Object.entries(reviewMap).map(([reviewId, review]) => [reviewId, {
        ...normalizeReview(review),
        updated_at: review?.updated_at || null,
        ...(review?.copied_from_review_id ? { copied_from_review_id: review.copied_from_review_id } : {}),
      }])
    );
  }

  function validateCompareImportPayload(payload, currentData = data) {
    if (payload.mode !== "compare") {
      throw new Error("This is not a comparison-review export.");
    }
    if (!payload.dataset_id || payload.dataset_id !== currentData?.dataset_id) {
      throw new Error("This comparison export belongs to a different dataset.");
    }
    const runId = (run) => run?.generation_run_id || run?.run_id || "";
    if (!runId(payload.previous_run) || runId(payload.previous_run) !== runId(currentData?.previous_run)) {
      throw new Error("This comparison export has a different previous run.");
    }
    if (!runId(payload.current_run) || runId(payload.current_run) !== runId(currentData?.current_run)) {
      throw new Error("This comparison export has a different current run.");
    }
  }

  function validateImportedReviews(reviewMap) {
    if (!reviewMap || typeof reviewMap !== "object" || Array.isArray(reviewMap)) {
      throw new Error("Bad review file format.");
    }
    const internalStatuses = new Set([
      "accepted",
      "prompt_improvement_recommended",
      "input_data_improvement_recommended",
      "not_applicable",
      "excluded",
    ]);
    const assessments = new Set([
      "accepted",
      "prompt_improvement_recommended",
      "input_data_improvement_recommended",
      "not_applicable",
    ]);
    const dispositions = new Set(["included", "excluded", "withheld"]);
    for (const [reviewId, review] of Object.entries(reviewMap || {})) {
      if (!review || typeof review !== "object") throw new Error(`Bad review record: ${reviewId}`);
      if (review.status && !internalStatuses.has(review.status)) {
        throw new Error(`Unsupported legacy decision in review ${reviewId}.`);
      }
      if (review.pipeline_assessment && !assessments.has(review.pipeline_assessment)) {
        throw new Error(`Unknown pipeline assessment in review ${reviewId}.`);
      }
      if (review.benchmark_disposition && !dispositions.has(review.benchmark_disposition)) {
        throw new Error(`Unknown benchmark disposition in review ${reviewId}.`);
      }
      if (review.benchmark_disposition === "included" && !review.pipeline_assessment) {
        throw new Error(`Included review ${reviewId} has no pipeline assessment.`);
      }
      if (review.benchmark_disposition === "excluded" && review.pipeline_assessment) {
        throw new Error(`Excluded review ${reviewId} cannot carry a pipeline assessment.`);
      }
    }
  }

  function extractLiteralWordingFromNote(note) {
    const match = String(note || "").match(/(?:^|\n)\s*literal:\s*([^\n]+)/i);
    return match ? match[1].trim() : "";
  }

  function normalizeInterpretive(interpretive) {
    return {
      naturalness: toNullableScore(interpretive?.naturalness),
      pragmatism: toNullableScore(interpretive?.pragmatism),
      room_for_interpretation: toNullableScore(interpretive?.room_for_interpretation),
      requires_graph_context_knowledge: Boolean(interpretive?.requires_graph_context_knowledge),
    };
  }

  function toNullableScore(value) {
    if (value === null || value === undefined || value === "") return null;
    const number = Number(value);
    return Number.isFinite(number) ? number : null;
  }

  function readInterpretiveInputs() {
    return {
      naturalness: readSliderValue(els.naturalnessInput),
      pragmatism: readSliderValue(els.pragmatismInput),
      room_for_interpretation: readSliderValue(els.roomForInterpretationInput),
      requires_graph_context_knowledge: els.requiresGraphContextKnowledgeInput.checked,
    };
  }

  function readSliderValue(input) {
    return input.dataset.hasValue === "true" ? Number(input.value) : null;
  }

  function setInterpretiveInputs(interpretive) {
    setSliderValue(els.naturalnessInput, els.naturalnessValue, interpretive.naturalness);
    setSliderValue(els.pragmatismInput, els.pragmatismValue, interpretive.pragmatism);
    setSliderValue(els.roomForInterpretationInput, els.roomForInterpretationValue, interpretive.room_for_interpretation);
    els.requiresGraphContextKnowledgeInput.checked = Boolean(interpretive.requires_graph_context_knowledge);
  }

  function syncSliderOutputs() {
    els.naturalnessValue.textContent = els.naturalnessInput.dataset.hasValue === "true" ? els.naturalnessInput.value : "-";
    els.pragmatismValue.textContent = els.pragmatismInput.dataset.hasValue === "true" ? els.pragmatismInput.value : "-";
    els.roomForInterpretationValue.textContent = els.roomForInterpretationInput.dataset.hasValue === "true" ? els.roomForInterpretationInput.value : "-";
  }

  function setSliderValue(input, output, value) {
    if (value === null || value === undefined || value === "") {
      input.value = "50";
      input.dataset.hasValue = "false";
      output.textContent = "-";
      return;
    }
    input.value = String(value);
    input.dataset.hasValue = "true";
    output.textContent = String(value);
  }

  function clearInterpretiveInputs() {
    setSliderValue(els.naturalnessInput, els.naturalnessValue, null);
    setSliderValue(els.pragmatismInput, els.pragmatismValue, null);
    setSliderValue(els.roomForInterpretationInput, els.roomForInterpretationValue, null);
    els.requiresGraphContextKnowledgeInput.checked = false;
  }

  function isEmptyInterpretive(interpretive) {
    return (
      interpretive.naturalness === null &&
      interpretive.pragmatism === null &&
      interpretive.room_for_interpretation === null &&
      !interpretive.requires_graph_context_knowledge
    );
  }

  function getReviewScope(record) {
    return record.review_scope || "new";
  }

  function scopeSummary(record) {
    if (getReviewScope(record) !== "previously_reviewed") return "";
    const previous = record.previous_review || {};
    const normalized = normalizeReview(previous);
    if (normalized.status) return ` · prior ${escapeHtml(normalized.status)}`;
    return " · previously reviewed";
  }

  function formatOrigin(origin) {
    if (!origin) return "-";
    const evidenceIds = (origin.evidence_ids || []).join(", ") || "none";
    return `${origin.mode || "unknown"} · evidence ${evidenceIds}`;
  }

  function exportReviews() {
    const payload = {
      dataset_id: data.dataset_id,
      run_id: data.single_run_id,
      run_ids: data.run_ids || [],
      runs: data.runs || [],
      exported_at: new Date().toISOString(),
      reviews: exportableReviews(reviews),
    };
    const blob = new Blob([JSON.stringify(payload, null, 2)], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = `musparql-review-${data.dataset_id}-${timestampForFilename(new Date())}.json`;
    link.click();
    URL.revokeObjectURL(url);
  }

  function importReviews(event) {
    const [file] = event.target.files || [];
    if (!file) return;
    const reader = new FileReader();
    reader.onload = () => {
      try {
        const payload = JSON.parse(String(reader.result || "{}"));
        if (payload.dataset_id && payload.dataset_id !== data.dataset_id) {
          if (!window.confirm("This review file was exported from a different dataset. Import anyway?")) {
            event.target.value = "";
            return;
          }
        }
        if (payload.run_id && data.single_run_id && payload.run_id !== data.single_run_id) {
          if (!window.confirm("This review file points to a different run. Import anyway?")) {
            event.target.value = "";
            return;
          }
        }
        const imported = payload.reviews;
        if (!imported || typeof imported !== "object") {
          throw new Error("Bad review file format.");
        }
        validateImportedReviews(imported);
        reviews = internalReviews(imported);
        saveReviews();
        render();
      } catch (err) {
        window.alert(`Could not import reviews: ${err}`);
      } finally {
        event.target.value = "";
      }
    };
    reader.readAsText(file);
  }

  function escapeHtml(value) {
    return String(value || "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;");
  }

  function formatInlineValue(value, fallback = "") {
    return value === null || value === undefined || value === "" ? fallback : String(value);
  }

  function timestampForFilename(date) {
    const pad = (value) => String(value).padStart(2, "0");
    return [
      date.getFullYear(),
      pad(date.getMonth() + 1),
      pad(date.getDate()),
    ].join("-") + "_" + [pad(date.getHours()), pad(date.getMinutes()), pad(date.getSeconds())].join("-");
  }

  function initCompareMode() {
    const compareStorageKey = `musparql-review-compare:schema2:${data.dataset_id}`;
    let compareReviews = loadCompareReviews();
    const compareState = {
      selectedPairId: data.records[0].pair_id,
      search: "",
      kg: "all",
      change: "all",
      status: "all",
      previousStatus: "all",
    };

    els.emptyState.classList.add("hidden");
    els.detailView.classList.remove("hidden");
    els.datasetId.textContent = data.dataset_id;
    els.recordCount.textContent = String(data.record_count);
    document.querySelector(".subtitle").textContent = "Compare changed pairs across two runs and carry forward reviewer decisions where appropriate.";
    relabelSelect(els.modeFilter, "Change");
    relabelSelect(els.statusFilter, "Current Review");
    relabelSelect(els.runFilter, "Previous Review");
    els.scopeFilter.closest("label").classList.add("hidden");
    fillSelect(els.kgFilter, ["all", ...uniqueValues(data.records.map((r) => r.kg_id))], "All KGs");
    fillSelect(els.modeFilter, ["all", "changed", "added", "removed", ...uniqueValues(data.records.flatMap((r) => r.change_flags || []))], "All changes");
    fillSelect(
      els.statusFilter,
      ["all", "unreviewed", "accepted", "excluded", "prompt_improvement_recommended", "input_data_improvement_recommended"],
      "All current states"
    );
    fillSelect(
      els.runFilter,
      ["all", "unreviewed", "accepted", "excluded", "prompt_improvement_recommended", "input_data_improvement_recommended"],
      "All previous states"
    );

    els.searchInput.addEventListener("input", () => {
      compareState.search = els.searchInput.value.trim().toLowerCase();
      renderCompare();
    });
    els.kgFilter.addEventListener("change", () => {
      compareState.kg = els.kgFilter.value;
      renderCompare();
    });
    els.modeFilter.addEventListener("change", () => {
      compareState.change = els.modeFilter.value;
      renderCompare();
    });
    els.statusFilter.addEventListener("change", () => {
      compareState.status = els.statusFilter.value;
      renderCompare();
    });
    els.runFilter.addEventListener("change", () => {
      compareState.previousStatus = els.runFilter.value;
      renderCompare();
    });
    els.prevBtn.addEventListener("click", () => moveCompareSelection(-1));
    els.nextBtn.addEventListener("click", () => moveCompareSelection(1));
    els.exportReviewsBtn.addEventListener("click", exportCompareReviews);
    els.importReviewsInput.addEventListener("change", importCompareReviews);
    document.addEventListener("keydown", (event) => {
      if (event.target && ["INPUT", "TEXTAREA", "SELECT"].includes(event.target.tagName)) return;
      if (event.key === "ArrowDown") moveCompareSelection(1);
      if (event.key === "ArrowUp") moveCompareSelection(-1);
    });

    renderCompare();

    function loadCompareReviews() {
      try {
        const raw = window.localStorage.getItem(compareStorageKey);
        const previousRaw = window.localStorage.getItem(`musparql-review-compare:${data.dataset_id}`);
        if (!raw && previousRaw) {
          window.alert("Comparison-review data from an earlier schema is still preserved in browser storage. Export it with the earlier workbench before clearing it; it cannot be imported automatically.");
        }
        if (!raw) return {};
        const parsed = JSON.parse(raw);
        return parsed && typeof parsed === "object" ? parsed : {};
      } catch (_err) {
        return {};
      }
    }

    function saveCompareReviews() {
      window.localStorage.setItem(compareStorageKey, JSON.stringify(compareReviews));
    }

    function getCurrentReview(pair) {
      const reviewId = pair.current?.review_id || pair.pair_id;
      return normalizeReview(compareReviews[reviewId]);
    }

    function getPreviousReview(pair) {
      return normalizeReview(pair.previous?.review);
    }

    function getFilteredCompareRecords() {
      return data.records.filter((pair) => {
        const currentReview = getCurrentReview(pair);
        const currentStatus = currentReview.status || "unreviewed";
        const previousStatus = getPreviousReview(pair).status || "unreviewed";
        const previousRecord = pair.previous?.record || {};
        const currentRecord = pair.current?.record || {};
        const haystack = [
          pair.query_label,
          pair.kg_id,
          pair.pair_status,
          ...(pair.change_flags || []),
          previousRecord.output?.nl_question,
          currentRecord.output?.nl_question,
          previousRecord.output?.confidence_rationale,
          currentRecord.output?.confidence_rationale,
          ...((previousRecord.input?.evidence || []).map((ev) => ev.snippet || "")),
          ...((currentRecord.input?.evidence || []).map((ev) => ev.snippet || "")),
        ]
          .join("\n")
          .toLowerCase();
        if (compareState.search && !haystack.includes(compareState.search)) return false;
        if (compareState.kg !== "all" && pair.kg_id !== compareState.kg) return false;
        if (compareState.change !== "all" && pair.pair_status !== compareState.change && !(pair.change_flags || []).includes(compareState.change)) return false;
        if (compareState.status !== "all" && currentStatus !== compareState.status) return false;
        if (compareState.previousStatus !== "all" && previousStatus !== compareState.previousStatus) return false;
        return true;
      });
    }

    function renderCompare() {
      const filtered = getFilteredCompareRecords();
      if (!filtered.some((pair) => pair.pair_id === compareState.selectedPairId)) {
        compareState.selectedPairId = filtered.length ? filtered[0].pair_id : null;
      }
      els.visibleCount.textContent = String(filtered.length);
      els.reviewedCount.textContent = String(Object.values(compareReviews).filter((review) => review && review.status).length);
      renderCompareList(filtered);
      renderCompareDetail(filtered.find((pair) => pair.pair_id === compareState.selectedPairId) || null);
    }

    function renderCompareList(records) {
      els.recordList.innerHTML = "";
      records.forEach((pair) => {
        const currentReview = getCurrentReview(pair);
        const previousReview = getPreviousReview(pair);
        const currentQuestion = pair.current?.record?.output?.nl_question || "No current question";
        const item = document.createElement("div");
        item.className = "record-item" + (pair.pair_id === compareState.selectedPairId ? " active" : "");
        item.tabIndex = 0;
        item.setAttribute("role", "button");
        item.addEventListener("click", () => {
          compareState.selectedPairId = pair.pair_id;
          renderCompare();
        });
        item.addEventListener("keydown", (event) => {
          if (event.key === "Enter" || event.key === " ") {
            event.preventDefault();
            compareState.selectedPairId = pair.pair_id;
            renderCompare();
          }
        });
        item.innerHTML = `
          <div class="record-item-title">
            <strong>${escapeHtml(pair.query_label)}</strong>
            <span class="pill">${escapeHtml(pair.kg_id)}</span>
          </div>
          <p>${escapeHtml(currentQuestion)}</p>
          <p class="record-subline">${escapeHtml(pair.pair_status)} · ${escapeHtml((pair.change_flags || []).join(", ") || "no field changes")}</p>
          <p class="record-subline">old: ${escapeHtml(previousReview.status || "unreviewed")} · new: ${escapeHtml(currentReview.status || "unreviewed")}${isHoldoutReview(currentReview) ? " · holdout" : ""}</p>
        `;
        els.recordList.appendChild(item);
      });
    }

    function renderCompareDetail(pair) {
      if (!pair) {
        els.detailView.classList.add("hidden");
        els.emptyState.classList.remove("hidden");
        return;
      }
      els.emptyState.classList.add("hidden");
      els.detailView.classList.remove("hidden");
      const previousRecord = pair.previous?.record || null;
      const currentRecord = pair.current?.record || null;
      const previousReview = getPreviousReview(pair);
      const currentReview = getCurrentReview(pair);
      const currentReviewId = pair.current?.review_id || pair.pair_id;
      const flags = pair.change_flags || [];

      els.detailView.innerHTML = `
        <section class="panel hero detail-top compare-top">
          <div class="hero-head">
            <div>
              <p class="eyebrow">${escapeHtml(pair.kg_id)} · ${escapeHtml(pair.pair_status)}</p>
              <h2>${escapeHtml(pair.query_label)}</h2>
            </div>
            <div class="hero-actions">
              <div class="pager">
                <button id="comparePrevBtn" class="btn small">Prev</button>
                <button id="compareNextBtn" class="btn small">Next</button>
              </div>
              <div class="hero-badges">
                ${flags.map((flag) => `<span class="badge">${escapeHtml(flag)}</span>`).join("") || '<span class="badge muted">unchanged</span>'}
              </div>
            </div>
          </div>
          <div class="compare-actions">
            <button id="reusePreviousBtn" class="btn small">Reuse Previous Decision</button>
            <button id="usePreviousWordingBtn" class="btn small">Use Previous Wording</button>
            <button id="acceptNewBtn" class="btn small solid">Accept New Question</button>
            <button id="editBetterBtn" class="btn small">Edit Better Wording</button>
          </div>
        </section>

        <section class="compare-grid">
          ${renderRunColumn("Previous", previousRecord, previousReview, pair.evidence_diff || {}, "previous", flags)}
          ${renderRunColumn("Current", currentRecord, currentReview, pair.evidence_diff || {}, "current", flags)}
        </section>
      `;

      document.getElementById("comparePrevBtn").addEventListener("click", () => moveCompareSelection(-1));
      document.getElementById("compareNextBtn").addEventListener("click", () => moveCompareSelection(1));
      document.getElementById("reusePreviousBtn").addEventListener("click", () => {
        compareReviews[currentReviewId] = {
          status: previousReview.status || "",
          preferred_question: previousReview.preferred_question || "",
          literal_wording: previousReview.literal_wording || "",
          note: previousReview.note || "",
          split: "",
          copied_from_review_id: pair.previous?.review_id || null,
          updated_at: new Date().toISOString(),
        };
        cleanupEmptyCompareReview(currentReviewId);
        saveCompareReviews();
        renderCompare();
      });
      document.getElementById("usePreviousWordingBtn").addEventListener("click", () => {
        updateCompareReview(currentReviewId, {
          preferred_question: previousReview.preferred_question || previousRecord?.output?.nl_question || "",
        });
      });
      document.getElementById("reusePreviousInlineBtn")?.addEventListener("click", () => {
        compareReviews[currentReviewId] = {
          status: previousReview.status || "",
          preferred_question: previousReview.preferred_question || "",
          literal_wording: previousReview.literal_wording || "",
          note: previousReview.note || "",
          split: "",
          copied_from_review_id: pair.previous?.review_id || null,
          updated_at: new Date().toISOString(),
        };
        cleanupEmptyCompareReview(currentReviewId);
        saveCompareReviews();
        renderCompare();
      });
      document.getElementById("usePreviousWordingInlineBtn")?.addEventListener("click", () => {
        updateCompareReview(currentReviewId, {
          preferred_question: previousReview.preferred_question || previousRecord?.output?.nl_question || "",
        });
      });
      document.getElementById("usePreviousNoteInlineBtn")?.addEventListener("click", () => {
        updateCompareReview(currentReviewId, {
          note: previousReview.note || "",
        });
      });
      document.getElementById("acceptNewBtn").addEventListener("click", () => {
        updateCompareReview(currentReviewId, {
          status: "accepted",
          preferred_question: "",
        });
      });
      document.getElementById("editBetterBtn").addEventListener("click", () => {
        document.getElementById("comparePreferredInput")?.focus();
      });
      Array.from(els.detailView.querySelectorAll(".decision-btn")).forEach((btn) => {
        btn.addEventListener("click", () => updateCompareReview(currentReviewId, { status: btn.dataset.status || "" }));
      });
      document.getElementById("comparePreferredInput")?.addEventListener("input", () => {
        updateCompareReview(currentReviewId, { preferred_question: document.getElementById("comparePreferredInput").value.trim() }, false);
      });
      document.getElementById("compareLiteralInput")?.addEventListener("input", () => {
        updateCompareReview(currentReviewId, { literal_wording: document.getElementById("compareLiteralInput").value.trim() }, false);
      });
      document.getElementById("compareNoteInput")?.addEventListener("input", () => {
        updateCompareReview(currentReviewId, { note: document.getElementById("compareNoteInput").value.trim() }, false);
      });
      document.getElementById("compareHoldoutInput")?.addEventListener("change", () => {
        updateCompareReview(
          currentReviewId,
          { split: document.getElementById("compareHoldoutInput").checked ? HOLDOUT_SPLIT : "" },
          true
        );
      });
    }

    function renderRunColumn(title, record, review, evidenceDiff, side, flags) {
      if (!record) {
        return `
          <section class="panel compare-column">
            <p class="section-label">${escapeHtml(title)}</p>
            <h2>No ${escapeHtml(title.toLowerCase())} record</h2>
            <p class="muted-meta">This pair is ${side === "previous" ? "new in the current run" : "missing from the current run"}.</p>
          </section>
        `;
      }
      const output = record.output || {};
      const evidence = record.input?.evidence || [];
      const ranked = output.ranked_evidence_phrases || [];
      const sparqlChanged = (flags || []).includes("sparql_changed");
      return `
        <section class="panel compare-column">
          <p class="section-label">${escapeHtml(title)} Run</p>
          <h2>${escapeHtml(record.run_label || "-")}</h2>
          ${renderSparqlDetails(record, sparqlChanged)}
          <div class="question-block compare-question">
            <p class="section-label">Model question</p>
            <p class="big-question">${escapeHtml(output.nl_question || "No model question")}</p>
          </div>
          <div class="meta-grid compact">
            <div><p class="section-label">Review</p><p>${escapeHtml(review.status || "unreviewed")}</p></div>
            <div><p class="section-label">Confidence</p><p>${escapeHtml(formatInlineValue(output.confidence, "-"))}</p></div>
            <div><p class="section-label">Origin</p><p>${escapeHtml(formatOrigin(output.nl_question_origin))}</p></div>
            <div><p class="section-label">Model</p><p>${escapeHtml(record.output_meta?.model || "-")}</p></div>
          </div>
          <p class="section-label">Retained evidence phrases</p>
          <div class="stack-list compact-list">${renderRankedEvidence(ranked)}</div>
          ${side === "previous" ? renderPreviousReviewPanel(review, record) : renderCurrentReviewPanel(review, flags)}
          <div class="compare-rationale">
            <p class="section-label">Justification</p>
            <p>${escapeHtml(output.confidence_rationale || "-")}</p>
          </div>
          <p class="section-label compare-section-gap">All input evidence (${evidence.length})</p>
          <div class="stack-list">${renderEvidenceList(evidence, evidenceDiff, side)}</div>
        </section>
      `;
    }

    function renderPreviousReviewPanel(review, record) {
      return `
        <section class="compare-review-panel previous-review-panel">
          <div class="panel-head">
            <h2>Previous Review</h2>
            <button id="reusePreviousInlineBtn" class="btn small">Reuse</button>
          </div>
          <div class="compare-review-fields">
            <div class="compare-review-note">
            <div class="compare-note-head">
              <p class="section-label">Preferred wording</p>
              <button id="usePreviousWordingInlineBtn" class="btn small">Reuse Wording</button>
            </div>
            <p>${escapeHtml(review.preferred_question || record.output?.nl_question || "No preferred wording")}</p>
            </div>
            <div class="compare-review-note">
            <div class="compare-note-head">
              <p class="section-label">Literal wording</p>
            </div>
            <p>${escapeHtml(review.literal_wording || "No literal wording")}</p>
            </div>
            <div class="compare-review-note">
            <div class="compare-note-head">
              <p class="section-label">Note</p>
              <button id="usePreviousNoteInlineBtn" class="btn small">Reuse Note</button>
            </div>
            <p>${escapeHtml(review.note || "No note")}</p>
            </div>
          </div>
        </section>
      `;
    }

    function renderCurrentReviewPanel(review, flags) {
      const canHoldout = (flags || []).includes("new_pair");
      return `
        <section class="compare-review-panel current-review-panel">
          <div class="panel-head compare-review-head">
            <h2>Current Review</h2>
            <div class="decision-grid">
              <button data-status="accepted" class="decision-btn accepted ${review.status === "accepted" ? "active" : ""}">Accept</button>
              <button data-status="excluded" class="decision-btn excluded ${review.status === "excluded" ? "active" : ""}">Exclude</button>
              <span class="decision-row-break" aria-hidden="true"></span>
              <button data-status="prompt_improvement_recommended" class="decision-btn prompt ${review.status === "prompt_improvement_recommended" ? "active" : ""}">Recommend Prompt Improvement</button>
              <button data-status="input_data_improvement_recommended" class="decision-btn data ${review.status === "input_data_improvement_recommended" ? "active" : ""}">Recommend Input-Data Improvement</button>
              <button data-status="" class="decision-btn clear ${!review.status ? "active" : ""}">Clear</button>
            </div>
          </div>
          <div class="compare-review-fields">
            ${
              canHoldout || isHoldoutReview(review)
                ? `<label class="checkbox-field compare-holdout-field">
                    <input id="compareHoldoutInput" type="checkbox" ${isHoldoutReview(review) ? "checked" : ""} />
                    <span>Private holdout</span>
                    <small>Exclude this new pair from the public/dev benchmark, prompt work, and normal autoeval.</small>
                  </label>`
                : ""
            }
            <label>
              <span>Preferred / corrected NL question</span>
              <textarea id="comparePreferredInput" rows="2" placeholder="Optional better wording">${escapeHtml(review.preferred_question || "")}</textarea>
            </label>
            <label>
              <span>Literal SPARQL wording</span>
              <textarea id="compareLiteralInput" rows="2" placeholder="Optional literal wording for exact query semantics">${escapeHtml(review.literal_wording || "")}</textarea>
            </label>
            <label>
              <span>Reviewer note</span>
              <textarea id="compareNoteInput" rows="4" placeholder="Why this is good, bad, or needs a fix">${escapeHtml(review.note || "")}</textarea>
            </label>
          </div>
        </section>
      `;
    }

    function renderSparqlDetails(record, sparqlChanged) {
      return `
        <details class="sparql-details" ${sparqlChanged ? "open" : ""}>
          <summary>
            <span>SPARQL</span>
            ${sparqlChanged ? '<span class="pill diff-pill">changed</span>' : '<span class="muted-meta">show query</span>'}
          </summary>
          <pre class="code-block compare-code">${escapeHtml(record.input?.sparql_clean || "No SPARQL found.")}</pre>
        </details>
      `;
    }

    function renderRankedEvidence(ranked) {
      if (!ranked.length) return '<p class="muted-meta">No retained evidence phrases.</p>';
      return ranked.map((item) => `
        <div class="evidence-card used">
          <div class="evidence-meta">
            <span class="pill">rank ${escapeHtml(formatInlineValue(item.rank, "-"))}</span>
            <span class="pill">${escapeHtml(item.source_type || "")}</span>
            <span class="pill">${escapeHtml(item.evidence_id || "")}</span>
          </div>
          <p class="snippet">${escapeHtml(item.text || "")}</p>
        </div>
      `).join("");
    }

    function renderEvidenceList(evidence, diff, side) {
      if (!evidence.length) return '<p class="muted-meta">No input evidence on this record.</p>';
      return evidence.map((item, idx) => {
        const evidenceId = item.evidence_id || `idx-${idx}`;
        const tags = [];
        if (side === "current" && (diff.added || []).includes(evidenceId)) tags.push("new");
        if (side === "previous" && (diff.removed || []).includes(evidenceId)) tags.push("removed");
        if ((diff.changed || []).includes(evidenceId)) tags.push("changed");
        return `
          <div class="evidence-card ${tags.includes("new") ? "diff-added" : ""} ${tags.includes("removed") ? "diff-removed" : ""} ${tags.includes("changed") ? "diff-changed" : ""}">
            <div class="evidence-meta">
              <span class="pill">${escapeHtml(evidenceId)}</span>
              <span class="pill">${escapeHtml(item.type || "unknown")}</span>
              ${tags.map((tag) => `<span class="pill diff-pill">${escapeHtml(tag)}</span>`).join("")}
            </div>
            <p class="snippet">${escapeHtml(item.snippet || "")}</p>
          </div>
        `;
      }).join("");
    }

    function updateCompareReview(reviewId, patch, rerender = true) {
      const existing = compareReviews[reviewId] || { status: "", preferred_question: "", literal_wording: "", note: "", split: "" };
      compareReviews[reviewId] = {
        ...existing,
        ...patch,
        updated_at: new Date().toISOString(),
      };
      cleanupEmptyCompareReview(reviewId);
      saveCompareReviews();
      if (rerender) renderCompare();
    }

    function cleanupEmptyCompareReview(reviewId) {
      const review = compareReviews[reviewId];
      if (review && !review.status && !review.preferred_question && !review.literal_wording && !review.note && !review.split) {
        delete compareReviews[reviewId];
      }
    }

    function moveCompareSelection(delta) {
      const filtered = getFilteredCompareRecords();
      if (!filtered.length) return;
      const idx = filtered.findIndex((pair) => pair.pair_id === compareState.selectedPairId);
      const nextIdx = Math.max(0, Math.min(filtered.length - 1, (idx >= 0 ? idx : 0) + delta));
      compareState.selectedPairId = filtered[nextIdx].pair_id;
      renderCompare();
    }

    function exportCompareReviews() {
      const payload = {
        dataset_id: data.dataset_id,
        mode: "compare",
        previous_run: data.previous_run,
        current_run: data.current_run,
        exported_at: new Date().toISOString(),
        reviews: exportableReviews(compareReviews),
      };
      const blob = new Blob([JSON.stringify(payload, null, 2)], { type: "application/json" });
      const url = URL.createObjectURL(blob);
      const link = document.createElement("a");
      link.href = url;
      link.download = `musparql-review-compare-${data.dataset_id}-${timestampForFilename(new Date())}.json`;
      link.click();
      URL.revokeObjectURL(url);
    }

    function importCompareReviews(event) {
      const [file] = event.target.files || [];
      if (!file) return;
      const reader = new FileReader();
      reader.onload = () => {
        try {
          const payload = JSON.parse(String(reader.result || "{}"));
          validateCompareImportPayload(payload);
          const imported = payload.reviews;
          validateImportedReviews(imported);
          compareReviews = internalReviews(imported);
          saveCompareReviews();
          renderCompare();
        } catch (err) {
          window.alert(`Could not import reviews: ${err}`);
        } finally {
          event.target.value = "";
        }
      };
      reader.readAsText(file);
    }
  }

  function relabelSelect(select, label) {
    const labelNode = select.closest("label")?.querySelector("span");
    if (labelNode) labelNode.textContent = label;
  }
})();
