(function () {
  const data = window.REVIEW_DATA || null;
  const hosted = window.MUSPARQL_HOSTED_CONTEXT || null;
  const hostedNoHoldout = Boolean(hosted && !hosted.holdout_capability);
  const HOLDOUT_SPLIT = "private_holdout";

  const els = {
    emptyState: document.getElementById("emptyState"),
    detailView: document.getElementById("detailView"),
    datasetId: document.getElementById("datasetId"),
    recordCount: document.getElementById("recordCount"),
    visibleCount: document.getElementById("visibleCount"),
    reviewedCount: document.getElementById("reviewedCount"),
    holdoutCount: document.getElementById("holdoutCount"),
    searchInput: document.getElementById("searchInput"),
    kgFilter: document.getElementById("kgFilter"),
    modeFilter: document.getElementById("modeFilter"),
    statusFilter: document.getElementById("statusFilter"),
    holdoutFilter: document.getElementById("holdoutFilter"),
    scopeFilter: document.getElementById("scopeFilter"),
    runFilter: document.getElementById("runFilter"),
    recordList: document.getElementById("recordList"),
    prevBtn: document.getElementById("prevBtn"),
    nextBtn: document.getElementById("nextBtn"),
    exportReviewsBtn: document.getElementById("exportReviewsBtn"),
    exportPrivateReviewsBtn: document.getElementById("exportPrivateReviewsBtn"),
    exportHoldoutSelectorsBtn: document.getElementById("exportHoldoutSelectorsBtn"),
    holdoutSelectorsInput: document.getElementById("holdoutSelectorsInput"),
    holdoutSelectorDialog: document.getElementById("holdoutSelectorDialog"),
    chooseExistingSelectorsBtn: document.getElementById("chooseExistingSelectorsBtn"),
    createNewSelectorsBtn: document.getElementById("createNewSelectorsBtn"),
    clearPrivateStateBtn: document.getElementById("clearPrivateStateBtn"),
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
    publicCommentInput: document.getElementById("publicCommentInput"),
    internalCommentInput: document.getElementById("internalCommentInput"),
    holdoutSplitInput: document.getElementById("holdoutSplitInput"),
    decisionButtons: Array.from(document.querySelectorAll(".decision-btn")),
  };

  window.MUSPARQL_REVIEW_SCHEMA = {
    normalizeReview,
    exportableReview,
    internalReviews,
    partitionReviewMap,
    validateCompareImportPayload,
    validateReviewerImport,
    validateImportedReviews,
    matchPrivateRecords,
    rejectPrivateImport,
    hasReviewerDecision,
    reviewDecisionCount,
    initialHoldoutEligibility,
    compareHoldoutEligibility,
    sparqlEditHoldoutEligibility,
    reusedPreviousReview,
    resetFormulationAttribution,
    editedFormulationAttribution,
    parseHoldoutSelectors,
    validateHoldoutSelector,
    selectorForRecord,
    mergeHoldoutSelectors,
    selectorUpdateForReview,
  };

  initHostedSession();

  if (!data || !Array.isArray(data.records) || !data.records.length) {
    return;
  }
  if (!/^reviewer-[0-9]{4,}$/.test(data.reviewer_id || "")) {
    throw new Error("Review bundle requires a pseudonymous reviewer-NNNN identifier.");
  }

  const selectorExportAllowed = !hostedNoHoldout
    && data.holdout_input_policy !== "identity_private_filtered_upstream";
  els.exportHoldoutSelectorsBtn.classList.toggle("hidden", !selectorExportAllowed);
  if (hostedNoHoldout) hideHostedHoldoutControls();

  if (data.mode === "compare") {
    initCompareMode();
    return;
  }

  const reviewStorageKey = hosted
    ? `musparql-review:schema5:${data.dataset_id}:${data.reviewer_id}:${hosted.assignment_id}`
    : `musparql-review:schema4:${data.dataset_id}:${data.reviewer_id}`;
  let reviews = loadReviews();
  let privateExportReady = false;
  const state = {
    selectedReviewId: data.records[0].review_id,
    search: "",
    kg: "all",
    mode: "all",
    status: "all",
    holdout: "all",
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
      const legacyRaw = !hosted && data.reviewer_id === "reviewer-0001"
        ? window.localStorage.getItem(`musparql-review:schema3:${data.dataset_id}`)
          || window.localStorage.getItem(`musparql-review:schema2:${data.dataset_id}`)
          || window.localStorage.getItem(`musparql-review:${data.dataset_id}`)
        : null;
      const raw = window.localStorage.getItem(reviewStorageKey) || legacyRaw;
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
    fillSelect(els.holdoutFilter, ["all", "holdout", "non_holdout"], "All sets");
    els.holdoutFilter.options[1].textContent = "Holdout only";
    els.holdoutFilter.options[2].textContent = "Non-holdout only";
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
    els.holdoutFilter.addEventListener("change", () => {
      state.holdout = els.holdoutFilter.value;
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
    els.publicCommentInput.addEventListener("input", () => updateCurrentReview({ rerender: false }));
    els.internalCommentInput.addEventListener("input", () => updateCurrentReview({ rerender: false }));
    els.holdoutSplitInput.addEventListener("change", () => updateCurrentReview({ rerender: true, holdoutSelectionTouched: true }));
    els.decisionButtons.forEach((btn) => {
      btn.addEventListener("click", () => {
        updateCurrentReview({ forcedStatus: btn.dataset.status || "", rerender: true });
      });
    });
    els.exportReviewsBtn.addEventListener("click", exportReviews);
    els.exportPrivateReviewsBtn.addEventListener("click", exportPrivateReviews);
    bindHoldoutSelectorExport(() => selectorUpdatesForInitialReview());
    els.clearPrivateStateBtn.addEventListener("click", clearPrivateState);
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
      if (state.holdout === "holdout" && !isHoldoutReview(review)) return false;
      if (state.holdout === "non_holdout" && isHoldoutReview(review)) return false;
      if (state.scope !== "all" && getReviewScope(record) !== state.scope) return false;
      if (state.run !== "all" && record.run_label !== state.run) return false;
      return true;
    });
  }

  function reviewedCount() {
    return reviewDecisionCount(reviews);
  }

  function holdoutCount(reviewMap) {
    return Object.values(reviewMap || {}).filter((review) => isHoldoutReview(normalizeReview(review))).length;
  }

  function render() {
    const filtered = getFilteredRecords();
    if (!filtered.some((rec) => rec.review_id === state.selectedReviewId)) {
      state.selectedReviewId = filtered.length ? filtered[0].review_id : null;
    }
    els.visibleCount.textContent = String(filtered.length);
    els.reviewedCount.textContent = String(reviewedCount());
    els.holdoutCount.textContent = String(holdoutCount(reviews));
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
    els.publicCommentInput.value = review.public_comment || "";
    els.internalCommentInput.value = review.internal_comment || "";
    els.holdoutSplitInput.checked = review.holdout_selector_selected;
    const holdoutEligibility = initialHoldoutEligibility(record, data.holdout_review_provenance_complete);
    const savedIneligibleHoldout = isHoldoutReview(review) && !holdoutEligibility.eligible;
    els.holdoutSplitInput.disabled = !holdoutEligibility.eligible && !review.holdout_selector_selected;
    document.getElementById("holdoutEligibilityHelp").textContent = savedIneligibleHoldout
      ? review.holdout_selector_selected
        ? "This saved holdout is no longer eligible. Uncheck it to mark an identity-visible selector removal; its annotations will remain private."
        : "Selector removal marked. This pair's annotations remain private and must still be privately exported and cleared."
      : holdoutEligibility.reason;
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
    privateExportReady = false;
    const current = getReviewById(reviewId);
    const nextStatus = Object.prototype.hasOwnProperty.call(options, "forcedStatus")
      ? options.forcedStatus
      : current.status;
    // The current review UI does not collect linguistic dimensions. Preserve
    // values from historical imports until that work has its own interface.
    const interpretive = current.interpretive;
    const record = data.records.find((candidate) => candidate.review_id === reviewId);
    const holdoutEligible = initialHoldoutEligibility(record, data.holdout_review_provenance_complete).eligible;
    const selectorSelected = els.holdoutSplitInput.checked;
    const keepAnnotationPrivate = isHoldoutReview(current);
    reviews[reviewId] = {
      review_id: current.review_id || `${reviewId}::${data.reviewer_id}`,
      reviewer_id: data.reviewer_id || current.reviewer_id || "",
      status: nextStatus,
      preferred_question: els.preferredQuestionInput.value.trim(),
      literal_wording: els.literalWordingInput.value.trim(),
      public_comment: els.publicCommentInput.value.trim(),
      internal_comment: els.internalCommentInput.value.trim(),
      split: keepAnnotationPrivate || (selectorSelected && holdoutEligible) ? HOLDOUT_SPLIT : "",
      holdout_selection_touched: options.holdoutSelectionTouched === true || current.holdout_selection_touched,
      holdout_selector_selected: selectorSelected,
      interpretive,
      reviewed_at: new Date().toISOString(),
      prior_review_ids: Array.isArray(record?.prior_review_ids) ? record.prior_review_ids : [],
      authored_formulation_ids: current.authored_formulation_ids,
      approved_formulation_ids: current.approved_formulation_ids,
    };
    if (
      !reviews[reviewId].status &&
      !reviews[reviewId].preferred_question &&
      !reviews[reviewId].literal_wording &&
      !reviews[reviewId].public_comment &&
      !reviews[reviewId].internal_comment &&
      !reviews[reviewId].split &&
      !reviews[reviewId].holdout_selection_touched &&
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
    const legacyNote = review?.note || "";
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
    const literalWording = review?.literal_wording || extractLiteralWordingFromNote(legacyNote);
    const hasPublicComment = review != null
      && Object.prototype.hasOwnProperty.call(review, "public_comment");
    const split = review?.split || (review?.benchmark_disposition === "withheld" ? HOLDOUT_SPLIT : "");
    const copiedFormulationRoles = Array.isArray(review?.copied_formulation_roles)
      ? review.copied_formulation_roles.filter((role) => role === "preferred" || role === "literal")
      : review?.copied_from_review_id
        ? [review?.preferred_question ? "preferred" : "", literalWording ? "literal" : ""].filter(Boolean)
        : [];
    return {
      review_id: review?.review_id || "",
      reviewer_id: review?.reviewer_id || data?.reviewer_id || "",
      status,
      preferred_question: review?.preferred_question || "",
      literal_wording: literalWording,
      public_comment: hasPublicComment ? (review?.public_comment || "") : "",
      internal_comment: hasPublicComment
        ? (review?.internal_comment || "")
        : removeLiteralFromComment(review?.internal_comment || legacyNote, literalWording),
      split,
      holdout_selection_touched: review?.holdout_selection_touched === true,
      holdout_selector_selected: typeof review?.holdout_selector_selected === "boolean"
        ? review.holdout_selector_selected
        : split === HOLDOUT_SPLIT,
      interpretive: normalizeInterpretive(review?.interpretive),
      reviewed_at: review?.reviewed_at || review?.updated_at || "",
      prior_review_ids: Array.isArray(review?.prior_review_ids) ? review.prior_review_ids : [],
      authored_formulation_ids: Array.isArray(review?.authored_formulation_ids) ? review.authored_formulation_ids : [],
      approved_formulation_ids: Array.isArray(review?.approved_formulation_ids) ? review.approved_formulation_ids : [],
      copied_formulation_roles: copiedFormulationRoles,
    };
  }

  function exportableReview(review, reviewId = "") {
    const normalized = normalizeReview(review);
    const eventReviewId = normalized.review_id || `${reviewId}::${normalized.reviewer_id}`;
    const benchmarkDisposition = normalized.split === HOLDOUT_SPLIT
      ? "withheld"
      : normalized.status === "excluded"
        ? "excluded"
        : normalized.status
          ? "included"
          : null;
    const preferredId = `${eventReviewId}::formulation::preferred`;
    const literalId = `${eventReviewId}::formulation::literal`;
    const candidateId = `${eventReviewId}::formulation::candidate`;
    const authored = [...normalized.authored_formulation_ids];
    if (normalized.preferred_question && !normalized.copied_formulation_roles.includes("preferred") && !authored.includes(preferredId)) authored.push(preferredId);
    if (normalized.literal_wording && !normalized.copied_formulation_roles.includes("literal") && !authored.includes(literalId)) authored.push(literalId);
    const approved = normalized.status && normalized.status !== "excluded"
      ? [...normalized.approved_formulation_ids]
      : [];
    if (normalized.status && normalized.status !== "excluded") {
      const approvedId = normalized.preferred_question ? preferredId : candidateId;
      const selectedRole = normalized.preferred_question ? "preferred" : "candidate";
      const copiedApprovalRetained = normalized.copied_formulation_roles.includes(selectedRole)
        && approved.length > 0;
      if (!copiedApprovalRetained && !approved.includes(approvedId)) approved.push(approvedId);
    }
    const priorReviewIds = [...normalized.prior_review_ids];
    if (review?.copied_from_review_id && !priorReviewIds.includes(review.copied_from_review_id)) {
      priorReviewIds.push(review.copied_from_review_id);
    }
    return {
      review_id: eventReviewId,
      reviewer_id: normalized.reviewer_id,
      reviewed_at: normalized.reviewed_at || null,
      prior_review_ids: priorReviewIds,
      authored_formulation_ids: authored,
      approved_formulation_ids: approved,
      benchmark_disposition: benchmarkDisposition,
      pipeline_assessment: normalized.status && normalized.status !== "excluded" ? normalized.status : null,
      preferred_question: normalized.preferred_question,
      literal_wording: normalized.literal_wording,
      public_comment: normalized.public_comment,
      internal_comment: normalized.internal_comment,
      split: normalized.split,
      interpretive: normalized.interpretive,
      ...(review?.copied_from_review_id ? { copied_from_review_id: review.copied_from_review_id } : {}),
    };
  }

  function exportableReviews(reviewMap) {
    return Object.fromEntries(
      Object.entries(reviewMap).map(([reviewId, review]) => [reviewId, exportableReview(review, reviewId)])
    );
  }

  function internalReviews(reviewMap) {
    return Object.fromEntries(
      Object.entries(reviewMap).map(([reviewId, review]) => [reviewId, {
        ...normalizeReview(review),
        reviewed_at: review?.reviewed_at || review?.updated_at || null,
        ...(review?.copied_from_review_id ? { copied_from_review_id: review.copied_from_review_id } : {}),
      }])
    );
  }

  function partitionReviewMap(reviewMap) {
    const publicReviews = {};
    const privateReviews = {};
    Object.entries(reviewMap || {}).forEach(([reviewId, review]) => {
      const exported = exportableReview(review, reviewId);
      if (exported.split === HOLDOUT_SPLIT || exported.benchmark_disposition === "withheld") {
        privateReviews[reviewId] = exported;
      } else {
        publicReviews[reviewId] = exported;
      }
    });
    return { publicReviews, privateReviews };
  }

  function matchPrivateRecords(privateReviews, records, getId, label) {
    const privateIds = Object.keys(privateReviews || {});
    const recordsById = new Map();
    for (const record of records || []) {
      const reviewId = getId(record);
      if (!reviewId || recordsById.has(reviewId)) {
        window.alert(`Cannot export or clear private state: ${label} has a missing or duplicate review identity.`);
        return null;
      }
      recordsById.set(reviewId, record);
    }
    const missing = privateIds.filter((reviewId) => !recordsById.has(reviewId));
    if (missing.length) {
      window.alert(`Cannot export or clear private state: ${missing.length} private annotation identity is absent from this ${label}.`);
      return null;
    }
    return privateIds.map((reviewId) => recordsById.get(reviewId));
  }

  function rejectPrivateImport(payload, imported) {
    if (payload.kind && payload.kind !== "non_holdout_review_export") {
      throw new Error(`Unsupported review export kind: ${payload.kind}.`);
    }
    const { privateReviews } = partitionReviewMap(imported);
    if (Object.keys(privateReviews).length) {
      throw new Error("Private holdout annotations cannot be imported into the agent-visible workbench.");
    }
  }

  function validateReviewerImport(payload, currentData = data) {
    if (payload.reviewer_id && payload.reviewer_id !== currentData?.reviewer_id) {
      throw new Error("This review export belongs to a different reviewer.");
    }
    if (!payload.reviewer_id && currentData?.reviewer_id !== "reviewer-0001") {
      throw new Error("Only reviewer-0001 may import a legacy export without reviewer provenance.");
    }
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
      if (review.reviewer_id && !/^reviewer-[0-9]{4,}$/.test(review.reviewer_id)) {
        throw new Error(`Review ${reviewId} has an invalid pseudonymous reviewer ID.`);
      }
      for (const field of ["prior_review_ids", "authored_formulation_ids", "approved_formulation_ids"]) {
        if (field in review && (!Array.isArray(review[field]) || review[field].some((value) => typeof value !== "string" || !value))) {
          throw new Error(`Review ${reviewId} has invalid ${field}.`);
        }
      }
      if (
        Object.prototype.hasOwnProperty.call(review, "holdout_selection_touched")
        || Object.prototype.hasOwnProperty.call(review, "holdout_selector_selected")
      ) {
        throw new Error(`Review ${reviewId} contains a browser-only holdout selector control.`);
      }
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

  function removeLiteralFromComment(comment, literal) {
    const expected = String(literal || "").trim().replace(/\s+/g, " ").toLocaleLowerCase();
    if (!expected) return String(comment || "").trim();
    return String(comment || "")
      .split(/\r?\n/)
      .filter((line) => {
        const match = line.replace(/\u00a0/g, " ").trim().match(/^literal:\s*(.*)$/i);
        return !match || match[1].trim().replace(/\s+/g, " ").toLocaleLowerCase() !== expected;
      })
      .join("\n")
      .trim();
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

  function isEmptyInterpretive(interpretive) {
    return (
      interpretive.naturalness === null &&
      interpretive.pragmatism === null &&
      interpretive.room_for_interpretation === null &&
      !interpretive.requires_graph_context_knowledge
    );
  }

  function getReviewScope(record) {
    return record?.review_scope || "new";
  }

  function hasReviewerDecision(review) {
    if (!review || typeof review !== "object") return false;
    if (review.reviewed === true) return true;
    const fields = [
      "status",
      "pipeline_assessment",
      "benchmark_disposition",
      "preferred_question",
      "literal_wording",
      "public_comment",
      "internal_comment",
      "note",
      "split",
      "reviewed_at",
      "updated_at",
      "copied_from_review_id",
    ];
    if (fields.some((field) => review[field] !== null && review[field] !== undefined && review[field] !== "")) {
      return true;
    }
    const interpretive = review.interpretive;
    return Boolean(
      interpretive &&
      typeof interpretive === "object" &&
      Object.values(interpretive).some((value) => value !== null && value !== undefined && value !== "" && value !== false)
    );
  }

  function reviewDecisionCount(reviewMap) {
    return Object.values(reviewMap || {}).filter((review) =>
      hasReviewerDecision(normalizeReview(review))
    ).length;
  }

  function initialHoldoutEligibility(record, provenanceComplete = false) {
    if (!provenanceComplete) {
      return {
        eligible: false,
        reason: "Holdout selection is disabled: complete prior-review provenance was not attested when this bundle was built.",
      };
    }
    if (!record) {
      return { eligible: false, reason: "No current record is available for holdout selection." };
    }
    const editEligibility = sparqlEditHoldoutEligibility(record);
    if (!editEligibility.eligible) return editEligibility;
    if (record.has_prior_pair_review === true || getReviewScope(record) === "previously_reviewed" || hasReviewerDecision(record.previous_review)) {
      return {
        eligible: false,
        reason: "Ineligible for holdout: a reviewer decision was attached before this review session.",
      };
    }
    return {
      eligible: true,
      reason: "Eligible while it has no decision from an earlier review; generation or display alone does not make it ineligible.",
    };
  }

  function compareHoldoutEligibility(pair, provenanceComplete = false) {
    if (!provenanceComplete) {
      return {
        eligible: false,
        reason: "Holdout selection is disabled: complete prior-review provenance was not attested when this bundle was built.",
      };
    }
    if (!pair?.current?.record) {
      return { eligible: false, reason: "Ineligible for holdout: there is no current record." };
    }
    const editEligibility = sparqlEditHoldoutEligibility(pair.current.record);
    if (!editEligibility.eligible) return editEligibility;
    if (hasReviewerDecision(pair.previous?.review)) {
      return {
        eligible: false,
        reason: "Ineligible for holdout: a reviewer decision was attached in an earlier review.",
      };
    }
    return {
      eligible: true,
      reason: "Eligible because no earlier reviewer decision is attached; appearing in a previous run is allowed.",
    };
  }

  function sparqlEditHoldoutEligibility(record) {
    const provenance = record?.input?.sparql_provenance;
    const count = provenance?.retained_edit_count;
    if (!Number.isInteger(count) || count < 0) {
      return {
        eligible: false,
        reason: "Ineligible for holdout: retained SPARQL edit provenance is missing or invalid.",
      };
    }
    if (count !== 0) {
      return {
        eligible: false,
        reason: "Ineligible for holdout: this query identity retains SPARQL edit history.",
      };
    }
    return { eligible: true, reason: "No retained SPARQL edit history." };
  }

  function reusedPreviousReview(previousReview, currentReview, copiedFromReviewId) {
    const priorReviewId = previousReview.review_id || copiedFromReviewId;
    const priorApproved = Array.isArray(previousReview.approved_formulation_ids)
      && previousReview.approved_formulation_ids.length
      ? previousReview.approved_formulation_ids
      : priorReviewId
        ? [`${priorReviewId}::formulation::${previousReview.preferred_question ? "preferred" : "candidate"}`]
        : [];
    return {
      review_id: currentReview.review_id || "",
      reviewer_id: data?.reviewer_id || currentReview?.reviewer_id || "",
      status: previousReview.status || "",
      preferred_question: previousReview.preferred_question || "",
      literal_wording: previousReview.literal_wording || "",
      public_comment: previousReview.public_comment || "",
      internal_comment: previousReview.internal_comment || "",
      split: isHoldoutReview(currentReview) ? HOLDOUT_SPLIT : "",
      copied_from_review_id: priorReviewId || null,
      copied_formulation_roles: [
        previousReview.preferred_question ? "preferred" : "",
        previousReview.literal_wording ? "literal" : "",
      ].filter(Boolean),
      reviewed_at: new Date().toISOString(),
      prior_review_ids: priorReviewId ? [priorReviewId] : [],
      authored_formulation_ids: [],
      approved_formulation_ids: priorApproved,
    };
  }

  function resetFormulationAttribution(patch = {}) {
    return {
      ...patch,
      copied_from_review_id: null,
      authored_formulation_ids: [],
      approved_formulation_ids: [],
      copied_formulation_roles: [],
    };
  }

  function editedFormulationAttribution(review, role) {
    const normalized = normalizeReview(review);
    const copiedRoles = normalized.copied_formulation_roles.filter((value) => value !== role);
    return {
      copied_from_review_id: copiedRoles.length ? review?.copied_from_review_id || null : null,
      copied_formulation_roles: copiedRoles,
      authored_formulation_ids: normalized.authored_formulation_ids.filter(
        (value) => !value.endsWith(`::formulation::${role}`)
      ),
      approved_formulation_ids: normalized.approved_formulation_ids.filter(
        (value) => !value.endsWith(`::formulation::${role}`)
      ),
    };
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
    const { publicReviews } = partitionReviewMap(reviews);
    const payload = {
      schema: "musparql.review-export.v2",
      kind: "non_holdout_review_export",
      reviewer_id: data.reviewer_id,
      dataset_id: data.dataset_id,
      run_id: data.single_run_id,
      run_ids: data.run_ids || [],
      runs: data.runs || [],
      exported_at: new Date().toISOString(),
      reviews: publicReviews,
    };
    const timestamp = timestampForFilename(new Date());
    downloadJson(payload, `musparql-review-non-holdout-${data.dataset_id}-${timestamp}.json`);
  }

  function selectorUpdatesForInitialReview() {
    return data.records.flatMap((record) => selectorUpdateForReview(record, getReview(record)));
  }

  function exportPrivateReviews() {
    const { privateReviews } = partitionReviewMap(reviews);
    const records = matchPrivateRecords(
      privateReviews,
      data.records,
      (record) => record.review_id,
      "review dataset"
    );
    if (records === null) return;
    if (!records.length) {
      window.alert("No private holdout annotations are stored for this review dataset.");
      return;
    }
    const timestamp = timestampForFilename(new Date());
    downloadJson(
      {
        schema: "musparql.private-holdout-export.v2",
        kind: "private_holdout_export",
        reviewer_id: data.reviewer_id,
        schema_version: 1,
        dataset_id: data.dataset_id,
        exported_at: new Date().toISOString(),
        holdouts: records.map((record) => ({
          review_id: record.review_id,
          annotation: privateReviews[record.review_id],
          record,
        })),
      },
      `musparql-holdout-private-${timestamp}.json`
    );
    privateExportReady = true;
    window.alert(`Private export started for ${records.length} holdout record${records.length === 1 ? "" : "s"}. Open the downloaded file and verify that count before clearing private state.`);
  }

  function clearPrivateState() {
    const { publicReviews, privateReviews } = partitionReviewMap(reviews);
    const count = Object.keys(privateReviews).length;
    if (!count) {
      window.alert("No private holdout annotations are stored for this review dataset.");
      return;
    }
    if (matchPrivateRecords(privateReviews, data.records, (record) => record.review_id, "review dataset") === null) return;
    if (!privateExportReady) {
      window.alert("Export the private holdout with the separate Private Export button before clearing it.");
      return;
    }
    if (!window.confirm(`Have you verified that the private export contains ${count} holdout annotation${count === 1 ? "" : "s"} and opens correctly? If yes, remove private browser state for this dataset and initial-review mode now.`)) {
      return;
    }
    reviews = internalReviews(publicReviews);
    saveReviews();
    window.localStorage.removeItem(`musparql-review:schema2:${data.dataset_id}`);
    window.localStorage.removeItem(`musparql-review:${data.dataset_id}`);
    privateExportReady = false;
    render();
  }

  function importReviews(event) {
    const [file] = event.target.files || [];
    if (!file) return;
    const reader = new FileReader();
    reader.onload = () => {
      try {
        const payload = JSON.parse(String(reader.result || "{}"));
        if (payload.kind === "private_holdout_export") {
          throw new Error("Private holdout exports cannot be imported into the agent-visible workbench.");
        }
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
        validateReviewerImport(payload);
        rejectPrivateImport(payload, imported);
        reviews = internalReviews(imported);
        privateExportReady = false;
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

  function downloadJson(payload, filename) {
    const blob = new Blob([JSON.stringify(payload, null, 2)], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = filename;
    link.hidden = true;
    document.body.appendChild(link);
    link.click();
    link.remove();
    window.setTimeout(() => URL.revokeObjectURL(url), 1000);
  }

  function parseHoldoutSelectors(text) {
    const source = String(text || "").trim();
    if (!source) return [];
    let records;
    if (source.startsWith("[")) {
      records = JSON.parse(source);
    } else {
      records = source.split(/\r?\n/).filter((line) => line.trim()).map((line, index) => {
        try {
          return JSON.parse(line);
        } catch (error) {
          throw new Error(`Invalid holdout selector JSON on line ${index + 1}: ${error.message}`);
        }
      });
    }
    if (!Array.isArray(records)) throw new Error("Holdout selectors must be a JSON array or JSONL records.");
    const seen = new Set();
    return records.map((record) => {
      const selector = validateHoldoutSelector(record);
      const key = selectorKey(selector);
      if (seen.has(key)) throw new Error(`Duplicate holdout selector identity: ${selector.kg_id}/${selector.query_id}.`);
      seen.add(key);
      return selector;
    });
  }

  function validateHoldoutSelector(record) {
    if (!record || typeof record !== "object" || Array.isArray(record)) {
      throw new Error("Each holdout selector must be an object.");
    }
    const allowed = new Set(["kg_id", "query_id", "sparql_version", "sparql_hash"]);
    if (Object.keys(record).some((field) => !allowed.has(field))) {
      throw new Error("Holdout selector files may contain identity/version fields only.");
    }
    const kgId = record.kg_id;
    const queryId = record.query_id;
    if (typeof kgId !== "string" || !kgId.trim() || typeof queryId !== "string" || !queryId.trim()) {
      throw new Error("Each holdout selector requires nonempty kg_id and query_id strings.");
    }
    const hasVersion = Object.prototype.hasOwnProperty.call(record, "sparql_version");
    const hasHash = Object.prototype.hasOwnProperty.call(record, "sparql_hash");
    if (hasVersion !== hasHash) throw new Error("Holdout selector SPARQL version and hash must be supplied together.");
    const selector = { kg_id: kgId, query_id: queryId };
    if (hasVersion) {
      const digest = record.sparql_hash;
      if (!Number.isInteger(record.sparql_version) || record.sparql_version < 0) {
        throw new Error("Holdout selector sparql_version must be a non-negative integer.");
      }
      if (!/^[0-9a-f]{64}$/.test(digest)) {
        throw new Error("Holdout selector sparql_hash must be a lowercase SHA-256 digest.");
      }
      selector.sparql_version = record.sparql_version;
      selector.sparql_hash = digest;
    }
    return selector;
  }

  function selectorForRecord(record) {
    const selector = { kg_id: record?.kg_id, query_id: record?.query_id };
    const version = record?.input?.sparql_version;
    const digest = record?.input?.sparql_hash;
    const hasVersion = version !== null && version !== undefined;
    const hasHash = digest !== null && digest !== undefined && digest !== "";
    if (hasVersion !== hasHash) {
      throw new Error(`Current review record ${selector.kg_id || "?"}/${selector.query_id || "?"} has an incomplete SPARQL pin.`);
    }
    if (hasVersion) {
      selector.sparql_version = version;
      selector.sparql_hash = typeof digest === "string" ? digest.replace(/^sha256:/, "") : digest;
    }
    return validateHoldoutSelector(selector);
  }

  function selectorUpdateForReview(record, review) {
    const normalized = normalizeReview(review);
    if (!normalized.holdout_selection_touched) return [];
    if (!record) throw new Error("Cannot update a holdout selector without a current review record.");
    return [{ record, selected: normalized.holdout_selector_selected }];
  }

  function selectorKey(selector) {
    return `${selector.kg_id}\u0000${selector.query_id}`;
  }

  function mergeHoldoutSelectors(existingSelectors, updates) {
    const merged = new Map();
    for (const record of existingSelectors || []) {
      const selector = validateHoldoutSelector(record);
      const key = selectorKey(selector);
      if (merged.has(key)) throw new Error(`Duplicate holdout selector identity: ${selector.kg_id}/${selector.query_id}.`);
      merged.set(key, selector);
    }
    const touched = new Set();
    let additions = 0;
    let removals = 0;
    for (const update of updates || []) {
      const selector = selectorForRecord(update?.record);
      const key = selectorKey(selector);
      if (touched.has(key)) throw new Error(`Duplicate current review identity: ${selector.kg_id}/${selector.query_id}.`);
      touched.add(key);
      const existed = merged.has(key);
      if (update.selected) {
        if (!existed) {
          merged.set(key, selector);
          additions += 1;
        }
      } else if (existed) {
        merged.delete(key);
        removals += 1;
      }
    }
    const compareCodeUnits = (left, right) => left < right ? -1 : left > right ? 1 : 0;
    const selectors = [...merged.values()].sort((left, right) =>
      compareCodeUnits(left.kg_id, right.kg_id) || compareCodeUnits(left.query_id, right.query_id)
    );
    return { selectors, additions, removals };
  }

  function bindHoldoutSelectorExport(getUpdates) {
    els.exportHoldoutSelectorsBtn.addEventListener("click", () => {
      if (!selectorExportAllowed) {
        window.alert("Holdout selector export is disabled for the identity-private filtered-upstream policy.");
        return;
      }
      if (typeof els.holdoutSelectorDialog?.showModal === "function") {
        els.holdoutSelectorDialog.showModal();
      } else {
        // Older browsers get the safe update-existing path directly.
        els.holdoutSelectorsInput.value = "";
        els.holdoutSelectorsInput.click();
      }
    });
    els.chooseExistingSelectorsBtn.addEventListener("click", () => {
      els.holdoutSelectorDialog.close();
      els.holdoutSelectorsInput.value = "";
      // This runs directly from the in-page button gesture, which Firefox
      // requires before opening a hidden file input.
      els.holdoutSelectorsInput.click();
    });
    els.createNewSelectorsBtn.addEventListener("click", () => {
      els.holdoutSelectorDialog.close();
      try {
        exportUpdatedHoldoutSelectors([], getUpdates(), { requireNonempty: true });
      } catch (error) {
        window.alert(`Could not update holdout selectors: ${error}`);
      }
    });
    els.holdoutSelectorsInput.addEventListener("change", (event) => {
      const [file] = event.target.files || [];
      if (!file) return;
      const reader = new FileReader();
      reader.onload = () => {
        try {
          exportUpdatedHoldoutSelectors(parseHoldoutSelectors(reader.result), getUpdates());
        } catch (error) {
          window.alert(`Could not update holdout selectors: ${error}`);
        } finally {
          event.target.value = "";
        }
      };
      reader.readAsText(file);
    });
  }

  function exportUpdatedHoldoutSelectors(existingSelectors, updates, options = {}) {
    try {
      const result = mergeHoldoutSelectors(existingSelectors, updates);
      if (options.requireNonempty && !result.selectors.length) {
        throw new Error("A new selector file would be empty. Leave the project in --no-holdout mode instead.");
      }
      const content = result.selectors.map((selector) => JSON.stringify(selector)).join("\n")
        + (result.selectors.length ? "\n" : "");
      downloadText(content, "selectors.jsonl", "application/x-ndjson");
      window.alert(
        `Selector export started with ${result.selectors.length} identit${result.selectors.length === 1 ? "y" : "ies"} (${result.additions} added, ${result.removals} removed). Move the downloaded file to var/holdout/selectors.jsonl.`
      );
    } catch (error) {
      window.alert(`Could not update holdout selectors: ${error}`);
    }
  }

  function downloadText(content, filename, type) {
    const blob = new Blob([content], { type });
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = filename;
    link.hidden = true;
    document.body.appendChild(link);
    link.click();
    link.remove();
    window.setTimeout(() => URL.revokeObjectURL(url), 1000);
  }

  function initCompareMode() {
    const compareStorageKey = hosted
      ? `musparql-review-compare:schema5:${data.dataset_id}:${data.reviewer_id}:${hosted.assignment_id}`
      : `musparql-review-compare:schema4:${data.dataset_id}:${data.reviewer_id}`;
    let compareReviews = loadCompareReviews();
    let privateCompareExportReady = false;
    const compareState = {
      selectedPairId: data.records[0].pair_id,
      search: "",
      kg: "all",
      change: "all",
      status: "all",
      holdout: "all",
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
    fillSelect(els.holdoutFilter, ["all", "holdout", "non_holdout"], "All sets");
    els.holdoutFilter.options[1].textContent = "Holdout only";
    els.holdoutFilter.options[2].textContent = "Non-holdout only";
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
    els.holdoutFilter.addEventListener("change", () => {
      compareState.holdout = els.holdoutFilter.value;
      renderCompare();
    });
    els.runFilter.addEventListener("change", () => {
      compareState.previousStatus = els.runFilter.value;
      renderCompare();
    });
    els.prevBtn.addEventListener("click", () => moveCompareSelection(-1));
    els.nextBtn.addEventListener("click", () => moveCompareSelection(1));
    els.exportReviewsBtn.addEventListener("click", exportCompareReviews);
    els.exportPrivateReviewsBtn.addEventListener("click", exportPrivateCompareReviews);
    bindHoldoutSelectorExport(() => selectorUpdatesForCompareReview());
    els.clearPrivateStateBtn.addEventListener("click", clearPrivateCompareState);
    els.importReviewsInput.addEventListener("change", importCompareReviews);
    document.addEventListener("keydown", (event) => {
      if (event.target && ["INPUT", "TEXTAREA", "SELECT"].includes(event.target.tagName)) return;
      if (event.key === "ArrowDown") moveCompareSelection(1);
      if (event.key === "ArrowUp") moveCompareSelection(-1);
    });

    renderCompare();

    function loadCompareReviews() {
      try {
        const legacyRaw = !hosted && data.reviewer_id === "reviewer-0001"
          ? window.localStorage.getItem(`musparql-review-compare:schema3:${data.dataset_id}`)
            || window.localStorage.getItem(`musparql-review-compare:schema2:${data.dataset_id}`)
            || window.localStorage.getItem(`musparql-review-compare:${data.dataset_id}`)
          : null;
        const raw = window.localStorage.getItem(compareStorageKey) || legacyRaw;
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
        if (compareState.holdout === "holdout" && !isHoldoutReview(currentReview)) return false;
        if (compareState.holdout === "non_holdout" && isHoldoutReview(currentReview)) return false;
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
      els.reviewedCount.textContent = String(reviewDecisionCount(compareReviews));
      els.holdoutCount.textContent = String(holdoutCount(compareReviews));
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
          ${renderRunColumn("Previous", previousRecord, previousReview, pair.evidence_diff || {}, "previous", flags, pair)}
          ${renderRunColumn("Current", currentRecord, currentReview, pair.evidence_diff || {}, "current", flags, pair)}
        </section>
      `;

      document.getElementById("comparePrevBtn").addEventListener("click", () => moveCompareSelection(-1));
      document.getElementById("compareNextBtn").addEventListener("click", () => moveCompareSelection(1));
      document.getElementById("reusePreviousBtn").addEventListener("click", () => {
        updateCompareReview(
          currentReviewId,
          reusedPreviousReview(previousReview, currentReview, pair.previous?.review_id)
        );
      });
      document.getElementById("usePreviousWordingBtn").addEventListener("click", () => {
        updateCompareReview(currentReviewId, {
          preferred_question: previousReview.preferred_question || previousRecord?.output?.nl_question || "",
          copied_from_review_id: previousReview.review_id || pair.previous?.review_id || null,
          copied_formulation_roles: ["preferred"],
          authored_formulation_ids: [],
          approved_formulation_ids: (previousReview.review_id || pair.previous?.review_id)
            ? [`${previousReview.review_id || pair.previous.review_id}::formulation::${previousReview.preferred_question ? "preferred" : "candidate"}`]
            : [],
        });
      });
      document.getElementById("reusePreviousInlineBtn")?.addEventListener("click", () => {
        updateCompareReview(
          currentReviewId,
          reusedPreviousReview(previousReview, currentReview, pair.previous?.review_id)
        );
      });
      document.getElementById("usePreviousWordingInlineBtn")?.addEventListener("click", () => {
        updateCompareReview(currentReviewId, {
          preferred_question: previousReview.preferred_question || previousRecord?.output?.nl_question || "",
          copied_from_review_id: previousReview.review_id || pair.previous?.review_id || null,
          copied_formulation_roles: ["preferred"],
          authored_formulation_ids: [],
          approved_formulation_ids: (previousReview.review_id || pair.previous?.review_id)
            ? [`${previousReview.review_id || pair.previous.review_id}::formulation::${previousReview.preferred_question ? "preferred" : "candidate"}`]
            : [],
        });
      });
      document.getElementById("usePreviousPublicCommentInlineBtn")?.addEventListener("click", () => {
        updateCompareReview(currentReviewId, {
          public_comment: previousReview.public_comment || "",
        });
      });
      document.getElementById("acceptNewBtn").addEventListener("click", () => {
        updateCompareReview(currentReviewId, {
          status: "accepted",
          preferred_question: "",
          ...editedFormulationAttribution(compareReviews[currentReviewId] || currentReview, "preferred"),
        });
      });
      document.getElementById("editBetterBtn").addEventListener("click", () => {
        document.getElementById("comparePreferredInput")?.focus();
      });
      Array.from(els.detailView.querySelectorAll(".decision-btn")).forEach((btn) => {
        btn.addEventListener("click", () => {
          const status = btn.dataset.status || "";
          updateCompareReview(currentReviewId, {
            status,
            ...(status === "excluded" ? resetFormulationAttribution() : {}),
          });
        });
      });
      document.getElementById("comparePreferredInput")?.addEventListener("input", () => {
        updateCompareReview(currentReviewId, {
          preferred_question: document.getElementById("comparePreferredInput").value.trim(),
          ...editedFormulationAttribution(compareReviews[currentReviewId] || currentReview, "preferred"),
        }, false);
      });
      document.getElementById("compareLiteralInput")?.addEventListener("input", () => {
        updateCompareReview(currentReviewId, {
          literal_wording: document.getElementById("compareLiteralInput").value.trim(),
          ...editedFormulationAttribution(compareReviews[currentReviewId] || currentReview, "literal"),
        }, false);
      });
      document.getElementById("comparePublicCommentInput")?.addEventListener("input", () => {
        updateCompareReview(currentReviewId, { public_comment: document.getElementById("comparePublicCommentInput").value.trim() }, false);
      });
      document.getElementById("compareInternalCommentInput")?.addEventListener("input", () => {
        updateCompareReview(currentReviewId, { internal_comment: document.getElementById("compareInternalCommentInput").value.trim() }, false);
      });
      document.getElementById("compareHoldoutInput")?.addEventListener("change", () => {
        const input = document.getElementById("compareHoldoutInput");
        const selectorSelected = input.checked;
        const keepAnnotationPrivate = isHoldoutReview(currentReview);
        updateCompareReview(
          currentReviewId,
          {
            split: keepAnnotationPrivate || (selectorSelected && compareHoldoutEligibility(pair, data.holdout_review_provenance_complete).eligible) ? HOLDOUT_SPLIT : "",
            holdout_selection_touched: true,
            holdout_selector_selected: selectorSelected,
          },
          true
        );
      });
    }

    function renderRunColumn(title, record, review, evidenceDiff, side, flags, pair) {
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
          ${side === "previous" ? renderPreviousReviewPanel(review, record) : renderCurrentReviewPanel(review, pair)}
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
              <p class="section-label">Public comment</p>
              <button id="usePreviousPublicCommentInlineBtn" class="btn small">Reuse Comment</button>
            </div>
            <p>${escapeHtml(review.public_comment || "No public comment")}</p>
            </div>
            <div class="compare-review-note">
            <div class="compare-note-head">
              <p class="section-label">Internal comment</p>
            </div>
            <p>${escapeHtml(review.internal_comment || "No internal comment")}</p>
            </div>
          </div>
        </section>
      `;
    }

    function renderCurrentReviewPanel(review, pair) {
      const eligibility = compareHoldoutEligibility(pair, data.holdout_review_provenance_complete);
      const savedIneligibleHoldout = isHoldoutReview(review) && !eligibility.eligible;
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
              (eligibility.eligible || isHoldoutReview(review))
                && !hostedNoHoldout ? `<label class="checkbox-field compare-holdout-field">
                    <input id="compareHoldoutInput" type="checkbox" ${review.holdout_selector_selected ? "checked" : ""} ${!eligibility.eligible && !review.holdout_selector_selected ? "disabled" : ""} />
                    <span>Private holdout / selector member</span>
                    <small>${escapeHtml(savedIneligibleHoldout
                      ? review.holdout_selector_selected
                        ? "This saved holdout is no longer eligible. Uncheck it to mark an identity-visible selector removal; its annotations will remain private."
                        : "Selector removal marked. This pair's annotations remain private and must still be privately exported and cleared."
                      : eligibility.reason)}</small>
                  </label>`
                : `<p class="muted-meta compare-holdout-field">${escapeHtml(eligibility.reason)}</p>`
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
              <span>Public reviewer comment</span>
              <small>Published with the benchmark.</small>
              <textarea id="comparePublicCommentInput" rows="4" placeholder="Why this decision was made">${escapeHtml(review.public_comment || "")}</textarea>
            </label>
            <label>
              <span>Internal reviewer comment</span>
              <small>Excluded from the public release.</small>
              <textarea id="compareInternalCommentInput" rows="3" placeholder="Private operational note">${escapeHtml(review.internal_comment || "")}</textarea>
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
      privateCompareExportReady = false;
      const existing = compareReviews[reviewId] || { status: "", preferred_question: "", literal_wording: "", public_comment: "", internal_comment: "", split: "" };
      const pair = data.records.find((item) => (item.current?.review_id || item.pair_id) === reviewId);
      const priorReviewIds = pair?.previous?.review_id ? [pair.previous.review_id] : [];
      compareReviews[reviewId] = {
        ...existing,
        ...patch,
        review_id: existing.review_id || `${reviewId}::${data.reviewer_id}`,
        reviewer_id: data.reviewer_id,
        reviewed_at: new Date().toISOString(),
        prior_review_ids: patch.prior_review_ids
          || (Array.isArray(existing.prior_review_ids) && existing.prior_review_ids.length ? existing.prior_review_ids : priorReviewIds),
      };
      cleanupEmptyCompareReview(reviewId);
      saveCompareReviews();
      if (rerender) renderCompare();
    }

    function cleanupEmptyCompareReview(reviewId) {
      const review = compareReviews[reviewId];
      if (review && !review.status && !review.preferred_question && !review.literal_wording && !review.public_comment && !review.internal_comment && !review.split && !review.holdout_selection_touched) {
        delete compareReviews[reviewId];
      }
    }

    function selectorUpdatesForCompareReview() {
      return data.records.flatMap((pair) => {
        const reviewId = pair.current?.review_id || pair.pair_id;
        return selectorUpdateForReview(pair.current?.record, compareReviews[reviewId]);
      });
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
      const { publicReviews } = partitionReviewMap(compareReviews);
      const payload = {
        schema: "musparql.review-export.v2",
        kind: "non_holdout_review_export",
        reviewer_id: data.reviewer_id,
        dataset_id: data.dataset_id,
        mode: "compare",
        previous_run: data.previous_run,
        current_run: data.current_run,
        exported_at: new Date().toISOString(),
        reviews: publicReviews,
      };
      const timestamp = timestampForFilename(new Date());
      downloadJson(payload, `musparql-review-non-holdout-compare-${data.dataset_id}-${timestamp}.json`);
    }

    function exportPrivateCompareReviews() {
      const { privateReviews } = partitionReviewMap(compareReviews);
      const pairs = matchPrivateRecords(
        privateReviews,
        data.records,
        (pair) => pair.current?.review_id || pair.pair_id,
        "comparison dataset"
      );
      if (pairs === null) return;
      if (!pairs.length) {
        window.alert("No private holdout annotations are stored for this comparison dataset.");
        return;
      }
      const timestamp = timestampForFilename(new Date());
      downloadJson(
        {
          schema: "musparql.private-holdout-export.v2",
          kind: "private_holdout_export",
          reviewer_id: data.reviewer_id,
          schema_version: 1,
          mode: "compare",
          dataset_id: data.dataset_id,
          exported_at: new Date().toISOString(),
          holdouts: pairs.map((pair) => {
            const reviewId = pair.current?.review_id || pair.pair_id;
            return { review_id: reviewId, annotation: privateReviews[reviewId], pair };
          }),
        },
        `musparql-holdout-private-${timestamp}.json`
      );
      privateCompareExportReady = true;
      window.alert(`Private export started for ${pairs.length} holdout record${pairs.length === 1 ? "" : "s"}. Open the downloaded file and verify that count before clearing private state.`);
    }

    function clearPrivateCompareState() {
      const { publicReviews, privateReviews } = partitionReviewMap(compareReviews);
      const count = Object.keys(privateReviews).length;
      if (!count) {
        window.alert("No private holdout annotations are stored for this comparison dataset.");
        return;
      }
      if (matchPrivateRecords(privateReviews, data.records, (pair) => pair.current?.review_id || pair.pair_id, "comparison dataset") === null) return;
      if (!privateCompareExportReady) {
        window.alert("Export the private holdout with the separate Private Export button before clearing it.");
        return;
      }
      if (!window.confirm(`Have you verified that the private export contains ${count} holdout annotation${count === 1 ? "" : "s"} and opens correctly? If yes, remove private browser state for this dataset and comparison mode now.`)) {
        return;
      }
      compareReviews = internalReviews(publicReviews);
      saveCompareReviews();
      window.localStorage.removeItem(`musparql-review-compare:schema2:${data.dataset_id}`);
      window.localStorage.removeItem(`musparql-review-compare:${data.dataset_id}`);
      privateCompareExportReady = false;
      renderCompare();
    }

    function importCompareReviews(event) {
      const [file] = event.target.files || [];
      if (!file) return;
      const reader = new FileReader();
      reader.onload = () => {
        try {
          const payload = JSON.parse(String(reader.result || "{}"));
          if (payload.kind === "private_holdout_export") {
            throw new Error("Private holdout exports cannot be imported into the agent-visible workbench.");
          }
          validateCompareImportPayload(payload);
          validateReviewerImport(payload);
          const imported = payload.reviews;
          validateImportedReviews(imported);
          rejectPrivateImport(payload, imported);
          compareReviews = internalReviews(imported);
          privateCompareExportReady = false;
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

  function hideHostedHoldoutControls() {
    els.exportPrivateReviewsBtn.classList.add("hidden");
    els.exportHoldoutSelectorsBtn.classList.add("hidden");
    els.clearPrivateStateBtn.classList.add("hidden");
    els.holdoutFilter.closest("label")?.classList.add("hidden");
    els.holdoutCount.closest(".chip")?.classList.add("hidden");
    els.holdoutSplitInput.closest("label")?.classList.add("hidden");
  }

  function initHostedSession() {
    if (!hosted) return;
    document.getElementById("localCorrectionLink")?.classList.add("hidden");
    const bar = document.createElement("nav");
    bar.className = "hosted-session";
    bar.setAttribute("aria-label", "Authenticated session");

    const identity = document.createElement("span");
    identity.textContent = `Signed in as ${hosted.reviewer_id}`;
    bar.appendChild(identity);

    const assignment = document.createElement("a");
    assignment.href = hosted.assignment_url;
    assignment.textContent = "Assignment";
    bar.appendChild(assignment);

    const profile = document.createElement("a");
    profile.href = hosted.profile_url;
    profile.textContent = "My profile";
    bar.appendChild(profile);

    const form = document.createElement("form");
    form.method = "post";
    form.action = hosted.logout_url;
    const csrf = document.createElement("input");
    csrf.type = "hidden";
    csrf.name = "csrf_token";
    csrf.value = hosted.csrf_token;
    const button = document.createElement("button");
    button.type = "submit";
    button.className = "btn small";
    button.textContent = "Sign out";
    form.append(csrf, button);
    bar.appendChild(form);
    document.body.prepend(bar);
  }
})();
