(function () {
  const data = window.REVIEW_DATA || null;

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
    reviewNoteInput: document.getElementById("reviewNoteInput"),
    decisionButtons: Array.from(document.querySelectorAll(".decision-btn")),
  };

  if (!data || !Array.isArray(data.records) || !data.records.length) {
    return;
  }

  const reviewStorageKey = `musparql-review:${data.dataset_id}`;
  let reviews = loadReviews();
  const state = {
    selectedReviewId: data.records[0].review_id,
    search: "",
    kg: "all",
    mode: "all",
    status: "all",
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
      ["all", "unreviewed", "approve", "dismiss", "needs_prompt_fix", "needs_data_fix"],
      "All review states"
    );
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
    els.runFilter.addEventListener("change", () => {
      state.run = els.runFilter.value;
      render();
    });
    els.prevBtn.addEventListener("click", () => moveSelection(-1));
    els.nextBtn.addEventListener("click", () => moveSelection(1));
    els.preferredQuestionInput.addEventListener("input", () => updateCurrentReview({ rerender: false }));
    els.reviewNoteInput.addEventListener("input", () => updateCurrentReview({ rerender: false }));
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
    return reviews[record.review_id] || { status: "", note: "", preferred_question: "" };
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
        <p class="record-subline">${escapeHtml(record.run_label)} · ${escapeHtml(getMode(record))} · confidence ${record.output?.confidence ?? "-"}</p>
        <p class="record-subline">${escapeHtml(review.status || "unreviewed")}</p>
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
    els.reviewNoteInput.value = review.note || "";
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
            <span class="pill">rank ${item.rank}</span>
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
    reviews[reviewId] = {
      status: nextStatus,
      preferred_question: els.preferredQuestionInput.value.trim(),
      note: els.reviewNoteInput.value.trim(),
      updated_at: new Date().toISOString(),
    };
    if (!reviews[reviewId].status && !reviews[reviewId].preferred_question && !reviews[reviewId].note) {
      delete reviews[reviewId];
    }
    saveReviews();
    if (options.rerender !== false) {
      render();
    }
  }

  function getReviewById(reviewId) {
    return reviews[reviewId] || { status: "", note: "", preferred_question: "" };
  }

  function formatOrigin(origin) {
    if (!origin) return "-";
    const evidenceIds = (origin.evidence_ids || []).join(", ") || "none";
    return `${origin.mode || "unknown"} · evidence ${evidenceIds}`;
  }

  function exportReviews() {
    const payload = {
      dataset_id: data.dataset_id,
      exported_at: new Date().toISOString(),
      reviews,
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
        const imported = payload.reviews;
        if (!imported || typeof imported !== "object") {
          throw new Error("Bad review file format.");
        }
        reviews = imported;
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

  function timestampForFilename(date) {
    const pad = (value) => String(value).padStart(2, "0");
    return [
      date.getFullYear(),
      pad(date.getMonth() + 1),
      pad(date.getDate()),
    ].join("-") + "_" + [pad(date.getHours()), pad(date.getMinutes()), pad(date.getSeconds())].join("-");
  }
})();
