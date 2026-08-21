"use strict";

function configureRepeatRows(containerId, templateId, addButtonId, maximum, initializeRow) {
  const container = document.getElementById(containerId);
  const template = document.getElementById(templateId);
  const addButton = document.getElementById(addButtonId);
  if (!container || !template || !addButton) return;

  function updateButton() {
    addButton.disabled = container.children.length >= maximum;
  }

  container.addEventListener("click", (event) => {
    const removeButton = event.target.closest(".remove-row");
    if (!removeButton) return;
    const row = removeButton.closest(".repeat-row");
    if (row) row.remove();
    updateButton();
  });

  addButton.addEventListener("click", () => {
    if (container.children.length >= maximum) return;
    container.appendChild(template.content.cloneNode(true));
    const row = container.lastElementChild;
    if (initializeRow && row) initializeRow(row);
    const firstControl = row && row.querySelector("select, input");
    if (firstControl) firstControl.focus();
    updateButton();
  });

  updateButton();
}

const domainSuggestions = Array.from(
  document.querySelectorAll("#domain-suggestion-data option"),
  (option) => ({
    label: option.value,
    detail: option.dataset.detail || "",
    search: option.value.normalize("NFKD").replace(/[\u0300-\u036f]/g, "").toLocaleLowerCase(),
  }),
);

let domainResultSequence = 0;

function domainMatches(value) {
  const query = value.normalize("NFKD").replace(/[\u0300-\u036f]/g, "").trim().toLocaleLowerCase();
  if (!query) return [];
  return domainSuggestions
    .map((suggestion) => {
      let rank = 3;
      if (suggestion.search.startsWith(query)) rank = 0;
      else if (suggestion.search.split(/[^\p{L}\p{N}]+/u).some((word) => word.startsWith(query))) rank = 1;
      else if (suggestion.search.includes(query)) rank = 2;
      return { suggestion, rank };
    })
    .filter((match) => match.rank < 3)
    .sort((left, right) => left.rank - right.rank || left.suggestion.label.length - right.suggestion.label.length || left.suggestion.label.localeCompare(right.suggestion.label))
    .slice(0, 8)
    .map((match) => match.suggestion);
}

function configureDomainAutocomplete(row) {
  const input = row.querySelector(".domain-search");
  const results = row.querySelector(".domain-results");
  if (!input || !results || input.dataset.autocompleteReady) return;
  input.dataset.autocompleteReady = "yes";
  const listId = `domain-results-${++domainResultSequence}`;
  results.id = listId;
  input.setAttribute("aria-controls", listId);
  let activeIndex = -1;

  function closeResults() {
    results.hidden = true;
    results.replaceChildren();
    input.setAttribute("aria-expanded", "false");
    input.removeAttribute("aria-activedescendant");
    activeIndex = -1;
  }

  function selectResult(button) {
    input.value = button.dataset.value;
    closeResults();
    input.focus();
  }

  function renderResults() {
    const matches = domainMatches(input.value);
    results.replaceChildren();
    activeIndex = -1;
    for (const [index, suggestion] of matches.entries()) {
      const button = document.createElement("button");
      button.type = "button";
      button.className = "domain-result";
      button.id = `${listId}-${index}`;
      button.dataset.value = suggestion.label;
      button.setAttribute("role", "option");
      const label = document.createElement("span");
      label.textContent = suggestion.label;
      const detail = document.createElement("span");
      detail.className = "domain-result-detail";
      detail.textContent = suggestion.detail;
      button.append(label, detail);
      button.addEventListener("mousedown", (event) => event.preventDefault());
      button.addEventListener("click", () => selectResult(button));
      results.appendChild(button);
    }
    results.hidden = matches.length === 0;
    input.setAttribute("aria-expanded", matches.length ? "true" : "false");
  }

  input.addEventListener("input", renderResults);
  input.addEventListener("focus", renderResults);
  input.addEventListener("blur", () => setTimeout(closeResults, 100));
  input.addEventListener("keydown", (event) => {
    const options = Array.from(results.querySelectorAll(".domain-result"));
    if (event.key === "Escape") {
      closeResults();
      return;
    }
    if (event.key === "Enter") {
      event.preventDefault();
      if (activeIndex >= 0 && options[activeIndex]) selectResult(options[activeIndex]);
      else {
        closeResults();
        input.blur();
      }
      return;
    }
    if (!options.length || !["ArrowDown", "ArrowUp"].includes(event.key)) return;
    event.preventDefault();
    if (event.key === "ArrowDown") activeIndex = (activeIndex + 1) % options.length;
    if (event.key === "ArrowUp") activeIndex = (activeIndex - 1 + options.length) % options.length;
    options.forEach((option, index) => option.classList.toggle("active", index === activeIndex));
    input.setAttribute("aria-activedescendant", options[activeIndex].id);
  });
}

document.querySelectorAll("#domain-rows .repeat-row").forEach(configureDomainAutocomplete);
configureRepeatRows("language-rows", "language-row-template", "add-language", 20);
configureRepeatRows("domain-rows", "domain-row-template", "add-domain", 20, configureDomainAutocomplete);
