"use strict";

function configureRepeatRows(containerId, templateId, addButtonId, maximum) {
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
    const firstControl = row && row.querySelector("select, input");
    if (firstControl) firstControl.focus();
    updateButton();
  });

  updateButton();
}

configureRepeatRows("language-rows", "language-row-template", "add-language", 20);
configureRepeatRows("domain-rows", "domain-row-template", "add-domain", 20);
