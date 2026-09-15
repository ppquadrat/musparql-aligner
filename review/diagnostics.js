(function () {
  const errors = [];
  const remember = (value) => {
    const message = String(value || "Unknown JavaScript error")
      .replace(/[\r\n\t]+/g, " ")
      .slice(0, 300);
    if (message && !errors.includes(message)) errors.push(message);
  };

  window.addEventListener("error", (event) => remember(event.message));
  window.addEventListener("unhandledrejection", (event) => {
    remember(event.reason instanceof Error ? event.reason.message : event.reason);
  });

  window.addEventListener("load", () => {
    window.setTimeout(() => {
      if (window.MUSPARQL_WORKBENCH_READY === true) return;
      const hosted = window.MUSPARQL_HOSTED_CONTEXT || null;
      const data = window.REVIEW_DATA || null;
      const stage = !hosted
        ? "host-context-missing"
        : !data
          ? "review-data-missing"
          : !Array.isArray(data.records)
            ? "record-list-invalid"
            : !data.records.length
              ? "record-list-empty"
              : "initialization-failed";
      const empty = document.getElementById("emptyState");
      if (empty) {
        empty.classList.remove("hidden");
        const explanation = empty.querySelector("p");
        if (explanation) {
          explanation.textContent =
            "The batch reached the browser but could not initialize. A content-free diagnostic was sent to the server; reload after the facilitator confirms the fix.";
        }
      }
      if (!hosted?.client_diagnostic_url) return;
      window.fetch(hosted.client_diagnostic_url, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-CSRF-Token": hosted.csrf_token,
        },
        body: JSON.stringify({stage, errors: errors.slice(0, 3)}),
        keepalive: true,
      }).catch(() => {});
    }, 0);
  });
})();
