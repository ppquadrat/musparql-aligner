from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import correction_service
from scripts import run_queries
from scripts.build_llm_inputs import build_prompt_input
from scripts.build_sparql_correction_bundle import build_payload
from scripts.benchmark.build_benchmark import neutral_execution_snapshot
from scripts.correction_service import Workbench, allowed_static_path, safe_endpoint, safe_error
from musparql.holdout_selectors import validate_selectors_current
from musparql.sparql_corrections import REVIEW_EXPORT_SCHEMA, apply_reviews, build_candidate, candidate_digest, safe_error as correction_safe_error
from musparql.sparql_versions import sparql_hash


BASE = "SELECT * WHERE { ?s ?p ?o }"
PROPOSAL = "SELECT DISTINCT ?s WHERE { ?s ?p ?o }"


def record(status: str | None = "http_error") -> dict:
    row = {
        "kg_id": "synthetic-kg", "query_id": "synthetic-q", "query_label": "synthetic-0001",
        "sparql_clean": BASE, "sparql_hash": sparql_hash(BASE), "sparql_edits": [],
        "evidence": [{"evidence_id": "e1", "type": "synthetic", "snippet": "Synthetic evidence."}],
        "execution_history": [],
    }
    if status is not None:
        row["execution_history"].append({
            "status": status, "ran_at": "2026-08-05T10:00:00+00:00", "endpoint": None,
            "sparql_version": 0, "sparql_hash": sparql_hash(BASE),
        })
    return row


def candidate_for(row: dict, status: str = "http_error") -> dict:
    observation = {
        "kg_id": row["kg_id"], "query_id": row["query_id"], "sparql_version": 0,
        "sparql_hash": sparql_hash(BASE), "status": status,
        "observed_at": "2026-08-05T10:00:00+00:00",
    }
    return build_candidate(observation, row)


def workbench(tmp_path: Path, row: dict, candidate: dict, provider=None, holdouts=set()) -> Workbench:
    seeds = tmp_path / "seeds.yaml"
    seeds.write_text("kgs: []\n", encoding="utf-8")
    bundle = build_payload([candidate], selector_keys=set(), source_path="synthetic", input_policy="synthetic")
    return Workbench(
        bundle=bundle, queries=[row], holdout_pairs=holdouts, seeds_path=seeds,
        attempts_path=tmp_path / "attempts.jsonl", suggestions_path=tmp_path / "suggestions.jsonl",
        model="synthetic-model", suggestion_provider=provider,
    )


@pytest.mark.parametrize("pipeline_status,expected", [
    ("ok", "ok"), ("empty", "empty"), ("unavailable", "unavailable"),
    ("unsupported", "unsupported"), ("query_error", "error"),
])
def test_execution_api_records_bounded_authoritative_attempt(tmp_path, monkeypatch, pipeline_status, expected):
    row = record(); candidate = candidate_for(row); wb = workbench(tmp_path, row, candidate)
    def fake(**kwargs):
        return {"status": expected, "ran_at": "2026-08-05T12:00:00+00:00", "sparql_version": kwargs["sparql_version"],
                "sparql_hash": kwargs["retained_hash"], "effective_sparql_hash": kwargs["retained_hash"],
                "duration_ms": 3, "result_count": 1 if expected == "ok" else 0, "sample_rows": [{"s": "public"}],
                "endpoint": None, "graph": None, "error": None}
    monkeypatch.setattr(correction_service, "execute_sparql_observation", fake)
    digest = candidate_digest(candidate)
    result = wb.execute({"candidate_id": candidate["candidate_id"], "candidate_digest": digest, "target": "proposal", "sparql": PROPOSAL, "sparql_hash": sparql_hash(PROPOSAL)})
    assert result["status"] == expected
    assert result["sparql_hash"] == sparql_hash(PROPOSAL)
    assert json.loads((tmp_path / "attempts.jsonl").read_text())["attempt_id"] == result["attempt_id"]


def test_execution_rejects_stale_hash_and_holdout_before_work(tmp_path):
    row = record(); candidate = candidate_for(row)
    wb = workbench(tmp_path, row, candidate)
    with pytest.raises(ValueError, match="mismatched"):
        wb.execute({"candidate_id": candidate["candidate_id"], "candidate_digest": candidate_digest(candidate), "target": "proposal", "sparql": PROPOSAL, "sparql_hash": sparql_hash(BASE)})
    excluded = workbench(tmp_path, row, candidate, holdouts={(row["kg_id"], row["query_id"])})
    assert excluded.candidates == {}
    with pytest.raises(PermissionError):
        excluded.execute({"candidate_id": candidate["candidate_id"]})


def test_executor_rejects_sparql_updates_before_network(monkeypatch):
    called = False
    def network(*args, **kwargs):
        nonlocal called
        called = True
        return {"status": "ok"}
    monkeypatch.setattr(run_queries, "run_select_query", network)
    endpoint = run_queries.KGEndpoint("synthetic-kg", run_queries.SparqlTarget("https://example.invalid/sparql"), [])
    text = "PREFIX ex: <https://example.test/> DELETE WHERE { ?s ?p ?o }"
    result = run_queries.execute_sparql_observation(
        kg_id="synthetic-kg", retained_sparql=text, retained_hash=sparql_hash(text), sparql_version=None,
        endpoints={"synthetic-kg": endpoint}, datasets={},
    )
    assert result["status"] == "unsupported"
    assert result["skip_reason"] == "non_read_only_operation"
    assert not called


@pytest.mark.parametrize("text", ["ASK { ?s ?p ?o }", "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }", "DESCRIBE ?s WHERE { ?s ?p ?o }"])
def test_executor_rejects_unparsed_read_only_result_forms_before_network(monkeypatch, text):
    called = False
    def network(*args, **kwargs):
        nonlocal called
        called = True
        return {"status": "ok"}
    monkeypatch.setattr(run_queries, "run_select_query", network)
    endpoint = run_queries.KGEndpoint("synthetic-kg", run_queries.SparqlTarget("https://example.invalid/sparql"), [])
    result = run_queries.execute_sparql_observation(
        kg_id="synthetic-kg", retained_sparql=text, retained_hash=sparql_hash(text), sparql_version=None,
        endpoints={"synthetic-kg": endpoint}, datasets={},
    )
    assert result["status"] == "unsupported"
    assert not called


def test_service_redacts_endpoint_credentials_and_error_secrets():
    assert safe_endpoint("https://user:pass@example.test/sparql?token=secret") == "https://example.test/sparql"
    assert "secret-value" not in safe_error("token=secret-value request failed")
    assert "abc123" not in safe_error("Authorization: Bearer abc123")
    signed = "failed https://example.test/object?X-Amz-Signature=SECRET&sig=ALSOSECRET#fragment"
    assert "SECRET" not in safe_error(signed)
    assert "SECRET" not in correction_safe_error(signed)


def test_service_rejects_candidate_when_canonical_provenance_changes(tmp_path):
    row = record(); candidate = candidate_for(row); wb = workbench(tmp_path, row, candidate)
    row["sparql_correction_history"] = [{"decision": "defer", "reviewed_at": "2026-08-05T11:00:00+00:00"}]
    with pytest.raises(ValueError, match="provenance is stale"):
        wb.suggest({"candidate_id": candidate["candidate_id"], "candidate_digest": candidate_digest(candidate)})


def test_suggestion_input_defensively_sanitizes_legacy_execution_and_graph(tmp_path):
    row = record(); candidate = candidate_for(row)
    wb = workbench(tmp_path, row, candidate)
    candidate["execution"] = {
        "status": "http_error",
        "endpoint": "https://user:pass@example.test/sparql?X-Amz-Signature=SECRET",
        "error": "failed https://example.test/x?sig=SECRET",
        "body_snippet": "private response body",
    }
    wb.endpoints["synthetic-kg"] = run_queries.KGEndpoint(
        "synthetic-kg", run_queries.SparqlTarget("https://example.test/sparql", graph="https://example.test/graph?sig=SECRET"), []
    )
    payload = wb.suggestion_input(candidate, row)
    encoded = json.dumps(payload)
    assert "SECRET" not in encoded and "private response body" not in encoded and "user:pass" not in encoded


def test_candidate_and_downstream_execution_projection_redacts_secrets():
    row = record()
    failure = {
        "kg_id": row["kg_id"], "query_id": row["query_id"], "sparql_version": 0,
        "sparql_hash": sparql_hash(BASE), "status": "http_error", "http_status": 500,
        "endpoint": "https://user:pass@example.test/sparql?token=secret", "body_snippet": "private body",
        "error": "Bearer abc123 token=secret-value", "observed_at": "2026-08-05T10:00:00+00:00",
    }
    candidate = build_candidate(failure, row)
    encoded = json.dumps(candidate)
    assert "user:pass" not in encoded and "private body" not in encoded and "abc123" not in encoded
    assert candidate["execution"]["endpoint"] == "https://example.test/sparql"


def test_bundle_rejects_legacy_candidate_execution_that_would_expose_secrets():
    row = record(); candidate = candidate_for(row)
    candidate["execution"] = {
        **candidate["execution"],
        "endpoint": "https://user:pass@example.test/sparql?X-Amz-Signature=SECRET",
        "error": "failed https://example.test/x?sig=SECRET",
        "body_snippet": "private response body",
    }
    with pytest.raises(ValueError, match="not safely projected"):
        build_payload([candidate], selector_keys=set(), source_path="synthetic", input_policy="synthetic")


def test_neutral_benchmark_snapshot_counts_missing_and_failed_execution():
    records = [
        {"sparql_provenance": {"execution_observation": {"status": "http_error", "attempted": True, "observed_at": "2026-08-05T10:00:00+00:00"}}},
        {"sparql_provenance": {"execution_observation": {"status": "not_attempted", "attempted": False}}},
    ]
    snapshot = neutral_execution_snapshot(records, captured_through="2026-08-05T12:00:00+00:00")
    assert snapshot["selected_queries"] == 2
    assert snapshot["selected_versions_with_execution"] == 1
    assert snapshot["status_counts"] == {"http_error": 1, "not_attempted": 1}


@pytest.mark.parametrize("path", [
    "/private/example.json", "/exports/example.json", "/local_workbench_suggestions.jsonl",
    "/../seeds.yaml", "/%2e%2e/seeds.yaml", "/index.html",
])
def test_static_service_denies_private_logs_and_non_workbench_files(path):
    assert allowed_static_path(path) is None


@pytest.mark.parametrize("path", ["/corrections.html", "/correction_app.js?v=1", "/styles.css", "/sparql_correction_data.js"])
def test_static_service_allows_only_correction_assets(path):
    assert allowed_static_path(path) is not None


def test_holdout_filter_precedes_candidate_validation_and_evidence_access():
    bad = {"kg_id": "synthetic-kg", "query_id": "holdout", "evidence": "must-not-inspect"}
    payload = build_payload([bad], selector_keys={("synthetic-kg", "holdout")}, source_path="synthetic", input_policy="synthetic")
    assert payload["records"] == [] and payload["holdout_excluded"] == 1


def test_stale_holdout_selector_version_hash_fails_closed():
    row = record()
    selector = {"kg_id": row["kg_id"], "query_id": row["query_id"], "sparql_version": 0, "sparql_hash": "0" * 64}
    with pytest.raises(ValueError, match="Stale holdout selector"):
        validate_selectors_current([selector], [row])


def test_generate_approve_round_trip_retains_authoritative_agent_provenance(tmp_path):
    row = record(); candidate = candidate_for(row)
    output = {"recommendation": "edit", "proposed_sparql": PROPOSAL, "edit_type": "syntax_correction", "rationale": "Synthetic correction.", "evidence_ids": [], "uncertainty": "Low."}
    wb = workbench(tmp_path, row, candidate, provider=lambda _payload: (output, {"id": "req-synthetic", "model": "synthetic-model"}))
    digest = candidate_digest(candidate)
    suggestion = wb.suggest({"candidate_id": candidate["candidate_id"], "candidate_digest": digest})
    attempt = wb.execute({"candidate_id": candidate["candidate_id"], "candidate_digest": digest, "target": "proposal", "sparql": PROPOSAL, "sparql_hash": sparql_hash(PROPOSAL)})
    review = {
        "candidate_id": candidate["candidate_id"], "candidate_digest": digest,
        "review_id": f"{candidate['candidate_id']}::reviewer-0001",
        "candidate_reason_code": candidate["triage"]["reason_code"], "kg_id": row["kg_id"], "query_id": row["query_id"],
        "base_sparql_version": 0, "base_sparql_hash": sparql_hash(BASE), "decision": "approve_edit",
        "proposed_sparql": PROPOSAL, "edit_type": "syntax_correction", "rationale": "Synthetic correction.",
        "evidence_ids": [], "proposal_origin": "agent", "proposal_model": "synthetic-model",
        "agent_suggestion": suggestion, "execution_attempts": [attempt], "reviewed_at": "2026-08-05T12:00:00+00:00",
        "reviewer_id": "reviewer-0001", "prior_review_ids": [],
        "authored_formulation_ids": [],
        "approved_formulation_ids": [f"{candidate['candidate_id']}::reviewer-0001::formulation::sparql"],
    }
    export = {"schema": REVIEW_EXPORT_SCHEMA, "mode": "sparql_correction", "reviewer_id": "reviewer-0001", "dataset_id": "synthetic", "exported_at": "2026-08-05T12:00:00+00:00", "reviews": [review]}
    apply_reviews([row], export, export_path="synthetic.json", candidates=[candidate], authoritative_suggestions=[suggestion], authoritative_executions=[attempt])
    provenance = row["sparql_edits"][0]["provenance"]
    assert provenance["agent_suggestion"]["request_id"] == "req-synthetic"
    assert provenance["approved_sparql_hash"] == sparql_hash(PROPOSAL)
    downstream = build_prompt_input(row, include_raw=False, include_sparql_blocks=False)
    assert downstream["sparql_provenance"]["execution_observation"]["status"] == "unavailable"
    assert downstream["sparql_provenance"]["selected_edit"]["agent_provenance"]["request_id"] == "req-synthetic"


def test_suggestion_supports_chat_completions_api(tmp_path, monkeypatch):
    row = record(); candidate = candidate_for(row)
    output = {
        "recommendation": "edit", "proposed_sparql": PROPOSAL,
        "edit_type": "syntax_correction", "rationale": "Synthetic correction.",
        "evidence_ids": [], "uncertainty": "Low.",
    }
    captured = {}

    class FakeClient:
        def __init__(self, **_kwargs):
            self.chat = SimpleNamespace(completions=SimpleNamespace(create=self.create))
            self.responses = SimpleNamespace(create=lambda **_kwargs: pytest.fail("Responses API should not be used"))

        def create(self, **kwargs):
            captured.update(kwargs)
            message = SimpleNamespace(content=json.dumps(output))
            return SimpleNamespace(id="req-chat", model="chat-model", choices=[SimpleNamespace(message=message)])

    monkeypatch.setattr(correction_service, "OpenAI", FakeClient)
    wb = workbench(tmp_path, row, candidate)
    wb.api_method = "chat.completions.create"
    suggestion = wb.suggest({"candidate_id": candidate["candidate_id"], "candidate_digest": candidate_digest(candidate)})

    assert captured["model"] == "synthetic-model"
    assert captured["messages"][0]["role"] == "system"
    assert suggestion["request_id"] == "req-chat"
    assert suggestion["model"] == "chat-model"


@pytest.mark.parametrize("status", ["ok", "http_error", "skipped_no_endpoint", None])
def test_downstream_includes_latest_approved_for_every_execution_state(status):
    row = record(status)
    row["sparql_edits"] = [{"version": 1, "sparql": PROPOSAL, "note": "Synthetic approved edit."}]
    if status is not None:
        row["execution_history"] = [{**row["execution_history"][0], "sparql_version": 1, "sparql_hash": sparql_hash(PROPOSAL)}]
    payload = build_prompt_input(row, include_raw=False, include_sparql_blocks=False)
    expected = "not_attempted" if status is None else status
    assert payload["sparql_clean"] == PROPOSAL
    assert payload["sparql_provenance"]["execution_observation"]["status"] == expected


def test_explicit_older_version_is_respected_and_identified():
    row = record(None)
    row["sparql_edits"] = [{"version": 1, "sparql": PROPOSAL, "note": "Synthetic approved edit."}]
    payload = build_prompt_input(row, include_raw=False, include_sparql_blocks=False, sparql_version="0")
    assert payload["sparql_clean"] == BASE and payload["sparql_version"] == 0
    assert payload["sparql_provenance"]["selected_version"] == 0
