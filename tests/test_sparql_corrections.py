from __future__ import annotations

import json
import subprocess
from copy import deepcopy
from pathlib import Path

import pytest

from build_sparql_correction_bundle import build_payload
from build_llm_inputs import build_prompt_input
from holdout_selectors import assert_selectors_unedited
from sparql_corrections import (
    REVIEW_EXPORT_SCHEMA,
    apply_reviews,
    build_candidate,
    candidate_digest,
    classify_failure,
    exclude_candidate_pairs,
    merge_candidates,
    retained_sparql_edit_count,
    validate_candidate,
    write_jsonl,
)
from sparql_versions import resolve_sparql_version, sparql_hash


ORIGINAL = "SELECT * WHERE { ?s ?p ?o }"
EDITED = "SELECT DISTINCT ?s WHERE { ?s ?p ?o }"


def query_record() -> dict:
    record = {
        "kg_id": "synthetic-kg",
        "query_id": "synthetic-q1",
        "query_label": "synthetic-0001",
        "sparql_clean": ORIGINAL,
        "sparql_hash": sparql_hash(ORIGINAL),
        "sparql_edits": [],
        "evidence": [
            {
                "evidence_id": "e1",
                "type": "query_comment",
                "snippet": "Synthetic evidence only.",
            }
        ],
        "execution_history": [],
    }
    record["execution_history"].append(
        {
            "status": "http_error",
            "endpoint": "https://example.invalid/sparql",
            "http_status": 400,
            "ran_at": "2026-08-05T10:00:00+00:00",
            "sparql_version": 0,
            "sparql_hash": sparql_hash(ORIGINAL),
        }
    )
    return record


def failure(**overrides) -> dict:
    row = {
        "kg_id": "synthetic-kg",
        "query_id": "synthetic-q1",
        "query_label": "synthetic-0001",
        "sparql_version": 0,
        "sparql_hash": sparql_hash(ORIGINAL),
        "status": "http_error",
        "http_status": 400,
        "endpoint": "https://example.invalid/sparql",
        "observed_at": "2026-08-05T10:00:00+00:00",
    }
    row.update(overrides)
    return row


def review_export(candidate: dict, **overrides) -> dict:
    review = {
        "candidate_id": candidate["candidate_id"],
        "candidate_digest": candidate_digest(candidate),
        "candidate_reason_code": candidate["triage"]["reason_code"],
        "kg_id": candidate["kg_id"],
        "query_id": candidate["query_id"],
        "query_label": candidate["query_label"],
        "base_sparql_version": candidate["base_sparql_version"],
        "base_sparql_hash": candidate["base_sparql_hash"],
        "decision": "approve_edit",
        "proposed_sparql": EDITED,
        "edit_type": "syntax_correction",
        "rationale": "Synthetic parser correction.",
        "evidence_ids": ["e1"],
        "proposal_origin": "human",
        "proposal_model": None,
        "candidate_execution": candidate["execution"],
        "reviewed_at": "2026-08-05T11:00:00+00:00",
    }
    review.update(overrides)
    return {
        "schema": REVIEW_EXPORT_SCHEMA,
        "mode": "sparql_correction",
        "dataset_id": "synthetic-dataset",
        "exported_at": "2026-08-05T11:00:00+00:00",
        "reviews": [review],
    }


def test_failure_triage_distinguishes_correction_runtime_and_infrastructure():
    assert classify_failure(failure())["category"] == "likely_correction"
    assert classify_failure(failure(status="skipped_local_query", http_status=None, skip_reason="parameterized_template"))["category"] == "instantiation_required"
    assert classify_failure(failure(status="skipped_endpoint_unavailable", http_status=None))["category"] == "infrastructure"
    assert classify_failure(failure(status="http_error", http_status=500))["category"] == "investigate"


def test_candidate_capture_is_deterministic_and_partial_runs_preserve_other_jobs():
    query = query_record()
    first = build_candidate(failure(), query)
    again = build_candidate(failure(), query)
    assert first["candidate_id"] == again["candidate_id"]
    old = {**first, "query_id": "other", "candidate_id": "other-candidate"}
    merged = merge_candidates([old], [failure()], [query], {("synthetic-kg", "synthetic-q1", 0)})
    assert {item["candidate_id"] for item in merged} == {"other-candidate", first["candidate_id"]}


def test_approved_review_appends_version_and_complete_provenance():
    query = query_record()
    candidate = build_candidate(failure(), query)
    records = [query]
    stats = apply_reviews(records, review_export(candidate), export_path="synthetic-review.json", candidates=[candidate])
    assert stats == {"approved": 1, "no_edit": 0, "deferred": 0}
    assert resolve_sparql_version(query, "latest")["sparql"] == EDITED
    edit = query["sparql_edits"][0]
    assert edit["version"] == 1
    assert edit["edit_type"] == "syntax_correction"
    assert edit["evidence_ids"] == ["e1"]
    assert edit["provenance"]["candidate_id"] == candidate["candidate_id"]
    assert edit["provenance"]["proposal_origin"] == "human"
    assert edit["provenance"]["review_export_hash"].startswith("sha256:")
    assert retained_sparql_edit_count(query) == 1


def test_approved_edit_is_included_without_successful_execution():
    query = query_record()
    candidate = build_candidate(failure(), query)
    apply_reviews([query], review_export(candidate), export_path="synthetic.json", candidates=[candidate])
    payload = build_prompt_input(query, include_raw=False, include_sparql_blocks=False)
    assert payload["sparql_provenance"]["execution_observation"]["status"] == "not_attempted"
    resolved = resolve_sparql_version(query, "latest")
    query["execution_history"].append(
        {"status": "ok", "ran_at": "2026-08-05T12:00:00+00:00", "sparql_version": 1, "sparql_hash": resolved["sparql_hash"]}
    )
    payload = build_prompt_input(query, include_raw=False, include_sparql_blocks=False)
    assert payload["sparql_provenance"]["execution_observation"]["status"] == "ok"
    assert payload["sparql_provenance"]["selected_edit"]["candidate_id"] == candidate["candidate_id"]


def test_apply_rejects_stale_unchanged_duplicate_and_holdout_pairs():
    query = query_record()
    candidate = build_candidate(failure(), query)
    stale = review_export(candidate)
    stale["reviews"][0]["base_sparql_hash"] = "sha256:stale"
    with pytest.raises(ValueError, match="Stale"):
        apply_reviews([deepcopy(query)], stale, export_path="synthetic.json", candidates=[candidate])
    unchanged = review_export(candidate, proposed_sparql=ORIGINAL)
    with pytest.raises(ValueError, match="unchanged"):
        apply_reviews([deepcopy(query)], unchanged, export_path="synthetic.json", candidates=[candidate])
    with pytest.raises(ValueError, match="holdout-selected"):
        apply_reviews(
            [deepcopy(query)],
            review_export(candidate),
            export_path="synthetic.json",
            candidates=[candidate],
            forbidden_pairs={("synthetic-kg", "synthetic-q1")},
        )
    applied = deepcopy(query)
    payload = review_export(candidate)
    apply_reviews([applied], payload, export_path="synthetic.json", candidates=[candidate])
    with pytest.raises(ValueError, match="Stale|already applied"):
        apply_reviews([applied], payload, export_path="synthetic.json", candidates=[candidate])


def test_no_edit_records_durable_history_without_adding_version():
    query = query_record()
    candidate = build_candidate(failure(), query)
    payload = review_export(candidate, decision="no_edit", proposed_sparql="", edit_type=None)
    stats = apply_reviews([query], payload, export_path="synthetic.json", candidates=[candidate])
    assert stats["no_edit"] == 1
    assert query["sparql_edits"] == []
    assert query["sparql_correction_history"][0]["decision"] == "no_edit"


def test_review_allows_missing_evidence_and_requires_only_type_or_rationale():
    query = query_record()
    candidate = build_candidate(failure(), query)
    target = deepcopy(query)
    apply_reviews([target], review_export(candidate, evidence_ids=[], rationale=""), export_path="x", candidates=[candidate])
    invalid = review_export(candidate, evidence_ids=[], rationale="", edit_type=None)
    with pytest.raises(ValueError, match="either"):
        apply_reviews([deepcopy(query)], invalid, export_path="x", candidates=[candidate])


def test_candidate_validation_and_kg_scoping_fail_closed():
    query = query_record()
    candidate = build_candidate(failure(), query)
    tampered = deepcopy(candidate)
    tampered["execution"]["status"] = "skipped_no_endpoint"
    with pytest.raises(ValueError, match="identity or triage"):
        validate_candidate(tampered)
    other = {**query, "kg_id": "other-kg"}
    other_failure = failure(kg_id="other-kg")
    merged = merge_candidates([], [failure(), other_failure], [query, other], {("synthetic-kg", "synthetic-q1", 0), ("other-kg", "synthetic-q1", 0)})
    assert len(merged) == 2
    assert merge_candidates(merged, [], [query, other], {("synthetic-kg", "synthetic-q1", 0)})[0]["kg_id"] == "other-kg"


def test_atomic_jsonl_write_preserves_original_if_replace_fails(tmp_path, monkeypatch):
    path = tmp_path / "canonical.jsonl"
    path.write_text("original\n", encoding="utf-8")
    monkeypatch.setattr("sparql_corrections.os.replace", lambda *_: (_ for _ in ()).throw(OSError("synthetic")))
    with pytest.raises(OSError, match="synthetic"):
        write_jsonl(path, [{"replacement": True}])
    assert path.read_text(encoding="utf-8") == "original\n"


def test_edited_identity_is_rejected_for_holdout_even_when_selecting_v0():
    query = query_record()
    query["sparql_edits"] = [{"version": 1, "sparql": EDITED, "note": "Synthetic edit."}]
    assert resolve_sparql_version(query, "original")["sparql_version"] == 0
    with pytest.raises(ValueError, match="permanently ineligible"):
        assert_selectors_unedited({("synthetic-kg", "synthetic-q1")}, [query])


def test_correction_bundle_excludes_holdout_selectors_pair_wide():
    candidate = build_candidate(failure(), query_record())
    payload = build_payload(
        [candidate],
        selector_keys={("synthetic-kg", "synthetic-q1")},
        source_path="synthetic.jsonl",
        input_policy="identity_visible_selectors",
    )
    assert payload["records"] == []
    assert payload["holdout_excluded"] == 1
    assert exclude_candidate_pairs([candidate], {("synthetic-kg", "synthetic-q1")}) == []


def test_correction_ui_schema_validates_synthetic_review(tmp_path):
    root = Path(__file__).resolve().parents[1]
    script = r'''
const fs = require("fs"); const vm = require("vm");
const sandbox = { window:{SPARQL_CORRECTION_DATA:null}, document:{getElementById:()=>null, querySelectorAll:()=>[]}};
vm.runInNewContext(fs.readFileSync("review/correction_app.js", "utf8"), sandbox);
const schema = sandbox.window.MUSPARQL_CORRECTION_SCHEMA;
const normalized = schema.normalizeReview({decision:"approve_edit", evidence_ids:["e1"]});
if (normalized.decision !== "approve_edit" || normalized.evidence_ids[0] !== "e1") process.exit(2);
'''
    subprocess.run(["node", "-e", script], check=True, cwd=root)
