from __future__ import annotations

import pytest

from musparql.reviewer_provenance import (
    validate_kg_familiarities,
    validate_review_provenance,
    validate_reviewer,
    validate_reviewer_id,
)
from scripts.migrations.add_reviewer_provenance import migrate_payload, migrate_query_catalog
from scripts.benchmark.build_benchmark import add_formulation


SYNTHETIC_REVIEWER = {
    "id": "reviewer-0042",
    "name": "Synthetic Reviewer",
    "affiliation": "Example Institute",
    "email": "synthetic@example.invalid",
    "domain_expertise": "expert",
    "kg_ontology_experience": "regular",
    "sparql_experience": "occasional",
    "nlp_llm_experience": "none",
    "language_expertise": {"en": "fluent"},
    "privacy_notice_version": "synthetic-v1",
    "privacy_notice_acknowledged_at": "2026-08-16T12:00:00Z",
}


def test_confidential_reviewer_and_familiarity_validate() -> None:
    validate_reviewer(SYNTHETIC_REVIEWER)
    validate_kg_familiarities(
        [{"reviewer_id": "reviewer-0042", "kg_id": "synthetic-kg", "familiarity": "queried"}],
        reviewer_ids={"reviewer-0042"},
    )


def test_reviewer_ids_are_pseudonymous_numeric_ids() -> None:
    assert validate_reviewer_id("reviewer-0001") == "reviewer-0001"
    with pytest.raises(ValueError, match="reviewer-NNNN"):
        validate_reviewer_id("alice")


def test_duplicate_reviewer_kg_familiarity_is_rejected() -> None:
    row = {"reviewer_id": "reviewer-0042", "kg_id": "synthetic-kg", "familiarity": "queried"}
    with pytest.raises(ValueError, match="Duplicate"):
        validate_kg_familiarities([row, row])


def test_legacy_sanitized_export_migrates_without_private_data() -> None:
    payload = {
        "kind": "non_holdout_review_export",
        "dataset_id": "synthetic",
        "exported_at": "2026-08-16T12:00:00Z",
        "reviews": {
            "synthetic-review": {
                "benchmark_disposition": "included",
                "pipeline_assessment": "accepted",
                "preferred_question": "Who created the synthetic item?",
                "literal_wording": "Which creator is attached?",
                "updated_at": "2026-08-16T11:00:00Z",
            }
        },
    }
    migrated = migrate_payload(payload, "reviewer-0042")
    review = migrated["reviews"]["synthetic-review"]
    assert migrated["schema"] == "musparql.review-export.v2"
    assert review["reviewer_id"] == "reviewer-0042"
    assert review["review_id"] == "synthetic-review::reviewer-0042"
    assert review["reviewed_at"] == "2026-08-16T11:00:00Z"
    assert review["authored_formulation_ids"] == [
        "synthetic-review::reviewer-0042::formulation::preferred",
        "synthetic-review::reviewer-0042::formulation::literal",
    ]
    assert review["approved_formulation_ids"] == ["synthetic-review::reviewer-0042::formulation::preferred"]
    validate_review_provenance(review)
    other = migrate_payload(payload, "reviewer-0043")["reviews"]["synthetic-review"]
    assert other["review_id"] != review["review_id"]


def test_migration_upgrades_unsuffixed_v2_event_ids_and_is_idempotent() -> None:
    payload = {
        "schema": "musparql.review-export.v2",
        "kind": "non_holdout_review_export",
        "reviewer_id": "reviewer-0042",
        "exported_at": "2026-08-16T11:00:00Z",
        "reviews": {
            "synthetic-review": {
                "review_id": "synthetic-review",
                "reviewer_id": "reviewer-0042",
                "reviewed_at": "2026-08-16T11:00:00Z",
                "preferred_question": "Synthetic question?",
                "prior_review_ids": [],
                "authored_formulation_ids": ["synthetic-review::formulation::preferred"],
                "approved_formulation_ids": ["synthetic-review::formulation::preferred"],
            }
        },
    }
    migrated = migrate_payload(payload, "reviewer-0042")
    review = migrated["reviews"]["synthetic-review"]
    assert review["review_id"] == "synthetic-review::reviewer-0042"
    assert review["authored_formulation_ids"] == [
        "synthetic-review::reviewer-0042::formulation::preferred"
    ]
    assert migrate_payload(migrated, "reviewer-0042") == migrated


def test_legacy_correction_export_migrates_formulation_links() -> None:
    payload = {
        "schema": "musparql.sparql-correction-review-export.v1",
        "mode": "sparql_correction",
        "exported_at": "2026-08-16T12:00:00Z",
        "reviews": [{
            "candidate_id": "synthetic-candidate",
            "decision": "approve_edit",
            "proposal_origin": "human",
            "reviewed_at": "2026-08-16T11:00:00Z",
        }],
    }
    migrated = migrate_payload(payload, "reviewer-0042")
    review = migrated["reviews"][0]
    assert migrated["schema"] == "musparql.sparql-correction-review-export.v2"
    assert review["review_id"] == "synthetic-candidate::reviewer-0042"
    assert review["authored_formulation_ids"] == ["synthetic-candidate::reviewer-0042::formulation::sparql"]
    assert review["approved_formulation_ids"] == ["synthetic-candidate::reviewer-0042::formulation::sparql"]


def test_repeated_approval_merges_without_overwriting_authorship() -> None:
    record = {
        "accepted_alternatives": [{
            "formulation_id": "formulation-1",
            "text": "Synthetic wording",
            "normalized_text": "synthetic wording",
            "source_type": "reviewer_rewrite",
            "authored_by_reviewer_id": "reviewer-0001",
            "approval_review_ids": ["review-1"],
            "approval_reviewer_ids": ["reviewer-0001"],
        }]
    }
    add_formulation(record, "accepted_alternatives", {
        "formulation_id": "formulation-1",
        "text": "Synthetic wording",
        "normalized_text": "synthetic wording",
        "source_type": "reviewer_rewrite",
        "authored_by_reviewer_id": None,
        "approval_review_ids": ["review-2"],
        "approval_reviewer_ids": ["reviewer-0002"],
    })
    formulation = record["accepted_alternatives"][0]
    assert formulation["authored_by_reviewer_id"] == "reviewer-0001"
    assert formulation["approval_review_ids"] == ["review-1", "review-2"]
    assert formulation["approval_reviewer_ids"] == ["reviewer-0001", "reviewer-0002"]


def test_query_catalogue_correction_history_is_backfilled() -> None:
    records = [{
        "kg_id": "synthetic-kg",
        "query_id": "synthetic-query",
        "sparql_correction_history": [{
            "candidate_id": "synthetic-candidate",
            "decision": "approve_edit",
            "proposal_origin": "human",
            "reviewed_at": "2026-08-16T11:00:00Z",
        }],
        "sparql_edits": [{
            "version": 1,
            "provenance": {"candidate_id": "synthetic-candidate"},
        }],
    }]
    migrated = migrate_query_catalog(records, "reviewer-0042")[0]
    review = migrated["sparql_correction_history"][0]
    assert review["reviewer_id"] == "reviewer-0042"
    assert review["approved_formulation_ids"] == ["synthetic-candidate::reviewer-0042::formulation::sparql"]
    assert migrated["sparql_edits"][0]["provenance"]["reviewer_id"] == "reviewer-0042"
