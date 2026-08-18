from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from musparql.reviewer_provenance import (
    validate_kg_domain_assessments,
    validate_resource_familiarity_assessments,
    validate_reviewer_profile_v2,
)
from musparql.source_catalog import (
    load_expertise_domain_suggestions,
    load_hydrated_seeds,
    validate_kg_seeds,
)


ROOT = Path(__file__).resolve().parents[1]
EXAMPLES = ROOT / "schemas/examples"


def _json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


@pytest.mark.parametrize(
    ("schema_name", "artifact_schema"),
    [
        ("reviewer_profile_v2.schema.json", "musparql.reviewer-profile.v2"),
        (
            "reviewer_kg_domain_assessment.schema.json",
            "musparql.reviewer-kg-domain-assessment.v1",
        ),
        (
            "reviewer_resource_familiarity_assessment.schema.json",
            "musparql.reviewer-resource-familiarity-assessment.v1",
        ),
        ("kg_seeds.schema.json", "musparql.kg-seeds.v2"),
        (
            "expertise_domain_suggestions.schema.json",
            "musparql.expertise-domain-suggestions.v1",
        ),
    ],
)
def test_phase1_json_schemas_are_draft_2020_12_and_versioned(
    schema_name: str, artifact_schema: str
) -> None:
    schema = _json(ROOT / "schemas" / schema_name)
    assert schema["$schema"] == "https://json-schema.org/draft/2020-12/schema"
    assert schema["properties"]["schema"]["const"] == artifact_schema


def test_python_validators_match_confidential_v2_examples() -> None:
    profile = _json(EXAMPLES / "reviewer-profile-v2.synthetic.json")
    domain = _json(EXAMPLES / "reviewer-kg-domain-assessment.synthetic.json")
    familiarity = _json(EXAMPLES / "reviewer-resource-familiarity-assessment.synthetic.json")
    validate_reviewer_profile_v2(profile)
    validate_kg_domain_assessments([domain], reviewer_ids={profile["id"]})
    validate_resource_familiarity_assessments([familiarity], reviewer_ids={profile["id"]})


def test_profile_context_cannot_claim_an_assignment() -> None:
    assessment = _json(EXAMPLES / "reviewer-kg-domain-assessment.synthetic.json")
    assessment.update(context="profile", assignment_id="synthetic-assignment-0001")
    with pytest.raises(ValueError, match="null assignment_id"):
        validate_kg_domain_assessments([assessment])


def test_profile_vocabulary_selection_requires_complete_provenance() -> None:
    profile = _json(EXAMPLES / "reviewer-profile-v2.synthetic.json")
    profile["domain_expertise"][0]["vocabulary_name"] = "synthetic-vocabulary"
    with pytest.raises(ValueError, match="requires URI and version"):
        validate_reviewer_profile_v2(profile)


def test_nullable_provenance_fields_must_still_be_explicit() -> None:
    profile = _json(EXAMPLES / "reviewer-profile-v2.synthetic.json")
    del profile["domain_expertise"][0]["vocabulary_version"]
    with pytest.raises(ValueError, match="missing fields"):
        validate_reviewer_profile_v2(profile)


def test_versioned_real_kg_seeds_validate_and_cover_every_kg() -> None:
    seeds_path = ROOT / "catalog/seeds.yaml"
    payload = yaml.safe_load(seeds_path.read_text(encoding="utf-8-sig"))
    validate_kg_seeds(payload, location=str(seeds_path))
    seeds = load_hydrated_seeds(seeds_path, ROOT / "catalog/sources.yaml")
    assert seeds
    for seed in seeds:
        assert seed["seed_version"]
        assert seed["review_domains"]
        assert seed["familiarity_scopes"]


def test_seed_validator_rejects_unverified_vocabulary_mapping() -> None:
    payload = yaml.safe_load((EXAMPLES / "kg-seeds.synthetic.yaml").read_text(encoding="utf-8"))
    payload["kgs"][0]["review_domains"][0]["vocabulary_mappings"] = [
        {
            "vocabulary": "synthetic-vocabulary",
            "concept_uri": "https://example.invalid/concept/1",
            "owner_verified": False,
        }
    ]
    with pytest.raises(ValueError, match="owner_verified"):
        validate_kg_seeds(payload)


def test_versioned_expertise_suggestion_snapshot_validates() -> None:
    path = ROOT / "catalog/expertise_domain_suggestions.yaml"
    payload = load_expertise_domain_suggestions(path)
    assert payload["suggestions"]
    assert all(item["source_id"] == "musparql-owner-reviewed-terms" for item in payload["suggestions"])
