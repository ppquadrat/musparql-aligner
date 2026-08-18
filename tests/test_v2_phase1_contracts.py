from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path

import pytest
import yaml
from jsonschema import Draft202012Validator, FormatChecker
from referencing import Registry, Resource

from musparql.reviewer_provenance import (
    validate_assessments_against_kg_seed_snapshots,
    validate_kg_domain_assessments,
    validate_resource_familiarity_assessments,
    validate_reviewer_domain_expertise_assertions,
    validate_reviewer_profile_projection,
    validate_reviewer_profile_v2,
)
from musparql.source_catalog import (
    kg_seed_digest,
    load_expertise_domain_suggestions,
    load_current_kg_seeds,
    load_hydrated_seeds,
    load_kg_seed_snapshots,
    validate_current_kg_seed_snapshots,
    validate_kg_seed_snapshots,
    validate_kg_seeds,
)
from scripts.build_kgs import kgseed_to_record, parse_kg_seed
from scripts.snapshot_kg_seeds import update_snapshot_archive


ROOT = Path(__file__).resolve().parents[1]
EXAMPLES = ROOT / "schemas/examples"


def _json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _schema_registry() -> tuple[dict[str, dict], Registry]:
    names = (
        "reviewer_profile_v2.schema.json",
        "reviewer_domain_expertise_assertion.schema.json",
        "reviewer_kg_domain_assessment.schema.json",
        "reviewer_resource_familiarity_assessment.schema.json",
        "kg_seeds.schema.json",
        "kg_seed_snapshots.schema.json",
        "expertise_domain_suggestions.schema.json",
    )
    schemas = {name: _json(ROOT / "schemas" / name) for name in names}
    registry = Registry().with_resources(
        (schema["$id"], Resource.from_contents(schema))
        for schema in schemas.values()
    )
    return schemas, registry


def _json_schema_errors(schema_name: str, instance: object) -> list[str]:
    schemas, registry = _schema_registry()
    schema = schemas[schema_name]
    Draft202012Validator.check_schema(schema)
    validator = Draft202012Validator(
        schema,
        registry=registry,
        format_checker=FormatChecker(),
    )
    return [error.message for error in validator.iter_errors(instance)]


@pytest.mark.parametrize(
    ("schema_name", "artifact_schema"),
    [
        ("reviewer_profile_v2.schema.json", "musparql.reviewer-profile.v2"),
        (
            "reviewer_domain_expertise_assertion.schema.json",
            "musparql.reviewer-domain-expertise-assertion.v1",
        ),
        (
            "reviewer_kg_domain_assessment.schema.json",
            "musparql.reviewer-kg-domain-assessment.v1",
        ),
        (
            "reviewer_resource_familiarity_assessment.schema.json",
            "musparql.reviewer-resource-familiarity-assessment.v1",
        ),
        ("kg_seeds.schema.json", "musparql.kg-seeds.v2"),
        ("kg_seed_snapshots.schema.json", "musparql.kg-seed-snapshots.v1"),
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


def test_phase1_json_schemas_execute_against_every_synthetic_contract() -> None:
    artifacts = (
        ("reviewer_profile_v2.schema.json", _json(EXAMPLES / "reviewer-profile-v2.synthetic.json")),
        (
            "reviewer_domain_expertise_assertion.schema.json",
            _json(EXAMPLES / "reviewer-domain-expertise-assertion.synthetic.json"),
        ),
        (
            "reviewer_kg_domain_assessment.schema.json",
            _json(EXAMPLES / "reviewer-kg-domain-assessment.synthetic.json"),
        ),
        (
            "reviewer_resource_familiarity_assessment.schema.json",
            _json(EXAMPLES / "reviewer-resource-familiarity-assessment.synthetic.json"),
        ),
        (
            "kg_seeds.schema.json",
            yaml.safe_load((EXAMPLES / "kg-seeds.synthetic.yaml").read_text(encoding="utf-8")),
        ),
        (
            "kg_seed_snapshots.schema.json",
            yaml.safe_load(
                (ROOT / "catalog/kg_seed_snapshots.yaml").read_text(encoding="utf-8")
            ),
        ),
        (
            "expertise_domain_suggestions.schema.json",
            yaml.safe_load(
                (ROOT / "catalog/expertise_domain_suggestions.yaml").read_text(encoding="utf-8")
            ),
        ),
    )
    for schema_name, artifact in artifacts:
        assert _json_schema_errors(schema_name, artifact) == []


def test_python_validators_match_confidential_v2_examples() -> None:
    profile = _json(EXAMPLES / "reviewer-profile-v2.synthetic.json")
    expertise = _json(EXAMPLES / "reviewer-domain-expertise-assertion.synthetic.json")
    domain = _json(EXAMPLES / "reviewer-kg-domain-assessment.synthetic.json")
    familiarity = _json(EXAMPLES / "reviewer-resource-familiarity-assessment.synthetic.json")
    validate_reviewer_profile_v2(profile)
    validate_reviewer_domain_expertise_assertions([expertise], reviewer_ids={profile["id"]})
    validate_reviewer_profile_projection(profile, [expertise])
    validate_kg_domain_assessments([domain], reviewer_ids={profile["id"]})
    validate_resource_familiarity_assessments([familiarity], reviewer_ids={profile["id"]})
    projection = profile["domain_expertise"][0]
    assert projection["assertion_id"] == expertise["id"]
    assert projection["domain_id"] == expertise["domain_id"]


def test_profile_projection_must_point_to_latest_assertion() -> None:
    profile = _json(EXAMPLES / "reviewer-profile-v2.synthetic.json")
    first = _json(EXAMPLES / "reviewer-domain-expertise-assertion.synthetic.json")
    second = deepcopy(first)
    second.update(
        id="synthetic-domain-expertise-0002",
        expertise_level="advanced",
        asserted_at="2026-02-01T10:00:00Z",
        supersedes_id=first["id"],
    )
    profile["domain_expertise"][0].update(
        assertion_id=second["id"],
        expertise_level="advanced",
        updated_at=second["asserted_at"],
    )
    validate_reviewer_profile_projection(profile, [first, second])
    profile["domain_expertise"][0]["assertion_id"] = first["id"]
    with pytest.raises(ValueError, match="current history head"):
        validate_reviewer_profile_projection(profile, [first, second])


def test_synthetic_assessments_snapshot_their_referenced_seed_contract() -> None:
    payload = yaml.safe_load((EXAMPLES / "kg-seeds.synthetic.yaml").read_text(encoding="utf-8"))
    seed = validate_kg_seeds(payload)[0]
    domain = _json(EXAMPLES / "reviewer-kg-domain-assessment.synthetic.json")
    familiarity = _json(EXAMPLES / "reviewer-resource-familiarity-assessment.synthetic.json")
    snapshot = {
        "kg_id": seed["kg_id"],
        "seed_version": seed["seed_version"],
        "seed_digest": kg_seed_digest(seed),
        "previous_seed_digest": None,
        "seed": seed,
    }
    archive = {
        "schema": "musparql.kg-seed-snapshots.v1",
        "snapshots": [snapshot],
    }
    validate_kg_seed_snapshots(archive)
    validate_assessments_against_kg_seed_snapshots([domain], [familiarity], archive)
    mismatched = deepcopy(familiarity)
    mismatched["familiarity_scope_label"] = "Wrong synthetic label"
    with pytest.raises(ValueError, match="label does not match"):
        validate_assessments_against_kg_seed_snapshots([domain], [mismatched], archive)

    tampered = deepcopy(archive)
    tampered["snapshots"][0]["seed"]["name"] = "Tampered synthetic graph"
    with pytest.raises(ValueError, match="seed_digest does not match"):
        validate_assessments_against_kg_seed_snapshots([domain], [familiarity], tampered)

    duplicated = deepcopy(archive)
    duplicated["snapshots"].append(deepcopy(duplicated["snapshots"][0]))
    with pytest.raises(ValueError, match="Duplicate KG seed snapshot version"):
        validate_assessments_against_kg_seed_snapshots([domain], [familiarity], duplicated)

    assert (domain["kg_id"], domain["seed_version"]) == (
        seed["kg_id"], seed["seed_version"],
    )
    matching_domain = next(
        item for item in seed["review_domains"]
        if item["domain_id"] == domain["review_domain_id"]
    )
    assert domain["review_domain_label"] == matching_domain["label"]
    assert (familiarity["kg_id"], familiarity["seed_version"]) == (
        seed["kg_id"], seed["seed_version"],
    )
    matching_scope = next(
        item for item in seed["familiarity_scopes"]
        if item["scope_id"] == familiarity["familiarity_scope_id"]
    )
    assert familiarity["familiarity_scope_label"] == matching_scope["label"]


def test_longitudinal_assertion_and_assessment_chains_are_integral() -> None:
    first = _json(EXAMPLES / "reviewer-domain-expertise-assertion.synthetic.json")
    second = deepcopy(first)
    second.update(
        id="synthetic-domain-expertise-0002",
        expertise_level="advanced",
        asserted_at="2026-02-01T10:00:00Z",
        supersedes_id=first["id"],
    )
    validate_reviewer_domain_expertise_assertions([first, second])

    dangling = deepcopy(second)
    dangling["supersedes_id"] = "missing-assertion"
    with pytest.raises(ValueError, match="Dangling supersedes_id"):
        validate_reviewer_domain_expertise_assertions([first, dangling])

    non_chronological = deepcopy(second)
    non_chronological["asserted_at"] = first["asserted_at"]
    with pytest.raises(ValueError, match="earlier than its successor"):
        validate_reviewer_domain_expertise_assertions([first, non_chronological])

    branch = deepcopy(second)
    branch.update(id="synthetic-domain-expertise-0003", asserted_at="2026-03-01T10:00:00Z")
    with pytest.raises(ValueError, match="chain branches"):
        validate_reviewer_domain_expertise_assertions([first, second, branch])

    disconnected = deepcopy(second)
    disconnected["supersedes_id"] = None
    with pytest.raises(ValueError, match="exactly one root"):
        validate_reviewer_domain_expertise_assertions([first, disconnected])

    domain_first = _json(EXAMPLES / "reviewer-kg-domain-assessment.synthetic.json")
    domain_second = deepcopy(domain_first)
    domain_second.update(
        id="synthetic-domain-assessment-0002",
        assessed_at="2026-02-02T10:00:00Z",
        assignment_id="synthetic-assignment-0002",
        previous_assessment_id=domain_first["id"],
    )
    validate_kg_domain_assessments([domain_first, domain_second])
    domain_second["review_domain_id"] = "different-domain"
    with pytest.raises(ValueError, match="same subject"):
        validate_kg_domain_assessments([domain_first, domain_second])


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


def test_current_seeds_match_immutable_snapshot_heads() -> None:
    seeds_path = ROOT / "catalog/seeds.yaml"
    payload = yaml.safe_load(seeds_path.read_text(encoding="utf-8-sig"))
    seeds = validate_kg_seeds(payload, location=str(seeds_path))
    snapshots = load_kg_seed_snapshots(ROOT / "catalog/kg_seed_snapshots.yaml")
    validate_current_kg_seed_snapshots(seeds, snapshots)
    validate_current_kg_seed_snapshots(seeds[1:], snapshots)


def test_seed_snapshot_update_is_idempotent_and_rejects_version_reuse() -> None:
    seeds_payload = yaml.safe_load((ROOT / "catalog/seeds.yaml").read_text(encoding="utf-8-sig"))
    snapshots_payload = yaml.safe_load(
        (ROOT / "catalog/kg_seed_snapshots.yaml").read_text(encoding="utf-8-sig")
    )
    unchanged, added = update_snapshot_archive(seeds_payload, snapshots_payload)
    assert added == 0
    validate_kg_seed_snapshots(unchanged)

    next_version = deepcopy(seeds_payload)
    next_version["kgs"][0]["seed_version"] = "2026-08-18.2"
    extended, added = update_snapshot_archive(next_version, snapshots_payload)
    assert added == 1
    validated = validate_kg_seed_snapshots(extended)
    validate_current_kg_seed_snapshots(validate_kg_seeds(next_version), validated)
    organs_history = [item for item in validated if item["kg_id"] == "organs"]
    assert len(organs_history) == 2
    assert organs_history[-1]["previous_seed_digest"] == organs_history[0]["seed_digest"]

    changed = deepcopy(seeds_payload)
    changed["kgs"][0]["review_domains"][0]["description"] += " Changed without a version bump."
    with pytest.raises(ValueError, match="version was reused"):
        update_snapshot_archive(changed, snapshots_payload)


def test_normal_seed_loader_requires_matching_snapshot_archive(tmp_path: Path) -> None:
    seeds_payload = yaml.safe_load(
        (EXAMPLES / "kg-seeds.synthetic.yaml").read_text(encoding="utf-8")
    )
    seeds_path = tmp_path / "seeds.yaml"
    snapshots_path = tmp_path / "alternate-snapshots.yaml"
    seeds_path.write_text(yaml.safe_dump(seeds_payload, sort_keys=False), encoding="utf-8")
    with pytest.raises(FileNotFoundError, match="snapshot archive"):
        load_current_kg_seeds(seeds_path, snapshots_path)

    archive, _ = update_snapshot_archive(seeds_payload, None)
    snapshots_path.write_text(yaml.safe_dump(archive, sort_keys=False), encoding="utf-8")
    load_current_kg_seeds(seeds_path, snapshots_path)

    seeds_payload["kgs"][0]["name"] = "Changed without a version bump"
    seeds_path.write_text(yaml.safe_dump(seeds_payload, sort_keys=False), encoding="utf-8")
    with pytest.raises(ValueError, match="version was reused"):
        load_current_kg_seeds(seeds_path, snapshots_path)


def test_generated_kg_metadata_preserves_graph_and_fallback_targets() -> None:
    seeds = load_hydrated_seeds(ROOT / "catalog/seeds.yaml", ROOT / "catalog/sources.yaml")
    generated = {
        raw["kg_id"]: kgseed_to_record(parse_kg_seed(raw))
        for raw in seeds
    }
    organs = next(seed for seed in seeds if seed["kg_id"] == "organs")
    assert generated["organs"]["sparql"]["graph"] == organs["sparql"]["graph"]
    assert generated["organs"]["sparql"]["fallbacks"] == organs["sparql"]["fallbacks"]

    tracked = {
        record["kg_id"]: record
        for record in (
            json.loads(line)
            for line in (ROOT / "catalog/kgs.jsonl").read_text(encoding="utf-8").splitlines()
            if line.strip()
        )
    }
    for kg_id, generated_record in generated.items():
        assert tracked[kg_id]["sparql"] == generated_record["sparql"]


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


def test_seed_validator_rejects_incomplete_runtime_contract() -> None:
    payload = yaml.safe_load((EXAMPLES / "kg-seeds.synthetic.yaml").read_text(encoding="utf-8"))
    del payload["kgs"][0]["name"]
    assert _json_schema_errors("kg_seeds.schema.json", payload)
    with pytest.raises(ValueError, match="missing fields.*name"):
        validate_kg_seeds(payload)

    payload = yaml.safe_load((EXAMPLES / "kg-seeds.synthetic.yaml").read_text(encoding="utf-8"))
    del payload["kgs"][0]["sparql"]
    with pytest.raises(ValueError, match="either sparql or dataset"):
        validate_kg_seeds(payload)


@pytest.mark.parametrize(
    "targets",
    [
        {"sparql": None},
        {"dataset": None},
        {"sparql": None, "dataset": None},
        {
            "sparql": {"endpoint": "https://example.invalid/sparql", "auth": "none"},
            "dataset": None,
        },
    ],
)
def test_seed_validator_rejects_explicitly_null_targets(targets: dict) -> None:
    payload = yaml.safe_load((EXAMPLES / "kg-seeds.synthetic.yaml").read_text(encoding="utf-8"))
    payload["kgs"][0].pop("sparql", None)
    payload["kgs"][0].pop("dataset", None)
    payload["kgs"][0].update(targets)
    assert _json_schema_errors("kg_seeds.schema.json", payload)
    with pytest.raises(ValueError, match="must be a mapping"):
        validate_kg_seeds(payload)


def test_seed_validator_rejects_unknown_nested_fields() -> None:
    payload = yaml.safe_load((EXAMPLES / "kg-seeds.synthetic.yaml").read_text(encoding="utf-8"))
    payload["kgs"][0]["sparql"]["unsupported"] = True
    with pytest.raises(ValueError, match="unsupported fields"):
        validate_kg_seeds(payload)


def test_versioned_expertise_suggestion_snapshot_validates() -> None:
    path = ROOT / "catalog/expertise_domain_suggestions.yaml"
    payload = load_expertise_domain_suggestions(path)
    assert payload["suggestions"]
    assert all(item["source_id"] == "musparql-owner-reviewed-terms" for item in payload["suggestions"])


def test_expertise_suggestion_loader_enforces_required_metadata(tmp_path: Path) -> None:
    source = ROOT / "catalog/expertise_domain_suggestions.yaml"
    payload = yaml.safe_load(source.read_text(encoding="utf-8"))
    del payload["sources"][0]["title"]
    assert _json_schema_errors("expertise_domain_suggestions.schema.json", payload)
    invalid = tmp_path / "suggestions.yaml"
    invalid.write_text(yaml.safe_dump(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="missing fields.*title"):
        load_expertise_domain_suggestions(invalid)

    payload = yaml.safe_load(source.read_text(encoding="utf-8"))
    payload["created_on"] = "not-a-date"
    assert _json_schema_errors("expertise_domain_suggestions.schema.json", payload)
    invalid.write_text(yaml.safe_dump(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="ISO 8601 date"):
        load_expertise_domain_suggestions(invalid)


def test_expertise_vocabulary_uri_and_version_are_paired(tmp_path: Path) -> None:
    source = ROOT / "catalog/expertise_domain_suggestions.yaml"
    payload = yaml.safe_load(source.read_text(encoding="utf-8"))
    suggestion = deepcopy(payload["suggestions"][0])
    suggestion["vocabulary_version"] = "synthetic-v1"
    payload["suggestions"][0] = suggestion
    assert _json_schema_errors("expertise_domain_suggestions.schema.json", payload)
    invalid = tmp_path / "suggestions.yaml"
    invalid.write_text(yaml.safe_dump(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="URI and version together"):
        load_expertise_domain_suggestions(invalid)
