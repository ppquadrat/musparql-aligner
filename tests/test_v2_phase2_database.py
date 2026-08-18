from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
import json
from pathlib import Path

import pytest
import yaml
from alembic import command
from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext
from sqlalchemy import inspect, select
from sqlalchemy.exc import IntegrityError

from musparql.database import Base, create_database_engine, session_factory
from musparql.database.migrations import alembic_config, current_revision, upgrade_database
from musparql.database.models import (
    AssignmentKgSeed,
    Reviewer,
    ReviewerDomainExpertise,
    ReviewerExperience,
    ReviewAssignment,
)
from musparql.database.services import ProvenanceService, SeedSnapshotService
from scripts.snapshot_kg_seeds import update_snapshot_archive


ROOT = Path(__file__).resolve().parents[1]
EXAMPLES = ROOT / "schemas/examples"


@pytest.fixture
def database(tmp_path: Path):
    database_path = tmp_path / "musparql.sqlite3"
    upgrade_database(database_path)
    engine = create_database_engine(database_path)
    sessions = session_factory(engine)
    try:
        yield database_path, engine, sessions
    finally:
        engine.dispose()


def _reviewer(reviewer_id: str = "reviewer-0042") -> Reviewer:
    return Reviewer(
        id=reviewer_id,
        name="Synthetic Reviewer",
        affiliation="Synthetic Institute",
        email_display=f"{reviewer_id}@example.invalid",
        email_normalized=f"{reviewer_id}@example.invalid",
        status="active",
        created_at="2026-01-01T09:00:00Z",
        updated_at="2026-01-01T09:00:00Z",
        privacy_notice_version="synthetic-v1",
        privacy_notice_acknowledged_at="2026-01-01T09:00:00Z",
    )


def _json(name: str) -> dict:
    return json.loads((EXAMPLES / name).read_text(encoding="utf-8"))


def _seed_archive() -> dict:
    seeds = yaml.safe_load((EXAMPLES / "kg-seeds.synthetic.yaml").read_text(encoding="utf-8"))
    archive, added = update_snapshot_archive(seeds, None)
    assert added == 1
    return archive


def _seed_database(sessions) -> None:
    with sessions.begin() as session:
        session.add(_reviewer())
    assert SeedSnapshotService(sessions).import_archive(_seed_archive()) == 1
    with sessions.begin() as session:
        session.add(
            ReviewAssignment(
                id="synthetic-assignment-0001",
                reviewer_id="reviewer-0042",
                mode="initial",
                status="active",
                bundle_path="synthetic/bundle.json",
                bundle_digest="sha256:" + "a" * 64,
                previous_benchmark_path=None,
                processing_recipe="validate_initial_review",
                holdout_capability=False,
                created_at="2026-01-01T09:30:00Z",
                opened_at=None,
                submitted_at=None,
            )
        )
        session.flush()
        snapshot = _seed_archive()["snapshots"][0]
        session.add(
            AssignmentKgSeed(
                assignment_id="synthetic-assignment-0001",
                kg_id=snapshot["kg_id"],
                seed_version=snapshot["seed_version"],
                seed_digest=snapshot["seed_digest"],
            )
        )


def test_alembic_upgrade_creates_complete_schema_and_sqlite_safety(database) -> None:
    database_path, engine, _sessions = database
    assert current_revision(database_path) == "20260818_01"
    assert database_path.stat().st_mode & 0o777 == 0o600
    tables = set(inspect(engine).get_table_names())
    assert {
        "reviewers", "reviewer_experience", "reviewer_languages", "expertise_domains",
        "reviewer_domain_expertise", "kg_seed_snapshots", "review_assignments",
        "assignment_kg_seeds", "reviewer_kg_domain_assessments",
        "reviewer_resource_familiarity_assessments", "login_codes", "auth_sessions",
        "review_submissions", "processing_jobs",
    } <= tables
    with engine.connect() as connection:
        assert connection.exec_driver_sql("PRAGMA foreign_keys").scalar_one() == 1
        assert connection.exec_driver_sql("PRAGMA journal_mode").scalar_one() == "wal"
        assert connection.exec_driver_sql("PRAGMA synchronous").scalar_one() == 2
        assert connection.exec_driver_sql("PRAGMA integrity_check").scalar_one() == "ok"
        context = MigrationContext.configure(connection)
        assert compare_metadata(context, Base.metadata) == []


def test_alembic_downgrade_and_reupgrade(tmp_path: Path) -> None:
    database_path = tmp_path / "migration-cycle.sqlite3"
    upgrade_database(database_path)
    command.downgrade(alembic_config(database_path), "base")
    assert current_revision(database_path) is None
    upgrade_database(database_path)
    assert current_revision(database_path) == "20260818_01"


def test_seed_archive_import_is_idempotent_and_assignment_digest_is_enforced(database) -> None:
    _path, _engine, sessions = database
    service = SeedSnapshotService(sessions)
    archive = _seed_archive()
    assert service.import_archive(archive) == 1
    assert service.import_archive(archive) == 0
    with sessions.begin() as session:
        session.add(_reviewer())
        session.add(
            ReviewAssignment(
                id="assignment-wrong-digest", reviewer_id="reviewer-0042", mode="initial",
                status="draft", bundle_path="synthetic/bundle.json",
                bundle_digest="sha256:" + "b" * 64, previous_benchmark_path=None,
                processing_recipe="validate_initial_review", holdout_capability=False,
                created_at="2026-01-01T09:30:00Z", opened_at=None, submitted_at=None,
            )
        )
        session.add(
            AssignmentKgSeed(
                assignment_id="assignment-wrong-digest", kg_id="synthetic-kg",
                seed_version="synthetic-seed-v1", seed_digest="sha256:" + "0" * 64,
            )
        )
        with pytest.raises(IntegrityError):
            session.flush()


def test_seed_archive_import_does_not_depend_on_serialized_chain_order(database) -> None:
    _path, _engine, sessions = database
    archive = _seed_archive()
    next_seeds = yaml.safe_load(
        (EXAMPLES / "kg-seeds.synthetic.yaml").read_text(encoding="utf-8")
    )
    next_seeds["kgs"][0]["seed_version"] = "synthetic-seed-v2"
    extended, added = update_snapshot_archive(next_seeds, archive)
    assert added == 1
    extended["snapshots"].reverse()
    assert SeedSnapshotService(sessions).import_archive(extended) == 2


def test_append_only_expertise_requires_linear_chronological_history(database) -> None:
    _path, engine, sessions = database
    with sessions.begin() as session:
        session.add(_reviewer())
    service = ProvenanceService(sessions)
    root = _json("reviewer-domain-expertise-assertion.synthetic.json")
    service.append_domain_expertise(root)
    successor = deepcopy(root)
    successor.update(
        id="synthetic-domain-expertise-0002",
        asserted_at="2026-02-01T10:00:00Z",
        expertise_level="advanced",
        supersedes_id=root["id"],
    )
    service.append_domain_expertise(successor)

    stale = deepcopy(successor)
    stale.update(
        id="synthetic-domain-expertise-0003",
        asserted_at="2025-12-01T10:00:00Z",
        supersedes_id=successor["id"],
    )
    with pytest.raises(ValueError, match="earlier"):
        service.append_domain_expertise(stale)

    branch = deepcopy(successor)
    branch.update(id="synthetic-domain-expertise-0004", asserted_at="2026-03-01T10:00:00Z")
    with pytest.raises(ValueError, match="current head"):
        service.append_domain_expertise(branch)

    with sessions.begin() as session:
        stored = session.get(ReviewerDomainExpertise, successor["id"])
        assert stored is not None
        stored.expertise_level = "expert"
        with pytest.raises(IntegrityError, match="append-only"):
            session.flush()
    with engine.connect() as connection:
        count = connection.execute(select(ReviewerDomainExpertise)).all()
        assert len(count) == 2


def test_assessments_resolve_frozen_prompt_and_assignment_reviewer(database) -> None:
    _path, _engine, sessions = database
    _seed_database(sessions)
    service = ProvenanceService(sessions)
    domain = _json("reviewer-kg-domain-assessment.synthetic.json")
    familiarity = _json("reviewer-resource-familiarity-assessment.synthetic.json")
    service.append_domain_assessment(domain)
    service.append_familiarity_assessment(familiarity)

    wrong_label = deepcopy(domain)
    wrong_label.update(
        id="synthetic-domain-assessment-0002",
        assessed_at="2026-02-02T10:00:00Z",
        previous_assessment_id=domain["id"],
        review_domain_label="Changed prompt",
    )
    with pytest.raises(IntegrityError):
        service.append_domain_assessment(wrong_label)


def test_failed_multirow_transaction_is_atomic(database) -> None:
    _path, _engine, sessions = database
    with pytest.raises(IntegrityError):
        with sessions.begin() as session:
            session.add(_reviewer())
            session.add(
                ReviewerExperience(
                    reviewer_id="reviewer-0042",
                    kg_ontology_experience="invented",
                    sparql_experience="none",
                    nlp_llm_experience="none",
                    assessed_at="2026-01-01T09:00:00Z",
                )
            )
    with sessions() as session:
        assert session.get(Reviewer, "reviewer-0042") is None


def test_ten_concurrent_short_writes_complete(database) -> None:
    _path, _engine, sessions = database

    def insert(index: int) -> str:
        reviewer_id = f"reviewer-{1000 + index}"
        with sessions.begin() as session:
            session.add(_reviewer(reviewer_id))
        return reviewer_id

    with ThreadPoolExecutor(max_workers=10) as executor:
        inserted = list(executor.map(insert, range(10)))
    with sessions() as session:
        found = set(session.scalars(select(Reviewer.id)).all())
    assert found == set(inserted)


def test_schema_cli_diagnostics_do_not_print_profile_fields(tmp_path: Path, capsys) -> None:
    from musparql.database.cli import main

    database_path = tmp_path / "diagnostic.sqlite3"
    assert main(["upgrade", "--database", str(database_path)]) == 0
    output = capsys.readouterr().out
    assert output == "Database schema upgraded to 20260818_01.\n"
    assert "Synthetic Reviewer" not in output
    assert "@example.invalid" not in output
    engine = create_database_engine(database_path)
    sessions = session_factory(engine)
    with sessions.begin() as session:
        session.add(_reviewer())
    engine.dispose()
    assert main(["check", "--database", str(database_path)]) == 0
    output = capsys.readouterr().out
    assert "Reviewer IDs: reviewer-0042." in output
    assert "Synthetic Reviewer" not in output
    assert "@example.invalid" not in output
