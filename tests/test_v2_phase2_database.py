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
    ExpertiseDomain,
    Reviewer,
    ReviewerDomainExpertise,
    ReviewerExperience,
    ReviewerKgDomainAssessment,
    ReviewerResourceFamiliarityAssessment,
    ReviewAssignment,
    ReviewGroup,
    ReviewGroupMember,
    ReviewSubmission,
    WorkshopEntryCode,
    WorkshopEntryRedemption,
    WorkshopRound,
    WorkshopWorkPackage,
)
from musparql.database.services import (
    ProvenanceService,
    SeedSnapshotService,
    WorkshopAdmissionError,
    WorkshopAdmissionService,
)
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
        registration_method="email_invitation",
        email_verified_at="2026-01-01T09:00:00Z",
        consent_statement_version=None,
        consented_at=None,
    )


def _workshop_reviewer(index: int) -> Reviewer:
    reviewer_id = f"reviewer-{index:04d}"
    return Reviewer(
        id=reviewer_id,
        name=f"Synthetic Workshop Reviewer {index}",
        affiliation="",
        email_display=f"workshop-{index}@example.invalid",
        email_normalized=f"workshop-{index}@example.invalid",
        status="invited",
        created_at="2026-09-10T10:00:00Z",
        updated_at="2026-09-10T10:00:00Z",
        privacy_notice_version=None,
        privacy_notice_acknowledged_at=None,
        registration_method="workshop_code",
        email_verified_at=None,
        consent_statement_version=None,
        consented_at=None,
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
    assert current_revision(database_path) == "20260911_08"
    assert database_path.stat().st_mode & 0o777 == 0o600
    tables = set(inspect(engine).get_table_names())
    assert {
        "reviewers", "reviewer_experience", "reviewer_languages", "expertise_domains",
        "reviewer_domain_expertise", "kg_seed_snapshots", "review_assignments",
        "assignment_kg_seeds", "reviewer_kg_domain_assessments",
        "reviewer_resource_familiarity_assessments", "login_codes", "auth_sessions",
        "review_submissions", "processing_jobs",
        "workshop_rounds", "workshop_entry_codes",
        "workshop_entry_redemptions", "review_groups",
        "review_group_members", "workshop_work_packages",
        "workshop_session_resets",
    } <= tables
    with engine.connect() as connection:
        assert connection.exec_driver_sql("PRAGMA foreign_keys").scalar_one() == 1
        assert connection.exec_driver_sql("PRAGMA journal_mode").scalar_one() == "wal"
        assert connection.exec_driver_sql("PRAGMA synchronous").scalar_one() == 2
        assert connection.exec_driver_sql("PRAGMA integrity_check").scalar_one() == "ok"
        context = MigrationContext.configure(connection)
        assert compare_metadata(context, Base.metadata) == []


def test_workshop_groups_allow_one_or_more_members_and_cross_group_help(database) -> None:
    _path, _engine, sessions = database
    with sessions.begin() as session:
        session.add_all([_reviewer(), _reviewer("reviewer-0043")])
        session.add(
            WorkshopRound(
                id="workshop-synthetic",
                name="Synthetic workshop",
                status="open",
                opens_at="2026-09-10T09:00:00Z",
                closes_at="2026-09-10T17:00:00Z",
                max_participants=20,
                allow_additional_assignments=False,
                created_at="2026-09-10T08:00:00Z",
            )
        )
        session.flush()
        session.add_all(
            [
                ReviewGroup(
                    id="group-synthetic-1",
                    workshop_round_id="workshop-synthetic",
                    join_code_digest="digest-one",
                    created_at="2026-09-10T10:00:00Z",
                ),
                ReviewGroup(
                    id="group-synthetic-2",
                    workshop_round_id="workshop-synthetic",
                    join_code_digest="digest-two",
                    created_at="2026-09-10T10:01:00Z",
                ),
            ]
        )
        session.flush()
        session.add_all(
            [
                ReviewGroupMember(
                    group_id="group-synthetic-1",
                    reviewer_id="reviewer-0042",
                    joined_at="2026-09-10T10:00:00Z",
                ),
                ReviewGroupMember(
                    group_id="group-synthetic-2",
                    reviewer_id="reviewer-0042",
                    joined_at="2026-09-10T10:02:00Z",
                ),
                ReviewGroupMember(
                    group_id="group-synthetic-2",
                    reviewer_id="reviewer-0043",
                    joined_at="2026-09-10T10:03:00Z",
                ),
            ]
        )

    with sessions() as session:
        assert len(session.scalars(select(ReviewGroupMember)).all()) == 3
        assert session.get(Reviewer, "reviewer-0042").registration_method == "email_invitation"


def test_workshop_allows_only_one_unrevoked_shared_code_per_round(database) -> None:
    _path, _engine, sessions = database
    with sessions.begin() as session:
        session.add(
            WorkshopRound(
                id="workshop-code-test",
                name="Synthetic workshop",
                status="open",
                opens_at="2026-09-10T09:00:00Z",
                closes_at="2026-09-10T17:00:00Z",
                max_participants=20,
                allow_additional_assignments=False,
                created_at="2026-09-10T08:00:00Z",
            )
        )
        session.flush()
        session.add(
            WorkshopEntryCode(
                id="entry-code-1",
                workshop_round_id="workshop-code-test",
                code_digest="digest-one",
                expires_at="2026-09-10T12:00:00Z",
                max_redemptions=20,
                redemption_count=0,
                revoked_at=None,
                created_at="2026-09-10T08:00:00Z",
            )
        )

    with pytest.raises(IntegrityError):
        with sessions.begin() as session:
            session.add(
                WorkshopEntryCode(
                    id="entry-code-2",
                    workshop_round_id="workshop-code-test",
                    code_digest="digest-two",
                    expires_at="2026-09-10T13:00:00Z",
                    max_redemptions=20,
                    redemption_count=0,
                    revoked_at=None,
                    created_at="2026-09-10T08:01:00Z",
                )
            )


def test_entry_code_configuration_cannot_exceed_round_capacity(database) -> None:
    _path, _engine, sessions = database
    with sessions.begin() as session:
        session.add(
            WorkshopRound(
                id="workshop-code-cap",
                name="Synthetic workshop",
                status="open",
                opens_at="2026-09-10T09:00:00Z",
                closes_at="2026-09-10T17:00:00Z",
                max_participants=2,
                allow_additional_assignments=False,
                created_at="2026-09-10T08:00:00Z",
            )
        )
    with pytest.raises(IntegrityError, match="entry code exceeds round capacity"):
        with sessions.begin() as session:
            session.add(
                WorkshopEntryCode(
                    id="entry-code-over-cap",
                    workshop_round_id="workshop-code-cap",
                    code_digest="digest-over-cap",
                    expires_at="2026-09-10T12:00:00Z",
                    max_redemptions=3,
                    redemption_count=0,
                    revoked_at=None,
                    created_at="2026-09-10T08:00:00Z",
                )
            )


def test_reissued_code_admission_serializes_against_round_capacity(database) -> None:
    _path, _engine, sessions = database
    with sessions.begin() as session:
        session.add(
            WorkshopRound(
                id="workshop-reissue-cap",
                name="Synthetic workshop",
                status="open",
                opens_at="2026-09-10T09:00:00Z",
                closes_at="2026-09-10T17:00:00Z",
                max_participants=2,
                allow_additional_assignments=False,
                created_at="2026-09-10T08:00:00Z",
            )
        )
        session.flush()
        session.add(
            WorkshopEntryCode(
                id="entry-code-original",
                workshop_round_id="workshop-reissue-cap",
                code_digest="digest-original",
                expires_at="2026-09-10T16:00:00Z",
                max_redemptions=2,
                redemption_count=0,
                revoked_at=None,
                created_at="2026-09-10T08:00:00Z",
            )
        )

    admission = WorkshopAdmissionService(sessions)
    admission.redeem(
        entry_code_id="entry-code-original",
        presented_code_digest="digest-original",
        reviewer=_workshop_reviewer(101),
        redeemed_at="2026-09-10T10:00:00Z",
    )
    with sessions.begin() as session:
        session.get(WorkshopEntryCode, "entry-code-original").revoked_at = (
            "2026-09-10T10:01:00Z"
        )
        session.add(
            WorkshopEntryCode(
                id="entry-code-reissued",
                workshop_round_id="workshop-reissue-cap",
                code_digest="digest-reissued",
                expires_at="2026-09-10T16:00:00Z",
                max_redemptions=2,
                redemption_count=0,
                revoked_at=None,
                created_at="2026-09-10T10:01:00Z",
            )
        )

    def redeem(index: int) -> bool:
        try:
            admission.redeem(
                entry_code_id="entry-code-reissued",
                presented_code_digest="digest-reissued",
                reviewer=_workshop_reviewer(index),
                redeemed_at="2026-09-10T10:02:00Z",
            )
            return True
        except WorkshopAdmissionError:
            return False

    with ThreadPoolExecutor(max_workers=2) as executor:
        admitted = list(executor.map(redeem, (102, 103)))

    assert sorted(admitted) == [False, True]
    with sessions() as session:
        assert session.scalar(select(WorkshopEntryCode.redemption_count).where(
            WorkshopEntryCode.id == "entry-code-original"
        )) == 1
        assert session.scalar(select(WorkshopEntryCode.redemption_count).where(
            WorkshopEntryCode.id == "entry-code-reissued"
        )) == 1
        assert len(session.scalars(select(WorkshopEntryRedemption)).all()) == 2


def test_reviewer_registration_and_consent_provenance_is_consistent(database) -> None:
    _path, _engine, sessions = database
    invalid_method = _reviewer()
    invalid_method.registration_method = "fallback"
    with pytest.raises(IntegrityError, match="ck_reviewers_registration_method"):
        with sessions.begin() as session:
            session.add(invalid_method)

    incomplete_consent = _reviewer("reviewer-0043")
    incomplete_consent.consent_statement_version = "synthetic-consent-v1"
    with pytest.raises(IntegrityError, match="ck_reviewers_consent_pair"):
        with sessions.begin() as session:
            session.add(incomplete_consent)


def test_workshop_package_is_bound_to_a_frozen_kg_seed(database) -> None:
    _path, _engine, sessions = database
    archive = _seed_archive()
    assert SeedSnapshotService(sessions).import_archive(archive) == 1
    snapshot = archive["snapshots"][0]
    with sessions.begin() as session:
        session.add(
            WorkshopRound(
                id="workshop-package-test",
                name="Synthetic workshop",
                status="draft",
                opens_at="2026-09-10T09:00:00Z",
                closes_at="2026-09-10T17:00:00Z",
                max_participants=20,
                allow_additional_assignments=False,
                created_at="2026-09-10T08:00:00Z",
            )
        )
        session.flush()
        session.add(
            WorkshopWorkPackage(
                id="package-synthetic",
                workshop_round_id="workshop-package-test",
                kg_id=snapshot["kg_id"],
                seed_version=snapshot["seed_version"],
                seed_digest=snapshot["seed_digest"],
                display_name="Synthetic KG",
                short_description="Synthetic package",
                display_order=1,
                bundle_path="synthetic/bundle.js",
                bundle_digest="sha256:" + "a" * 64,
                processing_recipe="validate_initial_review",
                enabled=True,
                created_at="2026-09-10T08:00:00Z",
            )
        )

    with sessions() as session:
        package = session.get(WorkshopWorkPackage, "package-synthetic")
        assert package is not None
        assert package.seed_digest == snapshot["seed_digest"]


def test_group_assignment_and_submission_freeze_member_provenance(database) -> None:
    _path, _engine, sessions = database
    archive = _seed_archive()
    assert SeedSnapshotService(sessions).import_archive(archive) == 1
    snapshot = archive["snapshots"][0]
    with sessions.begin() as session:
        session.add_all(
            [_workshop_reviewer(201), _workshop_reviewer(202), _workshop_reviewer(203)]
        )
        session.add(
            WorkshopRound(
                id="workshop-group-provenance",
                name="Synthetic workshop",
                status="open",
                opens_at="2026-09-10T09:00:00Z",
                closes_at="2026-09-10T17:00:00Z",
                max_participants=3,
                allow_additional_assignments=False,
                created_at="2026-09-10T08:00:00Z",
            )
        )
        session.flush()
        session.add(
            ReviewGroup(
                id="group-provenance",
                workshop_round_id="workshop-group-provenance",
                join_code_digest="group-provenance-digest",
                created_at="2026-09-10T10:00:00Z",
            )
        )
        session.add(
            WorkshopWorkPackage(
                id="package-provenance",
                workshop_round_id="workshop-group-provenance",
                kg_id=snapshot["kg_id"],
                seed_version=snapshot["seed_version"],
                seed_digest=snapshot["seed_digest"],
                display_name="Synthetic KG",
                short_description="Synthetic package",
                display_order=1,
                bundle_path="synthetic/bundle.js",
                bundle_digest="sha256:" + "b" * 64,
                processing_recipe="validate_initial_review",
                enabled=True,
                created_at="2026-09-10T10:00:00Z",
            )
        )
        session.flush()
        session.add_all(
            [
                ReviewGroupMember(
                    group_id="group-provenance",
                    reviewer_id="reviewer-0201",
                    joined_at="2026-09-10T10:00:00Z",
                ),
                ReviewGroupMember(
                    group_id="group-provenance",
                    reviewer_id="reviewer-0202",
                    joined_at="2026-09-10T10:05:00Z",
                ),
            ]
        )
        session.add(
            ReviewAssignment(
                id="group-assignment-provenance",
                reviewer_id=None,
                review_group_id="group-provenance",
                work_package_id="package-provenance",
                participant_status="active",
                claimed_at="2026-09-10T10:01:00Z",
                completed_at=None,
                completion_item_count=None,
                completion_total_count=None,
                closed_contributor_ids=None,
                mode="initial",
                status="active",
                bundle_path="synthetic/bundle.js",
                bundle_digest="sha256:" + "b" * 64,
                previous_benchmark_path=None,
                processing_recipe="validate_initial_review",
                holdout_capability=False,
                created_at="2026-09-10T10:01:00Z",
                opened_at="2026-09-10T10:01:00Z",
                submitted_at=None,
            )
        )
        session.flush()
        session.add(
            AssignmentKgSeed(
                assignment_id="group-assignment-provenance",
                kg_id=snapshot["kg_id"],
                seed_version=snapshot["seed_version"],
                seed_digest=snapshot["seed_digest"],
            )
        )

    def submission(submission_id: str, contributors: list[str]) -> ReviewSubmission:
        return ReviewSubmission(
            id=submission_id,
            assignment_id="group-assignment-provenance",
            reviewer_id=None,
            review_group_id="group-provenance",
            submitted_by_reviewer_id="reviewer-0202",
            contributor_reviewer_ids=contributors,
            export_path="synthetic/submission.json",
            export_digest="sha256:" + submission_id[-1] * 64,
            submitted_at="2026-09-10T11:00:00Z",
            revision=1,
            validation_status="schema_valid",
            inclusion_status="pending",
        )

    with pytest.raises(IntegrityError, match="submission contributors must be group members"):
        with sessions.begin() as session:
            session.add(
                submission(
                    "group-submission-invalid-3",
                    ["reviewer-0201", "reviewer-0202", "reviewer-0203"],
                )
            )
    with sessions.begin() as session:
        session.add(
            submission(
                "group-submission-valid-4", ["reviewer-0201", "reviewer-0202"]
            )
        )
    with sessions() as session:
        stored = session.get(ReviewSubmission, "group-submission-valid-4")
        assert stored is not None
        assert stored.review_group_id == "group-provenance"
        assert stored.submitted_by_reviewer_id == "reviewer-0202"
        assert stored.contributor_reviewer_ids == ["reviewer-0201", "reviewer-0202"]

    domain = _json("reviewer-kg-domain-assessment.synthetic.json")
    familiarity = _json("reviewer-resource-familiarity-assessment.synthetic.json")
    for record in (domain, familiarity):
        record["reviewer_id"] = "reviewer-0202"
        record["assignment_id"] = "group-assignment-provenance"
        record["id"] = record["id"].replace("0001", "0202")
    ProvenanceService(sessions).append_pre_review_assessments([domain], [familiarity])

    nonmember_domain = deepcopy(domain)
    nonmember_familiarity = deepcopy(familiarity)
    for record in (nonmember_domain, nonmember_familiarity):
        record["reviewer_id"] = "reviewer-0203"
        record["id"] = record["id"].replace("0202", "0203")
    with pytest.raises(ValueError, match="assigned group member"):
        ProvenanceService(sessions).append_pre_review_assessments(
            [nonmember_domain], [nonmember_familiarity]
        )
    with sessions.begin() as session:
        assignment = session.get(ReviewAssignment, "group-assignment-provenance")
        assignment.participant_status = "completed"
        assignment.completed_at = "2026-09-10T11:00:00Z"
        assignment.closed_contributor_ids = ["reviewer-0201", "reviewer-0202"]
    with pytest.raises(IntegrityError, match="closed contributors are immutable"):
        with sessions.begin() as session:
            assignment = session.get(ReviewAssignment, "group-assignment-provenance")
            assignment.closed_contributor_ids = ["reviewer-0201"]


def test_alembic_downgrade_and_reupgrade(tmp_path: Path) -> None:
    database_path = tmp_path / "migration-cycle.sqlite3"
    upgrade_database(database_path)
    command.downgrade(alembic_config(database_path), "base")
    assert current_revision(database_path) is None
    upgrade_database(database_path)
    assert current_revision(database_path) == "20260911_08"


def test_migration_marks_preexisting_active_email_accounts_verified(tmp_path: Path) -> None:
    database_path = tmp_path / "active-account-migration.sqlite3"
    command.upgrade(alembic_config(database_path), "20260820_06")
    engine = create_database_engine(database_path)
    with engine.begin() as connection:
        connection.exec_driver_sql(
            "INSERT INTO reviewers ("
            "id, name, affiliation, email_display, email_normalized, status, "
            "disabled_from_status, created_at, updated_at, privacy_notice_version, "
            "privacy_notice_acknowledged_at"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "reviewer-0099",
                "Synthetic Migrated Reviewer",
                "",
                "migrated@example.invalid",
                "migrated@example.invalid",
                "active",
                None,
                "2026-08-01T09:00:00Z",
                "2026-08-02T10:00:00Z",
                None,
                None,
            ),
        )
    engine.dispose()

    upgrade_database(database_path)
    engine = create_database_engine(database_path)
    sessions = session_factory(engine)
    try:
        with sessions() as session:
            migrated = session.get(Reviewer, "reviewer-0099")
            assert migrated is not None
            assert migrated.registration_method == "email_invitation"
            assert migrated.email_verified_at == "2026-08-02T10:00:00Z"
    finally:
        engine.dispose()


def test_database_path_with_url_delimiters_is_not_reparsed(tmp_path: Path) -> None:
    database_path = tmp_path / "musparql?review%2.sqlite3"
    upgrade_database(database_path)
    assert database_path.is_file()
    assert not (tmp_path / "musparql").exists()
    assert current_revision(database_path) == "20260911_08"
    engine = create_database_engine(database_path)
    try:
        assert set(inspect(engine).get_table_names()) >= {"reviewers", "review_assignments"}
    finally:
        engine.dispose()


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

    with sessions.begin() as session:
        domain = session.get(ExpertiseDomain, root["domain_id"])
        assert domain is not None
        domain.normalized_label = "rewritten-history"
        with pytest.raises(IntegrityError, match="referenced expertise domain is immutable"):
            session.flush()


def test_assessments_resolve_frozen_prompt_and_assignment_reviewer(database) -> None:
    _path, _engine, sessions = database
    _seed_database(sessions)
    service = ProvenanceService(sessions)
    domain = _json("reviewer-kg-domain-assessment.synthetic.json")
    familiarity = _json("reviewer-resource-familiarity-assessment.synthetic.json")
    service.append_pre_review_assessments([domain], [familiarity])

    wrong_label = deepcopy(domain)
    wrong_label.update(
        id="synthetic-domain-assessment-0002",
        assessed_at="2026-02-02T10:00:00Z",
        previous_assessment_id=domain["id"],
        review_domain_label="Changed prompt",
        context="profile",
        assignment_id=None,
    )
    with pytest.raises(IntegrityError):
        service.append_domain_assessment(wrong_label)


def test_pre_review_assessment_set_is_complete_and_atomic(database) -> None:
    _path, _engine, sessions = database
    _seed_database(sessions)
    service = ProvenanceService(sessions)
    domain = _json("reviewer-kg-domain-assessment.synthetic.json")
    familiarity = _json("reviewer-resource-familiarity-assessment.synthetic.json")
    invalid_familiarity = deepcopy(familiarity)
    invalid_familiarity["familiarity_level"] = "invented"

    with pytest.raises(ValueError, match="atomic batch service"):
        service.append_domain_assessment(domain)
    with pytest.raises(ValueError, match="Unsupported familiarity_level"):
        service.append_pre_review_assessments([domain], [invalid_familiarity])
    with sessions() as session:
        assert session.get(ReviewerKgDomainAssessment, domain["id"]) is None
        assert session.get(ReviewerResourceFamiliarityAssessment, familiarity["id"]) is None

    with pytest.raises(ValueError, match="familiarity assessments do not match"):
        service.append_pre_review_assessments([domain], [])
    service.append_pre_review_assessments([domain], [familiarity])
    with sessions() as session:
        assert session.get(ReviewerKgDomainAssessment, domain["id"]) is not None
        assert session.get(ReviewerResourceFamiliarityAssessment, familiarity["id"]) is not None


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


def test_assignment_modes_accept_only_matching_supported_recipes(database) -> None:
    _path, _engine, sessions = database
    with sessions.begin() as session:
        session.add(_reviewer())

    def assignment(assignment_id: str, mode: str, recipe: str) -> ReviewAssignment:
        return ReviewAssignment(
            id=assignment_id,
            reviewer_id="reviewer-0042",
            mode=mode,
            status="draft",
            bundle_path="synthetic/bundle.json",
            bundle_digest="sha256:" + "a" * 64,
            previous_benchmark_path=None,
            processing_recipe=recipe,
            holdout_capability=False,
            created_at="2026-01-01T09:30:00Z",
            opened_at=None,
            submitted_at=None,
        )

    with pytest.raises(IntegrityError, match="ck_assignment_mode_recipe"):
        with sessions.begin() as session:
            session.add(
                assignment(
                    "synthetic-mismatched-recipe",
                    "compare",
                    "validate_initial_review",
                )
            )
    with pytest.raises(IntegrityError, match="ck_assignment_mode"):
        with sessions.begin() as session:
            session.add(
                assignment(
                    "synthetic-unsupported-mode",
                    "sparql_correction",
                    "validate_initial_review",
                )
            )
    with sessions.begin() as session:
        session.add(
            assignment(
                "synthetic-comparative-assignment",
                "compare",
                "validate_comparative_review",
            )
        )


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
    assert output == "Database schema upgraded to 20260911_08.\n"
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
