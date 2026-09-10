from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
import hashlib
import json
from pathlib import Path
import threading

import pytest
from sqlalchemy import func, select

from musparql.database import create_database_engine, session_factory
from musparql.database.migrations import upgrade_database
from musparql.database.models import (
    AssignmentKgSeed,
    ExpertiseDomain,
    KgSeedFamiliarityScope,
    KgSeedReviewDomain,
    KgSeedSnapshot,
    Reviewer,
    ReviewerDomainExpertise,
    ReviewerExperience,
    ReviewerLanguage,
    ReviewAssignment,
    ReviewGroupMember,
    WorkshopRound,
    WorkshopWorkPackage,
)
from musparql.web import create_app
from musparql.web.auth import timestamp, utc_now
from musparql.web.email import SyntheticEmailSender
from musparql.web.workshops import WorkshopAccessError


ROOT = Path(__file__).resolve().parents[1]
SECRET = "synthetic-ipl-group-secret-at-least-32-bytes"
OWNER_ID = "reviewer-0001"
FIRST_ID = "reviewer-0042"
SECOND_ID = "reviewer-0043"
THIRD_ID = "reviewer-0044"


def _reviewer(
    reviewer_id: str, email: str, *, consented: bool = True
) -> Reviewer:
    now = timestamp(utc_now())
    return Reviewer(
        id=reviewer_id,
        name=f"Synthetic {reviewer_id}",
        affiliation="Synthetic Institute",
        email_display=email,
        email_normalized=email,
        status="active",
        created_at=now,
        updated_at=now,
        privacy_notice_version="synthetic-ipl-v1",
        privacy_notice_acknowledged_at=now,
        registration_method="email_invitation",
        email_verified_at=now,
        consent_statement_version="synthetic-consent-v1" if consented else None,
        consented_at=now if consented else None,
    )


def _complete_profile(session, reviewer_id: str) -> None:
    now = timestamp(utc_now())
    session.add(
        ReviewerExperience(
            reviewer_id=reviewer_id,
            kg_ontology_experience="regular",
            sparql_experience="regular",
            nlp_llm_experience="regular",
            assessed_at=now,
        )
    )
    session.add(
        ReviewerLanguage(
            reviewer_id=reviewer_id,
            language_tag="en",
            level="native",
            first_asserted_at=now,
            updated_at=now,
        )
    )
    domain_id = f"synthetic-domain-{reviewer_id}"
    session.add(
        ExpertiseDomain(
            id=domain_id,
            entered_label="Synthetic workshop expertise",
            normalized_label=f"synthetic workshop expertise {reviewer_id}",
            vocabulary_name=None,
            vocabulary_concept_uri=None,
            vocabulary_version=None,
            created_by="reviewer",
        )
    )
    session.add(
        ReviewerDomainExpertise(
            id=f"synthetic-assertion-{reviewer_id}",
            reviewer_id=reviewer_id,
            domain_id=domain_id,
            expertise_level="working",
            asserted_at=now,
            supersedes_id=None,
        )
    )


def _write_bundle(path: Path) -> str:
    payload = {
        "schema": "musparql.review-bundle.v2",
        "mode": "initial",
        "dataset_id": "synthetic-ipl-package",
        "built_at": "2026-09-11T09:00:00Z",
        "holdout_input_policy": "no_holdout",
        "record_count": 1,
        "records": [
            {
                "review_id": "synthetic-kg::synthetic-query::one",
                "kg_id": "synthetic-kg",
            }
        ],
    }
    raw = (json.dumps(payload, sort_keys=True) + "\n").encode()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(raw)
    return "sha256:" + hashlib.sha256(raw).hexdigest()


@pytest.fixture
def workshop_app(tmp_path: Path):
    database_path = tmp_path / "workshop.sqlite3"
    bundle_root = tmp_path / "bundles"
    bundle_digest = _write_bundle(bundle_root / "synthetic-package.json")
    upgrade_database(database_path)
    engine = create_database_engine(database_path)
    sessions = session_factory(engine)
    current = utc_now()
    now = timestamp(current)
    with sessions.begin() as session:
        session.add(_reviewer(OWNER_ID, "owner@example.invalid", consented=False))
        for reviewer_id, email in (
            (FIRST_ID, "first@example.invalid"),
            (SECOND_ID, "second@example.invalid"),
            (THIRD_ID, "third@example.invalid"),
        ):
            session.add(_reviewer(reviewer_id, email))
            _complete_profile(session, reviewer_id)
        session.add(
            WorkshopRound(
                id="workshop-ipl",
                name="Synthetic IPL workshop",
                status="open",
                opens_at=timestamp(current - timedelta(hours=1)),
                closes_at=timestamp(current + timedelta(hours=1)),
                max_participants=30,
                allow_additional_assignments=False,
                created_at=now,
            )
        )
        session.add(
            KgSeedSnapshot(
                kg_id="synthetic-kg",
                seed_version="synthetic-seed-v1",
                seed_digest="sha256:" + "a" * 64,
                previous_seed_digest=None,
                seed_json={"name": "Synthetic graph"},
            )
        )
        session.flush()
        session.add(
            KgSeedReviewDomain(
                kg_id="synthetic-kg",
                seed_version="synthetic-seed-v1",
                domain_id="synthetic-review-domain",
                label="Synthetic subject expertise",
                description="Synthetic prompt for a workshop test.",
            )
        )
        session.add(
            KgSeedFamiliarityScope(
                kg_id="synthetic-kg",
                seed_version="synthetic-seed-v1",
                scope_id="synthetic-resource",
                label="Synthetic resource familiarity",
                description="Synthetic familiarity prompt.",
            )
        )
        session.add(
            WorkshopWorkPackage(
                id="package-synthetic",
                workshop_round_id="workshop-ipl",
                kg_id="synthetic-kg",
                seed_version="synthetic-seed-v1",
                seed_digest="sha256:" + "a" * 64,
                display_name="Synthetic Knowledge Graph",
                short_description="A fictional package for tests.",
                display_order=1,
                bundle_path="synthetic-package.json",
                bundle_digest=bundle_digest,
                processing_recipe="validate_initial_review",
                enabled=True,
                created_at=now,
            )
        )
    engine.dispose()

    sender = SyntheticEmailSender()
    app = create_app(
        {
            "TESTING": True,
            "DATABASE_PATH": database_path,
            "APP_SECRET": SECRET,
            "OWNER_REVIEWER_ID": OWNER_ID,
            "COOKIE_SECURE": False,
            "EMAIL_SENDER": sender,
            "EXPERTISE_SUGGESTIONS_PATH": ROOT
            / "catalog/expertise_domain_suggestions.yaml",
            "ASSIGNMENT_BUNDLE_ROOT": bundle_root,
            "SUBMISSION_ROOT": tmp_path / "submissions",
            "CANDIDATE_ROOT": tmp_path / "candidates",
            "PRIVACY_NOTICE_VERSION": "synthetic-ipl-v1",
            "PRIVACY_NOTICE_BODY": "Synthetic notice. Do not enter real data.",
        }
    )
    yield app, sender, database_path, bundle_root
    app.extensions["musparql_email_dispatcher"].shutdown()
    app.extensions["musparql_engine"].dispose()


def _csrf(client) -> str:
    cookie = client.get_cookie("musparql_csrf", path="/")
    if cookie is None:
        client.get("/")
        cookie = client.get_cookie("musparql_csrf", path="/")
    assert cookie is not None
    return cookie.value


def _login(client, app, sender, email: str) -> None:
    position = sender.position()
    assert client.post(
        "/auth/login", data={"csrf_token": _csrf(client), "email": email}
    ).status_code == 302
    message = sender.wait_for("login_code", email, after_index=position)
    app.extensions["musparql_email_dispatcher"].wait_for_idle()
    assert client.post(
        "/auth/verify", data={"csrf_token": _csrf(client), "code": message.value}
    ).status_code == 302


def _assessment_form(client) -> dict[str, str]:
    return {
        "csrf_token": _csrf(client),
        "domain_level": "advanced",
        "familiarity_level": "worked",
        "confirmed": "yes",
    }


def test_group_journey_is_isolated_and_opens_after_initial_members_assess(
    workshop_app,
) -> None:
    app, sender, database_path, _bundle_root = workshop_app
    first = app.test_client()
    second = app.test_client()
    third = app.test_client()
    _login(first, app, sender, "first@example.invalid")
    _login(second, app, sender, "second@example.invalid")
    _login(third, app, sender, "third@example.invalid")

    assert b"Open IPL workshop" in first.get("/").data
    created = first.post(
        "/workshop/groups", data={"csrf_token": _csrf(first)}
    )
    assert created.status_code == 302
    dashboard = app.extensions["musparql_workshops"].dashboard(FIRST_ID)
    assert len(dashboard.groups) == 1
    group = dashboard.groups[0]
    assert group.member_count == 1
    assert group.join_code.encode() not in created.data

    joined = second.post(
        "/workshop/groups/join",
        data={
            "csrf_token": _csrf(second),
            "join_code": (
                group.join_code[:5].lower() + " " + group.join_code[5:].lower()
            ),
        },
    )
    assert joined.status_code == 302
    assert app.extensions["musparql_workshops"].dashboard(
        FIRST_ID
    ).groups[0].member_count == 2

    denied = third.post(
        f"/workshop/groups/{group.id}/packages/package-synthetic/claim",
        data={"csrf_token": _csrf(third)},
    )
    assert denied.status_code == 302
    assert "error=claim-unavailable" in denied.location

    claimed = first.post(
        f"/workshop/groups/{group.id}/packages/package-synthetic/claim",
        data={"csrf_token": _csrf(first)},
    )
    assert claimed.status_code == 302
    assert "/assignments/assignment-" in claimed.location
    assignment_id = claimed.location.rsplit("/", 1)[-1]

    assert first.get(f"/assignments/{assignment_id}").status_code == 200
    assert second.get(f"/assignments/{assignment_id}").status_code == 200
    assert third.get(f"/assignments/{assignment_id}").status_code == 404
    assert first.get(f"/assignments/{assignment_id}/bundle").status_code == 403

    assert first.post(
        f"/assignments/{assignment_id}", data=_assessment_form(first)
    ).status_code == 302
    waiting = first.get(f"/assignments/{assignment_id}")
    assert b"when every current group member" in waiting.data
    assert first.get(f"/assignments/{assignment_id}/bundle").status_code == 403

    assert second.post(
        f"/assignments/{assignment_id}", data=_assessment_form(second)
    ).status_code == 302
    first_bundle = first.get(f"/assignments/{assignment_id}/bundle").get_json()
    second_bundle = second.get(f"/assignments/{assignment_id}/bundle").get_json()
    assert first_bundle["reviewer_id"] == FIRST_ID
    assert second_bundle["reviewer_id"] == SECOND_ID
    assert first_bundle["review_group_id"] == group.id
    assert second_bundle["review_group_id"] == group.id

    late_join = third.post(
        "/workshop/groups/join",
        data={"csrf_token": _csrf(third), "join_code": group.join_code},
    )
    assert late_join.status_code == 302
    late_page = third.get(f"/assignments/{assignment_id}")
    assert late_page.status_code == 200
    assert b"may contribute now" in late_page.data
    assert b"Synthetic subject expertise" in late_page.data
    assert third.get(f"/assignments/{assignment_id}/bundle").status_code == 200

    engine = create_database_engine(database_path)
    sessions = session_factory(engine)
    try:
        with sessions() as session:
            assignment = session.get(ReviewAssignment, assignment_id)
            assert assignment is not None
            assert assignment.reviewer_id is None
            assert assignment.review_group_id == group.id
            assert assignment.work_package_id == "package-synthetic"
            assert assignment.status == "active"
            assert assignment.participant_status == "active"
            assert assignment.claimed_at is not None
            assert assignment.opened_at is not None
            seed = session.get(AssignmentKgSeed, (assignment_id, "synthetic-kg"))
            assert seed is not None
            assert seed.seed_digest == "sha256:" + "a" * 64
    finally:
        engine.dispose()


def test_workshop_routes_fail_closed_without_consent_or_complete_profile(
    workshop_app,
) -> None:
    app, sender, database_path, _bundle_root = workshop_app
    engine = create_database_engine(database_path)
    sessions = session_factory(engine)
    with sessions.begin() as session:
        first = session.get(Reviewer, FIRST_ID)
        assert first is not None
        first.consent_statement_version = None
        first.consented_at = None
    engine.dispose()

    client = app.test_client()
    _login(client, app, sender, "first@example.invalid")
    assert b"Open IPL workshop" not in client.get("/").data
    assert client.get("/workshop").status_code == 403
    assert client.post(
        "/workshop/groups", data={"csrf_token": _csrf(client)}
    ).status_code == 403


def test_simultaneous_claims_create_only_one_active_assignment(workshop_app) -> None:
    app, _sender, database_path, _bundle_root = workshop_app
    service = app.extensions["musparql_workshops"]
    group_id = service.create_group(FIRST_ID)
    code = service.dashboard(FIRST_ID).groups[0].join_code
    service.join_group(SECOND_ID, code)
    barrier = threading.Barrier(2)

    def claim(reviewer_id: str) -> tuple[str, str]:
        barrier.wait()
        try:
            return "accepted", service.claim_package(
                reviewer_id=reviewer_id,
                group_id=group_id,
                package_id="package-synthetic",
            )
        except WorkshopAccessError as exc:
            return "rejected", str(exc)

    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = list(pool.map(claim, (FIRST_ID, SECOND_ID)))
    assert sorted(item[0] for item in outcomes) == ["accepted", "rejected"]

    other_group_id = service.create_group(THIRD_ID)
    other_assignment_id = service.claim_package(
        reviewer_id=THIRD_ID,
        group_id=other_group_id,
        package_id="package-synthetic",
    )
    assert other_assignment_id != next(
        value for status, value in outcomes if status == "accepted"
    )

    engine = create_database_engine(database_path)
    sessions = session_factory(engine)
    try:
        with sessions() as session:
            assert session.scalar(
                select(func.count())
                .select_from(ReviewAssignment)
                .where(ReviewAssignment.review_group_id == group_id)
            ) == 1
            assert session.scalar(
                select(func.count()).select_from(ReviewAssignment)
            ) == 2
    finally:
        engine.dispose()


def test_claim_rejects_a_changed_package_bundle(workshop_app) -> None:
    app, _sender, database_path, bundle_root = workshop_app
    service = app.extensions["musparql_workshops"]
    group_id = service.create_group(FIRST_ID)
    with (bundle_root / "synthetic-package.json").open("ab") as handle:
        handle.write(b" ")
    with pytest.raises(ValueError, match="frozen bundle"):
        service.claim_package(
            reviewer_id=FIRST_ID,
            group_id=group_id,
            package_id="package-synthetic",
        )

    engine = create_database_engine(database_path)
    sessions = session_factory(engine)
    try:
        with sessions() as session:
            assert session.scalar(select(func.count()).select_from(ReviewAssignment)) == 0
    finally:
        engine.dispose()


def test_joining_is_idempotent_but_closed_groups_reject_new_members(
    workshop_app,
) -> None:
    app, _sender, database_path, _bundle_root = workshop_app
    service = app.extensions["musparql_workshops"]
    group_id = service.create_group(FIRST_ID)
    code = service.dashboard(FIRST_ID).groups[0].join_code
    assert service.join_group(SECOND_ID, code) == group_id
    assert service.join_group(SECOND_ID, code) == group_id
    assignment_id = service.claim_package(
        reviewer_id=FIRST_ID,
        group_id=group_id,
        package_id="package-synthetic",
    )

    engine = create_database_engine(database_path)
    sessions = session_factory(engine)
    with sessions.begin() as session:
        assignment = session.get(ReviewAssignment, assignment_id)
        assert assignment is not None
        assignment.participant_status = "abandoned"
        assignment.completed_at = timestamp(utc_now())
        assignment.closed_contributor_ids = [FIRST_ID, SECOND_ID]
    engine.dispose()

    with pytest.raises(WorkshopAccessError, match="closed"):
        service.join_group(THIRD_ID, code)

    check_engine = create_database_engine(database_path)
    check_sessions = session_factory(check_engine)
    try:
        with check_sessions() as session:
            assert session.get(ReviewGroupMember, (group_id, THIRD_ID)) is None
    finally:
        check_engine.dispose()
