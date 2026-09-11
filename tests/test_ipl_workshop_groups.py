from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import timedelta
import hashlib
import json
from pathlib import Path
import re
import threading

import pytest
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError

from musparql.database import create_database_engine, session_factory
from musparql.database.migrations import upgrade_database
from musparql.database.models import (
    AssignmentKgSeed,
    AuthSession,
    ExpertiseDomain,
    KgSeedFamiliarityScope,
    KgSeedReviewDomain,
    KgSeedSnapshot,
    LoginCode,
    Reviewer,
    ReviewerDomainExpertise,
    ReviewerExperience,
    ReviewerLanguage,
    ReviewAssignment,
    ReviewGroupMember,
    ReviewSubmission,
    ProcessingJob,
    WorkshopEntryCode,
    WorkshopEntryRedemption,
    WorkshopAdmissionAttempt,
    WorkshopAdmissionNonce,
    WorkshopRound,
    WorkshopSessionReset,
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
        privacy_notice_version="synthetic-ipl-v1" if consented else None,
        privacy_notice_acknowledged_at=now if consented else None,
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


def _write_bundle(path: Path, *, kg_id: str = "synthetic-kg") -> str:
    payload = {
        "schema": "musparql.review-bundle.v2",
        "mode": "initial",
        "dataset_id": "synthetic-ipl-package",
        "built_at": "2026-09-11T09:00:00Z",
        "holdout_input_policy": "no_holdout",
        "record_count": 2,
        "records": [
            {
                "review_id": "synthetic-kg::synthetic-query::one",
                "kg_id": kg_id,
            },
            {
                "review_id": "synthetic-kg::synthetic-query::two",
                "kg_id": kg_id,
            },
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
    notice_path = tmp_path / "participant-notice.txt"
    summary_path = tmp_path / "consent-summary.txt"
    statement_path = tmp_path / "consent-statement.txt"
    notice_path.write_text(
        "Synthetic notice from the configured file. Do not enter real data.",
        encoding="utf-8",
    )
    summary_path.write_text(
        "Synthetic consent summary from the configured file.", encoding="utf-8"
    )
    statement_path.write_text(
        "Synthetic affirmative statement from the configured file.",
        encoding="utf-8",
    )
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
            "PRIVACY_NOTICE_BODY": None,
            "PRIVACY_NOTICE_PATH": notice_path,
            "CONSENT_STATEMENT_VERSION": "synthetic-consent-v1",
            "CONSENT_SUMMARY_PATH": summary_path,
            "CONSENT_STATEMENT_PATH": statement_path,
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


def _issue_workshop_entry_code(client) -> str:
    response = client.post(
        "/owner/workshop-entry/workshop-ipl/issue",
        data={"csrf_token": _csrf(client)},
    )
    assert response.status_code == 200
    match = re.search(rb"[0-9]{3}-[0-9]{3}", response.data)
    assert match is not None
    return match.group().decode("ascii")


def test_owner_issues_and_revokes_digest_only_workshop_entry_code(workshop_app) -> None:
    app, sender, database_path, _bundle_root = workshop_app
    owner = app.test_client()
    _login(owner, app, sender, "owner@example.invalid")

    code = _issue_workshop_entry_code(owner)
    status_page = owner.get("/owner/workshop-entry")
    assert code.encode() not in status_page.data
    assert status_page.location is None

    engine = create_database_engine(database_path)
    sessions = session_factory(engine)
    with sessions() as session:
        stored = session.scalar(select(WorkshopEntryCode))
        assert stored is not None
        assert stored.code_digest != code.replace("-", "")
        code_id = stored.id
    engine.dispose()

    login_page = app.test_client().get("/auth/login")
    assert b"IPL workshop entry" in login_page.data
    revoked = owner.post(
        f"/owner/workshop-entry/{code_id}/revoke",
        data={"csrf_token": _csrf(owner)},
    )
    assert revoked.status_code == 302
    assert b"IPL workshop entry" not in app.test_client().get("/auth/login").data


def test_shared_code_creates_distinct_accounts_and_sessions_then_routes_to_consent(
    workshop_app,
) -> None:
    app, sender, database_path, _bundle_root = workshop_app
    owner = app.test_client()
    _login(owner, app, sender, "owner@example.invalid")
    code = _issue_workshop_entry_code(owner)

    clients = [app.test_client(), app.test_client()]
    for client in clients:
        response = client.post(
            "/auth/workshop",
            data={"csrf_token": _csrf(client), "code": code},
        )
        assert response.status_code == 302
        assert response.location == "/consent"
        assert code not in response.location
        consent = client.get(response.location)
        assert b"Before you take part" in consent.data
        assert b'name="consent_affirmed" value="yes" required' in consent.data
        assert b'name="consent_affirmed" value="yes" checked' not in consent.data
        assert client.get("/").location == "/consent"
        assert client.get("/profile").location == "/consent"
        assert client.post(
            "/profile", data={"csrf_token": _csrf(client), "name": "Must not save"}
        ).location == "/consent"

    engine = create_database_engine(database_path)
    sessions = session_factory(engine)
    with sessions() as session:
        reviewers = list(
            session.scalars(
                select(Reviewer).where(Reviewer.registration_method == "workshop_code")
            )
        )
        assert len(reviewers) == 2
        assert len({reviewer.id for reviewer in reviewers}) == 2
        assert all(reviewer.email_verified_at is None for reviewer in reviewers)
        assert all(reviewer.consented_at is None for reviewer in reviewers)
        assert all(reviewer.email_normalized.endswith("@example.invalid") for reviewer in reviewers)
        assert session.scalar(select(func.count()).select_from(WorkshopEntryRedemption)) == 2
        assert session.scalar(
            select(func.count())
            .select_from(AuthSession)
            .where(AuthSession.reviewer_id.in_([item.id for item in reviewers]))
        ) == 2
        stored = session.scalar(select(WorkshopEntryCode))
        assert stored is not None and stored.redemption_count == 2
    engine.dispose()


def test_shared_code_replay_from_active_browser_does_not_consume_another_seat(
    workshop_app,
) -> None:
    app, sender, database_path, _bundle_root = workshop_app
    owner = app.test_client()
    _login(owner, app, sender, "owner@example.invalid")
    code = _issue_workshop_entry_code(owner)
    participant = app.test_client()

    first = participant.post(
        "/auth/workshop", data={"csrf_token": _csrf(participant), "code": code}
    )
    assert first.location == "/consent"
    replay = participant.post(
        "/auth/workshop", data={"csrf_token": _csrf(participant), "code": code}
    )
    assert replay.status_code == 200

    engine = create_database_engine(database_path)
    sessions = session_factory(engine)
    with sessions() as session:
        assert session.scalar(select(func.count()).select_from(WorkshopEntryRedemption)) == 1
        assert session.scalar(select(func.count()).select_from(WorkshopAdmissionNonce)) == 1
    engine.dispose()


def test_parallel_shared_code_posts_with_same_nonce_are_idempotent(workshop_app) -> None:
    app, sender, database_path, _bundle_root = workshop_app
    owner = app.test_client()
    _login(owner, app, sender, "owner@example.invalid")
    code = _issue_workshop_entry_code(owner)
    auth = app.extensions["musparql_auth"]

    def redeem(index: int):
        return auth.redeem_workshop_code(
            code,
            f"192.0.2.{index}",
            current_token=None,
            admission_nonce="same-browser-form",
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(redeem, (60, 60)))
    assert sum(result is not None for result in results) == 1

    engine = create_database_engine(database_path)
    sessions = session_factory(engine)
    with sessions() as session:
        assert session.scalar(select(func.count()).select_from(WorkshopEntryRedemption)) == 1
        assert session.scalar(select(func.count()).select_from(WorkshopAdmissionNonce)) == 1
    engine.dispose()


def test_invalid_shared_code_does_not_create_identity_or_session(workshop_app) -> None:
    app, sender, database_path, _bundle_root = workshop_app
    owner = app.test_client()
    _login(owner, app, sender, "owner@example.invalid")
    _issue_workshop_entry_code(owner)
    participant = app.test_client()

    response = participant.post(
        "/auth/workshop",
        data={"csrf_token": _csrf(participant), "code": "000-000"},
    )
    assert response.status_code == 200
    assert b"not available" in response.data
    assert participant.get_cookie("musparql_session") is None

    engine = create_database_engine(database_path)
    sessions = session_factory(engine)
    with sessions() as session:
        assert session.scalar(
            select(func.count())
            .select_from(Reviewer)
            .where(Reviewer.registration_method == "workshop_code")
        ) == 0
        assert session.scalar(select(func.count()).select_from(WorkshopEntryRedemption)) == 0
    engine.dispose()


def test_concurrent_shared_code_redemption_cannot_exceed_round_cap(workshop_app) -> None:
    app, sender, database_path, _bundle_root = workshop_app
    engine = create_database_engine(database_path)
    sessions = session_factory(engine)
    with sessions.begin() as session:
        session.get(WorkshopRound, "workshop-ipl").max_participants = 1
    engine.dispose()

    owner = app.test_client()
    _login(owner, app, sender, "owner@example.invalid")
    code = _issue_workshop_entry_code(owner)
    auth = app.extensions["musparql_auth"]

    def redeem(index: int):
        return auth.redeem_workshop_code(
            code,
            f"192.0.2.{index}",
            current_token=None,
            admission_nonce=f"synthetic-nonce-{index}",
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(redeem, (10, 11)))
    assert sum(result is not None for result in results) == 1

    engine = create_database_engine(database_path)
    sessions = session_factory(engine)
    with sessions() as session:
        assert session.scalar(select(func.count()).select_from(WorkshopEntryRedemption)) == 1
        workshop_reviewers = list(
            session.scalars(
                select(Reviewer.id).where(Reviewer.registration_method == "workshop_code")
            )
        )
        assert len(workshop_reviewers) == 1
        assert session.scalar(
            select(func.count())
            .select_from(AuthSession)
            .where(AuthSession.reviewer_id == workshop_reviewers[0])
        ) == 1
    engine.dispose()


def test_shared_code_attempts_are_throttled_by_request_context(workshop_app) -> None:
    app, sender, database_path, _bundle_root = workshop_app
    owner = app.test_client()
    _login(owner, app, sender, "owner@example.invalid")
    code = _issue_workshop_entry_code(owner)
    app.config["WORKSHOP_CODE_ATTEMPTS_PER_CONTEXT"] = 2
    participant = app.test_client()
    csrf = _csrf(participant)
    for candidate, user_agent in (
        ("000-000", "synthetic-agent-a"),
        ("111-111", "synthetic-agent-b"),
        (code, "synthetic-agent-c"),
    ):
        response = participant.post(
            "/auth/workshop",
            data={"csrf_token": csrf, "code": candidate},
            headers={"User-Agent": user_agent},
            environ_overrides={"REMOTE_ADDR": "192.0.2.50"},
        )
        assert response.status_code == 200

    engine = create_database_engine(database_path)
    sessions = session_factory(engine)
    with sessions() as session:
        assert session.scalar(select(func.count()).select_from(WorkshopEntryRedemption)) == 0
        assert session.scalar(
            select(func.count())
            .select_from(Reviewer)
            .where(Reviewer.registration_method == "workshop_code")
        ) == 0
    engine.dispose()


def test_workshop_throttle_ignores_user_agent_and_survives_app_restart(
    workshop_app,
) -> None:
    app, sender, database_path, bundle_root = workshop_app
    owner = app.test_client()
    _login(owner, app, sender, "owner@example.invalid")
    code = _issue_workshop_entry_code(owner)
    app.config["WORKSHOP_CODE_ATTEMPTS_PER_CONTEXT"] = 2
    auth = app.extensions["musparql_auth"]
    assert auth.redeem_workshop_code(
        "000-000",
        "192.0.2.77",
        current_token=None,
        admission_nonce="restart-a",
    ) is None

    restarted = create_app(
        {
            "TESTING": True,
            "DATABASE_PATH": database_path,
            "APP_SECRET": SECRET,
            "OWNER_REVIEWER_ID": OWNER_ID,
            "COOKIE_SECURE": False,
            "EMAIL_SENDER": SyntheticEmailSender(),
            "EXPERTISE_SUGGESTIONS_PATH": ROOT
            / "catalog/expertise_domain_suggestions.yaml",
            "ASSIGNMENT_BUNDLE_ROOT": bundle_root,
            "SUBMISSION_ROOT": database_path.parent / "restarted-submissions",
            "CANDIDATE_ROOT": database_path.parent / "restarted-candidates",
            "PRIVACY_NOTICE_VERSION": "synthetic-ipl-v1",
            "PRIVACY_NOTICE_BODY": "Synthetic notice. Do not enter real data.",
            "CONSENT_STATEMENT_VERSION": "synthetic-consent-v1",
            "WORKSHOP_CODE_ATTEMPTS_PER_CONTEXT": 2,
        }
    )
    try:
        restarted_auth = restarted.extensions["musparql_auth"]
        assert restarted_auth.redeem_workshop_code(
            "111-111",
            "192.0.2.77",
            current_token=None,
            admission_nonce="restart-b",
        ) is None
        assert restarted_auth.redeem_workshop_code(
            code,
            "192.0.2.77",
            current_token=None,
            admission_nonce="restart-c",
        ) is None
    finally:
        restarted.extensions["musparql_email_dispatcher"].shutdown()
        restarted.extensions["musparql_engine"].dispose()

    engine = create_database_engine(database_path)
    sessions = session_factory(engine)
    with sessions() as session:
        assert session.scalar(select(func.count()).select_from(WorkshopAdmissionAttempt)) == 2
        assert session.scalar(select(func.count()).select_from(WorkshopEntryRedemption)) == 0
    engine.dispose()


def test_owner_can_issue_one_time_audited_workshop_session_recovery(workshop_app) -> None:
    app, sender, database_path, _bundle_root = workshop_app
    owner = app.test_client()
    _login(owner, app, sender, "owner@example.invalid")
    entry_code = _issue_workshop_entry_code(owner)
    participant = app.test_client()
    admitted = participant.post(
        "/auth/workshop",
        data={"csrf_token": _csrf(participant), "code": entry_code},
    )
    assert admitted.location == "/consent"

    engine = create_database_engine(database_path)
    sessions = session_factory(engine)
    with sessions() as session:
        reviewer = session.scalar(
            select(Reviewer).where(Reviewer.registration_method == "workshop_code")
        )
        assert reviewer is not None
        reviewer_id = reviewer.id
    engine.dispose()

    reset = owner.post(
        f"/owner/workshop-entry/reviewers/{reviewer_id}/reset",
        data={"csrf_token": _csrf(owner)},
    )
    assert reset.status_code == 200
    match = re.search(rb'<p class="join-code">([0-9]{6})</p>', reset.data)
    assert match is not None
    recovery_code = match.group(1).decode("ascii")
    assert recovery_code.encode() not in owner.get("/owner/workshop-entry").data
    assert participant.get("/consent").location == "/auth/login"

    recovered = app.test_client()
    response = recovered.post(
        "/auth/workshop/recover",
        data={
            "csrf_token": _csrf(recovered),
            "reviewer_id": reviewer_id,
            "code": recovery_code,
        },
    )
    assert response.status_code == 302
    assert response.location == "/consent"
    assert recovered.get("/consent").status_code == 200

    replay = app.test_client()
    replay_response = replay.post(
        "/auth/workshop/recover",
        data={
            "csrf_token": _csrf(replay),
            "reviewer_id": reviewer_id,
            "code": recovery_code,
        },
    )
    assert replay_response.status_code == 200
    assert replay.get_cookie("musparql_session") is None

    engine = create_database_engine(database_path)
    sessions = session_factory(engine)
    with sessions() as session:
        reviewer = session.get(Reviewer, reviewer_id)
        assert reviewer is not None and reviewer.email_verified_at is None
        assert session.scalar(select(func.count()).select_from(WorkshopSessionReset)) == 1
        active_sessions = session.scalar(
            select(func.count())
            .select_from(AuthSession)
            .where(
                AuthSession.reviewer_id == reviewer_id,
                AuthSession.revoked_at.is_(None),
            )
        )
        assert active_sessions == 1
    engine.dispose()


def test_workshop_recovery_is_serialized_for_successes_and_wrong_codes(
    workshop_app,
) -> None:
    app, sender, database_path, _bundle_root = workshop_app
    owner = app.test_client()
    _login(owner, app, sender, "owner@example.invalid")
    entry_code = _issue_workshop_entry_code(owner)
    participant = app.test_client()
    assert participant.post(
        "/auth/workshop",
        data={"csrf_token": _csrf(participant), "code": entry_code},
    ).location == "/consent"

    engine = create_database_engine(database_path)
    sessions = session_factory(engine)
    with sessions() as session:
        reviewer_id = session.scalar(
            select(Reviewer.id).where(Reviewer.registration_method == "workshop_code")
        )
    engine.dispose()
    assert reviewer_id is not None
    auth = app.extensions["musparql_auth"]

    wrong_code = auth.issue_workshop_recovery_code(OWNER_ID, reviewer_id)

    def wrong(index: int):
        return auth.recover_workshop_session(
            reviewer_id,
            "999999" if wrong_code != "999999" else "888888",
            f"192.0.2.{80 + index}",
            current_token=None,
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        assert list(executor.map(wrong, (0, 1))) == [None, None]

    recovery_code = auth.issue_workshop_recovery_code(OWNER_ID, reviewer_id)

    def recover(index: int):
        return auth.recover_workshop_session(
            reviewer_id,
            recovery_code,
            f"192.0.2.{90 + index}",
            current_token=None,
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(recover, (0, 1)))
    assert sum(result is not None for result in results) == 1

    engine = create_database_engine(database_path)
    sessions = session_factory(engine)
    with sessions() as session:
        challenges = list(
            session.scalars(
                select(LoginCode)
                .where(LoginCode.email_normalized.like("workshop-%"))
                .order_by(LoginCode.requested_at)
            )
        )
        assert challenges[0].failed_attempt_count == 2
        assert challenges[-1].consumed_at is not None
    engine.dispose()


def test_workshop_reset_audit_is_immutable_for_update_and_delete(workshop_app) -> None:
    app, sender, database_path, _bundle_root = workshop_app
    owner = app.test_client()
    _login(owner, app, sender, "owner@example.invalid")
    entry_code = _issue_workshop_entry_code(owner)
    participant = app.test_client()
    participant.post(
        "/auth/workshop",
        data={"csrf_token": _csrf(participant), "code": entry_code},
    )
    engine = create_database_engine(database_path)
    sessions = session_factory(engine)
    with sessions() as session:
        reviewer_id = session.scalar(
            select(Reviewer.id).where(Reviewer.registration_method == "workshop_code")
        )
    assert reviewer_id is not None
    app.extensions["musparql_auth"].issue_workshop_recovery_code(OWNER_ID, reviewer_id)

    with pytest.raises(IntegrityError, match="append-only"):
        with sessions.begin() as session:
            reset = session.scalar(select(WorkshopSessionReset))
            assert reset is not None
            reset.created_at = "2099-01-01T00:00:00Z"
    with pytest.raises(IntegrityError, match="append-only"):
        with sessions.begin() as session:
            reset = session.scalar(select(WorkshopSessionReset))
            assert reset is not None
            session.delete(reset)
    engine.dispose()


def test_workshop_accounts_cannot_use_email_login_or_be_marked_verified(
    workshop_app,
) -> None:
    app, sender, database_path, _bundle_root = workshop_app
    owner = app.test_client()
    _login(owner, app, sender, "owner@example.invalid")
    entry_code = _issue_workshop_entry_code(owner)
    participant = app.test_client()
    participant.post(
        "/auth/workshop",
        data={"csrf_token": _csrf(participant), "code": entry_code},
    )

    engine = create_database_engine(database_path)
    sessions = session_factory(engine)
    with sessions() as session:
        reviewer = session.scalar(
            select(Reviewer).where(Reviewer.registration_method == "workshop_code")
        )
        assert reviewer is not None
        reviewer_id = reviewer.id
        email = reviewer.email_normalized
    before = sender.position()
    app.extensions["musparql_auth"].request_login_code(email, "192.0.2.100")
    app.extensions["musparql_email_dispatcher"].wait_for_idle()
    assert sender.position() == before

    with pytest.raises(
        IntegrityError, match="ck_reviewers_workshop_email_unverified"
    ):
        with sessions.begin() as session:
            session.get(Reviewer, reviewer_id).email_verified_at = timestamp(utc_now())
    engine.dispose()


def test_stale_workshop_consent_stays_behind_consent_boundary(workshop_app) -> None:
    app, sender, database_path, _bundle_root = workshop_app
    owner = app.test_client()
    _login(owner, app, sender, "owner@example.invalid")
    entry_code = _issue_workshop_entry_code(owner)
    participant = app.test_client()
    assert participant.post(
        "/auth/workshop",
        data={"csrf_token": _csrf(participant), "code": entry_code},
    ).location == "/consent"

    engine = create_database_engine(database_path)
    sessions = session_factory(engine)
    with sessions.begin() as session:
        reviewer = session.scalar(
            select(Reviewer).where(Reviewer.registration_method == "workshop_code")
        )
        reviewer.consent_statement_version = "obsolete-consent-v0"
        reviewer.consented_at = timestamp(utc_now())
        reviewer_id = reviewer.id
    engine.dispose()

    assert participant.get("/").location == "/consent"
    assert participant.get("/profile").location == "/consent"
    assert participant.get("/consent").status_code == 200

    recovery_code = app.extensions["musparql_auth"].issue_workshop_recovery_code(
        OWNER_ID, reviewer_id
    )
    recovered = app.test_client()
    response = recovered.post(
        "/auth/workshop/recover",
        data={
            "csrf_token": _csrf(recovered),
            "reviewer_id": reviewer_id,
            "code": recovery_code,
        },
    )
    assert response.location == "/consent"


def test_affirmative_consent_records_both_versions_before_profile_collection(
    workshop_app,
) -> None:
    app, sender, database_path, _bundle_root = workshop_app
    owner = app.test_client()
    _login(owner, app, sender, "owner@example.invalid")
    entry_code = _issue_workshop_entry_code(owner)
    participant = app.test_client()
    assert participant.post(
        "/auth/workshop",
        data={"csrf_token": _csrf(participant), "code": entry_code},
    ).location == "/consent"

    consent_page = participant.get("/consent")
    assert (
        b"Synthetic consent summary from the configured file." in consent_page.data
    )
    assert (
        b"Synthetic affirmative statement from the configured file."
        in consent_page.data
    )

    rejected = participant.post(
        "/consent", data={"csrf_token": _csrf(participant)}
    )
    assert rejected.status_code == 200
    assert b"must select the consent checkbox" in rejected.data
    accepted = participant.post(
        "/consent",
        data={"csrf_token": _csrf(participant), "consent_affirmed": "yes"},
    )
    assert accepted.location == "/profile"

    engine = create_database_engine(database_path)
    sessions = session_factory(engine)
    with sessions() as session:
        reviewer = session.scalar(
            select(Reviewer).where(Reviewer.registration_method == "workshop_code")
        )
        assert reviewer is not None
        assert reviewer.privacy_notice_version == "synthetic-ipl-v1"
        assert reviewer.consent_statement_version == "synthetic-consent-v1"
        assert reviewer.privacy_notice_acknowledged_at == reviewer.consented_at
        assert reviewer.consented_at is not None
    engine.dispose()

    assert participant.get("/consent").location == "/"
    profile = participant.get("/profile")
    assert profile.status_code == 200
    assert b"does not use a verified email address" in profile.data
    assert b"@example.invalid" not in profile.data
    notice = app.test_client().get("/participant-notice")
    assert notice.status_code == 200
    assert (
        b"Synthetic notice from the configured file. Do not enter real data."
        in notice.data
    )
    withdrawal = participant.get("/consent/withdrawal")
    assert b"musparql@industrycommons.net" in withdrawal.data


def test_email_login_with_missing_consent_routes_to_the_same_gate(workshop_app) -> None:
    app, sender, database_path, _bundle_root = workshop_app
    engine = create_database_engine(database_path)
    sessions = session_factory(engine)
    with sessions.begin() as session:
        reviewer = session.get(Reviewer, FIRST_ID)
        assert reviewer is not None
        reviewer.consent_statement_version = None
        reviewer.consented_at = None
    engine.dispose()

    client = app.test_client()
    position = sender.position()
    client.post(
        "/auth/login",
        data={"csrf_token": _csrf(client), "email": "first@example.invalid"},
    )
    message = sender.wait_for(
        "login_code", "first@example.invalid", after_index=position
    )
    app.extensions["musparql_email_dispatcher"].wait_for_idle()
    verified = client.post(
        "/auth/verify",
        data={"csrf_token": _csrf(client), "code": message.value},
    )
    assert verified.location == "/consent"
    assert client.get("/profile").location == "/consent"
    accepted = client.post(
        "/consent",
        data={"csrf_token": _csrf(client), "consent_affirmed": "yes"},
    )
    assert accepted.location == "/profile"
    engine = create_database_engine(database_path)
    sessions = session_factory(engine)
    with sessions() as session:
        reviewer = session.get(Reviewer, FIRST_ID)
        assert reviewer is not None
        assert reviewer.privacy_notice_version == "synthetic-ipl-v1"
        assert reviewer.consent_statement_version == "synthetic-consent-v1"
    engine.dispose()


def test_owner_access_does_not_require_a_consent_record(workshop_app) -> None:
    app, sender, database_path, _bundle_root = workshop_app
    engine = create_database_engine(database_path)
    sessions = session_factory(engine)
    with sessions() as session:
        owner = session.get(Reviewer, OWNER_ID)
        assert owner is not None
        assert owner.consent_statement_version is None
        assert owner.privacy_notice_version is None
    engine.dispose()

    client = app.test_client()
    _login(client, app, sender, "owner@example.invalid")
    assert client.get("/owner/reviewers").status_code == 200


def test_notice_only_version_change_on_restart_revokes_consent(workshop_app) -> None:
    app, _sender, database_path, bundle_root = workshop_app
    restarted_sender = SyntheticEmailSender()
    restarted = create_app(
        {
            "TESTING": True,
            "DATABASE_PATH": database_path,
            "APP_SECRET": SECRET,
            "OWNER_REVIEWER_ID": OWNER_ID,
            "COOKIE_SECURE": False,
            "EMAIL_SENDER": restarted_sender,
            "EXPERTISE_SUGGESTIONS_PATH": ROOT
            / "catalog/expertise_domain_suggestions.yaml",
            "ASSIGNMENT_BUNDLE_ROOT": bundle_root,
            "SUBMISSION_ROOT": database_path.parent / "notice-restart-submissions",
            "CANDIDATE_ROOT": database_path.parent / "notice-restart-candidates",
            "PRIVACY_NOTICE_VERSION": "synthetic-ipl-v2",
            "PRIVACY_NOTICE_PATH": app.config["PRIVACY_NOTICE_PATH"],
            "CONSENT_STATEMENT_VERSION": "synthetic-consent-v1",
            "CONSENT_SUMMARY_PATH": app.config["CONSENT_SUMMARY_PATH"],
            "CONSENT_STATEMENT_PATH": app.config["CONSENT_STATEMENT_PATH"],
        }
    )
    try:
        client = restarted.test_client()
        _login(client, restarted, restarted_sender, "first@example.invalid")
        assert client.get("/").location == "/consent"
        assert client.get("/workshop").location == "/consent"
    finally:
        restarted.extensions["musparql_email_dispatcher"].shutdown()
        restarted.extensions["musparql_engine"].dispose()


def test_reissued_entry_code_reports_round_wide_capacity(workshop_app) -> None:
    app, sender, database_path, _bundle_root = workshop_app
    engine = create_database_engine(database_path)
    sessions = session_factory(engine)
    with sessions.begin() as session:
        session.get(WorkshopRound, "workshop-ipl").max_participants = 2
    engine.dispose()
    owner = app.test_client()
    _login(owner, app, sender, "owner@example.invalid")
    first_code = _issue_workshop_entry_code(owner)
    first = app.test_client()
    first.post(
        "/auth/workshop", data={"csrf_token": _csrf(first), "code": first_code}
    )
    second_code = _issue_workshop_entry_code(owner)
    status = app.extensions["musparql_workshop_admission"].list_rounds()[0]
    assert status.redemption_count == 1
    assert status.active is True
    second = app.test_client()
    second.post(
        "/auth/workshop", data={"csrf_token": _csrf(second), "code": second_code}
    )
    status = app.extensions["musparql_workshop_admission"].list_rounds()[0]
    assert status.redemption_count == 2
    assert status.active is False
    assert app.extensions["musparql_workshop_admission"].available() is False


def _assessment_form(client) -> dict[str, str]:
    return {
        "csrf_token": _csrf(client),
        "domain_level": "advanced",
        "familiarity_level": "worked",
        "confirmed": "yes",
    }


def _review_payload(
    assignment_id: str,
    bundle_digest: str,
    *,
    submitter_id: str,
    event_reviewer_id: str,
) -> dict:
    def review(record_id: str) -> dict:
        review_id = f"{record_id}::{event_reviewer_id}"
        return {
            "review_id": review_id,
            "reviewer_id": event_reviewer_id,
            "reviewed_at": "2026-09-11T09:29:00Z",
            "prior_review_ids": [],
            "authored_formulation_ids": [],
            "approved_formulation_ids": [
                f"{review_id}::formulation::candidate"
            ],
            "benchmark_disposition": "included",
            "pipeline_assessment": "accepted",
            "preferred_question": "",
            "literal_wording": "",
            "public_comment": "",
            "internal_comment": "",
            "split": "",
            "interpretive": {
                "naturalness": None,
                "pragmatism": None,
                "room_for_interpretation": None,
                "requires_graph_context_knowledge": False,
            },
        }
    return {
        "schema": "musparql.review-export.v2",
        "kind": "non_holdout_review_export",
        "assignment_id": assignment_id,
        "bundle_digest": bundle_digest,
        "reviewer_id": submitter_id,
        "dataset_id": "synthetic-ipl-package",
        "run_id": "synthetic-run",
        "run_ids": ["synthetic-run"],
        "runs": [],
        "exported_at": "2026-09-11T09:30:00Z",
        "reviews": {
            record_id: review(record_id)
            for record_id in (
                "synthetic-kg::synthetic-query::one",
                "synthetic-kg::synthetic-query::two",
            )
        },
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
                group.join_code[:3] + " " + group.join_code[3:]
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
    context = first.get(
        f"/assignments/{assignment_id}/workbench/host_context.js"
    ).data
    assert f'"draft_owner_id":"{group.id}"'.encode() in context
    assert b'"submission_url"' in context
    assert b'"partial_submission_url"' in context
    assert b'"workshop_url":"/workshop"' in context
    assert b'"abandon_url"' in context

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

    payload = _review_payload(
        assignment_id,
        first_bundle["bundle_digest"],
        submitter_id=FIRST_ID,
        event_reviewer_id=SECOND_ID,
    )
    forged = dict(payload)
    forged["review_group_id"] = "group-" + "f" * 24
    rejected = first.post(
        f"/assignments/{assignment_id}/submissions",
        json=forged,
        headers={"X-CSRF-Token": _csrf(first)},
    )
    assert rejected.status_code == 422
    assert b"Server-owned attribution" in rejected.data

    outsider = _review_payload(
        assignment_id,
        first_bundle["bundle_digest"],
        submitter_id=FIRST_ID,
        event_reviewer_id=OWNER_ID,
    )
    rejected = first.post(
        f"/assignments/{assignment_id}/submissions",
        json=outsider,
        headers={"X-CSRF-Token": _csrf(first)},
    )
    assert rejected.status_code == 422
    assert b"Review attribution does not match" in rejected.data

    partial = deepcopy(payload)
    partial["reviews"].pop("synthetic-kg::synthetic-query::two")
    rejected = first.post(
        f"/assignments/{assignment_id}/submissions",
        json=partial,
        headers={"X-CSRF-Token": _csrf(first)},
    )
    assert rejected.status_code == 422
    assert b"requires a review for every assigned item" in rejected.data
    engine = create_database_engine(database_path)
    sessions = session_factory(engine)
    try:
        with sessions() as session:
            assignment = session.get(ReviewAssignment, assignment_id)
            assert assignment is not None
            assert assignment.status == "active"
            assert assignment.participant_status == "active"
            assert assignment.closed_contributor_ids is None
            assert list(session.scalars(select(ReviewSubmission))) == []
    finally:
        engine.dispose()

    submissions = app.extensions["musparql_submissions"]
    with ThreadPoolExecutor(max_workers=2) as executor:
        concurrent = list(
            executor.map(
                lambda _index: submissions.submit(assignment_id, FIRST_ID, payload),
                range(2),
            )
        )
    assert len({item.receipt_id for item in concurrent}) == 1
    assert {item.duplicate for item in concurrent} == {False, True}
    receipt = concurrent[0].as_dict()
    browser_retry = deepcopy(payload)
    browser_retry["reviewer_id"] = SECOND_ID
    browser_retry["exported_at"] = "2026-09-11T09:31:00Z"
    retry = second.post(
        f"/assignments/{assignment_id}/submissions",
        json=browser_retry,
        headers={"X-CSRF-Token": _csrf(second)},
    )
    assert retry.status_code == 200
    assert retry.get_json()["receipt_id"] == receipt["receipt_id"]
    assert retry.get_json()["duplicate"] is True
    revised = dict(payload)
    revised["exported_at"] = "2026-09-11T09:32:00Z"
    revised["reviews"] = deepcopy(payload["reviews"])
    revised["reviews"]["synthetic-kg::synthetic-query::one"][
        "public_comment"
    ] = "A real content revision"
    assert first.post(
        f"/assignments/{assignment_id}/submissions",
        json=revised,
        headers={"X-CSRF-Token": _csrf(first)},
    ).status_code == 403

    engine = create_database_engine(database_path)
    sessions = session_factory(engine)
    try:
        with sessions() as session:
            submission = session.get(ReviewSubmission, receipt["receipt_id"])
            assignment = session.get(ReviewAssignment, assignment_id)
            jobs = list(session.scalars(select(ProcessingJob)))
            assert submission is not None
            assert submission.reviewer_id is None
            assert submission.review_group_id == group.id
            assert submission.submitted_by_reviewer_id == FIRST_ID
            assert submission.contributor_reviewer_ids == [
                FIRST_ID,
                SECOND_ID,
                THIRD_ID,
            ]
            assert assignment is not None
            assert assignment.closed_contributor_ids == [
                FIRST_ID,
                SECOND_ID,
                THIRD_ID,
            ]
            assert assignment.participant_status == "completed"
            assert assignment.completion_item_count == 2
            assert assignment.completion_total_count == 2
            assert len(jobs) == 1
            stored = json.loads(
                (Path(app.config["SUBMISSION_ROOT"]) / submission.export_path).read_text()
            )
            assert stored["review_group_id"] == group.id
            assert stored["submitted_by_reviewer_id"] == FIRST_ID
            assert stored["contributor_reviewer_ids"] == [
                FIRST_ID,
                SECOND_ID,
                THIRD_ID,
            ]
            assert stored["completion_type"] == "completed"
            assert stored["completion_item_count"] == 2
            assert stored["completion_total_count"] == 2
    finally:
        engine.dispose()

    assert app.extensions["musparql_processing"].process_next() == receipt["job_id"]
    audit = json.loads(
        (
            Path(app.config["CANDIDATE_ROOT"])
            / receipt["job_id"]
            / "audit.json"
        ).read_text()
    )
    assert audit["review_group_id"] == group.id
    assert audit["submitted_by_reviewer_id"] == FIRST_ID
    assert audit["contributor_reviewer_ids"] == [FIRST_ID, SECOND_ID, THIRD_ID]


def test_submission_and_abandonment_are_serialized(workshop_app) -> None:
    app, _sender, database_path, _bundle_root = workshop_app
    workshops = app.extensions["musparql_workshops"]
    assignments = app.extensions["musparql_assignments"]
    submissions = app.extensions["musparql_submissions"]
    group_id = workshops.create_group(FIRST_ID)
    assignment_id = workshops.claim_package(
        reviewer_id=FIRST_ID,
        group_id=group_id,
        package_id="package-synthetic",
    )
    assignments.assess(
        assignment_id, FIRST_ID, ["advanced"], ["worked"], confirmed=True
    )
    bundle = assignments.attributed_bundle(assignment_id, FIRST_ID)
    payload = _review_payload(
        assignment_id,
        bundle["bundle_digest"],
        submitter_id=FIRST_ID,
        event_reviewer_id=FIRST_ID,
    )
    barrier = threading.Barrier(2)

    def submit() -> str:
        barrier.wait()
        try:
            submissions.submit(assignment_id, FIRST_ID, payload)
            return "submitted"
        except PermissionError:
            return "rejected"

    def abandon() -> str:
        barrier.wait()
        try:
            workshops.abandon_assignment(
                reviewer_id=FIRST_ID, assignment_id=assignment_id
            )
            return "abandoned"
        except WorkshopAccessError:
            return "rejected"

    with ThreadPoolExecutor(max_workers=2) as executor:
        submit_future = executor.submit(submit)
        abandon_future = executor.submit(abandon)
        outcomes = {submit_future.result(), abandon_future.result()}
    assert outcomes in ({"submitted", "rejected"}, {"abandoned", "rejected"})

    engine = create_database_engine(database_path)
    sessions = session_factory(engine)
    try:
        with sessions() as session:
            assignment = session.get(ReviewAssignment, assignment_id)
            submission_count = int(
                session.scalar(select(func.count()).select_from(ReviewSubmission)) or 0
            )
            job_count = int(
                session.scalar(select(func.count()).select_from(ProcessingJob)) or 0
            )
            assert assignment is not None
            if assignment.participant_status == "abandoned":
                assert (submission_count, job_count) == (0, 0)
            else:
                assert assignment.participant_status == "completed"
                assert (submission_count, job_count) == (1, 1)
    finally:
        engine.dispose()


@pytest.mark.parametrize("closure", ["submit", "abandon"])
def test_late_join_and_terminal_closure_freeze_one_membership_snapshot(
    workshop_app, closure: str
) -> None:
    app, _sender, database_path, _bundle_root = workshop_app
    workshops = app.extensions["musparql_workshops"]
    assignments = app.extensions["musparql_assignments"]
    group_id = workshops.create_group(FIRST_ID)
    code = workshops.dashboard(FIRST_ID).groups[0].join_code
    assignment_id = workshops.claim_package(
        reviewer_id=FIRST_ID,
        group_id=group_id,
        package_id="package-synthetic",
    )
    assignments.assess(
        assignment_id, FIRST_ID, ["advanced"], ["worked"], confirmed=True
    )
    bundle = assignments.attributed_bundle(assignment_id, FIRST_ID)
    payload = _review_payload(
        assignment_id,
        bundle["bundle_digest"],
        submitter_id=FIRST_ID,
        event_reviewer_id=FIRST_ID,
    )
    barrier = threading.Barrier(2)

    def join() -> bool:
        barrier.wait()
        try:
            workshops.join_group(SECOND_ID, code)
            return True
        except WorkshopAccessError:
            return False

    def close() -> None:
        barrier.wait()
        if closure == "submit":
            app.extensions["musparql_submissions"].submit(
                assignment_id, FIRST_ID, payload
            )
        else:
            workshops.abandon_assignment(
                reviewer_id=FIRST_ID, assignment_id=assignment_id
            )

    with ThreadPoolExecutor(max_workers=2) as executor:
        join_future = executor.submit(join)
        close_future = executor.submit(close)
        joined = join_future.result()
        close_future.result()

    engine = create_database_engine(database_path)
    sessions = session_factory(engine)
    try:
        with sessions() as session:
            assignment = session.get(ReviewAssignment, assignment_id)
            members = set(
                session.scalars(
                    select(ReviewGroupMember.reviewer_id).where(
                        ReviewGroupMember.group_id == group_id
                    )
                )
            )
            assert assignment is not None
            assert set(assignment.closed_contributor_ids or ()) == members
            assert (SECOND_ID in members) is joined
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
    assert client.get("/workshop").location == "/consent"
    assert client.post(
        "/workshop/groups", data={"csrf_token": _csrf(client)}
    ).location == "/consent"


@pytest.mark.parametrize("withdrawn", [False, True])
def test_consent_change_revokes_a_claimed_group_assignment_immediately(
    workshop_app, withdrawn: bool
) -> None:
    app, sender, database_path, _bundle_root = workshop_app
    service = app.extensions["musparql_workshops"]
    group_id = service.create_group(FIRST_ID)
    assignment_id = service.claim_package(
        reviewer_id=FIRST_ID,
        group_id=group_id,
        package_id="package-synthetic",
    )
    client = app.test_client()
    _login(client, app, sender, "first@example.invalid")
    assert client.get(f"/assignments/{assignment_id}").status_code == 200

    engine = create_database_engine(database_path)
    sessions = session_factory(engine)
    with sessions.begin() as session:
        reviewer = session.get(Reviewer, FIRST_ID)
        assert reviewer is not None
        reviewer.consent_statement_version = (
            None if withdrawn else "synthetic-consent-obsolete"
        )
        reviewer.consented_at = None if withdrawn else timestamp(utc_now())
    engine.dispose()

    assert client.get("/workshop").location == "/consent"
    assert client.get(f"/assignments/{assignment_id}").location == "/consent"
    assert client.post(
        f"/assignments/{assignment_id}", data=_assessment_form(client)
    ).location == "/consent"
    assert client.post(
        "/workshop/groups", data={"csrf_token": _csrf(client)}
    ).location == "/consent"
    assert client.post(
        "/workshop/groups/join",
        data={"csrf_token": _csrf(client), "join_code": "000-000"},
    ).location == "/consent"
    assert client.post(
        f"/workshop/groups/{group_id}/packages/package-synthetic/claim",
        data={"csrf_token": _csrf(client)},
    ).location == "/consent"
    assert client.get(f"/assignments/{assignment_id}/bundle").location == "/consent"
    assert client.get(f"/assignments/{assignment_id}/workbench/").location == "/consent"
    assert client.get(
        f"/assignments/{assignment_id}/workbench/app.js"
    ).location == "/consent"
    assert client.post(
        f"/assignments/{assignment_id}/submissions",
        json={},
        headers={"X-CSRF-Token": _csrf(client)},
    ).location == "/consent"


def test_pre_issue_7_group_receipt_retry_backfills_counts_and_processes(
    workshop_app,
) -> None:
    app, _sender, database_path, _bundle_root = workshop_app
    workshops = app.extensions["musparql_workshops"]
    assignments = app.extensions["musparql_assignments"]
    submissions = app.extensions["musparql_submissions"]
    group_id = workshops.create_group(FIRST_ID)
    assignment_id = workshops.claim_package(
        reviewer_id=FIRST_ID,
        group_id=group_id,
        package_id="package-synthetic",
    )
    assignments.assess(
        assignment_id, FIRST_ID, ["advanced"], ["worked"], confirmed=True
    )
    bundle = assignments.attributed_bundle(assignment_id, FIRST_ID)
    payload = _review_payload(
        assignment_id,
        bundle["bundle_digest"],
        submitter_id=FIRST_ID,
        event_reviewer_id=FIRST_ID,
    )
    original = submissions.submit(assignment_id, FIRST_ID, payload)

    engine = create_database_engine(database_path)
    sessions = session_factory(engine)
    try:
        with sessions.begin() as session:
            assignment = session.get(ReviewAssignment, assignment_id)
            submission = session.get(ReviewSubmission, original.receipt_id)
            assert assignment is not None
            assert submission is not None
            stored_path = Path(app.config["SUBMISSION_ROOT"]) / submission.export_path
            legacy = json.loads(stored_path.read_text(encoding="utf-8"))
            legacy.pop("completion_type")
            legacy.pop("completion_item_count")
            legacy.pop("completion_total_count")
            assert submissions.validators["initial"].is_valid(legacy)
            raw = (
                json.dumps(
                    legacy,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                )
                + "\n"
            ).encode("utf-8")
            stored_path.write_bytes(raw)
            submission.export_digest = "sha256:" + hashlib.sha256(raw).hexdigest()
            assignment.completion_item_count = None
            assignment.completion_total_count = None

        retry_payload = deepcopy(payload)
        retry_payload["exported_at"] = "2026-09-11T12:00:00Z"
        retry = submissions.submit(assignment_id, FIRST_ID, retry_payload)
        assert retry.duplicate is True
        assert retry.receipt_id == original.receipt_id
        assert retry.completion_item_count == 2
        assert retry.completion_total_count == 2

        with sessions() as session:
            assignment = session.get(ReviewAssignment, assignment_id)
            assert assignment is not None
            assert assignment.completion_item_count == 2
            assert assignment.completion_total_count == 2

        assert app.extensions["musparql_processing"].process_next() == retry.job_id
        audit = json.loads(
            (
                Path(app.config["CANDIDATE_ROOT"])
                / retry.job_id
                / "audit.json"
            ).read_text(encoding="utf-8")
        )
        assert audit["item_count"] == 2
        assert audit["total_item_count"] == 2
        assert audit["completion_type"] == "completed"
    finally:
        engine.dispose()


def test_legacy_retry_cannot_race_into_abandonment(workshop_app) -> None:
    app, _sender, database_path, _bundle_root = workshop_app
    workshops = app.extensions["musparql_workshops"]
    assignments = app.extensions["musparql_assignments"]
    submissions = app.extensions["musparql_submissions"]
    group_id = workshops.create_group(FIRST_ID)
    assignment_id = workshops.claim_package(
        reviewer_id=FIRST_ID,
        group_id=group_id,
        package_id="package-synthetic",
    )
    assignments.assess(
        assignment_id, FIRST_ID, ["advanced"], ["worked"], confirmed=True
    )
    bundle = assignments.attributed_bundle(assignment_id, FIRST_ID)
    payload = _review_payload(
        assignment_id,
        bundle["bundle_digest"],
        submitter_id=FIRST_ID,
        event_reviewer_id=FIRST_ID,
    )
    legacy = dict(payload)
    legacy.update(
        review_group_id=group_id,
        submitted_by_reviewer_id=FIRST_ID,
        contributor_reviewer_ids=[FIRST_ID],
    )
    raw = (
        json.dumps(
            legacy, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        )
        + "\n"
    ).encode("utf-8")
    receipt_id = "receipt-" + "a" * 32
    job_id = "job-" + "b" * 32
    relative = f"{assignment_id}/{receipt_id}.json"
    stored_path = Path(app.config["SUBMISSION_ROOT"]) / relative
    stored_path.parent.mkdir(parents=True)
    stored_path.write_bytes(raw)
    engine = create_database_engine(database_path)
    sessions = session_factory(engine)
    with sessions.begin() as session:
        now = timestamp(utc_now())
        session.add(
            ReviewSubmission(
                id=receipt_id,
                assignment_id=assignment_id,
                reviewer_id=None,
                review_group_id=group_id,
                submitted_by_reviewer_id=FIRST_ID,
                contributor_reviewer_ids=[FIRST_ID],
                export_path=relative,
                export_digest="sha256:" + hashlib.sha256(raw).hexdigest(),
                submitted_at=now,
                revision=1,
                validation_status="schema_valid",
                inclusion_status="pending",
            )
        )
        session.flush()
        session.add(
            ProcessingJob(
                id=job_id,
                assignment_id=assignment_id,
                submission_id=receipt_id,
                recipe="validate_initial_review",
                status="queued",
                created_at=now,
                job_kind="submission",
                selected_submission_ids=None,
                approval_status="pending",
            )
        )
    engine.dispose()

    barrier = threading.Barrier(2)

    def retry() -> bool:
        barrier.wait()
        return submissions.submit(assignment_id, FIRST_ID, payload).duplicate

    def abandon() -> bool:
        barrier.wait()
        try:
            workshops.abandon_assignment(
                reviewer_id=FIRST_ID, assignment_id=assignment_id
            )
            return True
        except WorkshopAccessError:
            return False

    with ThreadPoolExecutor(max_workers=2) as executor:
        retry_future = executor.submit(retry)
        abandon_future = executor.submit(abandon)
        assert retry_future.result() is True
        assert abandon_future.result() is False

    engine = create_database_engine(database_path)
    sessions = session_factory(engine)
    try:
        with sessions() as session:
            assignment = session.get(ReviewAssignment, assignment_id)
            assert assignment is not None
            assert assignment.participant_status == "completed"
            assert assignment.completion_total_count == 2
            assert int(
                session.scalar(select(func.count()).select_from(ProcessingJob)) or 0
            ) == 1
            assert session.get(ProcessingJob, job_id) is not None
    finally:
        engine.dispose()


def test_round_closure_preserves_outstanding_terminal_assessment_access(
    workshop_app,
) -> None:
    app, sender, database_path, _bundle_root = workshop_app
    service = app.extensions["musparql_workshops"]
    group_id = service.create_group(FIRST_ID)
    assignment_id = service.claim_package(
        reviewer_id=FIRST_ID,
        group_id=group_id,
        package_id="package-synthetic",
    )
    client = app.test_client()
    _login(client, app, sender, "first@example.invalid")
    service.abandon_assignment(
        reviewer_id=FIRST_ID, assignment_id=assignment_id
    )

    engine = create_database_engine(database_path)
    sessions = session_factory(engine)
    with sessions.begin() as session:
        workshop_round = session.get(WorkshopRound, "workshop-ipl")
        assert workshop_round is not None
        workshop_round.status = "closed"
    engine.dispose()

    assert b"Open IPL workshop" in client.get("/").data
    assert client.get("/workshop").status_code == 200
    assert client.get(f"/assignments/{assignment_id}").status_code == 200
    assert client.get(f"/assignments/{assignment_id}/bundle").status_code == 403
    completed = client.post(
        f"/assignments/{assignment_id}", data=_assessment_form(client)
    )
    assert completed.status_code == 302
    assert completed.location == "/workshop"
    assert client.get(completed.location).status_code == 200
    assert client.get(f"/assignments/{assignment_id}").status_code == 404


def test_simultaneous_member_assessments_activate_without_sqlite_busy(
    workshop_app,
) -> None:
    app, _sender, database_path, _bundle_root = workshop_app
    workshops = app.extensions["musparql_workshops"]
    assignments = app.extensions["musparql_assignments"]
    group_id = workshops.create_group(FIRST_ID)
    code = workshops.dashboard(FIRST_ID).groups[0].join_code
    workshops.join_group(SECOND_ID, code)
    assignment_id = workshops.claim_package(
        reviewer_id=FIRST_ID,
        group_id=group_id,
        package_id="package-synthetic",
    )
    barrier = threading.Barrier(2)

    def assess(reviewer_id: str) -> None:
        barrier.wait()
        assignments.assess(
            assignment_id,
            reviewer_id,
            ["advanced"],
            ["worked"],
            confirmed=True,
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(assess, (FIRST_ID, SECOND_ID)))

    engine = create_database_engine(database_path)
    sessions = session_factory(engine)
    try:
        with sessions() as session:
            assignment = session.get(ReviewAssignment, assignment_id)
            assert assignment is not None
            assert assignment.status == "active"
    finally:
        engine.dispose()


def test_claim_rejects_package_without_any_frozen_assessment_prompts(
    workshop_app,
) -> None:
    app, _sender, database_path, bundle_root = workshop_app
    service = app.extensions["musparql_workshops"]
    group_id = service.create_group(FIRST_ID)
    empty_digest = _write_bundle(
        bundle_root / "synthetic-empty-package.json", kg_id="synthetic-empty-kg"
    )
    engine = create_database_engine(database_path)
    sessions = session_factory(engine)
    with sessions.begin() as session:
        session.add(
            KgSeedSnapshot(
                kg_id="synthetic-empty-kg",
                seed_version="synthetic-empty-seed-v1",
                seed_digest="sha256:" + "c" * 64,
                previous_seed_digest=None,
                seed_json={"name": "Synthetic empty graph"},
            )
        )
        session.flush()
        session.add(
            WorkshopWorkPackage(
                id="package-empty",
                workshop_round_id="workshop-ipl",
                kg_id="synthetic-empty-kg",
                seed_version="synthetic-empty-seed-v1",
                seed_digest="sha256:" + "c" * 64,
                display_name="Synthetic Empty Knowledge Graph",
                short_description="A fictional prompt-free package.",
                display_order=2,
                bundle_path="synthetic-empty-package.json",
                bundle_digest=empty_digest,
                processing_recipe="validate_initial_review",
                enabled=True,
                created_at=timestamp(utc_now()),
            )
        )
    engine.dispose()

    with pytest.raises(WorkshopAccessError, match="no frozen assessment prompts"):
        service.claim_package(
            reviewer_id=FIRST_ID,
            group_id=group_id,
            package_id="package-empty",
        )


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


def test_partial_submission_closes_assignment_and_preserves_outstanding_form(
    workshop_app,
) -> None:
    app, sender, database_path, _bundle_root = workshop_app
    client = app.test_client()
    _login(client, app, sender, "first@example.invalid")
    workshops = app.extensions["musparql_workshops"]
    assignments = app.extensions["musparql_assignments"]
    submissions = app.extensions["musparql_submissions"]
    group_id = workshops.create_group(FIRST_ID)
    code = workshops.dashboard(FIRST_ID).groups[0].join_code
    workshops.join_group(SECOND_ID, code)
    assignment_id = workshops.claim_package(
        reviewer_id=FIRST_ID,
        group_id=group_id,
        package_id="package-synthetic",
    )
    for reviewer_id in (FIRST_ID, SECOND_ID):
        assignments.assess(
            assignment_id,
            reviewer_id,
            ["advanced"],
            ["worked"],
            confirmed=True,
        )
    workshops.join_group(THIRD_ID, code)
    bundle = assignments.attributed_bundle(assignment_id, FIRST_ID)
    payload = _review_payload(
        assignment_id,
        bundle["bundle_digest"],
        submitter_id=FIRST_ID,
        event_reviewer_id=FIRST_ID,
    )
    payload["reviews"].pop("synthetic-kg::synthetic-query::two")

    response = client.post(
        f"/assignments/{assignment_id}/submissions?completion=partial",
        json=payload,
        headers={"X-CSRF-Token": _csrf(client)},
    )
    assert response.status_code == 202
    receipt_payload = response.get_json()
    assert receipt_payload["completion_type"] == "partial"
    assert receipt_payload["completion_item_count"] == 1
    assert receipt_payload["completion_total_count"] == 2
    receipt_id = receipt_payload["receipt_id"]
    job_id = receipt_payload["job_id"]
    assert submissions.submit(
        assignment_id,
        FIRST_ID,
        payload,
        completion_type="partial",
    ).duplicate is True
    assert workshops.dashboard(FIRST_ID).groups[0].can_claim is False
    outstanding = workshops.dashboard(THIRD_ID).groups[0].outstanding_assessments
    assert [item.assignment_id for item in outstanding] == [assignment_id]

    closed_view = assignments.view(assignment_id, THIRD_ID)
    assert closed_view.workbench_available is False
    assert closed_view.assessed is False
    third = app.test_client()
    _login(third, app, sender, "third@example.invalid")
    assessment_response = third.post(
        f"/assignments/{assignment_id}",
        data={
            "csrf_token": _csrf(third),
            "domain_level": "working",
            "familiarity_level": "inspected",
            "confirmed": "yes",
        },
    )
    assert assessment_response.status_code == 302
    assert assessment_response.location == "/workshop"
    assert third.get(assessment_response.location).status_code == 200
    assert workshops.dashboard(THIRD_ID).groups[0].outstanding_assessments == ()

    engine = create_database_engine(database_path)
    sessions = session_factory(engine)
    try:
        with sessions.begin() as session:
            assignment = session.get(ReviewAssignment, assignment_id)
            submission = session.get(ReviewSubmission, receipt_id)
            assert assignment is not None
            assert assignment.participant_status == "partial"
            assert assignment.status == "submitted"
            assert assignment.completion_item_count == 1
            assert assignment.completion_total_count == 2
            assert assignment.closed_contributor_ids == [FIRST_ID, SECOND_ID, THIRD_ID]
            assert submission is not None
            stored = json.loads(
                (Path(app.config["SUBMISSION_ROOT"]) / submission.export_path).read_text()
            )
            assert stored["completion_type"] == "partial"
            assert stored["completion_item_count"] == 1
            assert stored["completion_total_count"] == 2
            session.get(WorkshopRound, "workshop-ipl").allow_additional_assignments = True
        assert app.extensions["musparql_processing"].process_next() == job_id
        audit = json.loads(
            (
                Path(app.config["CANDIDATE_ROOT"])
                / job_id
                / "audit.json"
            ).read_text()
        )
        assert audit["completion_type"] == "partial"
        assert audit["item_count"] == 1
        assert audit["total_item_count"] == 2
        assert workshops.dashboard(FIRST_ID).groups[0].can_claim is True
        next_assignment_id = workshops.claim_package(
            reviewer_id=FIRST_ID,
            group_id=group_id,
            package_id="package-synthetic",
        )
        assert next_assignment_id != assignment_id
    finally:
        engine.dispose()


def test_leave_is_non_mutating_and_abandon_closes_without_submission(
    workshop_app,
) -> None:
    app, sender, database_path, _bundle_root = workshop_app
    client = app.test_client()
    _login(client, app, sender, "first@example.invalid")
    workshops = app.extensions["musparql_workshops"]
    group_id = workshops.create_group(FIRST_ID)
    assignment_id = workshops.claim_package(
        reviewer_id=FIRST_ID,
        group_id=group_id,
        package_id="package-synthetic",
    )

    assignment_page = client.get(f"/assignments/{assignment_id}")
    assert b"Leave for now" in assignment_page.data
    assert b"Abandon assignment" in assignment_page.data
    assert client.get("/workshop").status_code == 200

    response = client.post(
        f"/assignments/{assignment_id}/abandon",
        data={"csrf_token": _csrf(client)},
    )
    assert response.status_code == 302
    assert "result=assignment-abandoned" in response.location

    engine = create_database_engine(database_path)
    sessions = session_factory(engine)
    try:
        with sessions() as session:
            assignment = session.get(ReviewAssignment, assignment_id)
            assert assignment is not None
            assert assignment.participant_status == "abandoned"
            assert assignment.completed_at is not None
            assert assignment.closed_contributor_ids == [FIRST_ID]
            assert list(session.scalars(select(ReviewSubmission))) == []
            assert list(session.scalars(select(ProcessingJob))) == []
    finally:
        engine.dispose()
    assert workshops.dashboard(FIRST_ID).groups[0].can_claim is False
    assert [
        item.assignment_id
        for item in workshops.dashboard(FIRST_ID).groups[0].outstanding_assessments
    ] == [assignment_id]

    outsider = app.test_client()
    _login(outsider, app, sender, "third@example.invalid")
    assert outsider.post(
        f"/assignments/{assignment_id}/abandon",
        data={"csrf_token": _csrf(outsider)},
    ).status_code == 403
    assert client.post(f"/assignments/{assignment_id}/abandon").status_code == 400
    assert client.post(
        f"/assignments/{assignment_id}/abandon",
        data={"csrf_token": _csrf(client)},
    ).status_code == 403
