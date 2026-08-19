from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
import threading

import pytest
from sqlalchemy import inspect, select
from sqlalchemy.exc import IntegrityError

from musparql.database import create_database_engine, session_factory
from musparql.database.migrations import current_revision, upgrade_database
from musparql.database.models import AuthSession, LoginCode, OwnerAuditEvent, Reviewer
from musparql.web import create_app
from musparql.web.auth import DigestRateLimiter, timestamp, utc_now
from musparql.web.email import SyntheticEmailSender


SECRET = "synthetic-test-secret-that-is-at-least-32-bytes"
OWNER_ID = "reviewer-0001"


def reviewer(reviewer_id: str, email: str, *, status: str = "active") -> Reviewer:
    now = timestamp(utc_now())
    return Reviewer(
        id=reviewer_id,
        name=f"Synthetic {reviewer_id}",
        affiliation="Synthetic Institute",
        email_display=email,
        email_normalized=email,
        status=status,
        created_at=now,
        updated_at=now,
        privacy_notice_version=None,
        privacy_notice_acknowledged_at=None,
    )


@pytest.fixture
def portal_app(tmp_path: Path):
    database_path = tmp_path / "phase3.sqlite3"
    upgrade_database(database_path)
    engine = create_database_engine(database_path)
    sessions = session_factory(engine)
    with sessions.begin() as session:
        session.add(reviewer(OWNER_ID, "owner@example.invalid"))
        session.add(reviewer("reviewer-0042", "reviewer@example.invalid", status="invited"))
        session.add(reviewer("reviewer-0043", "active@example.invalid"))
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
        }
    )
    yield app, sender, database_path
    app.extensions["musparql_email_dispatcher"].shutdown()
    app.extensions["musparql_engine"].dispose()


def csrf(client) -> str:
    cookie = client.get_cookie("musparql_csrf", path="/")
    if cookie is None:
        client.get("/")
        cookie = client.get_cookie("musparql_csrf", path="/")
    assert cookie is not None
    return cookie.value


def login(client, sender: SyntheticEmailSender, email: str, *, remembered: bool = False):
    outbox_position = sender.position()
    response = client.post(
        "/auth/login",
        data={"csrf_token": csrf(client), "email": email},
    )
    assert response.status_code == 302
    message = sender.wait_for("login_code", email, after_index=outbox_position)
    client.application.extensions["musparql_email_dispatcher"].wait_for_idle()
    data = {"csrf_token": csrf(client), "code": message.value}
    if remembered:
        data["remembered"] = "yes"
    response = client.post("/auth/verify", data=data)
    assert response.status_code == 302
    return response


def sessions_for(database_path: Path, reviewer_id: str) -> list[AuthSession]:
    engine = create_database_engine(database_path)
    sessions = session_factory(engine)
    try:
        with sessions() as session:
            return list(
                session.scalars(
                    select(AuthSession)
                    .where(AuthSession.reviewer_id == reviewer_id)
                    .order_by(AuthSession.created_at)
                )
            )
    finally:
        engine.dispose()


def test_digest_limiter_has_hard_key_bounds() -> None:
    limiter = DigestRateLimiter(
        secret=SECRET.encode(),
        window_seconds=900,
        address_limit=3,
        context_limit=10,
        max_address_keys=3,
        max_context_keys=2,
    )
    now = datetime(2026, 8, 19, tzinfo=timezone.utc)
    for index in range(20):
        limiter.allow(
            f"synthetic-{index}@example.invalid",
            f"192.0.2.{index}\0synthetic-agent",
            now,
        )
    assert len(limiter._address_events) <= 3
    assert len(limiter._context_events) <= 2


def test_phase3_migration_is_current_and_audit_is_append_only(portal_app) -> None:
    _app, _sender, database_path = portal_app
    assert current_revision(database_path) == "20260819_04"
    engine = create_database_engine(database_path)
    sessions = session_factory(engine)
    try:
        with sessions.begin() as session:
            event = OwnerAuditEvent(
                id="synthetic-audit-event",
                actor_reviewer_id=OWNER_ID,
                target_reviewer_id="reviewer-0042",
                action="disable",
                created_at=timestamp(utc_now()),
            )
            session.add(event)
        with sessions.begin() as session:
            event = session.get(OwnerAuditEvent, "synthetic-audit-event")
            assert event is not None
            event.action = "restore"
            with pytest.raises(IntegrityError, match="append-only"):
                session.flush()
    finally:
        engine.dispose()


def test_auth_hardening_migrates_an_existing_phase3_database(tmp_path: Path) -> None:
    database_path = tmp_path / "existing-phase3.sqlite3"
    upgrade_database(database_path, "20260819_02")
    assert current_revision(database_path) == "20260819_02"
    upgrade_database(database_path)
    assert current_revision(database_path) == "20260819_04"

    engine = create_database_engine(database_path)
    try:
        columns = {column["name"] for column in inspect(engine).get_columns("reviewers")}
        assert "disabled_from_status" in columns
    finally:
        engine.dispose()


def test_login_response_does_not_disclose_membership_and_codes_are_hashed(
    portal_app, caplog
) -> None:
    app, sender, database_path = portal_app
    invited = app.test_client()
    unknown = app.test_client()
    invited_response = invited.post(
        "/auth/login",
        data={"csrf_token": csrf(invited), "email": "reviewer@example.invalid"},
    )
    unknown_response = unknown.post(
        "/auth/login",
        data={"csrf_token": csrf(unknown), "email": "unknown@example.invalid"},
    )
    assert (invited_response.status_code, invited_response.location) == (
        unknown_response.status_code,
        unknown_response.location,
    )
    code = sender.wait_for("login_code", "reviewer@example.invalid").value
    app.extensions["musparql_email_dispatcher"].wait_for_idle()
    assert code not in invited_response.get_data(as_text=True)
    assert "reviewer@example.invalid" not in caplog.text
    assert code not in caplog.text

    engine = create_database_engine(database_path)
    sessions = session_factory(engine)
    try:
        with sessions() as session:
            stored = session.scalar(select(LoginCode))
            assert stored is not None
            assert stored.code_hash != code
            assert "reviewer@example.invalid" not in stored.request_context_digest
    finally:
        engine.dispose()


def test_login_response_does_not_wait_for_email_delivery_or_hold_database_lock(
    portal_app,
) -> None:
    app, sender, database_path = portal_app
    started = threading.Event()
    release = threading.Event()

    class BlockingSender:
        def send_login_code(self, recipient: str, code: str) -> None:
            started.set()
            assert release.wait(2)
            sender.send_login_code(recipient, code)

        def send_invitation(self, recipient: str, login_path: str) -> None:
            sender.send_invitation(recipient, login_path)

    app.extensions["musparql_auth"].sender = BlockingSender()
    client = app.test_client()
    response = client.post(
        "/auth/login",
        data={"csrf_token": csrf(client), "email": "reviewer@example.invalid"},
    )
    assert response.status_code == 302
    assert started.wait(1)

    engine = create_database_engine(database_path, timeout_seconds=0.05)
    sessions = session_factory(engine)
    try:
        with sessions.begin() as session:
            active = session.get(Reviewer, "reviewer-0043")
            assert active is not None
            active.affiliation = "Updated while delivery is blocked"
    finally:
        engine.dispose()

    release.set()
    app.extensions["musparql_email_dispatcher"].wait_for_idle()


def test_failed_login_delivery_rolls_back_and_does_not_consume_limit(
    portal_app, caplog
) -> None:
    app, sender, database_path = portal_app

    class FailingSender:
        def send_login_code(self, recipient: str, code: str) -> None:
            raise RuntimeError("synthetic provider failure")

        def send_invitation(self, recipient: str, login_path: str) -> None:
            sender.send_invitation(recipient, login_path)

    auth = app.extensions["musparql_auth"]
    auth.sender = FailingSender()
    client = app.test_client()
    client.post(
        "/auth/login",
        data={"csrf_token": csrf(client), "email": "reviewer@example.invalid"},
    )
    app.extensions["musparql_email_dispatcher"].wait_for_idle()

    engine = create_database_engine(database_path)
    sessions = session_factory(engine)
    try:
        with sessions() as session:
            assert session.scalar(select(LoginCode)) is None
    finally:
        engine.dispose()

    auth.sender = sender
    for _ in range(app.config["LOGIN_REQUESTS_PER_ADDRESS"]):
        client.post(
            "/auth/login",
            data={"csrf_token": csrf(client), "email": "reviewer@example.invalid"},
        )
    app.extensions["musparql_email_dispatcher"].wait_for_idle()
    delivered = [
        message
        for message in sender.outbox
        if message.kind == "login_code" and message.recipient == "reviewer@example.invalid"
    ]
    assert len(delivered) == app.config["LOGIN_REQUESTS_PER_ADDRESS"]
    assert "synthetic provider failure" not in caplog.text


def test_failed_replacement_delivery_preserves_last_delivered_code(portal_app) -> None:
    app, sender, _database_path = portal_app
    auth = app.extensions["musparql_auth"]
    first_challenge = auth.request_login_code(
        "reviewer@example.invalid", "192.0.2.10\0synthetic-agent"
    )
    first_message = sender.wait_for("login_code", "reviewer@example.invalid")
    app.extensions["musparql_email_dispatcher"].wait_for_idle()

    class FailingSender:
        def send_login_code(self, recipient: str, code: str) -> None:
            raise RuntimeError("synthetic provider failure")

        def send_invitation(self, recipient: str, login_path: str) -> None:
            sender.send_invitation(recipient, login_path)

    auth.sender = FailingSender()
    auth.request_login_code(
        "reviewer@example.invalid", "192.0.2.11\0synthetic-agent"
    )
    app.extensions["musparql_email_dispatcher"].wait_for_idle()

    assert auth.verify_login_code(
        first_challenge,
        first_message.value,
        remembered=False,
        current_token=None,
    ) is not None


def test_code_is_single_use_and_attempt_limited(portal_app) -> None:
    app, sender, database_path = portal_app
    client = app.test_client()
    client.post(
        "/auth/login",
        data={"csrf_token": csrf(client), "email": "reviewer@example.invalid"},
    )
    code = sender.wait_for("login_code", "reviewer@example.invalid").value
    app.extensions["musparql_email_dispatcher"].wait_for_idle()
    for _ in range(app.config["LOGIN_CODE_MAX_ATTEMPTS"]):
        response = client.post(
            "/auth/verify", data={"csrf_token": csrf(client), "code": "000000"}
        )
        assert response.status_code == 200
    response = client.post(
        "/auth/verify", data={"csrf_token": csrf(client), "code": code}
    )
    assert response.status_code == 200
    assert client.get_cookie("musparql_session", path="/") is None

    engine = create_database_engine(database_path)
    sessions = session_factory(engine)
    try:
        with sessions() as session:
            stored = session.scalar(select(LoginCode))
            assert stored is not None
            assert stored.consumed_at is not None
            assert stored.failed_attempt_count == app.config["LOGIN_CODE_MAX_ATTEMPTS"]
    finally:
        engine.dispose()


def test_replacement_expires_prior_code_and_requests_are_throttled(portal_app) -> None:
    app, sender, database_path = portal_app
    client = app.test_client()
    for _ in range(app.config["LOGIN_REQUESTS_PER_ADDRESS"] + 1):
        client.post(
            "/auth/login",
            data={"csrf_token": csrf(client), "email": "reviewer@example.invalid"},
        )
    app.extensions["musparql_email_dispatcher"].wait_for_idle()
    delivered = [
        message for message in sender.outbox
        if message.kind == "login_code" and message.recipient == "reviewer@example.invalid"
    ]
    assert len(delivered) == app.config["LOGIN_REQUESTS_PER_ADDRESS"]

    engine = create_database_engine(database_path)
    sessions = session_factory(engine)
    try:
        with sessions() as session:
            codes = list(
                session.scalars(
                    select(LoginCode)
                    .where(LoginCode.email_normalized == "reviewer@example.invalid")
                    .order_by(LoginCode.requested_at)
                )
            )
            assert len(codes) == app.config["LOGIN_REQUESTS_PER_ADDRESS"]
            assert all(item.consumed_at is not None for item in codes[:-1])
            assert codes[-1].consumed_at is None
    finally:
        engine.dispose()


def test_expired_code_is_rejected(portal_app) -> None:
    app, sender, database_path = portal_app
    client = app.test_client()
    client.post(
        "/auth/login",
        data={"csrf_token": csrf(client), "email": "reviewer@example.invalid"},
    )
    code = sender.wait_for("login_code", "reviewer@example.invalid").value
    app.extensions["musparql_email_dispatcher"].wait_for_idle()
    engine = create_database_engine(database_path)
    sessions = session_factory(engine)
    try:
        with sessions.begin() as session:
            stored = session.scalar(select(LoginCode).where(LoginCode.consumed_at.is_(None)))
            stored.expires_at = timestamp(utc_now() - timedelta(seconds=1))
        response = client.post(
            "/auth/verify", data={"csrf_token": csrf(client), "code": code}
        )
        assert response.status_code == 200
        assert client.get_cookie("musparql_session", path="/") is None
        with sessions() as session:
            stored = session.scalar(select(LoginCode))
            assert stored.consumed_at is not None
    finally:
        engine.dispose()


def test_successful_login_activates_invitation_and_rotates_existing_session(portal_app) -> None:
    app, sender, database_path = portal_app
    client = app.test_client()
    login(client, sender, "active@example.invalid", remembered=True)
    first_token = client.get_cookie("musparql_session", path="/")
    assert first_token is not None
    login(client, sender, "reviewer@example.invalid")
    second_token = client.get_cookie("musparql_session", path="/")
    assert second_token is not None and second_token.value != first_token.value

    first_sessions = sessions_for(database_path, "reviewer-0043")
    assert len(first_sessions) == 1 and first_sessions[0].revoked_at is not None
    invited_sessions = sessions_for(database_path, "reviewer-0042")
    assert len(invited_sessions) == 1 and not invited_sessions[0].remembered
    engine = create_database_engine(database_path)
    sessions = session_factory(engine)
    try:
        with sessions() as session:
            assert session.get(Reviewer, "reviewer-0042").status == "active"
    finally:
        engine.dispose()


def test_logout_all_revokes_other_browser_and_shared_browser_default(portal_app) -> None:
    app, sender, database_path = portal_app
    first = app.test_client()
    second = app.test_client()
    login(first, sender, "active@example.invalid")
    login(second, sender, "active@example.invalid", remembered=True)
    cookie = first.get_cookie("musparql_session", path="/")
    assert cookie is not None and cookie.expires is None
    response = first.post("/auth/logout-all", data={"csrf_token": csrf(first)})
    assert response.status_code == 302
    assert first.get_cookie("musparql_session", path="/") is None
    assert second.get("/").status_code == 200
    assert b"Invitation-only review" in second.get("/").data
    assert all(item.revoked_at is not None for item in sessions_for(database_path, "reviewer-0043"))


def test_idle_expiry_revokes_server_session(portal_app) -> None:
    app, sender, database_path = portal_app
    client = app.test_client()
    login(client, sender, "active@example.invalid")
    engine = create_database_engine(database_path)
    sessions = session_factory(engine)
    try:
        with sessions.begin() as session:
            stored = session.scalar(
                select(AuthSession).where(AuthSession.reviewer_id == "reviewer-0043")
            )
            stored.last_used_at = timestamp(
                utc_now() - timedelta(seconds=app.config["REVIEWER_IDLE_SECONDS"] + 1)
            )
        response = client.get("/")
        assert b"Invitation-only review" in response.data
        with sessions() as session:
            stored = session.scalar(
                select(AuthSession).where(AuthSession.reviewer_id == "reviewer-0043")
            )
            assert stored.revoked_at is not None
    finally:
        engine.dispose()


def test_absolute_expiry_revokes_server_session(portal_app) -> None:
    app, sender, database_path = portal_app
    client = app.test_client()
    login(client, sender, "active@example.invalid", remembered=True)
    engine = create_database_engine(database_path)
    sessions = session_factory(engine)
    try:
        with sessions.begin() as session:
            stored = session.scalar(
                select(AuthSession).where(AuthSession.reviewer_id == "reviewer-0043")
            )
            stored.expires_at = timestamp(utc_now() - timedelta(seconds=1))
        assert b"Invitation-only review" in client.get("/").data
        with sessions() as session:
            stored = session.scalar(
                select(AuthSession).where(AuthSession.reviewer_id == "reviewer-0043")
            )
            assert stored.revoked_at is not None
    finally:
        engine.dispose()


def test_csrf_cookie_and_security_headers(portal_app) -> None:
    app, _sender, _database_path = portal_app
    client = app.test_client()
    response = client.get("/")
    assert "HttpOnly" in response.headers.getlist("Set-Cookie")[0]
    assert response.headers["Content-Security-Policy"].startswith("default-src 'self'")
    assert response.headers["X-Frame-Options"] == "DENY"
    assert response.headers["Cache-Control"] == "no-store"
    rejected = client.post("/auth/login", data={"email": "reviewer@example.invalid"})
    assert rejected.status_code == 400


def test_owner_controls_require_recent_auth_and_audit_actions(portal_app) -> None:
    app, sender, database_path = portal_app
    owner = app.test_client()
    reviewer_client = app.test_client()
    login(reviewer_client, sender, "active@example.invalid", remembered=True)
    login(owner, sender, "owner@example.invalid", remembered=True)
    owner_cookie = owner.get_cookie("musparql_session", path="/")
    assert owner_cookie is not None and owner_cookie.expires is None

    response = owner.post(
        "/owner/reviewers/reviewer-0043/disable",
        data={"csrf_token": csrf(owner)},
    )
    assert response.status_code == 302
    assert b"Invitation-only review" in reviewer_client.get("/").data

    response = owner.post(
        "/owner/reviewers/reviewer-0043/restore",
        data={"csrf_token": csrf(owner)},
    )
    assert response.status_code == 302
    response = owner.post(
        "/owner/invitations",
        data={
            "csrf_token": csrf(owner),
            "name": "Synthetic Invitee",
            "email": "invitee@example.invalid",
        },
    )
    assert response.status_code == 302
    assert sender.outbox[-1].kind == "invitation"

    engine = create_database_engine(database_path)
    sessions = session_factory(engine)
    try:
        with sessions() as session:
            invitee = session.scalar(
                select(Reviewer).where(Reviewer.email_normalized == "invitee@example.invalid")
            )
            invitee_id = invitee.id
        response = owner.post(
            f"/owner/reviewers/{invitee_id}/delete",
            data={"csrf_token": csrf(owner), "confirm": invitee_id},
        )
        assert response.status_code == 302
        with sessions() as session:
            invitee = session.get(Reviewer, invitee_id)
            assert invitee.status == "withdrawn"
            assert invitee.name == "Withdrawn reviewer"
            assert "invitee@example.invalid" not in invitee.email_normalized
            actions = list(
                session.scalars(select(OwnerAuditEvent.action).order_by(OwnerAuditEvent.created_at))
            )
            assert actions == ["disable", "restore", "invite", "delete"]

        with sessions.begin() as session:
            owner_session = session.scalar(
                select(AuthSession).where(AuthSession.reviewer_id == OWNER_ID)
            )
            owner_session.created_at = timestamp(
                utc_now() - timedelta(seconds=app.config["OWNER_RECENT_AUTH_SECONDS"] + 1)
            )
        response = owner.post(
            "/owner/reviewers/reviewer-0043/disable",
            data={"csrf_token": csrf(owner)},
        )
        assert response.status_code == 401
    finally:
        engine.dispose()


def test_failed_invitation_rolls_back_and_can_be_retried(portal_app) -> None:
    app, sender, database_path = portal_app
    owner = app.test_client()
    login(owner, sender, "owner@example.invalid")

    class FailingInvitationSender:
        def send_login_code(self, recipient: str, code: str) -> None:
            sender.send_login_code(recipient, code)

        def send_invitation(self, recipient: str, login_path: str) -> None:
            raise RuntimeError("synthetic provider failure")

    auth = app.extensions["musparql_auth"]
    auth.sender = FailingInvitationSender()
    response = owner.post(
        "/owner/invitations",
        data={
            "csrf_token": csrf(owner),
            "name": "Retry Invitee",
            "email": "retry@example.invalid",
        },
    )
    assert response.status_code == 302
    assert "delivery-failed" in response.location

    engine = create_database_engine(database_path)
    sessions = session_factory(engine)
    try:
        with sessions() as session:
            assert session.scalar(
                select(Reviewer).where(Reviewer.email_normalized == "retry@example.invalid")
            ) is None
            assert session.scalar(
                select(OwnerAuditEvent).where(OwnerAuditEvent.action == "invite")
            ) is None
    finally:
        engine.dispose()

    auth.sender = sender
    response = owner.post(
        "/owner/invitations",
        data={
            "csrf_token": csrf(owner),
            "name": "Retry Invitee",
            "email": "retry@example.invalid",
        },
    )
    assert response.status_code == 302
    assert "result=invited" in response.location


def test_invitation_delivery_does_not_hold_database_lock(portal_app) -> None:
    app, sender, database_path = portal_app
    started = threading.Event()
    release = threading.Event()
    failures: list[BaseException] = []

    class BlockingInvitationSender:
        def send_login_code(self, recipient: str, code: str) -> None:
            sender.send_login_code(recipient, code)

        def send_invitation(self, recipient: str, login_path: str) -> None:
            started.set()
            assert release.wait(2)
            sender.send_invitation(recipient, login_path)

    auth = app.extensions["musparql_auth"]
    auth.sender = BlockingInvitationSender()

    def invite() -> None:
        try:
            auth.invite(
                OWNER_ID,
                "Concurrent Invitee",
                "concurrent@example.invalid",
            )
        except BaseException as exc:
            failures.append(exc)

    worker = threading.Thread(target=invite)
    worker.start()
    assert started.wait(1)

    engine = create_database_engine(database_path, timeout_seconds=0.05)
    sessions = session_factory(engine)
    try:
        with sessions.begin() as session:
            active = session.get(Reviewer, "reviewer-0043")
            assert active is not None
            active.affiliation = "Updated during invitation delivery"
    finally:
        engine.dispose()

    release.set()
    worker.join(2)
    assert not worker.is_alive()
    assert failures == []


def test_restore_preserves_pending_invitation_state(portal_app) -> None:
    app, sender, database_path = portal_app
    owner = app.test_client()
    login(owner, sender, "owner@example.invalid")
    for action in ("disable", "restore"):
        response = owner.post(
            f"/owner/reviewers/reviewer-0042/{action}",
            data={"csrf_token": csrf(owner)},
        )
        assert response.status_code == 302

    engine = create_database_engine(database_path)
    sessions = session_factory(engine)
    try:
        with sessions() as session:
            restored = session.get(Reviewer, "reviewer-0042")
            assert restored is not None
            assert restored.status == "invited"
            assert restored.disabled_from_status is None
    finally:
        engine.dispose()


def test_non_owner_cannot_access_owner_accounts(portal_app) -> None:
    app, sender, _database_path = portal_app
    client = app.test_client()
    login(client, sender, "active@example.invalid")
    assert client.get("/owner/reviewers").status_code == 403
    assert client.post(
        "/owner/reviewers/reviewer-0042/disable",
        data={"csrf_token": csrf(client)},
    ).status_code == 403


def test_app_fails_closed_without_explicit_email_sender(tmp_path: Path) -> None:
    database_path = tmp_path / "fail-closed.sqlite3"
    upgrade_database(database_path)
    engine = create_database_engine(database_path)
    sessions = session_factory(engine)
    with sessions.begin() as session:
        session.add(reviewer(OWNER_ID, "owner@example.invalid"))
    engine.dispose()
    with pytest.raises(RuntimeError, match="No email sender"):
        create_app(
            {
                "TESTING": True,
                "DATABASE_PATH": database_path,
                "APP_SECRET": SECRET,
                "OWNER_REVIEWER_ID": OWNER_ID,
                "COOKIE_SECURE": False,
            }
        )
