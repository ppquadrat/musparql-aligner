"""Passwordless authentication and owner account-control services."""
from __future__ import annotations

from collections import OrderedDict, deque
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
import hashlib
import hmac
import secrets
import threading
import unicodedata
import uuid

from flask.config import Config
from sqlalchemy import func, select, update
from sqlalchemy.orm import Session, sessionmaker

from musparql.database.models import AuthSession, LoginCode, OwnerAuditEvent, Reviewer
from musparql.database.services import normalize_email
from .email import AsyncEmailDispatcher, EmailSender


UTC = timezone.utc


def utc_now() -> datetime:
    return datetime.now(UTC)


def timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def parse_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


class DigestRateLimiter:
    """Small single-process limiter that retains keyed digests, never addresses."""

    def __init__(
        self,
        *,
        secret: bytes,
        window_seconds: int,
        address_limit: int,
        context_limit: int,
        max_address_keys: int,
        max_context_keys: int,
    ) -> None:
        if max_address_keys < 1 or max_context_keys < 1:
            raise ValueError("Rate-limiter key bounds must be positive")
        self.secret = secret
        self.window_seconds = window_seconds
        self.address_limit = address_limit
        self.context_limit = context_limit
        self.max_address_keys = max_address_keys
        self.max_context_keys = max_context_keys
        self._address_events: OrderedDict[str, deque[float]] = OrderedDict()
        self._context_events: OrderedDict[str, deque[float]] = OrderedDict()
        self._lock = threading.Lock()

    def digest(self, namespace: str, value: str) -> str:
        payload = f"{namespace}\0{value}".encode("utf-8", "replace")
        return hmac.new(self.secret, payload, hashlib.sha256).hexdigest()

    def allow(self, email_normalized: str, request_context: str, now: datetime) -> tuple[bool, str]:
        address_key = self.digest("address", email_normalized)
        context_key = self.digest("context", request_context)
        cutoff = now.timestamp() - self.window_seconds
        with self._lock:
            address_queue = self._queue(
                self._address_events, address_key, cutoff, self.max_address_keys
            )
            context_queue = self._queue(
                self._context_events, context_key, cutoff, self.max_context_keys
            )
            allowed = (
                len(address_queue) < self.address_limit
                and len(context_queue) < self.context_limit
            )
            if allowed:
                address_queue.append(now.timestamp())
                context_queue.append(now.timestamp())
        return allowed, context_key

    def release(self, email_normalized: str, request_context: str, now: datetime) -> None:
        """Release a reservation whose asynchronous delivery failed."""

        keys_and_stores = (
            (self.digest("address", email_normalized), self._address_events),
            (self.digest("context", request_context), self._context_events),
        )
        with self._lock:
            for key, store in keys_and_stores:
                queue = store.get(key)
                if queue is None:
                    continue
                try:
                    queue.remove(now.timestamp())
                except ValueError:
                    continue
                if not queue:
                    del store[key]

    @staticmethod
    def _queue(
        store: OrderedDict[str, deque[float]],
        key: str,
        cutoff: float,
        max_keys: int,
    ) -> deque[float]:
        queue = store.get(key)
        if queue is not None:
            while queue and queue[0] <= cutoff:
                queue.popleft()
            if not queue:
                del store[key]
                queue = None
        if queue is None:
            while len(store) >= max_keys:
                store.popitem(last=False)
            queue = deque()
            store[key] = queue
        else:
            store.move_to_end(key)
        return queue


class AuthService:
    def __init__(
        self,
        *,
        sessions: sessionmaker[Session],
        sender: EmailSender,
        dispatcher: AsyncEmailDispatcher,
        limiter: DigestRateLimiter,
        config: Config,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        self.sessions = sessions
        self.sender = sender
        self.dispatcher = dispatcher
        self.limiter = limiter
        self.config = config
        self.clock = clock
        self.secret = config["APP_SECRET"].encode("utf-8")

    def _digest(self, namespace: str, value: str) -> str:
        return hmac.new(
            self.secret,
            f"{namespace}\0{value}".encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

    def request_login_code(self, email: str, request_context: str) -> str:
        now = self.clock()
        challenge_id = secrets.token_urlsafe(24)
        try:
            normalized = normalize_email(email)
        except ValueError:
            self._digest("dummy-login", email[:254])
            return challenge_id
        allowed, context_digest = self.limiter.allow(normalized, request_context, now)
        code = f"{secrets.randbelow(1_000_000):06d}"
        if allowed:
            future = self.dispatcher.submit(
                lambda: self._issue_and_send_login_code(
                    normalized, context_digest, challenge_id, code, now
                ),
                on_failure=lambda: self.limiter.release(normalized, request_context, now),
            )
            if future is None:
                self.limiter.release(normalized, request_context, now)
        else:
            self._digest("dummy-code", code)
        return challenge_id

    def _issue_and_send_login_code(
        self,
        normalized: str,
        context_digest: str,
        challenge_id: str,
        code: str,
        now: datetime,
    ) -> None:
        with self.sessions.begin() as session:
            reviewer = session.scalar(
                select(Reviewer).where(Reviewer.email_normalized == normalized)
            )
            if reviewer is None or reviewer.status not in {"invited", "active"}:
                self._digest("dummy-code", code)
                return
            cutoff = timestamp(
                now - timedelta(seconds=self.config["LOGIN_REQUEST_WINDOW_SECONDS"])
            )
            address_count = session.scalar(
                select(func.count())
                .select_from(LoginCode)
                .where(
                    LoginCode.email_normalized == normalized,
                    LoginCode.requested_at >= cutoff,
                )
            ) or 0
            context_count = session.scalar(
                select(func.count())
                .select_from(LoginCode)
                .where(
                    LoginCode.request_context_digest == context_digest,
                    LoginCode.requested_at >= cutoff,
                )
            ) or 0
            if (
                address_count >= self.config["LOGIN_REQUESTS_PER_ADDRESS"]
                or context_count >= self.config["LOGIN_REQUESTS_PER_CONTEXT"]
            ):
                return
            session.execute(
                update(LoginCode)
                .where(
                    LoginCode.email_normalized == normalized,
                    LoginCode.consumed_at.is_(None),
                )
                .values(consumed_at=timestamp(now))
            )
            session.add(
                LoginCode(
                    id=challenge_id,
                    email_normalized=normalized,
                    code_hash=self._digest("login-code", f"{challenge_id}\0{code}"),
                    requested_at=timestamp(now),
                    expires_at=timestamp(
                        now + timedelta(seconds=self.config["LOGIN_CODE_TTL_SECONDS"])
                    ),
                    consumed_at=None,
                    failed_attempt_count=0,
                    request_context_digest=context_digest,
                )
            )
            self.sender.send_login_code(reviewer.email_display, code)

    def verify_login_code(
        self, challenge_id: str, code: str, *, remembered: bool, current_token: str | None
    ) -> tuple[str, Reviewer] | None:
        now = self.clock()
        if not challenge_id or len(challenge_id) > 128 or not (len(code) == 6 and code.isascii() and code.isdigit()):
            self._digest("dummy-verify", code[:32])
            return None
        with self.sessions.begin() as session:
            login_code = session.get(LoginCode, challenge_id)
            if login_code is None or login_code.consumed_at is not None:
                self._digest("dummy-verify", f"{challenge_id}\0{code}")
                return None
            if (
                parse_timestamp(login_code.expires_at) <= now
                or login_code.failed_attempt_count >= self.config["LOGIN_CODE_MAX_ATTEMPTS"]
            ):
                login_code.consumed_at = timestamp(now)
                return None
            expected = self._digest("login-code", f"{challenge_id}\0{code}")
            if not hmac.compare_digest(login_code.code_hash, expected):
                login_code.failed_attempt_count += 1
                if login_code.failed_attempt_count >= self.config["LOGIN_CODE_MAX_ATTEMPTS"]:
                    login_code.consumed_at = timestamp(now)
                return None
            reviewer = session.scalar(
                select(Reviewer).where(Reviewer.email_normalized == login_code.email_normalized)
            )
            if reviewer is None or reviewer.status not in {"invited", "active"}:
                login_code.consumed_at = timestamp(now)
                return None
            login_code.consumed_at = timestamp(now)
            if reviewer.status == "invited":
                reviewer.status = "active"
                reviewer.updated_at = timestamp(now)
            if current_token:
                self._revoke_token(session, current_token, now)
            is_owner = reviewer.id == self.config["OWNER_REVIEWER_ID"]
            remembered = bool(remembered and not is_owner)
            absolute_seconds = self.config[
                "OWNER_ABSOLUTE_SECONDS"
                if is_owner
                else "REMEMBERED_ABSOLUTE_SECONDS"
                if remembered
                else "REVIEWER_ABSOLUTE_SECONDS"
            ]
            raw_token = secrets.token_urlsafe(32)
            session.add(
                AuthSession(
                    id=str(uuid.uuid4()),
                    reviewer_id=reviewer.id,
                    token_hash=self._digest("session", raw_token),
                    created_at=timestamp(now),
                    last_used_at=timestamp(now),
                    expires_at=timestamp(now + timedelta(seconds=absolute_seconds)),
                    revoked_at=None,
                    remembered=remembered,
                )
            )
            return raw_token, reviewer

    def authenticate(self, raw_token: str | None) -> tuple[Reviewer, AuthSession] | None:
        if not raw_token or len(raw_token) > 256:
            return None
        now = self.clock()
        with self.sessions.begin() as session:
            auth_session = session.scalar(
                select(AuthSession).where(
                    AuthSession.token_hash == self._digest("session", raw_token)
                )
            )
            if auth_session is None or auth_session.revoked_at is not None:
                return None
            reviewer = session.get(Reviewer, auth_session.reviewer_id)
            if reviewer is None or reviewer.status != "active":
                auth_session.revoked_at = timestamp(now)
                return None
            is_owner = reviewer.id == self.config["OWNER_REVIEWER_ID"]
            idle_seconds = self.config[
                "OWNER_IDLE_SECONDS"
                if is_owner
                else "REMEMBERED_IDLE_SECONDS"
                if auth_session.remembered
                else "REVIEWER_IDLE_SECONDS"
            ]
            idle_deadline = parse_timestamp(auth_session.last_used_at) + timedelta(seconds=idle_seconds)
            if parse_timestamp(auth_session.expires_at) <= now or idle_deadline <= now:
                auth_session.revoked_at = timestamp(now)
                return None
            auth_session.last_used_at = timestamp(now)
            session.flush()
            session.expunge(reviewer)
            session.expunge(auth_session)
            return reviewer, auth_session

    def logout(self, raw_token: str | None) -> None:
        if not raw_token:
            return
        now = self.clock()
        with self.sessions.begin() as session:
            self._revoke_token(session, raw_token, now)

    def logout_all(self, reviewer_id: str) -> None:
        now = timestamp(self.clock())
        with self.sessions.begin() as session:
            session.execute(
                update(AuthSession)
                .where(AuthSession.reviewer_id == reviewer_id, AuthSession.revoked_at.is_(None))
                .values(revoked_at=now)
            )

    def owner_is_recent(self, auth_session: AuthSession) -> bool:
        return self.clock() - parse_timestamp(auth_session.created_at) <= timedelta(
            seconds=self.config["OWNER_RECENT_AUTH_SECONDS"]
        )

    def invite(self, actor_id: str, name: str, email: str) -> Reviewer:
        name = unicodedata.normalize("NFC", name).strip()
        if not name or len(name) > 200:
            raise ValueError("A reviewer name of at most 200 characters is required")
        display = unicodedata.normalize("NFC", email).strip()
        normalized = normalize_email(display)
        now = self.clock()
        with self.sessions.begin() as session:
            if session.scalar(select(Reviewer.id).where(Reviewer.email_normalized == normalized)):
                raise ValueError("That email address already has an account")
            reviewer_id = self._allocate_reviewer_id(session)
            reviewer = Reviewer(
                id=reviewer_id,
                name=name,
                affiliation="",
                email_display=display,
                email_normalized=normalized,
                status="invited",
                created_at=timestamp(now),
                updated_at=timestamp(now),
                privacy_notice_version=None,
                privacy_notice_acknowledged_at=None,
            )
            session.add(reviewer)
            self._audit(session, actor_id, reviewer_id, "invite", now)
            self.sender.send_invitation(display, "/auth/login")
        return reviewer

    def change_reviewer_status(self, actor_id: str, target_id: str, action: str) -> None:
        if target_id == self.config["OWNER_REVIEWER_ID"]:
            raise ValueError("The configured owner account cannot be changed in the web UI")
        transitions = {"disable": {"invited", "active"}, "restore": {"disabled"}}
        if action not in transitions:
            raise ValueError("Unsupported account action")
        allowed_from = transitions[action]
        now = self.clock()
        with self.sessions.begin() as session:
            reviewer = session.get(Reviewer, target_id)
            if reviewer is None or reviewer.status not in allowed_from:
                raise ValueError("The account is not eligible for that action")
            if action == "disable":
                reviewer.disabled_from_status = reviewer.status
                reviewer.status = "disabled"
            else:
                reviewer.status = reviewer.disabled_from_status or "active"
                reviewer.disabled_from_status = None
            reviewer.updated_at = timestamp(now)
            if action == "disable":
                self._revoke_reviewer_sessions(session, target_id, now)
            self._audit(session, actor_id, target_id, action, now)

    def delete_reviewer_identity(self, actor_id: str, target_id: str) -> None:
        if target_id == self.config["OWNER_REVIEWER_ID"]:
            raise ValueError("The configured owner account cannot be changed in the web UI")
        now = self.clock()
        with self.sessions.begin() as session:
            reviewer = session.get(Reviewer, target_id)
            if reviewer is None or reviewer.status == "withdrawn":
                raise ValueError("The account is not eligible for deletion")
            old_email = reviewer.email_normalized
            reviewer.name = "Withdrawn reviewer"
            reviewer.affiliation = ""
            random_mailbox = secrets.token_hex(16)
            reviewer.email_display = f"withdrawn-{random_mailbox}@example.invalid"
            reviewer.email_normalized = reviewer.email_display
            reviewer.status = "withdrawn"
            reviewer.disabled_from_status = None
            reviewer.updated_at = timestamp(now)
            reviewer.privacy_notice_version = None
            reviewer.privacy_notice_acknowledged_at = None
            self._revoke_reviewer_sessions(session, target_id, now)
            session.query(LoginCode).filter(LoginCode.email_normalized == old_email).delete()
            self._audit(session, actor_id, target_id, "delete", now)

    def list_reviewers(self) -> list[Reviewer]:
        with self.sessions() as session:
            reviewers = list(session.scalars(select(Reviewer).order_by(Reviewer.id)))
            for reviewer in reviewers:
                session.expunge(reviewer)
            return reviewers

    def _allocate_reviewer_id(self, session: Session) -> str:
        for _ in range(100):
            candidate = f"reviewer-{secrets.randbelow(9999) + 1:04d}"
            if candidate != self.config["OWNER_REVIEWER_ID"] and session.get(Reviewer, candidate) is None:
                return candidate
        raise RuntimeError("Could not allocate a pseudonymous reviewer ID")

    def _revoke_token(self, session: Session, raw_token: str, now: datetime) -> None:
        session.execute(
            update(AuthSession)
            .where(
                AuthSession.token_hash == self._digest("session", raw_token),
                AuthSession.revoked_at.is_(None),
            )
            .values(revoked_at=timestamp(now))
        )

    @staticmethod
    def _revoke_reviewer_sessions(session: Session, reviewer_id: str, now: datetime) -> None:
        session.execute(
            update(AuthSession)
            .where(AuthSession.reviewer_id == reviewer_id, AuthSession.revoked_at.is_(None))
            .values(revoked_at=timestamp(now))
        )

    @staticmethod
    def _audit(session: Session, actor_id: str, target_id: str, action: str, now: datetime) -> None:
        session.add(
            OwnerAuditEvent(
                id=str(uuid.uuid4()),
                actor_reviewer_id=actor_id,
                target_reviewer_id=target_id,
                action=action,
                created_at=timestamp(now),
            )
        )
