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
from sqlalchemy import delete, func, select, text, update
from sqlalchemy.orm import Session, sessionmaker

from musparql.database.models import (
    AuthSession,
    ExpertiseDomain,
    LoginCode,
    OwnerAuditEvent,
    Reviewer,
    ReviewerDomainExpertise,
    ReviewerExperience,
    ReviewerLanguage,
    WorkshopAdmissionAttempt,
    WorkshopAdmissionNonce,
    WorkshopEntryCode,
    WorkshopEntryRedemption,
    WorkshopSessionReset,
)
from musparql.database.services import (
    WorkshopAdmissionError,
    WorkshopAdmissionService,
    normalize_email,
)
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
        recipient: str | None = None
        previous_code_ids: list[str] = []
        with self.sessions.begin() as session:
            reviewer = session.scalar(
                select(Reviewer).where(Reviewer.email_normalized == normalized)
            )
            if (
                reviewer is None
                or reviewer.status not in {"invited", "active"}
                or reviewer.registration_method == "workshop_code"
            ):
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
            previous_code_ids = list(
                session.scalars(
                    select(LoginCode.id).where(
                        LoginCode.email_normalized == normalized,
                        LoginCode.consumed_at.is_(None),
                    )
                )
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
            recipient = reviewer.email_display
        assert recipient is not None
        try:
            self.sender.send_login_code(recipient, code)
        except Exception:
            # The challenge was committed before external I/O so provider latency
            # never holds SQLite's write lock. Remove only an unused challenge;
            # a code that was already consumed must retain its audit evidence.
            with self.sessions.begin() as session:
                session.execute(
                    delete(LoginCode).where(
                        LoginCode.id == challenge_id,
                        LoginCode.consumed_at.is_(None),
                    )
                )
            raise
        # Invalidate only the challenges that existed when this replacement was
        # created. A failed delivery therefore leaves the last delivered code
        # usable, while overlapping requests cannot invalidate a newer code.
        if previous_code_ids:
            with self.sessions.begin() as session:
                session.execute(
                    update(LoginCode)
                    .where(
                        LoginCode.id.in_(previous_code_ids),
                        LoginCode.consumed_at.is_(None),
                    )
                    .values(consumed_at=timestamp(self.clock()))
                )

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
            if (
                reviewer is None
                or reviewer.status not in {"invited", "active"}
                or reviewer.registration_method == "workshop_code"
            ):
                login_code.consumed_at = timestamp(now)
                return None
            login_code.consumed_at = timestamp(now)
            if reviewer.email_verified_at is None:
                reviewer.email_verified_at = timestamp(now)
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

    def redeem_workshop_code(
        self,
        code: str,
        request_context: str,
        *,
        current_token: str | None,
        admission_nonce: str,
    ) -> tuple[str, Reviewer] | None:
        """Atomically redeem a shared code into a distinct identity and session."""

        from .workshop_admission import normalize_entry_code

        now = self.clock()
        try:
            normalized = normalize_entry_code(code)
        except ValueError:
            normalized = "INVALID0"
        if not self._record_workshop_attempt(normalized, request_context, now):
            self._digest("dummy-workshop-code", normalized)
            return None
        try:
            normalized = normalize_entry_code(code)
        except ValueError:
            self._digest("dummy-workshop-code", normalized)
            return None

        from .workshop_admission import WorkshopEntryCodeService

        now_text = timestamp(now)
        if not admission_nonce or len(admission_nonce) > 256:
            self._digest("dummy-workshop-nonce", admission_nonce[:256])
            return None
        nonce_digest = self._digest("workshop-admission-nonce", admission_nonce)
        expected = WorkshopEntryCodeService(
            sessions=self.sessions, secret=self.secret
        ).digest(normalized)
        session = self.sessions()
        try:
            session.execute(text("BEGIN IMMEDIATE"))
            if current_token and self._token_authenticates_active_reviewer(
                session, current_token, now
            ):
                session.rollback()
                return None
            if session.get(WorkshopAdmissionNonce, nonce_digest) is not None:
                session.rollback()
                return None
            entry_code = session.scalar(
                select(WorkshopEntryCode).where(
                    WorkshopEntryCode.code_digest == expected,
                    WorkshopEntryCode.revoked_at.is_(None),
                )
            )
            if entry_code is None or not hmac.compare_digest(entry_code.code_digest, expected):
                session.rollback()
                return None
            reviewer_id = self._allocate_reviewer_id(session)
            mailbox = uuid.uuid4().hex
            reviewer = Reviewer(
                id=reviewer_id,
                name="",
                affiliation="",
                email_display=f"workshop-{mailbox}@example.invalid",
                email_normalized=f"workshop-{mailbox}@example.invalid",
                status="active",
                created_at=now_text,
                updated_at=now_text,
                privacy_notice_version=None,
                privacy_notice_acknowledged_at=None,
                registration_method="workshop_code",
                email_verified_at=None,
                consent_statement_version=None,
                consented_at=None,
            )
            WorkshopAdmissionService.redeem_in_session(
                session,
                entry_code_id=entry_code.id,
                presented_code_digest=expected,
                reviewer=reviewer,
                redeemed_at=now_text,
            )
            session.add(
                WorkshopAdmissionNonce(
                    nonce_digest=nonce_digest,
                    reviewer_id=reviewer_id,
                    created_at=now_text,
                )
            )
            raw_token = secrets.token_urlsafe(32)
            session.add(
                AuthSession(
                    id=str(uuid.uuid4()),
                    reviewer_id=reviewer_id,
                    token_hash=self._digest("session", raw_token),
                    created_at=now_text,
                    last_used_at=now_text,
                    expires_at=timestamp(
                        now + timedelta(seconds=self.config["REVIEWER_ABSOLUTE_SECONDS"])
                    ),
                    revoked_at=None,
                    remembered=False,
                )
            )
            session.commit()
            session.expunge(reviewer)
            return raw_token, reviewer
        except WorkshopAdmissionError:
            session.rollback()
            return None
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def issue_workshop_recovery_code(self, actor_id: str, reviewer_id: str) -> str:
        """Revoke a fallback participant's sessions and issue one assisted reset code."""

        now = self.clock()
        now_text = timestamp(now)
        code = f"{secrets.randbelow(1_000_000):06d}"
        challenge_id = "workshop-recovery-" + secrets.token_urlsafe(24)
        with self.sessions.begin() as session:
            reviewer = session.get(Reviewer, reviewer_id)
            admitted = session.scalar(
                select(WorkshopEntryRedemption.id).where(
                    WorkshopEntryRedemption.reviewer_id == reviewer_id
                )
            )
            if (
                reviewer is None
                or reviewer.status != "active"
                or reviewer.registration_method != "workshop_code"
                or admitted is None
            ):
                raise ValueError("Reviewer is not eligible for workshop recovery")
            self._revoke_reviewer_sessions(session, reviewer_id, now)
            session.execute(
                update(LoginCode)
                .where(
                    LoginCode.email_normalized == reviewer.email_normalized,
                    LoginCode.consumed_at.is_(None),
                )
                .values(consumed_at=now_text)
            )
            session.add(
                LoginCode(
                    id=challenge_id,
                    email_normalized=reviewer.email_normalized,
                    code_hash=self._digest("login-code", f"{challenge_id}\0{code}"),
                    requested_at=now_text,
                    expires_at=timestamp(
                        now + timedelta(seconds=self.config["LOGIN_CODE_TTL_SECONDS"])
                    ),
                    consumed_at=None,
                    failed_attempt_count=0,
                    request_context_digest=self._digest(
                        "workshop-recovery", reviewer_id
                    ),
                )
            )
            session.add(
                WorkshopSessionReset(
                    id="workshop-reset-" + uuid.uuid4().hex,
                    actor_reviewer_id=actor_id,
                    target_reviewer_id=reviewer_id,
                    created_at=now_text,
                )
            )
        return code

    def issue_owner_recovery_code(self) -> str:
        """Issue a short-lived, single-use code from the production console."""

        now = self.clock()
        now_text = timestamp(now)
        reviewer_id = self.config["OWNER_REVIEWER_ID"]
        code = f"{secrets.randbelow(1_000_000):06d}"
        challenge_id = "owner-recovery-" + secrets.token_urlsafe(24)
        with self.sessions.begin() as session:
            reviewer = session.get(Reviewer, reviewer_id)
            if (
                reviewer is None
                or reviewer.status != "active"
                or reviewer.registration_method == "workshop_code"
            ):
                raise ValueError("The configured owner is not eligible for recovery")
            self._revoke_reviewer_sessions(session, reviewer_id, now)
            session.execute(
                update(LoginCode)
                .where(
                    LoginCode.email_normalized == reviewer.email_normalized,
                    LoginCode.consumed_at.is_(None),
                )
                .values(consumed_at=now_text)
            )
            session.add(
                LoginCode(
                    id=challenge_id,
                    email_normalized=reviewer.email_normalized,
                    code_hash=self._digest("login-code", f"{challenge_id}\0{code}"),
                    requested_at=now_text,
                    expires_at=timestamp(
                        now + timedelta(seconds=self.config["LOGIN_CODE_TTL_SECONDS"])
                    ),
                    consumed_at=None,
                    failed_attempt_count=0,
                    request_context_digest=self._digest(
                        "owner-recovery", reviewer_id
                    ),
                )
            )
            session.add(
                WorkshopSessionReset(
                    id="owner-reset-" + uuid.uuid4().hex,
                    actor_reviewer_id=reviewer_id,
                    target_reviewer_id=reviewer_id,
                    created_at=now_text,
                )
            )
        return code

    def recover_workshop_session(
        self,
        reviewer_id: str,
        code: str,
        request_context: str,
        *,
        current_token: str | None,
    ) -> tuple[str, Reviewer] | None:
        """Consume a server- or owner-issued recovery code."""

        now = self.clock()
        allowed = self._record_workshop_attempt(
            f"recovery:{reviewer_id}", request_context, now
        )
        valid_shape = (
            reviewer_id.startswith("reviewer-")
            and reviewer_id[9:].isascii()
            and reviewer_id[9:].isdigit()
            and len(code) == 6
            and code.isascii()
            and code.isdigit()
        )
        if not allowed or not valid_shape:
            self._digest("dummy-workshop-recovery", f"{reviewer_id[:64]}\0{code[:32]}")
            return None
        session = self.sessions()
        try:
            session.execute(text("BEGIN IMMEDIATE"))
            reviewer = session.get(Reviewer, reviewer_id)
            is_owner = reviewer_id == self.config["OWNER_REVIEWER_ID"]
            eligible = bool(
                reviewer is not None
                and reviewer.status == "active"
                and (
                    (is_owner and reviewer.registration_method != "workshop_code")
                    or (not is_owner and reviewer.registration_method == "workshop_code")
                )
            )
            if not eligible:
                self._digest("dummy-workshop-recovery", f"{reviewer_id}\0{code}")
                session.rollback()
                return None
            assert reviewer is not None
            recovery_namespace = "owner-recovery" if is_owner else "workshop-recovery"
            challenge = session.scalar(
                select(LoginCode)
                .where(
                    LoginCode.email_normalized == reviewer.email_normalized,
                    LoginCode.request_context_digest
                    == self._digest(recovery_namespace, reviewer_id),
                    LoginCode.consumed_at.is_(None),
                )
                .order_by(LoginCode.requested_at.desc())
            )
            if (
                challenge is None
                or parse_timestamp(challenge.expires_at) <= now
                or challenge.failed_attempt_count >= self.config["LOGIN_CODE_MAX_ATTEMPTS"]
            ):
                if challenge is not None:
                    challenge.consumed_at = timestamp(now)
                    session.commit()
                else:
                    session.rollback()
                return None
            expected = self._digest("login-code", f"{challenge.id}\0{code}")
            if not hmac.compare_digest(challenge.code_hash, expected):
                challenge.failed_attempt_count += 1
                if challenge.failed_attempt_count >= self.config["LOGIN_CODE_MAX_ATTEMPTS"]:
                    challenge.consumed_at = timestamp(now)
                session.commit()
                return None
            challenge.consumed_at = timestamp(now)
            if current_token:
                self._revoke_token(session, current_token, now)
            raw_token = secrets.token_urlsafe(32)
            session.add(
                AuthSession(
                    id=str(uuid.uuid4()),
                    reviewer_id=reviewer_id,
                    token_hash=self._digest("session", raw_token),
                    created_at=timestamp(now),
                    last_used_at=timestamp(now),
                    expires_at=timestamp(
                        now
                        + timedelta(
                            seconds=self.config[
                                "OWNER_ABSOLUTE_SECONDS"
                                if is_owner
                                else "REVIEWER_ABSOLUTE_SECONDS"
                            ]
                        )
                    ),
                    revoked_at=None,
                    remembered=False,
                )
            )
            session.flush()
            session.expunge(reviewer)
            session.commit()
            return raw_token, reviewer
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def _record_workshop_attempt(
        self, candidate: str, request_context: str, now: datetime
    ) -> bool:
        """Atomically apply the cross-process, cross-restart workshop throttle."""

        candidate_digest = self._digest("workshop-attempt-candidate", candidate)
        context_digest = self._digest("workshop-attempt-context", request_context)
        cutoff = timestamp(
            now
            - timedelta(
                seconds=self.config["WORKSHOP_CODE_ATTEMPT_WINDOW_SECONDS"]
            )
        )
        session = self.sessions()
        try:
            session.execute(text("BEGIN IMMEDIATE"))
            session.execute(
                delete(WorkshopAdmissionAttempt).where(
                    WorkshopAdmissionAttempt.requested_at < cutoff
                )
            )
            candidate_count = session.scalar(
                select(func.count())
                .select_from(WorkshopAdmissionAttempt)
                .where(WorkshopAdmissionAttempt.candidate_digest == candidate_digest)
            ) or 0
            context_count = session.scalar(
                select(func.count())
                .select_from(WorkshopAdmissionAttempt)
                .where(WorkshopAdmissionAttempt.context_digest == context_digest)
            ) or 0
            allowed = (
                candidate_count < self.config["WORKSHOP_CODE_ATTEMPTS_PER_CODE"]
                and context_count < self.config["WORKSHOP_CODE_ATTEMPTS_PER_CONTEXT"]
            )
            if allowed:
                session.add(
                    WorkshopAdmissionAttempt(
                        id="workshop-attempt-" + uuid.uuid4().hex,
                        candidate_digest=candidate_digest,
                        context_digest=context_digest,
                        requested_at=timestamp(now),
                    )
                )
            session.commit()
            return allowed
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def _token_authenticates_active_reviewer(
        self, session: Session, raw_token: str, now: datetime
    ) -> bool:
        auth_session = session.scalar(
            select(AuthSession).where(
                AuthSession.token_hash == self._digest("session", raw_token),
                AuthSession.revoked_at.is_(None),
            )
        )
        if auth_session is None or parse_timestamp(auth_session.expires_at) <= now:
            return False
        reviewer = session.get(Reviewer, auth_session.reviewer_id)
        if reviewer is None or reviewer.status != "active":
            return False
        idle_seconds = self.config[
            "OWNER_IDLE_SECONDS"
            if reviewer.id == self.config["OWNER_REVIEWER_ID"]
            else "REMEMBERED_IDLE_SECONDS"
            if auth_session.remembered
            else "REVIEWER_IDLE_SECONDS"
        ]
        return parse_timestamp(auth_session.last_used_at) + timedelta(
            seconds=idle_seconds
        ) > now

    def list_workshop_reviewers(self) -> list[Reviewer]:
        with self.sessions() as session:
            reviewers = list(
                session.scalars(
                    select(Reviewer)
                    .where(
                        Reviewer.registration_method == "workshop_code",
                        Reviewer.status != "withdrawn",
                    )
                    .order_by(Reviewer.id)
                )
            )
            for reviewer in reviewers:
                session.expunge(reviewer)
            return reviewers

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

    def invite(
        self,
        actor_id: str,
        title: str,
        first_name: str,
        last_name: str,
        email: str,
    ) -> Reviewer:
        title = unicodedata.normalize("NFC", title).strip()
        first_name = unicodedata.normalize("NFC", first_name).strip()
        last_name = unicodedata.normalize("NFC", last_name).strip()
        if len(title) > 50:
            raise ValueError("Title must be 50 characters or fewer")
        if not first_name or len(first_name) > 100:
            raise ValueError("A first name of at most 100 characters is required")
        if not last_name or len(last_name) > 100:
            raise ValueError("A last name of at most 100 characters is required")
        display = unicodedata.normalize("NFC", email).strip()
        normalized = normalize_email(display)
        now = self.clock()
        audit_id = str(uuid.uuid4())
        with self.sessions.begin() as session:
            if session.scalar(select(Reviewer.id).where(Reviewer.email_normalized == normalized)):
                raise ValueError("That email address already has an account")
            reviewer_id = self._allocate_reviewer_id(session)
            reviewer = Reviewer(
                id=reviewer_id,
                name=f"{first_name} {last_name}",
                title=title,
                first_name=first_name,
                last_name=last_name,
                affiliation="",
                email_display=display,
                email_normalized=normalized,
                status="invited",
                created_at=timestamp(now),
                updated_at=timestamp(now),
                privacy_notice_version=None,
                privacy_notice_acknowledged_at=None,
                registration_method="email_invitation",
                email_verified_at=None,
                consent_statement_version=None,
                consented_at=None,
            )
            session.add(reviewer)
            self._audit(
                session,
                actor_id,
                reviewer_id,
                "invite",
                now,
                event_id=audit_id,
            )
        try:
            self.sender.send_invitation(display, "/auth/login")
        except Exception:
            # Compensate only while the newly-created account is still pending.
            # If it was activated or changed concurrently, preserve both the
            # account and its append-only owner audit event.
            with self.sessions.begin() as session:
                pending = session.get(Reviewer, reviewer_id)
                if pending is not None and pending.status == "invited":
                    session.execute(
                        delete(OwnerAuditEvent).where(OwnerAuditEvent.id == audit_id)
                    )
                    session.delete(pending)
            raise
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
            reviewer.name = ""
            reviewer.title = ""
            reviewer.first_name = ""
            reviewer.last_name = ""
            reviewer.affiliation = ""
            random_mailbox = secrets.token_hex(16)
            reviewer.email_display = f"withdrawn-{random_mailbox}@example.invalid"
            reviewer.email_normalized = reviewer.email_display
            reviewer.status = "withdrawn"
            reviewer.disabled_from_status = None
            reviewer.updated_at = timestamp(now)
            reviewer.privacy_notice_version = None
            reviewer.privacy_notice_acknowledged_at = None
            # Domain history is append-only during ordinary profile correction.
            # The database permits deletion only after this reviewer has been
            # marked withdrawn, keeping the erasure exception narrow.
            session.flush()
            domain_ids = list(
                session.scalars(
                    select(ReviewerDomainExpertise.domain_id).where(
                        ReviewerDomainExpertise.reviewer_id == target_id
                    )
                )
            )
            session.execute(
                delete(ReviewerDomainExpertise).where(
                    ReviewerDomainExpertise.reviewer_id == target_id
                )
            )
            session.execute(
                delete(ReviewerExperience).where(ReviewerExperience.reviewer_id == target_id)
            )
            session.execute(
                delete(ReviewerLanguage).where(ReviewerLanguage.reviewer_id == target_id)
            )
            if domain_ids:
                session.execute(
                    delete(ExpertiseDomain).where(
                        ExpertiseDomain.id.in_(domain_ids),
                        ~select(ReviewerDomainExpertise.id)
                        .where(ReviewerDomainExpertise.domain_id == ExpertiseDomain.id)
                        .exists(),
                    )
                )
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
    def _audit(
        session: Session,
        actor_id: str,
        target_id: str,
        action: str,
        now: datetime,
        *,
        event_id: str | None = None,
    ) -> None:
        session.add(
            OwnerAuditEvent(
                id=event_id or str(uuid.uuid4()),
                actor_reviewer_id=actor_id,
                target_reviewer_id=target_id,
                action=action,
                created_at=timestamp(now),
            )
        )
