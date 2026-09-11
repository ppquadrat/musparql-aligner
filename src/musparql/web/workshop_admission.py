"""Owner controls and participant-facing state for IPL shared-code admission."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import hashlib
import hmac
import secrets
import uuid

from sqlalchemy import select, text
from sqlalchemy.orm import Session, sessionmaker

from musparql.database.models import WorkshopEntryCode, WorkshopRound

from .auth import timestamp, utc_now


ENTRY_CODE_ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"


def normalize_entry_code(value: str) -> str:
    normalized = value.strip().upper().replace("-", "").replace(" ", "")
    if len(normalized) != 8 or any(char not in ENTRY_CODE_ALPHABET for char in normalized):
        raise ValueError("Workshop entry code is invalid")
    return normalized


def display_entry_code(value: str) -> str:
    return f"{value[:4]}-{value[4:]}"


@dataclass(frozen=True)
class WorkshopEntryCodeStatus:
    round_id: str
    round_name: str
    round_status: str
    opens_at: str
    closes_at: str
    max_participants: int
    code_id: str | None
    redemption_count: int
    code_expires_at: str | None
    active: bool


class WorkshopEntryCodeService:
    """Issue digest-only codes and expose non-sensitive operational state."""

    def __init__(self, *, sessions: sessionmaker[Session], secret: bytes) -> None:
        self.sessions = sessions
        self.secret = secret

    def digest(self, normalized_code: str) -> str:
        return hmac.new(
            self.secret,
            f"workshop-entry-code\0{normalized_code}".encode("ascii"),
            hashlib.sha256,
        ).hexdigest()

    def list_rounds(self, *, now: datetime | None = None) -> list[WorkshopEntryCodeStatus]:
        current = timestamp(now or utc_now())
        with self.sessions() as session:
            rounds = list(session.scalars(select(WorkshopRound).order_by(WorkshopRound.opens_at)))
            result = []
            for workshop_round in rounds:
                code = session.scalar(
                    select(WorkshopEntryCode).where(
                        WorkshopEntryCode.workshop_round_id == workshop_round.id,
                        WorkshopEntryCode.revoked_at.is_(None),
                    )
                )
                active = bool(
                    code is not None
                    and workshop_round.status == "open"
                    and workshop_round.opens_at <= current < workshop_round.closes_at
                    and current < code.expires_at
                    and code.redemption_count < code.max_redemptions
                )
                result.append(
                    WorkshopEntryCodeStatus(
                        round_id=workshop_round.id,
                        round_name=workshop_round.name,
                        round_status=workshop_round.status,
                        opens_at=workshop_round.opens_at,
                        closes_at=workshop_round.closes_at,
                        max_participants=workshop_round.max_participants,
                        code_id=code.id if code else None,
                        redemption_count=code.redemption_count if code else 0,
                        code_expires_at=code.expires_at if code else None,
                        active=active,
                    )
                )
            return result

    def available(self) -> bool:
        return any(item.active for item in self.list_rounds())

    def issue(self, round_id: str) -> str:
        now = utc_now()
        now_text = timestamp(now)
        session = self.sessions()
        try:
            session.execute(text("BEGIN IMMEDIATE"))
            workshop_round = session.get(WorkshopRound, round_id)
            if (
                workshop_round is None
                or workshop_round.status not in {"draft", "open"}
                or now_text >= workshop_round.closes_at
            ):
                raise ValueError("Workshop round cannot accept an entry code")
            existing = session.scalar(
                select(WorkshopEntryCode).where(
                    WorkshopEntryCode.workshop_round_id == round_id,
                    WorkshopEntryCode.revoked_at.is_(None),
                )
            )
            if existing is not None:
                existing.revoked_at = now_text
                session.flush()
            normalized = "".join(secrets.choice(ENTRY_CODE_ALPHABET) for _ in range(8))
            session.add(
                WorkshopEntryCode(
                    id="entry-code-" + uuid.uuid4().hex,
                    workshop_round_id=round_id,
                    code_digest=self.digest(normalized),
                    expires_at=workshop_round.closes_at,
                    max_redemptions=workshop_round.max_participants,
                    redemption_count=0,
                    revoked_at=None,
                    created_at=now_text,
                )
            )
            session.commit()
            return display_entry_code(normalized)
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def revoke(self, code_id: str) -> None:
        now_text = timestamp(utc_now())
        with self.sessions.begin() as session:
            code = session.get(WorkshopEntryCode, code_id)
            if code is None or code.revoked_at is not None:
                raise ValueError("Workshop entry code is not active")
            code.revoked_at = now_text
