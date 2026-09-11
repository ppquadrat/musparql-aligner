"""Versioned affirmative-consent recording for participant access."""
from __future__ import annotations

from sqlalchemy.orm import Session, sessionmaker

from musparql.database.models import Reviewer

from .auth import timestamp, utc_now


class ConsentService:
    """Record one explicit acceptance of the configured notice and statement."""

    def __init__(
        self,
        sessions: sessionmaker[Session],
        *,
        notice_version: str,
        statement_version: str,
    ) -> None:
        self.sessions = sessions
        self.notice_version = notice_version
        self.statement_version = statement_version

    def accept(self, reviewer_id: str, *, affirmed: bool) -> None:
        if not affirmed:
            raise ValueError("You must select the consent checkbox to take part")
        now = timestamp(utc_now())
        with self.sessions.begin() as session:
            reviewer = session.get(Reviewer, reviewer_id)
            if reviewer is None or reviewer.status != "active":
                raise LookupError("Active reviewer not found")
            reviewer.privacy_notice_version = self.notice_version
            reviewer.privacy_notice_acknowledged_at = now
            reviewer.consent_statement_version = self.statement_version
            reviewer.consented_at = now
            reviewer.updated_at = now
