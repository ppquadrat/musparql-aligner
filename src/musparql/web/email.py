"""Replaceable email boundary; Phase 3 deliberately ships no real sender."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol


class EmailSender(Protocol):
    def send_login_code(self, recipient: str, code: str) -> None: ...

    def send_invitation(self, recipient: str, login_path: str) -> None: ...


@dataclass(frozen=True)
class SyntheticMessage:
    kind: str
    recipient: str
    value: str


@dataclass
class SyntheticEmailSender:
    """Test-only outbox. Never configure this sender for a real-data deployment."""

    outbox: list[SyntheticMessage] = field(default_factory=list)

    def send_login_code(self, recipient: str, code: str) -> None:
        self.outbox.append(SyntheticMessage("login_code", recipient, code))

    def send_invitation(self, recipient: str, login_path: str) -> None:
        self.outbox.append(SyntheticMessage("invitation", recipient, login_path))
