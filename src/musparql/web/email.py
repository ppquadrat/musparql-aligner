"""Replaceable email boundary; Phase 3 deliberately ships no real sender."""
from __future__ import annotations

from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
import logging
import threading
from typing import Protocol


class EmailSender(Protocol):
    def send_login_code(self, recipient: str, code: str) -> None: ...

    def send_invitation(self, recipient: str, login_path: str) -> None: ...


class AsyncEmailDispatcher:
    """Run login delivery outside the request and contain provider failures."""

    def __init__(self, *, max_pending: int) -> None:
        if max_pending < 1:
            raise ValueError("Email delivery queue bound must be positive")
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="musparql-email")
        self._pending = threading.BoundedSemaphore(max_pending)

    def submit(
        self,
        operation: Callable[[], None],
        *,
        on_failure: Callable[[], None] | None = None,
    ) -> Future[None] | None:
        if not self._pending.acquire(blocking=False):
            return None
        try:
            future = self._executor.submit(operation)
        except Exception:
            self._pending.release()
            raise

        def completed(result: Future[None]) -> None:
            self._pending.release()
            if result.exception() is None:
                return
            logging.getLogger(__name__).error("Email delivery failed")
            if on_failure is not None:
                try:
                    on_failure()
                except Exception:
                    logging.getLogger(__name__).error("Email delivery cleanup failed")

        future.add_done_callback(completed)
        return future

    def shutdown(self, *, wait: bool = True) -> None:
        self._executor.shutdown(wait=wait)

    def wait_for_idle(self, *, timeout_seconds: float = 2.0) -> None:
        self._executor.submit(lambda: None).result(timeout=timeout_seconds)


@dataclass(frozen=True)
class SyntheticMessage:
    kind: str
    recipient: str
    value: str


@dataclass
class SyntheticEmailSender:
    """Test-only outbox. Never configure this sender for a real-data deployment."""

    outbox: list[SyntheticMessage] = field(default_factory=list)
    _condition: threading.Condition = field(
        default_factory=threading.Condition, init=False, repr=False
    )

    def send_login_code(self, recipient: str, code: str) -> None:
        with self._condition:
            self.outbox.append(SyntheticMessage("login_code", recipient, code))
            self._condition.notify_all()

    def send_invitation(self, recipient: str, login_path: str) -> None:
        with self._condition:
            self.outbox.append(SyntheticMessage("invitation", recipient, login_path))
            self._condition.notify_all()

    def wait_for(
        self,
        kind: str,
        recipient: str,
        *,
        after_index: int = 0,
        timeout_seconds: float = 2.0,
    ) -> SyntheticMessage:
        """Wait for an asynchronous synthetic delivery in tests."""

        def matching() -> SyntheticMessage | None:
            return next(
                (
                    message
                    for message in reversed(self.outbox[after_index:])
                    if message.kind == kind and message.recipient == recipient
                ),
                None,
            )

        with self._condition:
            if (message := matching()) is not None:
                return message
            if not self._condition.wait_for(lambda: matching() is not None, timeout_seconds):
                raise TimeoutError(f"Timed out waiting for synthetic {kind} delivery")
            message = matching()
            assert message is not None
            return message

    def position(self) -> int:
        with self._condition:
            return len(self.outbox)
