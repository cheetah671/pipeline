from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, TypeVar

T = TypeVar("T")


@dataclass(frozen=True)
class RetryResult:
    attempts: int
    succeeded: bool


def run_with_retries(operation: Callable[[], T], retries: int = 2) -> tuple[T, RetryResult]:
    last_error: Exception | None = None
    for attempt in range(retries + 1):
        try:
            return operation(), RetryResult(attempts=attempt + 1, succeeded=True)
        except Exception as exc:  # pragma: no cover - demo helper
            last_error = exc
    assert last_error is not None
    raise last_error
