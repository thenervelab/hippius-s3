"""Minimal async circuit breaker.

Stops a high-concurrency worker from turning a sustained backend failure into a
retry storm: after `failure_threshold` consecutive failures the breaker trips
OPEN and, for `cooldown_seconds`, entering it raises `CircuitBreakerOpen` instead
of running the body. The message is phrased so the upload/backup error classifiers
treat it as transient — callers requeue with backoff rather than hammering the
backend. After the cooldown a single call is allowed through (half-open); success
closes the breaker, failure re-opens it.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from types import TracebackType


class CircuitBreakerOpen(Exception):
    """Raised on entry while the breaker is open. Transient-classified by message."""


class CircuitBreaker:
    def __init__(
        self,
        name: str,
        *,
        failure_threshold: int,
        cooldown_seconds: float,
        monotonic: Callable[[], float] = time.monotonic,
        should_count: Callable[[Exception], bool] | None = None,
    ) -> None:
        self.name = name
        self.failure_threshold = max(1, int(failure_threshold))
        self.cooldown_seconds = float(cooldown_seconds)
        # `should_count(exc)` decides whether an exception reflects backend health.
        # Per-account / permanent errors (e.g. billing 402, malformed object) must NOT
        # trip the breaker — the backend is fine, so tripping would stall everyone else.
        # Default: count every failure.
        self._should_count = should_count
        self._consecutive_failures = 0
        self._opened_at: float | None = None
        self._half_open = False
        self._lock = asyncio.Lock()
        self._monotonic = monotonic

    @property
    def is_open(self) -> bool:
        return self._opened_at is not None

    async def __aenter__(self) -> CircuitBreaker:
        async with self._lock:
            if self._opened_at is not None:
                elapsed = self._monotonic() - self._opened_at
                if elapsed < self.cooldown_seconds:
                    raise CircuitBreakerOpen(
                        f"{self.name} circuit open; backend temporarily unavailable "
                        f"({self.cooldown_seconds - elapsed:.1f}s cooldown left)"
                    )
                # Cooldown elapsed → half-open: admit exactly ONE probe; everyone else
                # keeps fast-failing until the probe resolves (no thundering herd).
                if self._half_open:
                    raise CircuitBreakerOpen(f"{self.name} circuit half-open; probe in flight")
                self._half_open = True
                # NOTE: keep `_opened_at` set — only a probe success (in __aexit__) closes it.
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool:
        async with self._lock:
            was_half_open = self._half_open
            self._half_open = False
            if exc is None:
                self._consecutive_failures = 0
                self._opened_at = None
            elif not isinstance(exc, Exception):
                # BaseException (e.g. CancelledError on shutdown) — not a backend signal.
                pass
            elif self._should_count is not None and not self._should_count(exc):
                # Not a backend-health signal (billing/permanent) — leave state unchanged.
                pass
            elif was_half_open:
                # Probe failed → straight back to open with a fresh cooldown.
                self._opened_at = self._monotonic()
            else:
                self._consecutive_failures += 1
                if self._consecutive_failures >= self.failure_threshold:
                    self._opened_at = self._monotonic()
        return False
