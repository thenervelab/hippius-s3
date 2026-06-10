"""Tests for the async CircuitBreaker used to fail-fast on backend outages."""

from __future__ import annotations

import pytest

from hippius_s3.services.circuit_breaker import CircuitBreaker
from hippius_s3.services.circuit_breaker import CircuitBreakerOpen
from hippius_s3.workers.errors import classify_upload_error


def _breaker(clock, **kw):
    return CircuitBreaker("arion-upload", monotonic=lambda: clock["t"], **kw)


@pytest.mark.asyncio
async def test_opens_after_threshold_consecutive_failures():
    clock = {"t": 0.0}
    cb = _breaker(clock, failure_threshold=3, cooldown_seconds=10.0)

    for _ in range(3):
        with pytest.raises(ValueError):
            async with cb:
                raise ValueError("backend boom")
    assert cb.is_open

    # While open, the body must NOT run — entry fast-fails.
    ran = False
    with pytest.raises(CircuitBreakerOpen):
        async with cb:
            ran = True
    assert ran is False


@pytest.mark.asyncio
async def test_success_resets_failure_count():
    clock = {"t": 0.0}
    cb = _breaker(clock, failure_threshold=3, cooldown_seconds=10.0)
    for _ in range(2):
        with pytest.raises(ValueError):
            async with cb:
                raise ValueError("x")
    async with cb:  # success
        pass
    assert not cb.is_open
    # Two more failures should NOT trip it (counter was reset by the success).
    for _ in range(2):
        with pytest.raises(ValueError):
            async with cb:
                raise ValueError("x")
    assert not cb.is_open


@pytest.mark.asyncio
async def test_open_message_classifies_as_transient():
    """An open breaker must surface as a transient error so the uploader requeues
    with backoff instead of dead-lettering."""
    clock = {"t": 0.0}
    cb = _breaker(clock, failure_threshold=1, cooldown_seconds=10.0)
    with pytest.raises(RuntimeError):
        async with cb:
            raise RuntimeError("boom")
    with pytest.raises(CircuitBreakerOpen) as ei:
        async with cb:
            pass
    assert classify_upload_error(ei.value) == "transient"


@pytest.mark.asyncio
async def test_half_open_probe_closes_on_success():
    clock = {"t": 0.0}
    cb = _breaker(clock, failure_threshold=2, cooldown_seconds=5.0)
    for _ in range(2):
        with pytest.raises(ValueError):
            async with cb:
                raise ValueError("x")
    assert cb.is_open

    clock["t"] = 4.9  # still within cooldown → fast-fail
    with pytest.raises(CircuitBreakerOpen):
        async with cb:
            pass

    clock["t"] = 5.1  # cooldown elapsed → probe allowed, succeeds → closes
    async with cb:
        pass
    assert not cb.is_open


@pytest.mark.asyncio
async def test_half_open_probe_reopens_on_failure():
    clock = {"t": 0.0}
    cb = _breaker(clock, failure_threshold=1, cooldown_seconds=5.0)
    with pytest.raises(ValueError):
        async with cb:
            raise ValueError("x")
    assert cb.is_open

    clock["t"] = 6.0  # cooldown elapsed → probe allowed
    with pytest.raises(ValueError):
        async with cb:
            raise ValueError("still down")
    assert cb.is_open  # probe failed → re-opened


@pytest.mark.asyncio
async def test_should_count_predicate_ignores_excluded_failures():
    """Failures for which should_count returns False (billing/permanent) must NOT
    trip the breaker — otherwise one no-credits account stalls the whole pod."""
    clock = {"t": 0.0}

    class Billing(Exception):
        pass

    cb = CircuitBreaker(
        "arion-upload",
        failure_threshold=2,
        cooldown_seconds=10.0,
        monotonic=lambda: clock["t"],
        should_count=lambda e: not isinstance(e, Billing),
    )
    # A flood of billing failures never opens the breaker.
    for _ in range(10):
        with pytest.raises(Billing):
            async with cb:
                raise Billing("no credits")
    assert not cb.is_open
    # A genuine backend failure still trips it after the threshold.
    for _ in range(2):
        with pytest.raises(ValueError):
            async with cb:
                raise ValueError("arion down")
    assert cb.is_open


@pytest.mark.asyncio
async def test_half_open_admits_single_probe():
    """When cooldown elapses, exactly ONE concurrent probe is admitted; the rest
    keep fast-failing (no thundering-herd of real backend calls)."""
    import asyncio

    clock = {"t": 0.0}
    cb = _breaker(clock, failure_threshold=1, cooldown_seconds=5.0)
    with pytest.raises(ValueError):
        async with cb:
            raise ValueError("down")
    assert cb.is_open

    clock["t"] = 6.0  # cooldown elapsed → half-open
    entered: list[int] = []
    rejected: list[int] = []
    gate = asyncio.Event()

    async def probe(i: int) -> None:
        try:
            async with cb:
                entered.append(i)
                await gate.wait()
        except CircuitBreakerOpen:
            rejected.append(i)

    tasks = [asyncio.create_task(probe(i)) for i in range(5)]
    await asyncio.sleep(0.05)  # let them race through __aenter__
    assert len(entered) == 1, f"expected 1 probe admitted, got {len(entered)}"
    assert len(rejected) == 4
    gate.set()
    await asyncio.gather(*tasks)
    assert not cb.is_open  # the probe succeeded → closed
