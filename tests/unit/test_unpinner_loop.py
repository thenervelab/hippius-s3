"""Tests for run_unpinner_loop's bounded-concurrent dispatch + the shared per-pod Arion-DELETE
semaphore.

The unpinner used to be serial on two axes: a serial outer consume loop AND a serial inner
`for row in rows` loop that awaited each of an object's N chunk DELETEs one at a time. It now (a)
dispatches up to `unpinner_max_inflight` requests concurrently, and (b) within each request runs the
identifier DELETEs concurrently bounded by a single shared per-pod semaphore. These tests verify both
axes plus graceful shutdown and best-effort per-identifier semantics.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from hippius_s3.workers import unpinner as un


def _make_config(*, max_inflight: int, parallelism: int = 5) -> MagicMock:
    cfg = MagicMock()
    cfg.unpinner_max_inflight = max_inflight
    cfg.unpinner_parallelism = parallelism
    cfg.unpinner_db_pool_max = 12
    cfg.unpinner_max_attempts = 5
    cfg.unpinner_backoff_base_ms = 1
    cfg.unpinner_backoff_max_ms = 10
    cfg.redis_url = "redis://localhost:6379"
    cfg.redis_queues_url = "redis://localhost:6382"
    cfg.database_url = "postgresql://localhost/test"
    return cfg


def _req(name: str) -> MagicMock:
    r = MagicMock()
    r.ray_id = f"ray-{name}"
    r.object_id = name
    r.object_version = 1
    r.address = "5Addr"
    r.attempts = 0
    r.name = name
    return r


class _Harness:
    def __init__(self, *, max_inflight: int) -> None:
        self.config = _make_config(max_inflight=max_inflight)
        self.dequeue_sequence: list = []
        self.process_calls: list = []
        self.process_fn = None  # type: ignore[var-annotated]

    async def _dequeue(self, queue_name: str):
        await asyncio.sleep(0)  # yield so spawned tasks get scheduled
        if not self.dequeue_sequence:
            raise KeyboardInterrupt()
        item = self.dequeue_sequence.pop(0)
        if isinstance(item, BaseException):
            raise item
        return item

    async def _with_redis_retry(self, func, client, url, name, **kw):
        return await func(client), client

    async def _process(self, request, **kwargs):
        self.process_calls.append(request)
        assert self.process_fn is not None
        return await self.process_fn(request)

    async def run(self) -> None:
        with (
            patch.object(un, "get_config", return_value=self.config),
            patch.object(un, "asyncpg") as mock_asyncpg,
            patch.object(un, "create_redis_client", return_value=MagicMock()),
            patch.object(un, "async_redis") as mock_async_redis,
            patch.object(un, "with_redis_retry", side_effect=self._with_redis_retry),
            patch.object(un, "dequeue_unpin_request", side_effect=self._dequeue),
            patch.object(un, "move_due_unpin_retries", new=AsyncMock(return_value=0)),
            patch.object(un, "initialize_metrics_collector"),
            patch.object(un, "UnpinDLQManager", return_value=MagicMock()),
            patch.object(un, "get_logger_with_ray_id", return_value=MagicMock()),
            patch("hippius_s3.queue.initialize_queue_client"),
            patch("hippius_s3.redis_cache.initialize_cache_client"),
            patch.object(un, "process_unpin_request", side_effect=self._process),
        ):
            pool = MagicMock()
            pool.close = AsyncMock()
            mock_asyncpg.create_pool = AsyncMock(return_value=pool)
            mock_async_redis.from_url = MagicMock(return_value=MagicMock())
            await un.run_unpinner_loop(
                backend_name="arion",
                backend_client_factory=MagicMock(),
                queue_name="arion_unpin_requests",
            )


@pytest.mark.asyncio
async def test_dispatches_requests_concurrently():
    """With max_inflight >= N, all N requests are in process_unpin_request at once."""
    h = _Harness(max_inflight=5)
    h.dequeue_sequence = [_req("a"), _req("b"), _req("c")]

    cur = 0
    peak = 0
    all_active = asyncio.Event()
    release = asyncio.Event()

    async def _slow(_req):
        nonlocal cur, peak
        cur += 1
        peak = max(peak, cur)
        if peak >= 3:
            all_active.set()
        await release.wait()
        cur -= 1

    h.process_fn = _slow

    async def controller():
        await asyncio.wait_for(all_active.wait(), timeout=2.0)
        release.set()

    await asyncio.gather(h.run(), controller())
    assert peak == 3, f"expected 3 concurrent, saw {peak}"
    assert len(h.process_calls) == 3


@pytest.mark.asyncio
async def test_respects_max_inflight_capacity():
    """When over capacity, concurrent process_unpin_request never exceeds max_inflight."""
    h = _Harness(max_inflight=2)
    h.dequeue_sequence = [_req(f"r{i}") for i in range(6)]

    cur = 0
    peak = 0

    async def _slow(_req):
        nonlocal cur, peak
        cur += 1
        peak = max(peak, cur)
        await asyncio.sleep(0.03)
        cur -= 1

    h.process_fn = _slow
    await asyncio.wait_for(h.run(), timeout=5.0)
    assert peak <= 2, f"inflight cap breached: saw {peak}"
    assert len(h.process_calls) == 6


@pytest.mark.asyncio
async def test_graceful_shutdown_cancels_inflight():
    """KeyboardInterrupt while tasks are in flight must cancel + drain them and return — not hang."""
    h = _Harness(max_inflight=4)
    h.dequeue_sequence = [_req("x"), _req("y")]  # then dequeue raises KeyboardInterrupt

    cancelled = {"n": 0}

    async def _block(_req):
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            cancelled["n"] += 1
            raise

    h.process_fn = _block
    await asyncio.wait_for(h.run(), timeout=5.0)
    assert len(h.process_calls) == 2
    assert cancelled["n"] == 2


# --------------------------------------------------------------------------- #
# Inner axis: the shared per-pod semaphore bounds concurrent Arion DELETEs
# across a request's identifiers AND across multiple in-flight requests.
# --------------------------------------------------------------------------- #


class _DeleteTracker:
    def __init__(self) -> None:
        self.cur = 0
        self.peak = 0
        self.calls: list = []

    def enter(self, ident: str) -> None:
        self.cur += 1
        self.peak = max(self.peak, self.cur)
        self.calls.append(ident)

    def leave(self) -> None:
        self.cur -= 1


def _fake_db_pool(rows: list) -> MagicMock:
    conn = AsyncMock()
    conn.fetch = AsyncMock(return_value=rows)
    conn.fetchval = AsyncMock(return_value=1)
    pool = MagicMock()
    pool.acquire = MagicMock(return_value=MagicMock(__aenter__=AsyncMock(return_value=conn), __aexit__=AsyncMock()))
    return pool


def _client_factory(tracker: _DeleteTracker, *, fail_on: str | None = None):
    class _FakeClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def unpin_file(self, identifier, **kw):
            tracker.enter(identifier)
            try:
                await asyncio.sleep(0.02)
                if fail_on is not None and identifier == fail_on:
                    raise RuntimeError("arion delete blip")
            finally:
                tracker.leave()

    return _FakeClient


@pytest.mark.asyncio
async def test_delete_semaphore_bounds_arion_deletes_across_requests():
    """One shared semaphore caps concurrent Arion DELETEs across MULTIPLE concurrent fat requests —
    the throttle that lets outer request-concurrency scale without stampeding the backend."""
    tracker = _DeleteTracker()
    shared = asyncio.Semaphore(3)
    rows_a = [{"backend_identifier": f"a-{i}", "chunk_id": i} for i in range(4)]
    rows_b = [{"backend_identifier": f"b-{i}", "chunk_id": 100 + i} for i in range(4)]

    common = {
        "backend_name": "arion",
        "worker_logger": MagicMock(),
        "dlq_manager": MagicMock(),
    }
    with (
        patch.object(un, "get_config", return_value=_make_config(max_inflight=4, parallelism=3)),
        patch.object(un, "get_query", return_value="SQL"),
        patch.object(un, "get_metrics_collector", return_value=MagicMock()),
    ):
        await asyncio.gather(
            un.process_unpin_request(
                _req("oa"),
                backend_client_factory=_client_factory(tracker),
                db_pool=_fake_db_pool(rows_a),
                sem=shared,
                **common,
            ),
            un.process_unpin_request(
                _req("ob"),
                backend_client_factory=_client_factory(tracker),
                db_pool=_fake_db_pool(rows_b),
                sem=shared,
                **common,
            ),
        )

    assert len(tracker.calls) == 8, "every identifier across both requests must be deleted once"
    assert tracker.peak > 1, "no overlap — shared semaphore not parallelizing"
    assert tracker.peak <= 3, "shared per-pod Arion-DELETE bound breached"


@pytest.mark.asyncio
async def test_failing_identifier_does_not_fail_request():
    """A single identifier's DELETE failure is best-effort (logged) — it must not fail the whole
    request, and the other identifiers still get processed."""
    tracker = _DeleteTracker()
    rows = [{"backend_identifier": f"id-{i}", "chunk_id": i} for i in range(4)]

    with (
        patch.object(un, "get_config", return_value=_make_config(max_inflight=4, parallelism=4)),
        patch.object(un, "get_query", return_value="SQL"),
        patch.object(un, "get_metrics_collector", return_value=MagicMock()),
    ):
        # Must not raise even though id-2's DELETE blows up.
        await un.process_unpin_request(
            _req("o"),
            backend_name="arion",
            backend_client_factory=_client_factory(tracker, fail_on="id-2"),
            worker_logger=MagicMock(),
            dlq_manager=MagicMock(),
            db_pool=_fake_db_pool(rows),
            sem=asyncio.Semaphore(4),
        )

    assert len(tracker.calls) == 4, "all identifiers attempted despite one failing"
