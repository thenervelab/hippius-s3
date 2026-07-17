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
    cfg.unpinner_batch_delete_enabled = False
    cfg.unpinner_batch_max_files = 1000
    cfg.unpinner_folder_hash = ""
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


class _NoopAsyncClient:
    """Async-CM client stub — the loop now enters `async with backend_client_factory()`."""

    async def __aenter__(self) -> "_NoopAsyncClient":
        return self

    async def __aexit__(self, *a) -> bool:
        return False

    async def unpin_file(self, *a, **k) -> None:
        return None


class _CountingFactory:
    """Callable factory that records how many clients it constructs (should be 1 per loop)."""

    def __init__(self) -> None:
        self.count = 0
        self.instances: list = []

    def __call__(self) -> _NoopAsyncClient:
        self.count += 1
        inst = _NoopAsyncClient()
        self.instances.append(inst)
        return inst


class _Harness:
    def __init__(self, *, max_inflight: int, factory=None) -> None:
        self.config = _make_config(max_inflight=max_inflight)
        self.dequeue_sequence: list = []
        self.process_calls: list = []
        self.client_args: list = []  # `client` kwarg seen by each process_unpin_request call
        self.pool_max = None  # max_size passed to asyncpg.create_pool
        self.factory = factory if factory is not None else _CountingFactory()
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
        self.client_args.append(kwargs.get("client"))
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

            async def _create_pool(*args, **kwargs):
                self.pool_max = kwargs.get("max_size")
                return pool

            mock_asyncpg.create_pool = AsyncMock(side_effect=_create_pool)
            mock_async_redis.from_url = MagicMock(return_value=MagicMock())
            await un.run_unpinner_loop(
                backend_name="arion",
                backend_client_factory=self.factory,
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


@pytest.mark.asyncio
async def test_client_closed_only_after_inflight_drained_on_shutdown():
    """Regression: the shared client must OUTLIVE in-flight requests. On shutdown the loop must
    cancel + drain in-flight tasks BEFORE the client's __aexit__ closes its connection pool — closing
    it first would fail every in-flight Arion DELETE mid-request. Guards the try/finally ordering."""
    events: list[str] = []

    class _OrderingClient:
        async def __aenter__(self) -> "_OrderingClient":
            return self

        async def __aexit__(self, *a) -> bool:
            events.append("client_closed")
            return False

        async def unpin_file(self, *a, **k) -> None:
            return None

    h = _Harness(max_inflight=4, factory=lambda: _OrderingClient())
    h.dequeue_sequence = [_req("x"), _req("y")]  # then dequeue raises KeyboardInterrupt

    async def _block(_req):
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            events.append("task_cancelled")
            raise

    h.process_fn = _block
    await asyncio.wait_for(h.run(), timeout=5.0)

    assert events.count("task_cancelled") == 2, f"both in-flight tasks must drain: {events}"
    assert events.count("client_closed") == 1
    assert events[-1] == "client_closed", f"client closed before in-flight tasks drained: {events}"


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
async def test_failing_identifier_fails_the_request_for_retry():
    """A9: a single identifier's DELETE failure now FAILS the whole request so it is retried/DLQ'd
    — the old best-effort path soft-deleted the other rows and reported success, stranding the
    still-pinned failed identifier forever. All identifiers are still ATTEMPTED (they run
    concurrently), but the request is surfaced as failed via the retry/DLQ route rather than a
    silent success."""
    tracker = _DeleteTracker()
    rows = [{"backend_identifier": f"id-{i}", "chunk_id": i} for i in range(4)]
    dlq_manager = AsyncMock()

    with (
        patch.object(un, "get_config", return_value=_make_config(max_inflight=4, parallelism=4)),
        patch.object(un, "get_query", return_value="SQL"),
        patch.object(un, "get_metrics_collector", return_value=MagicMock()),
        patch.object(un, "enqueue_unpin_retry_request", new=AsyncMock()) as retry_mock,
    ):
        # process_unpin_request catches the failure internally and routes it to retry/DLQ,
        # so it does NOT re-raise — but it must NOT report a silent success.
        await un.process_unpin_request(
            _req("o"),
            backend_name="arion",
            backend_client_factory=_client_factory(tracker, fail_on="id-2"),
            worker_logger=MagicMock(),
            dlq_manager=dlq_manager,
            db_pool=_fake_db_pool(rows),
            sem=asyncio.Semaphore(4),
        )

    assert len(tracker.calls) == 4, "all identifiers attempted despite one failing"
    # The request was surfaced as failed via exactly one of the two terminal routes (retry or DLQ).
    routed = retry_mock.await_count + dlq_manager.push.await_count
    assert routed >= 1, "a failed unpin must route the request to retry or DLQ, not silently succeed"


# --------------------------------------------------------------------------- #
# Client reuse: the loop constructs ONE backend client and hands it to every
# request (kills the per-request TLS handshake that starved throughput).
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_backend_client_constructed_once_and_reused_across_requests():
    """The Arion client must be built ONCE for the loop and the SAME live client passed to every
    request — not re-created per request (the handshake storm we are fixing)."""
    factory = _CountingFactory()
    h = _Harness(max_inflight=4, factory=factory)
    h.dequeue_sequence = [_req("a"), _req("b"), _req("c")]

    async def _noop(_req):
        return None

    h.process_fn = _noop
    await asyncio.wait_for(h.run(), timeout=5.0)

    assert factory.count == 1, f"client constructed {factory.count}x, expected 1 (per-loop reuse)"
    assert len(h.process_calls) == 3
    assert len(h.client_args) == 3
    assert all(c is factory.instances[0] for c in h.client_args), "every request must reuse the one live client"
    assert all(c is not None for c in h.client_args), "loop must pass a live client, not None"


class _ReuseClient:
    """A client handed straight to process_unpin_request. Re-entering it as a context manager is a
    bug (the loop already owns its lifecycle), so __aenter__ blows up to catch that regression."""

    def __init__(self, tracker: _DeleteTracker) -> None:
        self.tracker = tracker

    async def __aenter__(self):
        raise AssertionError("passed-in client must not be re-entered as a context manager")

    async def __aexit__(self, *a):
        return False

    async def unpin_file(self, identifier, **kw):
        self.tracker.enter(identifier)
        self.tracker.leave()


@pytest.mark.asyncio
async def test_process_uses_passed_client_and_never_calls_factory():
    """When a live `client` is supplied, process_unpin_request must use it directly and never touch
    backend_client_factory. DELETE + soft-delete still fire once per chunk."""
    tracker = _DeleteTracker()
    client = _ReuseClient(tracker)
    factory = MagicMock(side_effect=AssertionError("factory must not be called when client is supplied"))

    rows = [{"backend_identifier": f"id-{i}", "chunk_id": i} for i in range(3)]
    conn = AsyncMock()
    conn.fetch = AsyncMock(return_value=rows)
    conn.fetchval = AsyncMock(return_value=1)
    pool = MagicMock()
    pool.acquire = MagicMock(return_value=MagicMock(__aenter__=AsyncMock(return_value=conn), __aexit__=AsyncMock()))

    with (
        patch.object(un, "get_config", return_value=_make_config(max_inflight=4, parallelism=4)),
        patch.object(un, "get_query", return_value="SQL"),
        patch.object(un, "get_metrics_collector", return_value=MagicMock()),
    ):
        await un.process_unpin_request(
            _req("o"),
            backend_name="arion",
            client=client,
            backend_client_factory=factory,
            worker_logger=MagicMock(),
            dlq_manager=MagicMock(),
            db_pool=pool,
            sem=asyncio.Semaphore(4),
        )

    factory.assert_not_called()
    assert len(tracker.calls) == 3, "DELETE must run once per chunk on the reused client"
    assert conn.fetchval.await_count == 3, "soft-delete must run once per chunk"


# --------------------------------------------------------------------------- #
# DB pool cap: raising max_inflight must not balloon Postgres connections, but
# the pool is never sized below the deadlock-safe floor (parallelism + 1).
# --------------------------------------------------------------------------- #


async def _pool_max_for(*, max_inflight: int, parallelism: int, db_pool_max: int) -> int:
    h = _Harness(max_inflight=max_inflight)
    h.config.unpinner_parallelism = parallelism
    h.config.unpinner_db_pool_max = db_pool_max
    h.dequeue_sequence = []  # no work -> immediate KeyboardInterrupt after pool is created
    await asyncio.wait_for(h.run(), timeout=5.0)
    assert h.pool_max is not None
    return h.pool_max


@pytest.mark.asyncio
async def test_pool_uses_ideal_when_cap_is_high():
    # ideal = parallelism + max_inflight = 4 + 8 = 12; cap 50 -> min(12, 50) = 12
    assert await _pool_max_for(max_inflight=8, parallelism=4, db_pool_max=50) == 12


@pytest.mark.asyncio
async def test_pool_cap_bounds_growth_below_ideal():
    # ideal = 4 + 32 = 36 but cap 20 wins (>= deadlock floor 5) -> pool stays at 20, not 36.
    assert await _pool_max_for(max_inflight=32, parallelism=4, db_pool_max=20) == 20


@pytest.mark.asyncio
async def test_pool_never_below_deadlock_floor(caplog):
    # deadlock floor = parallelism + 1 = 8 + 1 = 9; a too-small cap (3) is clamped up to 9 + warns.
    import logging as _logging

    with caplog.at_level(_logging.WARNING, logger="hippius_s3.workers.unpinner"):
        pool_max = await _pool_max_for(max_inflight=16, parallelism=8, db_pool_max=3)

    assert pool_max == 9, "cap below the deadlock-safe floor must be overridden by the floor"
    assert "below the deadlock-safe floor" in caplog.text
