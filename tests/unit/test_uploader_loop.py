"""Tests for run_arion_uploader_loop's bounded-concurrent dispatch.

The uploader used to be a serial consumer (dequeue one → await process_upload →
repeat), which capped aggregate throughput on the single-chunk-dominated queue.
It now dispatches up to `uploader_max_inflight` requests concurrently, mirroring
the downloader loop. These tests verify:
- multiple requests run concurrently (peak == N when max_inflight >= N),
- the capacity gate holds (peak <= max_inflight when over capacity),
- graceful shutdown cancels and drains in-flight tasks.
"""

from __future__ import annotations

import asyncio
import time
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from workers import run_arion_uploader_in_loop as up


def _make_config(*, max_inflight: int) -> MagicMock:
    cfg = MagicMock()
    cfg.uploader_max_inflight = max_inflight
    cfg.uploader_db_pool_max = 20
    cfg.arion_upload_concurrency = 8
    cfg.uploader_max_attempts = 5
    cfg.uploader_backoff_base_ms = 1
    cfg.uploader_backoff_max_ms = 10
    cfg.uploader_not_ready_delay_seconds = 5.0
    cfg.uploader_not_ready_max_wait_seconds = 1800.0
    cfg.redis_url = "redis://localhost:6379"
    cfg.redis_queues_url = "redis://localhost:6382"
    cfg.database_url = "postgresql://localhost/test"
    return cfg


def _req(name: str) -> MagicMock:
    r = MagicMock()
    r.ray_id = f"ray-{name}"
    r.object_id = name
    r.object_version = 1
    r.chunks = []
    r.attempts = 0
    r.address = "5Addr"
    r.first_enqueued_at = time.time()
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

    def _make_uploader(self, *args, **kwargs):
        u = MagicMock()
        u.db = MagicMock()
        # Default: parts already on ceph, so the drain-gate is a no-op and the loop
        # behaves as before. The not-ready path is covered by the _handle_upload tests.
        u.all_parts_on_pool = AsyncMock(return_value=True)

        async def _process(req):
            self.process_calls.append(req)
            assert self.process_fn is not None
            return await self.process_fn(req)

        u.process_upload = _process
        return u

    async def run(self) -> None:
        with (
            patch.object(up, "config", self.config),
            patch.object(up, "asyncpg") as mock_asyncpg,
            patch.object(up, "ArionClient", return_value=MagicMock()),
            patch.object(up, "Uploader", side_effect=self._make_uploader),
            patch.object(up, "with_redis_retry", side_effect=self._with_redis_retry),
            patch.object(up, "dequeue_upload_request", side_effect=self._dequeue),
            patch.object(up, "move_due_upload_retries", new=AsyncMock(return_value=0)),
            patch.object(up, "initialize_cache_client"),
            patch.object(up, "initialize_metrics_collector"),
            patch.object(up, "get_logger_with_ray_id", return_value=MagicMock()),
            patch("hippius_s3.queue.initialize_queue_client"),
            patch("hippius_s3.redis_utils.create_redis_client", return_value=MagicMock(aclose=AsyncMock())),
            patch("redis.asyncio.Redis.from_url", return_value=MagicMock(aclose=AsyncMock())),
            patch("hippius_s3.cache.RedisObjectPartsCache", return_value=MagicMock()),
        ):
            pool = MagicMock()
            pool.close = AsyncMock()
            mock_asyncpg.create_pool = AsyncMock(return_value=pool)
            mock_asyncpg.InterfaceError = Exception
            await up.run_arion_uploader_loop()


@pytest.mark.asyncio
async def test_dispatches_requests_concurrently():
    """With max_inflight >= N, all N requests are in process_upload at once."""
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
        return []

    h.process_fn = _slow

    async def controller():
        await asyncio.wait_for(all_active.wait(), timeout=2.0)
        release.set()

    await asyncio.gather(h.run(), controller())
    assert peak == 3, f"expected 3 concurrent, saw {peak}"
    assert len(h.process_calls) == 3


@pytest.mark.asyncio
async def test_respects_max_inflight_capacity():
    """When over capacity, concurrent process_upload never exceeds max_inflight."""
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
        return []

    h.process_fn = _slow
    await asyncio.wait_for(h.run(), timeout=5.0)
    assert peak <= 2, f"inflight cap breached: saw {peak}"
    assert len(h.process_calls) == 6


@pytest.mark.asyncio
async def test_graceful_shutdown_cancels_inflight():
    """KeyboardInterrupt (SIGTERM-equiv) while tasks are in flight must cancel +
    drain them and return — not hang."""
    h = _Harness(max_inflight=4)
    h.dequeue_sequence = [_req("x"), _req("y")]  # then dequeue raises KeyboardInterrupt

    started = asyncio.Event()
    cancelled = {"n": 0}

    async def _block(_req):
        started.set()
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            cancelled["n"] += 1
            raise
        return []

    h.process_fn = _block
    # Loop dequeues x, y (both block/inflight), then dequeue → KeyboardInterrupt →
    # finally cancels the 2 inflight tasks and gathers them.
    await asyncio.wait_for(h.run(), timeout=5.0)
    assert len(h.process_calls) == 2
    assert cancelled["n"] == 2


@pytest.mark.asyncio
async def test_handle_upload_requeues_transient_failure():
    """A transient process_upload failure is requeued (not DLQ'd) — covers the
    failure-routing the rework moved into _handle_upload, under the new loop."""
    from unittest.mock import patch

    uploader = MagicMock()
    uploader.all_parts_on_pool = AsyncMock(return_value=True)
    uploader.process_upload = AsyncMock(side_effect=ValueError("arion blip"))
    uploader._push_to_dlq = AsyncMock()
    req = _req("obj-transient")

    with (
        patch.object(up, "classify_error", return_value="transient"),
        patch.object(up, "extract_http_status_code", return_value=""),
        patch.object(up, "enqueue_retry_request", new=AsyncMock()) as enqueue,
        patch.object(up, "get_metrics_collector", return_value=MagicMock()),
        patch.object(up, "get_logger_with_ray_id", return_value=MagicMock()),
    ):
        await up._handle_upload(uploader, MagicMock(), req)

    enqueue.assert_awaited_once()
    uploader._push_to_dlq.assert_not_called()


@pytest.mark.asyncio
async def test_handle_upload_dlqs_permanent_failure_and_marks_failed():
    """A permanent failure goes to the DLQ and marks object_versions failed —
    must NOT requeue (would loop forever)."""
    from unittest.mock import patch

    uploader = MagicMock()
    uploader.all_parts_on_pool = AsyncMock(return_value=True)
    uploader.process_upload = AsyncMock(side_effect=ValueError("malformed object"))
    uploader._push_to_dlq = AsyncMock()
    conn = AsyncMock()
    db_pool = MagicMock()
    db_pool.acquire = MagicMock(return_value=MagicMock(__aenter__=AsyncMock(return_value=conn), __aexit__=AsyncMock()))
    req = _req("obj-permanent")

    with (
        patch.object(up, "classify_error", return_value="permanent"),
        patch.object(up, "extract_http_status_code", return_value="400"),
        patch.object(up, "enqueue_retry_request", new=AsyncMock()) as enqueue,
        patch.object(up, "get_metrics_collector", return_value=MagicMock()),
        patch.object(up, "get_logger_with_ray_id", return_value=MagicMock()),
    ):
        await up._handle_upload(uploader, db_pool, req)

    uploader._push_to_dlq.assert_awaited_once()
    enqueue.assert_not_called()
    assert conn.execute.await_count == 1
    assert "status = 'failed'" in conn.execute.await_args.args[0]


@pytest.mark.asyncio
async def test_handle_upload_defers_when_not_yet_on_ceph():
    """An upload dequeued before the drain copied its parts to ceph is re-scheduled
    (defer_upload_request) WITHOUT touching the retry/DLQ paths or process_upload."""
    uploader = MagicMock()
    uploader.all_parts_on_pool = AsyncMock(return_value=False)
    uploader.process_upload = AsyncMock()
    uploader._push_to_dlq = AsyncMock()
    req = _req("obj-not-ready")
    req.first_enqueued_at = time.time()  # just enqueued → well within the wall-clock cap

    with (
        patch.object(up, "config", _make_config(max_inflight=4)),
        patch.object(up, "defer_upload_request", new=AsyncMock()) as defer,
        patch.object(up, "enqueue_retry_request", new=AsyncMock()) as enqueue,
        patch.object(up, "get_logger_with_ray_id", return_value=MagicMock()),
    ):
        await up._handle_upload(uploader, MagicMock(), req)

    defer.assert_awaited_once()
    assert defer.await_args.kwargs["backend_name"] == "arion"
    uploader.process_upload.assert_not_called()
    enqueue.assert_not_called()
    uploader._push_to_dlq.assert_not_called()


@pytest.mark.asyncio
async def test_handle_upload_stops_deferring_past_max_wait():
    """A part still absent from ceph past the wall-clock ceiling stops deferring and
    falls through to process_upload (which routes the genuine fault to the DLQ)."""
    uploader = MagicMock()
    uploader.all_parts_on_pool = AsyncMock(return_value=False)
    uploader.process_upload = AsyncMock(side_effect=RuntimeError("Missing meta"))
    uploader._push_to_dlq = AsyncMock()
    conn = AsyncMock()
    db_pool = MagicMock()
    db_pool.acquire = MagicMock(return_value=MagicMock(__aenter__=AsyncMock(return_value=conn), __aexit__=AsyncMock()))
    req = _req("obj-stuck")
    req.first_enqueued_at = time.time() - 4000.0  # older than the 1800s ceiling

    with (
        patch.object(up, "config", _make_config(max_inflight=4)),
        patch.object(up, "defer_upload_request", new=AsyncMock()) as defer,
        patch.object(up, "classify_error", return_value="permanent"),
        patch.object(up, "extract_http_status_code", return_value=""),
        patch.object(up, "enqueue_retry_request", new=AsyncMock()),
        patch.object(up, "get_metrics_collector", return_value=MagicMock()),
        patch.object(up, "get_logger_with_ray_id", return_value=MagicMock()),
    ):
        await up._handle_upload(uploader, db_pool, req)

    defer.assert_not_called()
    uploader.process_upload.assert_awaited_once()
    uploader._push_to_dlq.assert_awaited_once()
