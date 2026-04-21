"""Tests for run_downloader_loop's inflight pool and reconnect paths.

Verifies:
- Multiple DownloadChainRequests are processed concurrently (up to max_inflight).
- The loop waits when at max_inflight capacity before dequeuing more.
- Redis connection errors from a spawned task trigger redis_client rebuild.
- asyncpg.InterfaceError from a spawned task triggers DB pool recreation.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import asyncpg
import pytest
from redis.exceptions import ConnectionError as RedisConnectionError

from hippius_s3.queue import DownloadChainRequest
from hippius_s3.queue import PartChunkSpec
from hippius_s3.queue import PartToDownload
from hippius_s3.workers.downloader import run_downloader_loop


OBJ = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"


def _make_request(name: str = "req") -> DownloadChainRequest:
    parts = [PartToDownload(part_number=1, chunks=[PartChunkSpec(index=0, cid=f"cid-{name}")])]
    return DownloadChainRequest(
        object_id=OBJ,
        object_version=1,
        object_key=f"test/{name}.bin",
        bucket_name="test-bucket",
        object_storage_version=5,
        address="5TestAddr",
        subaccount="5TestAddr",
        subaccount_seed_phrase="",
        substrate_url="http://test",
        size=4096,
        multipart=False,
        chunks=parts,
    )


def _make_loop_config(*, max_inflight: int = 10, semaphore: int = 4, sleep_loop: float = 0.01) -> MagicMock:
    cfg = MagicMock()
    cfg.downloader_max_inflight = max_inflight
    cfg.downloader_semaphore = semaphore
    cfg.downloader_chunk_retries = 1
    cfg.downloader_retry_base_seconds = 0.0
    cfg.downloader_retry_jitter_seconds = 0.0
    cfg.downloader_sleep_loop = sleep_loop
    cfg.redis_url = "redis://localhost:6379"
    cfg.redis_queues_url = "redis://localhost:6382"
    cfg.database_url = "postgresql://localhost/test"
    cfg.arion_base_url = "https://arion.test"
    cfg.arion_verify_ssl = True
    cfg.object_cache_dir = "/tmp/object_cache_test"
    cfg.fs_cache_hot_retention_seconds = 3600
    return cfg


class _LoopHarness:
    """Mocks every external dep run_downloader_loop touches.

    Lets a test script the dequeue sequence, observe spawned tasks, and
    inspect whether the main loop rebuilt its redis/db clients after task
    errors. Tests stop the loop by raising KeyboardInterrupt inside
    dequeue once the test scenario is complete.
    """

    def __init__(self, *, max_inflight: int = 10, semaphore: int = 4) -> None:
        self.config = _make_loop_config(max_inflight=max_inflight, semaphore=semaphore)
        self.dequeue_sequence: list = []  # items: DownloadChainRequest | None | Exception
        self.process_calls: list[DownloadChainRequest] = []
        self.process_fn = None  # type: ignore[var-annotated]

        # Track redis/db client rebuild counts
        self.redis_create_count = 0
        self.redis_queues_create_count = 0
        self.db_pool_create_count = 0

    async def _dequeue(self, queue_name: str):
        # Yield the event loop so spawned _run_job tasks get scheduled.
        # Real BRPOP always yields; a no-op coroutine would not, and tasks
        # created via asyncio.create_task inside the main loop would never
        # run their bodies until the loop terminates.
        await asyncio.sleep(0)
        if not self.dequeue_sequence:
            raise KeyboardInterrupt()
        item = self.dequeue_sequence.pop(0)
        if isinstance(item, BaseException):
            raise item
        return item

    async def _process_download_request(self, request, **_kwargs):
        self.process_calls.append(request)
        assert self.process_fn is not None, "Test must set process_fn"
        return await self.process_fn(request)

    def _create_redis_client(self, *args, **kwargs):
        self.redis_create_count += 1
        client = MagicMock()
        client.aclose = AsyncMock()
        return client

    def _async_redis_from_url(self, *args, **kwargs):
        self.redis_queues_create_count += 1
        client = MagicMock()
        client.aclose = AsyncMock()
        return client

    async def _asyncpg_create_pool(self, *args, **kwargs):
        self.db_pool_create_count += 1
        pool = MagicMock()
        pool.close = AsyncMock()
        return pool

    async def run(self) -> None:
        """Execute run_downloader_loop under the harness' patches."""
        with (
            patch("hippius_s3.workers.downloader.get_config", return_value=self.config),
            patch("hippius_s3.workers.downloader.create_redis_client", side_effect=self._create_redis_client),
            patch(
                "hippius_s3.workers.downloader.async_redis.from_url",
                side_effect=self._async_redis_from_url,
            ),
            patch(
                "hippius_s3.workers.downloader.asyncpg.create_pool",
                side_effect=self._asyncpg_create_pool,
            ),
            patch("hippius_s3.workers.downloader.create_fs_store", return_value=MagicMock()),
            patch("hippius_s3.workers.downloader.RedisObjectPartsCache", return_value=MagicMock()),
            patch("hippius_s3.workers.downloader.process_download_request", side_effect=self._process_download_request),
            patch("hippius_s3.queue.dequeue_download_request", side_effect=self._dequeue),
            patch("hippius_s3.queue.initialize_queue_client"),
            patch("hippius_s3.redis_cache.initialize_cache_client"),
            patch("hippius_s3.monitoring.initialize_metrics_collector"),
            patch("hippius_s3.services.ray_id_service.get_logger_with_ray_id", return_value=MagicMock()),
        ):
            await run_downloader_loop(
                backend_name="arion",
                queue_name="arion_download_requests",
                fetch_fn=AsyncMock(),
            )


@pytest.mark.asyncio
async def test_loop_processes_multiple_dcrs_concurrently():
    """With max_inflight>1, several DCRs should be in process_download_request
    at the same moment (not serialized)."""
    harness = _LoopHarness(max_inflight=5)

    # Enqueue 3 requests then stop
    harness.dequeue_sequence = [_make_request("a"), _make_request("b"), _make_request("c")]

    concurrent_count = 0
    max_concurrent = 0
    started = asyncio.Event()
    release = asyncio.Event()

    async def _slow_process(_req):
        nonlocal concurrent_count, max_concurrent
        concurrent_count += 1
        max_concurrent = max(max_concurrent, concurrent_count)
        if max_concurrent >= 3:
            started.set()
        # Hold open until release
        await release.wait()
        concurrent_count -= 1
        return True

    harness.process_fn = _slow_process

    # Race the loop against a controller that waits until all 3 tasks are
    # concurrently active, then releases them and lets the loop stop.
    async def controller():
        await asyncio.wait_for(started.wait(), timeout=2.0)
        release.set()

    await asyncio.gather(harness.run(), controller())

    assert max_concurrent == 3, f"expected 3 concurrent DCRs, saw {max_concurrent}"
    assert len(harness.process_calls) == 3


@pytest.mark.asyncio
async def test_loop_respects_max_inflight_capacity():
    """When inflight == max_inflight, the loop waits before spawning more."""
    harness = _LoopHarness(max_inflight=2)

    # 4 requests, but cap at 2 concurrent
    harness.dequeue_sequence = [_make_request(f"r{i}") for i in range(4)]

    concurrent_count = 0
    max_concurrent = 0
    gate = asyncio.Event()

    async def _slow_process(_req):
        nonlocal concurrent_count, max_concurrent
        concurrent_count += 1
        max_concurrent = max(max_concurrent, concurrent_count)
        await asyncio.sleep(0.05)  # Hold briefly so next dequeue must wait
        concurrent_count -= 1
        return True

    harness.process_fn = _slow_process

    async def releaser():
        # Let the loop run to completion under its own momentum
        await asyncio.sleep(1.0)
        gate.set()

    # The loop will stop on its own when the sequence is exhausted (KeyboardInterrupt)
    await asyncio.gather(harness.run(), releaser())

    assert max_concurrent <= 2, f"inflight cap breached: saw {max_concurrent} concurrent"
    assert len(harness.process_calls) == 4


@pytest.mark.asyncio
async def test_loop_rebuilds_redis_on_task_redis_error():
    """RedisConnectionError raised by a spawned task must trigger the main
    loop to rebuild redis_client + obj_cache before the next dequeue."""
    harness = _LoopHarness(max_inflight=2)

    # First request fails with redis error, second should succeed after rebuild
    harness.dequeue_sequence = [_make_request("bad"), _make_request("good")]

    second_call_redis_count = None

    async def _process(req):
        nonlocal second_call_redis_count
        if req.object_key.endswith("bad.bin"):
            raise RedisConnectionError("connection reset")
        second_call_redis_count = harness.redis_create_count
        return True

    harness.process_fn = _process

    await harness.run()

    # Initial setup creates 1 redis_client; after task's RedisConnectionError,
    # the main loop rebuilds it → 2 total create calls.
    assert harness.redis_create_count >= 2, (
        f"redis_client was not rebuilt after task RedisConnectionError (create_count={harness.redis_create_count})"
    )
    # The second DCR was processed AFTER the rebuild occurred
    assert second_call_redis_count is not None and second_call_redis_count >= 2


@pytest.mark.asyncio
async def test_loop_recreates_db_pool_on_task_interface_error():
    """asyncpg.InterfaceError raised by a spawned task must trigger the main
    loop to recreate db_pool before the next dequeue."""
    harness = _LoopHarness(max_inflight=2)

    harness.dequeue_sequence = [_make_request("bad"), _make_request("good")]

    second_call_pool_count = None

    async def _process(req):
        nonlocal second_call_pool_count
        if req.object_key.endswith("bad.bin"):
            raise asyncpg.InterfaceError("pool closed")
        second_call_pool_count = harness.db_pool_create_count
        return True

    harness.process_fn = _process

    await harness.run()

    # Initial setup creates 1 pool; after InterfaceError, main loop creates a
    # second one.
    assert harness.db_pool_create_count >= 2, (
        f"db_pool was not recreated after InterfaceError (create_count={harness.db_pool_create_count})"
    )
    assert second_call_pool_count is not None and second_call_pool_count >= 2


@pytest.mark.asyncio
async def test_loop_ignores_generic_task_exceptions_without_reconnect():
    """A generic Exception from a task is logged but must NOT cause redis/db
    reconnects — those are reserved for specific infra error classes."""
    harness = _LoopHarness(max_inflight=2)
    harness.dequeue_sequence = [_make_request("boom"), _make_request("ok")]

    async def _process(req):
        if req.object_key.endswith("boom.bin"):
            raise ValueError("not an infra error")
        return True

    harness.process_fn = _process

    await harness.run()

    # Only the initial setup should have created clients — 1 each.
    assert harness.redis_create_count == 1
    assert harness.db_pool_create_count == 1
    assert len(harness.process_calls) == 2
