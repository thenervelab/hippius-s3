"""Unit tests for queue retry functionality."""

import asyncio
import json
import shutil
import socket
import time
from collections.abc import AsyncIterator
from typing import Any

import pytest
import pytest_asyncio
import redis.asyncio as async_redis
from fakeredis.aioredis import FakeRedis

from hippius_s3.queue import Chunk
from hippius_s3.queue import UnpinChainRequest
from hippius_s3.queue import UploadChainRequest
from hippius_s3.queue import enqueue_retry_request
from hippius_s3.queue import enqueue_unpin_retry_request
from hippius_s3.queue import initialize_queue_client
from hippius_s3.queue import move_due_upload_retries


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


@pytest_asyncio.fixture
async def real_redis() -> AsyncIterator[async_redis.Redis]:
    """A throwaway redis-server. The retry mover runs a Lua script, which fakeredis cannot execute."""
    binary = shutil.which("redis-server")
    if binary is None:
        pytest.skip("redis-server not installed; the atomic retry-mover needs real Lua")

    port = _free_port()
    proc = await asyncio.create_subprocess_exec(
        binary,
        "--port",
        str(port),
        "--save",
        "",
        "--appendonly",
        "no",
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    client = async_redis.Redis(host="127.0.0.1", port=port, decode_responses=True)
    try:
        for _ in range(100):
            try:
                await client.ping()
                break
            except Exception:  # noqa: BLE001 — server is still booting
                await asyncio.sleep(0.05)
        else:
            pytest.fail("redis-server did not come up")
        yield client
    finally:
        await client.aclose()
        proc.terminate()
        await proc.wait()


class _BarrierClient:
    """Forces every concurrent mover to reach the claim step before any of them claims.

    Without this the race is scheduling-dependent and the test passes against a racy
    implementation by luck.
    """

    def __init__(self, inner: async_redis.Redis, barrier: asyncio.Barrier) -> None:
        self._inner = inner
        self._barrier = barrier

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    async def zrangebyscore(self, *args: Any, **kwargs: Any) -> Any:
        result = await self._inner.zrangebyscore(*args, **kwargs)
        await self._barrier.wait()
        return result

    async def eval(self, *args: Any, **kwargs: Any) -> Any:
        await self._barrier.wait()
        return await self._inner.eval(*args, **kwargs)


@pytest.mark.asyncio
async def test_move_due_upload_retries_pushes_each_member_exactly_once(real_redis: async_redis.Redis) -> None:
    """Concurrent movers on N replicas must not re-enqueue the same due member N times."""
    movers = 6
    member = json.dumps({"object_id": "obj-race", "attempts": 1})
    await real_redis.zadd("arion_upload_retries", {member: time.time() - 10})

    barrier = asyncio.Barrier(movers)
    initialize_queue_client(_BarrierClient(real_redis, barrier))  # type: ignore[arg-type]

    results = await asyncio.gather(
        *(move_due_upload_retries(backend_name="arion", now_ts=time.time()) for _ in range(movers))
    )

    assert await real_redis.llen("arion_upload_requests") == 1
    assert await real_redis.zcard("arion_upload_retries") == 0
    assert sum(results) == 1


@pytest.mark.asyncio
async def test_enqueue_retry_request_sets_attempts_and_schedules() -> None:
    """Test that enqueue_retry_request increments attempts and schedules with delay."""
    redis = FakeRedis()
    initialize_queue_client(redis)
    payload = UploadChainRequest(
        substrate_url="http://test",
        address="user1",
        subaccount="user1",
        bucket_name="test-bucket",
        object_key="test-key",
        should_encrypt=False,
        object_id="obj-123",
        object_version=1,
        chunks=[Chunk(id=1)],
        upload_id=None,
        attempts=1,
        first_enqueued_at=time.time(),
        request_id="req-456",
    )

    await enqueue_retry_request(payload, backend_name="arion", delay_seconds=10.0, last_error="test error")

    # Check ZSET has the item (per-backend key)
    members = await redis.zrange("arion_upload_retries", 0, -1, withscores=True)
    assert len(members) == 1

    stored_payload = members[0][0]
    score = members[0][1]

    # Should be scheduled for future
    assert score > time.time()

    # Parse and verify payload
    import json

    data = json.loads(stored_payload)
    assert data["attempts"] == 2  # incremented
    assert data["last_error"] == "test error"
    assert data["request_id"] == "req-456"


@pytest.mark.asyncio
async def test_move_due_upload_retries(real_redis: async_redis.Redis) -> None:
    """Test that due retries are moved to the backend's upload queue."""
    redis = real_redis
    initialize_queue_client(redis)

    # Add a due retry (score = past time)
    past_time = time.time() - 10
    payload_data = {"test": "data", "attempts": 1}
    await redis.zadd("arion_upload_retries", {json.dumps(payload_data): past_time})

    # Add a not-due retry (score = future time)
    future_time = time.time() + 100
    await redis.zadd("arion_upload_retries", {json.dumps({"not_due": True}): future_time})

    moved = await move_due_upload_retries(backend_name="arion", now_ts=time.time())

    assert moved == 1  # Only the due one moved

    # Check backend queue has the item
    primary_item = await redis.lpop("arion_upload_requests")
    assert json.loads(primary_item) == payload_data

    # Check ZSET still has the not-due item
    remaining = await redis.zcard("arion_upload_retries")
    assert remaining == 1


@pytest.mark.asyncio
async def test_move_due_upload_retries_respects_max_items(real_redis: async_redis.Redis) -> None:
    """Test that move_due_upload_retries respects max_items limit."""
    redis = real_redis
    initialize_queue_client(redis)

    # Add multiple due retries
    past_time = time.time() - 10
    for i in range(5):
        payload_data = {"item": i}
        await redis.zadd("arion_upload_retries", {json.dumps(payload_data): past_time})

    # Move with limit
    moved = await move_due_upload_retries(backend_name="arion", max_items=2)

    assert moved == 2

    # Check backend queue has exactly 2 items
    primary_items = []
    for _ in range(3):  # Try to pop 3, but only 2 should exist
        item = await redis.lpop("arion_upload_requests")
        if item:
            primary_items.append(item)

    assert len(primary_items) == 2

    # Check ZSET has remaining items
    remaining = await redis.zcard("arion_upload_retries")
    assert remaining == 3


@pytest.mark.asyncio
async def test_enqueue_unpin_retry_request_increments_attempts() -> None:
    """Test that enqueue_unpin_retry_request increments attempts and stores correctly."""
    redis = FakeRedis()
    initialize_queue_client(redis)

    payload = UnpinChainRequest(
        address="5FakeAddress",
        object_id="obj-123",
        object_version=1,
        attempts=0,
        request_id="req-unpin-001",
        first_enqueued_at=time.time(),
    )

    t0 = time.time()
    await enqueue_unpin_retry_request(
        payload, backend_name="arion", delay_seconds=5.0, last_error="no_chunk_backend_rows"
    )

    members = await redis.zrange("arion_unpin_retries", 0, -1, withscores=True)
    assert len(members) == 1

    stored_payload = members[0][0]
    if isinstance(stored_payload, (bytes, bytearray)):
        stored_payload = stored_payload.decode()
    score = members[0][1]
    assert score >= t0 + 4.5  # delay_seconds=5.0 minus small tolerance

    data = json.loads(stored_payload)
    assert data["attempts"] == 1  # incremented from 0
    assert data["last_error"] == "no_chunk_backend_rows"
    assert data["request_id"] == "req-unpin-001"
