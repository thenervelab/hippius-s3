"""Unit tests for queue retry functionality."""

import json
import time

import pytest
from fakeredis.aioredis import FakeRedis

from hippius_s3.queue import Chunk
from hippius_s3.queue import UnpinChainRequest
from hippius_s3.queue import UploadChainRequest
from hippius_s3.queue import enqueue_retry_request
from hippius_s3.queue import enqueue_unpin_retry_request
from hippius_s3.queue import initialize_queue_client
from hippius_s3.queue import move_due_upload_retries


@pytest.mark.asyncio
async def test_enqueue_retry_request_sets_attempts_and_schedules() -> None:
    """Test that enqueue_retry_request increments attempts and schedules with delay."""
    redis = FakeRedis()
    initialize_queue_client(redis)
    payload = UploadChainRequest(
        substrate_url="http://test",
        address="user1",
        subaccount="user1",
        subaccount_seed_phrase="test-seed",
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

    await enqueue_retry_request(payload, backend_name="ipfs", delay_seconds=10.0, last_error="test error")

    # Check ZSET has the item (per-backend key)
    members = await redis.zrange("ipfs_upload_retries", 0, -1, withscores=True)
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
async def test_move_due_upload_retries() -> None:
    """Test that due retries are moved to the backend's upload queue."""
    redis = FakeRedis()
    initialize_queue_client(redis)

    # Add a due retry (score = past time)
    past_time = time.time() - 10
    payload_data = {"test": "data", "attempts": 1}
    await redis.zadd("ipfs_upload_retries", {json.dumps(payload_data): past_time})

    # Add a not-due retry (score = future time)
    future_time = time.time() + 100
    await redis.zadd("ipfs_upload_retries", {json.dumps({"not_due": True}): future_time})

    moved = await move_due_upload_retries(backend_name="ipfs", now_ts=time.time())

    assert moved == 1  # Only the due one moved

    # Check backend queue has the item
    primary_item = await redis.lpop("ipfs_upload_requests")
    assert json.loads(primary_item) == payload_data

    # Check ZSET still has the not-due item
    remaining = await redis.zcard("ipfs_upload_retries")
    assert remaining == 1


@pytest.mark.asyncio
async def test_move_due_upload_retries_respects_max_items() -> None:
    """Test that move_due_upload_retries respects max_items limit."""
    redis = FakeRedis()
    initialize_queue_client(redis)

    # Add multiple due retries
    past_time = time.time() - 10
    for i in range(5):
        payload_data = {"item": i}
        await redis.zadd("ipfs_upload_retries", {json.dumps(payload_data): past_time})

    # Move with limit
    moved = await move_due_upload_retries(backend_name="ipfs", max_items=2)

    assert moved == 2

    # Check backend queue has exactly 2 items
    primary_items = []
    for _ in range(3):  # Try to pop 3, but only 2 should exist
        item = await redis.lpop("ipfs_upload_requests")
        if item:
            primary_items.append(item)

    assert len(primary_items) == 2

    # Check ZSET has remaining items
    remaining = await redis.zcard("ipfs_upload_retries")
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
