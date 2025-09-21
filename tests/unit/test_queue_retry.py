"""Unit tests for queue retry functionality."""

import json
import time

import pytest
from fakeredis.aioredis import FakeRedis

from hippius_s3.queue import Chunk
from hippius_s3.queue import SimpleUploadChainRequest
from hippius_s3.queue import enqueue_retry_request
from hippius_s3.queue import move_due_retries_to_primary


@pytest.mark.asyncio
async def test_enqueue_retry_request_sets_attempts_and_schedules() -> None:
    """Test that enqueue_retry_request increments attempts and schedules with delay."""
    redis = FakeRedis()
    payload = SimpleUploadChainRequest(
        substrate_url="http://test",
        ipfs_node="http://test",
        address="user1",
        subaccount="user1",
        subaccount_seed_phrase="test-seed",
        bucket_name="test-bucket",
        object_key="test-key",
        should_encrypt=False,
        object_id="obj-123",
        chunk=Chunk(id=0),
        attempts=1,
        first_enqueued_at=time.time(),
        request_id="req-456",
    )

    await enqueue_retry_request(payload, redis, delay_seconds=10.0, last_error="test error")

    # Check ZSET has the item
    members = await redis.zrange("upload_retries", 0, -1, withscores=True)
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
async def test_move_due_retries_to_primary() -> None:
    """Test that due retries are moved to primary queue."""
    redis = FakeRedis()

    # Add a due retry (score = past time)
    past_time = time.time() - 10
    payload_data = {"test": "data", "attempts": 1}
    await redis.zadd("upload_retries", {json.dumps(payload_data): past_time})

    # Add a not-due retry (score = future time)
    future_time = time.time() + 100
    await redis.zadd("upload_retries", {json.dumps({"not_due": True}): future_time})

    moved = await move_due_retries_to_primary(redis, now_ts=time.time())

    assert moved == 1  # Only the due one moved

    # Check primary queue has the item
    primary_item = await redis.lpop("upload_requests")
    assert json.loads(primary_item) == payload_data

    # Check ZSET still has the not-due item
    remaining = await redis.zcard("upload_retries")
    assert remaining == 1


@pytest.mark.asyncio
async def test_move_due_retries_respects_max_items() -> None:
    """Test that move_due_retries_to_primary respects max_items limit."""
    redis = FakeRedis()

    # Add multiple due retries
    past_time = time.time() - 10
    for i in range(5):
        payload_data = {"item": i}
        await redis.zadd("upload_retries", {json.dumps(payload_data): past_time})

    # Move with limit
    moved = await move_due_retries_to_primary(redis, max_items=2)

    assert moved == 2

    # Check primary queue has exactly 2 items
    primary_items = []
    for _ in range(3):  # Try to pop 3, but only 2 should exist
        item = await redis.lpop("upload_requests")
        if item:
            primary_items.append(item)

    assert len(primary_items) == 2

    # Check ZSET has remaining items
    remaining = await redis.zcard("upload_retries")
    assert remaining == 3
