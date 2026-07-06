"""Unit tests for the A12 at-least-once reliable dequeue (in-flight ZSET + reaper)."""

import time

import pytest
from fakeredis.aioredis import FakeRedis

from hippius_s3.queue import _inflight_zset
from hippius_s3.queue import ack_reliable
from hippius_s3.queue import initialize_queue_client
from hippius_s3.queue import reliable_brpop
from hippius_s3.queue import requeue_stale_inflight


@pytest.mark.asyncio
async def test_reliable_brpop_moves_item_into_the_inflight_zset() -> None:
    redis = FakeRedis()
    initialize_queue_client(redis)
    await redis.lpush("arion_download_requests", "req-a")

    handle = await reliable_brpop("arion_download_requests", timeout=1, visibility_seconds=600)

    assert handle == "req-a", "the raw member is returned as the ack handle"
    # It is off the main list but tracked in the in-flight ZSET with a future deadline.
    assert await redis.llen("arion_download_requests") == 0
    members = await redis.zrange(_inflight_zset("arion_download_requests"), 0, -1, withscores=True)
    assert len(members) == 1
    assert members[0][1] > time.time(), "the visibility deadline is in the future"


@pytest.mark.asyncio
async def test_reliable_brpop_returns_none_on_empty_queue() -> None:
    redis = FakeRedis()
    initialize_queue_client(redis)
    assert await reliable_brpop("arion_download_requests", timeout=1) is None


@pytest.mark.asyncio
async def test_ack_removes_the_item_so_the_reaper_never_redelivers_it() -> None:
    redis = FakeRedis()
    initialize_queue_client(redis)
    await redis.lpush("arion_download_requests", "req-a")
    handle = await reliable_brpop("arion_download_requests", timeout=1, visibility_seconds=1)
    assert handle is not None

    await ack_reliable("arion_download_requests", handle)

    assert await redis.zcard(_inflight_zset("arion_download_requests")) == 0
    # Even well past the (1s) visibility window, an acked item is not redelivered.
    moved = await requeue_stale_inflight("arion_download_requests", now_ts=time.time() + 10)
    assert moved == 0
    assert await redis.llen("arion_download_requests") == 0


@pytest.mark.asyncio
async def test_reaper_redelivers_an_unacked_item_past_its_visibility_window() -> None:
    # The crash case: a consumer popped the item (it is in-flight) but never acked. Once its
    # visibility deadline passes, the reaper moves it back onto the main list for redelivery.
    redis = FakeRedis()
    initialize_queue_client(redis)
    await redis.lpush("arion_download_requests", "req-a")
    handle = await reliable_brpop("arion_download_requests", timeout=1, visibility_seconds=1)
    assert handle is not None
    assert await redis.llen("arion_download_requests") == 0

    # Before the deadline: nothing redelivered.
    assert await requeue_stale_inflight("arion_download_requests", now_ts=time.time()) == 0
    # After the deadline: redelivered exactly once, back onto the main list, out of in-flight.
    moved = await requeue_stale_inflight("arion_download_requests", now_ts=time.time() + 10)
    assert moved == 1
    assert await redis.llen("arion_download_requests") == 1
    assert await redis.zcard(_inflight_zset("arion_download_requests")) == 0


@pytest.mark.asyncio
async def test_reaper_respects_max_items() -> None:
    redis = FakeRedis()
    initialize_queue_client(redis)
    past = time.time() - 10
    for i in range(5):
        await redis.zadd(_inflight_zset("arion_download_requests"), {f"req-{i}": past})

    moved = await requeue_stale_inflight("arion_download_requests", now_ts=time.time(), max_items=2)
    assert moved == 2
    assert await redis.zcard(_inflight_zset("arion_download_requests")) == 3
