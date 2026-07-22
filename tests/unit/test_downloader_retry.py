"""A12: the download queue gains request-level retry (per-backend ZSET + 2s mover), mirroring
the uploader. Previously a failed DownloadChainRequest was dropped on ANY failure. These tests
cover the queue primitives; the worker wiring is covered in test_downloader_loop.py.
"""

import json
import time

import pytest
from fakeredis.aioredis import FakeRedis

from hippius_s3.queue import DownloadChainRequest
from hippius_s3.queue import PartChunkSpec
from hippius_s3.queue import PartToDownload
from hippius_s3.queue import enqueue_download_retry_request
from hippius_s3.queue import initialize_queue_client
from hippius_s3.queue import move_due_download_retries


def _req(name: str = "req", attempts: int = 0) -> DownloadChainRequest:
    return DownloadChainRequest(
        object_id="aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        object_version=1,
        object_key=f"test/{name}.bin",
        bucket_name="bkt",
        address="5Addr",
        subaccount="5Addr",
        substrate_url="http://test",
        size=4096,
        multipart=False,
        chunks=[PartToDownload(part_number=1, chunks=[PartChunkSpec(index=0, cid=f"cid-{name}")])],
        attempts=attempts,
        request_id=f"rid-{name}",
    )


@pytest.mark.asyncio
async def test_enqueue_download_retry_increments_attempts_and_schedules() -> None:
    redis = FakeRedis()
    initialize_queue_client(redis)

    t0 = time.time()
    await enqueue_download_retry_request(
        _req("a", attempts=1), backend_name="arion", delay_seconds=10.0, last_error="boom"
    )

    members = await redis.zrange("arion_download_retries", 0, -1, withscores=True)
    assert len(members) == 1
    payload, score = members[0]
    assert score >= t0 + 9.5  # scheduled ~10s out

    data = json.loads(payload)
    assert data["attempts"] == 2  # incremented from 1
    assert data["last_error"] == "boom"
    assert data["request_id"] == "rid-a"


@pytest.mark.asyncio
async def test_enqueue_download_retry_sets_request_id_when_missing() -> None:
    redis = FakeRedis()
    initialize_queue_client(redis)

    req = _req("b")
    req.request_id = None
    await enqueue_download_retry_request(req, backend_name="arion", delay_seconds=1.0)

    members = await redis.zrange("arion_download_retries", 0, -1)
    data = json.loads(members[0])
    assert data["request_id"] is not None


@pytest.mark.asyncio
async def test_move_due_download_retries_moves_only_due() -> None:
    redis = FakeRedis()
    initialize_queue_client(redis)

    due = json.dumps({"object_id": "due", "attempts": 1})
    not_due = json.dumps({"object_id": "not-due", "attempts": 1})
    await redis.zadd("arion_download_retries", {due: time.time() - 10})
    await redis.zadd("arion_download_retries", {not_due: time.time() + 100})

    moved = await move_due_download_retries(backend_name="arion", now_ts=time.time())

    assert moved == 1
    popped = await redis.lpop("arion_download_requests")
    assert json.loads(popped) == json.loads(due)
    assert await redis.zcard("arion_download_retries") == 1  # not-due remains


@pytest.mark.asyncio
async def test_move_due_download_retries_respects_max_items() -> None:
    redis = FakeRedis()
    initialize_queue_client(redis)

    past = time.time() - 10
    for i in range(5):
        await redis.zadd("arion_download_retries", {json.dumps({"item": i}): past})

    moved = await move_due_download_retries(backend_name="arion", max_items=2)

    assert moved == 2
    assert await redis.llen("arion_download_requests") == 2
    assert await redis.zcard("arion_download_retries") == 3
