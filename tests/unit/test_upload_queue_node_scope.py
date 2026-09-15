"""The node-scoped upload queue: a part on one ingest node's SSD is uploaded by the uploader
pod on THAT node, so its request, its retries and its DLQ re-queue must all land on that
node's lists. A pool-era request (no node_id) keeps the global lists the pool-reading
Deployment drains.
"""

from __future__ import annotations

import json
import os
import time
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from fakeredis.aioredis import FakeRedis

from hippius_s3 import queue as q
from hippius_s3.queue import Chunk
from hippius_s3.queue import UploadChainRequest
from hippius_s3.queue import upload_queue_name
from hippius_s3.workers.uploader import Uploader


def _request(node_id: str | None, object_id: str = "466916c0-d61b-4518-b81b-9576b574270a") -> UploadChainRequest:
    return UploadChainRequest(
        address="5Addr",
        bucket_name="b",
        object_key="k",
        object_id=object_id,
        object_version=5,
        chunks=[Chunk(id=1)],
        upload_backends=["arion"],
        node_id=node_id,
    )


def test_queue_name_is_node_scoped_only_with_a_node() -> None:
    # Mirrors the Rust producer (crates/hippius-drain-agent/src/enqueue.rs upload_queue_name);
    # the drain LPUSHes to exactly this name and the DaemonSet pod BRPOPs exactly this name.
    assert upload_queue_name("arion", "ingest-node-1") == "arion_upload_requests:ingest-node-1"
    assert upload_queue_name("arion", None) == "arion_upload_requests"
    assert upload_queue_name("arion", "") == "arion_upload_requests"


@pytest.mark.asyncio
async def test_enqueue_routes_by_the_payloads_node_and_retries_stay_on_that_node() -> None:
    # Routing by payload.node_id is what brings a re-driven request back to the node that
    # holds its bytes. A retry ZSET is scoped the same way, and only that node's mover moves it
    # back — onto that node's work queue.
    redis = FakeRedis()
    q.initialize_queue_client(redis)
    with patch.object(q, "get_config", return_value=MagicMock(upload_backends=["arion"])):
        await q.enqueue_upload_to_backends(_request("ingest-node-1"))
        await q.enqueue_upload_to_backends(_request(None))
    assert await redis.llen("arion_upload_requests:ingest-node-1") == 1
    assert await redis.llen("arion_upload_requests") == 1

    await q.enqueue_retry_request(_request("ingest-node-1"), backend_name="arion", delay_seconds=0)
    assert await redis.zcard("arion_upload_retries:ingest-node-1") == 1
    assert await redis.zcard("arion_upload_retries") == 0

    assert await q.move_due_upload_retries(backend_name="arion", now_ts=time.time() + 1) == 0, (
        "the global mover never sees it"
    )
    assert await q.move_due_upload_retries(backend_name="arion", node_id="ingest-node-1", now_ts=time.time() + 1) == 1
    assert await redis.llen("arion_upload_requests:ingest-node-1") == 2
    moved = json.loads(await redis.lpop("arion_upload_requests:ingest-node-1"))
    assert moved["node_id"] == "ingest-node-1", "the payload keeps its node through a retry"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("node_name", "expected_queue", "expected_node"),
    [("ingest-node-1", "arion_upload_requests:ingest-node-1", "ingest-node-1"), (None, "arion_upload_requests", None)],
)
async def test_the_loop_consumes_the_queue_its_node_name_selects(
    node_name: str | None, expected_queue: str, expected_node: str | None
) -> None:
    # NODE_NAME set: the DaemonSet pod reads its own node's list and moves its own node's retry
    # ZSET. Unset: the base Deployment reads the global list and the global ZSET — a node-scoped
    # retry ZSET it moved would strand every retry of that node on the global list.
    from workers import run_arion_uploader_in_loop as up

    seen: list[str] = []

    async def _dequeue(queue_name: str):
        seen.append(queue_name)
        raise KeyboardInterrupt()

    async def _with_redis_retry(func, client, url, name, **kw):
        return await func(client), client

    mover = AsyncMock(return_value=0)
    cfg = MagicMock(uploader_max_inflight=2, uploader_db_pool_max=4, arion_upload_concurrency=2)
    cfg.redis_url = "redis://localhost:6379"
    cfg.redis_queues_url = "redis://localhost:6382"
    cfg.database_url = "postgresql://localhost/test"
    env = {"NODE_NAME": node_name} if node_name is not None else {}
    with (
        patch.dict(os.environ, env, clear=False),
        patch.object(up, "config", cfg),
        patch.object(up, "asyncpg") as mock_asyncpg,
        patch.object(up, "ArionClient", return_value=MagicMock()),
        patch.object(up, "Uploader", return_value=MagicMock()),
        patch.object(up, "with_redis_retry", side_effect=_with_redis_retry),
        patch.object(up, "dequeue_upload_request", side_effect=_dequeue),
        patch.object(up, "move_due_upload_retries", new=mover),
        patch.object(up, "initialize_cache_client"),
        patch.object(up, "initialize_metrics_collector"),
        patch("hippius_s3.queue.initialize_queue_client"),
        patch("hippius_s3.redis_utils.create_redis_client", return_value=MagicMock(aclose=AsyncMock())),
        patch("redis.asyncio.Redis.from_url", return_value=MagicMock(aclose=AsyncMock())),
    ):
        if node_name is None:
            os.environ.pop("NODE_NAME", None)
        pool = MagicMock()
        pool.close = AsyncMock()
        mock_asyncpg.create_pool = AsyncMock(return_value=pool)
        await up.run_arion_uploader_loop()

    assert seen == [expected_queue]
    if mover.await_count:
        assert mover.await_args.kwargs.get("node_id") == expected_node, "the retry mover is scoped like the queue"


def _uploader(conn: AsyncMock, *, upload_backends: list[str], backup_backends: list[str]) -> Uploader:
    pool = MagicMock()
    pool.acquire = MagicMock(return_value=MagicMock(__aenter__=AsyncMock(return_value=conn), __aexit__=AsyncMock()))
    cfg = MagicMock(uploader_multipart_max_concurrency=2, arion_upload_concurrency=2, cache_ttl_seconds=60)
    cfg.object_cache_dir = "/tmp/test_cache"
    cfg.upload_backends = upload_backends
    cfg.backup_backends = backup_backends
    return Uploader(pool, FakeRedis(), FakeRedis(), cfg, backend_name="arion", backend_client=MagicMock())


@pytest.mark.asyncio
async def test_a_finished_part_flips_the_drains_row_from_uploading_to_replicated() -> None:
    # The uploader's half of the hand-off: only once EVERY chunk's chunk_backend row is written
    # may the drain's row leave `uploading` — `replicated` is what lets the evictor unlink the
    # SSD copy, so flipping per chunk would risk the only copy of a half-uploaded part. The
    # flip carries the digest of the bytes this upload sent, which the SQL fences on.
    from hippius_s3.workers.uploader import ChunkUploadResult

    conn = AsyncMock()
    uploader = _uploader(conn, upload_backends=["arion"], backup_backends=[])

    await uploader._confirm_uploaded(
        "466916c0-d61b-4518-b81b-9576b574270a",
        5,
        [
            ChunkUploadResult(cids=["a"], part_number=1, digest="d1"),
            ChunkUploadResult(cids=["b"], part_number=2, digest="d2"),
        ],
    )

    assert conn.execute.await_count == 2
    sql, object_id, version, part_number, digest = conn.execute.await_args_list[0].args
    assert "status = 'uploading'" in sql and "SET status = 'replicated'" in sql and "content_sha256 = $4" in sql
    assert (object_id, version, part_number, digest) == ("466916c0-d61b-4518-b81b-9576b574270a", 5, 1, "d1")
    assert conn.execute.await_args_list[1].args[3:] == (2, "d2")


@pytest.mark.asyncio
async def test_the_flip_is_left_to_the_sweep_when_a_backup_backend_is_required() -> None:
    # With a backup backend configured the part is not replicated until BOTH backends hold it,
    # and only the drain's sweep sees both; this uploader flipping on its own success would let
    # the evictor free the SSD copy before the backup upload has read it.
    from hippius_s3.workers.uploader import ChunkUploadResult

    conn = AsyncMock()
    uploader = _uploader(conn, upload_backends=["arion"], backup_backends=["ovh"])

    await uploader._confirm_uploaded(
        "466916c0-d61b-4518-b81b-9576b574270a", 5, [ChunkUploadResult(cids=["a"], part_number=1, digest="d1")]
    )

    assert conn.execute.await_count == 0


@pytest.mark.asyncio
async def test_a_dlq_requeue_goes_back_to_the_node_that_holds_the_bytes() -> None:
    # The operator's re-queue tool (dlq_requeue -> UploadDLQManager) is the only way a
    # dead-lettered drain request gets another go. On the global queue the base uploader
    # cannot read the node's SSD, finds no chunks, and dead-letters it again as permanent.
    from hippius_s3.dlq.upload_dlq import UploadDLQManager

    redis = FakeRedis()
    q.initialize_queue_client(redis)
    mgr = UploadDLQManager(redis, backend_name="arion")
    await mgr.push(_request("ingest-node-1", object_id="obj-node"), last_error="502", error_type="transient")
    await mgr.push(_request(None, object_id="obj-pool"), last_error="502", error_type="transient")

    assert await mgr.requeue("obj-node") is True
    assert await redis.llen("arion_upload_requests:ingest-node-1") == 1
    assert await redis.llen("arion_upload_requests") == 0

    assert await mgr.requeue_all() == 1
    assert await redis.llen("arion_upload_requests") == 1, "a pool-era request stays on the global queue"
    assert await redis.llen("arion_upload_requests:ingest-node-1") == 1
