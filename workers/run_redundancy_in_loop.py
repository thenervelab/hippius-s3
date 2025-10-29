#!/usr/bin/env python3
import asyncio
import contextlib
import logging
import sys
from pathlib import Path
from typing import Any

import asyncpg
import redis.asyncio as async_redis
from redis.exceptions import BusyLoadingError
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

# Add parent directory to path to import hippius_s3 modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from hippius_s3.cache import RedisObjectPartsCache
from hippius_s3.config import get_config
from hippius_s3.logging_config import setup_loki_logging
from hippius_s3.monitoring import initialize_metrics_collector
from hippius_s3.queue import DownloadChainRequest
from hippius_s3.queue import PartChunkSpec
from hippius_s3.queue import PartToDownload
from hippius_s3.queue import RedundancyRequest
from hippius_s3.queue import UploadChainRequest
from hippius_s3.queue import dequeue_ec_request
from hippius_s3.queue import enqueue_download_request
from hippius_s3.queue import enqueue_upload_request
from hippius_s3.redis_cache import initialize_cache_client
from hippius_s3.services.crypto_service import CryptoService
from hippius_s3.services.key_service import get_or_create_encryption_key_bytes
from hippius_s3.utils import get_query

config = get_config()
setup_loki_logging(config, "redundancy")
logger = logging.getLogger(__name__)


async def _get_part_row(conn: asyncpg.Connection, object_id: str, object_version: int, part_number: int):
    return await conn.fetchrow(
        """
        SELECT p.part_id, COALESCE(p.size_bytes, 0) AS size_bytes
          FROM parts p
         WHERE p.object_id = $1 AND p.object_version = $2 AND p.part_number = $3
         LIMIT 1
        """,
        object_id,
        int(object_version),
        int(part_number),
    )


async def _upsert_part_ec_replication(
    conn: asyncpg.Connection,
    *,
    part_id: str,
    policy_version: int,
    replication_factor: int,
    shard_size_bytes: int,
) -> None:
    await conn.execute(
        """
        INSERT INTO part_ec (part_id, policy_version, scheme, k, m, shard_size_bytes, stripes, state)
        VALUES ($1, $2, 'rep-v1', 1, $3, $4, 1, 'complete')
        ON CONFLICT (part_id, policy_version)
        DO UPDATE SET scheme='rep-v1', k=1, m=EXCLUDED.m, shard_size_bytes=EXCLUDED.shard_size_bytes, stripes=1, state='complete', updated_at=now()
        """,
        part_id,
        int(policy_version),
        int(max(0, replication_factor - 1)),
        int(shard_size_bytes),
    )


async def _ensure_ciphertext_in_cache(
    *,
    db: asyncpg.Connection,
    obj_cache: RedisObjectPartsCache,
    object_id: str,
    object_version: int,
    part_number: int,
    address: str,
    bucket_name: str,
) -> list[tuple[int, str, int | None]]:
    rows = await db.fetch(
        get_query("get_part_chunks_by_object_and_number"),
        object_id,
        int(object_version),
        int(part_number),
    )
    plan: list[tuple[int, str, int | None]] = [
        (int(r[0]), str(r[1]), int(r[2]) if r[2] is not None else None) for r in rows or []
    ]
    if not plan:
        return []

    missing: list[tuple[int, str, int | None]] = []
    for ci, cid, clen in plan:
        if not await obj_cache.chunk_exists(object_id, int(object_version), int(part_number), int(ci)):
            missing.append((ci, cid, clen))

    if missing:
        req = DownloadChainRequest(
            request_id=f"{object_id}::redundancy",
            object_id=object_id,
            object_version=int(object_version),
            object_key="",
            bucket_name=bucket_name,
            address=address,
            subaccount=address,
            subaccount_seed_phrase="",
            substrate_url=config.substrate_url,
            ipfs_node=config.ipfs_get_url,
            should_decrypt=False,
            size=0,
            multipart=True,
            chunks=[
                PartToDownload(
                    part_number=int(part_number),
                    chunks=[
                        PartChunkSpec(index=int(ci), cid=str(cid), cipher_size_bytes=int(clen) if clen else None)
                        for (ci, cid, clen) in missing
                    ],
                )
            ],
        )
        await enqueue_download_request(req)

        sleep_s = float(getattr(config, "http_download_sleep_loop", 0.1))
        retries = int(getattr(config, "http_redis_get_retries", 60))
        for _ in range(max(1, retries)):
            all_ready = True
            for ci, _, _ in missing:
                ok = await obj_cache.chunk_exists(object_id, int(object_version), int(part_number), int(ci))
                if not ok:
                    all_ready = False
                    break
            if all_ready:
                break
            await asyncio.sleep(sleep_s)

    return plan


async def process_redundancy_request(
    req: RedundancyRequest,
    db: asyncpg.Pool,
    redis_client: async_redis.Redis,
) -> bool:
    obj_cache = RedisObjectPartsCache(redis_client)

    async with db.acquire() as conn:
        part_row = await _get_part_row(conn, req.object_id, int(req.object_version), int(req.part_number))
        if not part_row:
            logger.error(
                f"redundancy: part row missing object_id={req.object_id} v={req.object_version} part={req.part_number}"
            )
            return False
        part_id = str(part_row[0])

        chunk_plan = await _ensure_ciphertext_in_cache(
            db=conn,
            obj_cache=obj_cache,
            object_id=req.object_id,
            object_version=int(req.object_version),
            part_number=int(req.part_number),
            address=req.address,
            bucket_name=req.bucket_name,
        )
        if not chunk_plan:
            logger.warning(f"redundancy: no chunk plan rows object_id={req.object_id} part={req.part_number}")
            return False

        meta = await obj_cache.get_meta(req.object_id, int(req.object_version), int(req.part_number))
        cipher_size = int((meta or {}).get("size_bytes", 0))
        threshold = int(getattr(config, "ec_k", 8)) * int(getattr(config, "ec_min_chunk_size_bytes", 128 * 1024))
        replication = cipher_size < threshold

        if replication:
            R = max(1, int(getattr(config, "ec_replication_factor", 2)))
            if R <= 1:
                logger.info("redundancy: replication factor <=1; nothing to do")
                return True

            key_bytes = await get_or_create_encryption_key_bytes(
                main_account_id=req.address,
                bucket_name=req.bucket_name,
            )

            staged: list[dict] = []
            for ci, _cid, _clen in sorted(chunk_plan, key=lambda x: int(x[0])):
                ct = await obj_cache.get_chunk(req.object_id, int(req.object_version), int(req.part_number), int(ci))
                if not isinstance(ct, (bytes, bytearray)):
                    continue
                pt = CryptoService.decrypt_chunk(
                    bytes(ct),
                    seed_phrase="",
                    object_id=req.object_id,
                    part_number=int(req.part_number),
                    chunk_index=int(ci),
                    key=key_bytes,
                )
                for r in range(1, R):
                    replica_chunks = CryptoService.encrypt_part_to_chunks(
                        pt,
                        object_id=req.object_id,
                        part_number=int(req.part_number),
                        seed_phrase="",
                        chunk_size=len(pt),
                        key=key_bytes,
                    )
                    replica_ct = replica_chunks[0] if replica_chunks else b""
                    if not replica_ct:
                        continue
                    # Stage replica bytes in Redis with short TTL
                    redis_key = f"obj:{req.object_id}:v:{int(req.object_version)}:part:{int(req.part_number)}:rep:{int(r)}:chunk:{int(ci)}"
                    await redis_client.setex(redis_key, int(getattr(config, "cache_ttl_seconds", 300)), replica_ct)
                    staged.append(
                        {
                            "redis_key": redis_key,
                            "chunk_index": int(ci),
                            "replica_index": int(r - 1),
                        }
                    )

            # Enqueue uploader to upload replicas and persist CIDs
            await enqueue_upload_request(
                UploadChainRequest(
                    address=req.address,
                    bucket_name=req.bucket_name,
                    object_key="",
                    object_id=req.object_id,
                    object_version=int(req.object_version),
                    chunks=[],
                    upload_id=None,
                    kind="replica",
                    policy_version=int(req.policy_version),
                    staged=staged,
                )
            )
            return True

        # TODO: EC mode to stage parity and enqueue uploader
        logger.info(f"redundancy: EC path pending implementation object_id={req.object_id} part={req.part_number}")
        return True


async def run_redundancy_loop() -> None:
    db_pool = await asyncpg.create_pool(config.database_url, min_size=2, max_size=10)
    redis_client = async_redis.from_url(config.redis_url)
    redis_queues_client = async_redis.from_url(config.redis_queues_url)

    from hippius_s3.queue import initialize_queue_client

    initialize_queue_client(redis_queues_client)
    initialize_cache_client(redis_client)
    initialize_metrics_collector(redis_client)

    logger.info("Starting redundancy service...")

    try:
        while True:
            try:
                req = await dequeue_ec_request()
            except (BusyLoadingError, RedisConnectionError, RedisTimeoutError) as e:
                logger.warning(f"Redis error while dequeuing redundancy request: {e}. Reconnecting in 2s...")
                with contextlib.suppress(Exception):
                    await redis_queues_client.aclose()
                await asyncio.sleep(2)
                redis_queues_client = async_redis.from_url(config.redis_queues_url)
                initialize_queue_client(redis_queues_client)
                continue
            except Exception as e:
                logger.error(f"Failed to dequeue/parse redundancy request, skipping: {e}", exc_info=True)
                continue

            if req:
                try:
                    ok = await process_redundancy_request(req, db_pool, redis_client)
                    if ok:
                        logger.info(
                            f"Successfully processed redundancy request object_id={req.object_id} part={req.part_number}"
                        )
                    else:
                        logger.error(
                            f"Failed to process redundancy request object_id={req.object_id} part={req.part_number}"
                        )
                except (RedisConnectionError, RedisTimeoutError, BusyLoadingError) as e:
                    logger.warning(
                        f"Redis connection issue during redundancy processing: {e}. Reconnecting in 2s and continuing..."
                    )
                    with contextlib.suppress(Exception):
                        await redis_client.aclose()
                    await asyncio.sleep(2)
                    redis_client = async_redis.from_url(config.redis_url)
                    initialize_cache_client(redis_client)
                    continue
            else:
                await asyncio.sleep(0.1)
    except KeyboardInterrupt:
        logger.info("Redundancy service stopping...")
    except Exception as e:
        logger.error(f"Error in redundancy loop: {e}")
        raise
    finally:
        await redis_client.aclose()
        await redis_queues_client.aclose()
        await db_pool.close()


if __name__ == "__main__":
    asyncio.run(run_redundancy_loop())
