#!/usr/bin/env python3
import asyncio
import contextlib
import logging
import sys
from pathlib import Path
from typing import Any
import math

import asyncpg
import redis.asyncio as async_redis
from redis.exceptions import BusyLoadingError
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

# Add parent directory to path to import hippius_s3 modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from hippius_s3.cache import FileSystemPartsStore
from hippius_s3.cache import RedisObjectPartsCache
from hippius_s3.config import get_config
from hippius_s3.logging_config import setup_loki_logging
from hippius_s3.monitoring import initialize_metrics_collector
from hippius_s3.queue import DownloadChainRequest
from hippius_s3.queue import PartChunkSpec
from hippius_s3.queue import PartToDownload
from hippius_s3.queue import RedundancyRequest
from hippius_s3.queue import ReplicaStagedItem, ParityStagedItem, ReplicaUploadRequest, ParityUploadRequest
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
        VALUES ($1, $2, 'rep-v1', 1, $3, $4, 1, 'pending_upload')
        ON CONFLICT (part_id, policy_version)
        DO UPDATE SET scheme='rep-v1', k=1, m=EXCLUDED.m, shard_size_bytes=EXCLUDED.shard_size_bytes, stripes=1, state='pending_upload', updated_at=now()
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
    fs_store = FileSystemPartsStore(getattr(config, "object_cache_dir", "/var/lib/hippius/object_cache"))

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

            # Ensure part_ec exists to satisfy FK
            await _upsert_part_ec_replication(
                conn,
                part_id=part_id,
                policy_version=int(req.policy_version),
                replication_factor=R,
                shard_size_bytes=int(cipher_size),
            )

            key_bytes = await get_or_create_encryption_key_bytes(
                main_account_id=req.address,
                bucket_name=req.bucket_name,
            )

            rep_staged: list[ReplicaStagedItem] = []
            # Stage replica files under FS and enqueue uploader
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
                    # Write staged file
                    part_dir = Path(fs_store.part_path(req.object_id, int(req.object_version), int(req.part_number)))
                    rep_dir = part_dir / f"rep_{int(r)}"
                    rep_dir.mkdir(parents=True, exist_ok=True)
                    file_path = rep_dir / f"chunk_{int(ci)}.bin"

                    def _write() -> None:
                        with file_path.open("wb") as f:
                            f.write(replica_ct)
                            f.flush()

                    await asyncio.to_thread(_write)
                    rep_staged.append(
                        ReplicaStagedItem(
                            file_path=str(file_path),
                            chunk_index=int(ci),
                            replica_index=int(r - 1),
                        )
                    )

            await enqueue_upload_request(
                ReplicaUploadRequest(
                    address=req.address,
                    bucket_name=req.bucket_name,
                    object_key="",
                    object_id=req.object_id,
                    object_version=int(req.object_version),
                    part_number=int(req.part_number),
                    kind="replica",
                    policy_version=int(req.policy_version),
                    staged=rep_staged,
                )
            )
            return True

        # EC mode (rs-v1) placeholder with m=1 using XOR across up to k chunks per stripe
        k = max(1, int(getattr(config, "ec_k", 8)))
        m = max(0, int(getattr(config, "ec_m", 1)))
        if m <= 0:
            logger.info("redundancy: EC m<=0; skipping parity generation")
            return True

        # Determine shard size: use max cipher chunk length observed (fallback to min_chunk_size)
        shard_size = int(getattr(config, "ec_min_chunk_size_bytes", 128 * 1024))
        # Attempt to use cipher_size from meta if available
        if isinstance(meta, dict):
            shard_size = max(int(meta.get("chunk_size_bytes", shard_size)), shard_size)

        stripes = int(math.ceil(len(chunk_plan) / float(k))) if chunk_plan else 0
        if stripes <= 0:
            logger.info("redundancy: EC found no stripes; skipping")
            return True

        # Upsert part_ec to 'pending_upload'
        await conn.execute(
            """
            INSERT INTO part_ec (part_id, policy_version, scheme, k, m, shard_size_bytes, stripes, state)
            VALUES ($1, $2, 'rs-v1', $3, $4, $5, $6, 'pending_upload')
            ON CONFLICT (part_id, policy_version)
            DO UPDATE SET scheme='rs-v1', k=EXCLUDED.k, m=EXCLUDED.m, shard_size_bytes=EXCLUDED.shard_size_bytes,
                          stripes=EXCLUDED.stripes, state='pending_upload', updated_at=now()
            """,
            part_id,
            int(req.policy_version),
            int(k),
            int(1),  # m=1 placeholder
            int(shard_size),
            int(stripes),
        )

        part_dir = Path(fs_store.part_path(req.object_id, int(req.object_version), int(req.part_number)))
        pv_dir = part_dir / f"pv_{int(req.policy_version)}"
        pv_dir.mkdir(parents=True, exist_ok=True)

        par_staged: list[ParityStagedItem] = []
        # Process stripes
        for s in range(stripes):
            start = s * k
            end = min(start + k, len(chunk_plan))
            stripe_items = chunk_plan[start:end]
            if not stripe_items:
                continue
            # Load ciphertext for this stripe
            blocks: list[bytes] = []
            max_len = 0
            for ci, _cid, _clen in stripe_items:
                ct = await obj_cache.get_chunk(req.object_id, int(req.object_version), int(req.part_number), int(ci))
                if not isinstance(ct, (bytes, bytearray)):
                    ct = b""
                b = bytes(ct)
                blocks.append(b)
                max_len = max(max_len, len(b))
            # Zero-extend and XOR
            if not blocks:
                continue
            max_len = max(max_len, shard_size)
            parity = bytearray(max_len)
            for b in blocks:
                if len(b) < max_len:
                    # zero-extend
                    for i in range(len(b)):
                        parity[i] ^= b[i]
                else:
                    for i in range(max_len):
                        parity[i] ^= b[i]

            # Write staged parity file
            file_path = pv_dir / f"stripe_{s}.parity_0.bin"

            def _write() -> None:
                with file_path.open("wb") as f:
                    f.write(parity)
                    f.flush()

            await asyncio.to_thread(_write)

            par_staged.append(ParityStagedItem(file_path=str(file_path), stripe_index=int(s), parity_index=0))

        if par_staged:
            await enqueue_upload_request(
                ParityUploadRequest(
                    address=req.address,
                    bucket_name=req.bucket_name,
                    object_key="",
                    object_id=req.object_id,
                    object_version=int(req.object_version),
                    part_number=int(req.part_number),
                    kind="parity",
                    policy_version=int(req.policy_version),
                    staged=par_staged,
                )
            )
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
