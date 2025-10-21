#!/usr/bin/env python3
import asyncio
import json
import logging
import sys
import time
from pathlib import Path
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

import asyncpg
import redis.asyncio as async_redis
from dotenv import load_dotenv


sys.path.insert(0, str(Path(__file__).parent.parent))

from hippius_s3.config import get_config
from hippius_s3.logging_config import setup_loki_logging
from hippius_s3.monitoring import get_metrics_collector
from hippius_s3.monitoring import initialize_metrics_collector
from hippius_s3.queue import SubstratePinningRequest
from hippius_s3.queue import enqueue_substrate_request


load_dotenv()
config = get_config()

setup_loki_logging(config, "chain-pin-checker")
logger = logging.getLogger(__name__)


async def get_user_cids_from_db(
    db: asyncpg.Connection,
    user: str,
) -> List[Tuple[str, int, str]]:
    """Get all CIDs for a user with their object metadata (object_id, object_version, cid).

    Performance note: This query uses a 3-way UNION across object_versions, parts, and part_chunks.
    For optimal performance, ensure indexes exist on:
    - buckets(main_account_id)
    - objects(bucket_id, created_at)
    - object_versions(object_id, object_version, status)
    - parts(object_id)
    - part_chunks(part_id)
    """
    rows = await db.fetch(
        """
        SELECT DISTINCT o.object_id, o.current_object_version as object_version, ov.ipfs_cid as cid
        FROM object_versions ov
        JOIN objects o ON o.object_id = ov.object_id AND o.current_object_version = ov.object_version
        JOIN buckets b ON o.bucket_id = b.bucket_id
        WHERE b.main_account_id = $1
        AND ov.ipfs_cid IS NOT NULL
        AND ov.ipfs_cid != ''
        AND ov.status = 'uploaded'
        AND o.created_at < NOW() - INTERVAL '1 hour'

        UNION

        SELECT DISTINCT o.object_id, o.current_object_version as object_version, p.ipfs_cid as cid
        FROM parts p
        JOIN objects o ON p.object_id = o.object_id
        JOIN buckets b ON o.bucket_id = b.bucket_id
        WHERE b.main_account_id = $1
        AND p.ipfs_cid IS NOT NULL
        AND p.ipfs_cid != ''
        AND o.created_at < NOW() - INTERVAL '1 hour'

        UNION

        SELECT DISTINCT o.object_id, o.current_object_version as object_version, pc.cid
        FROM part_chunks pc
        JOIN parts p ON p.part_id = pc.part_id
        JOIN objects o ON o.object_id = p.object_id
        JOIN buckets b ON b.bucket_id = o.bucket_id
        WHERE b.main_account_id = $1
        AND pc.cid IS NOT NULL
        AND pc.cid != ''
        AND o.created_at < NOW() - INTERVAL '1 hour'
        """,
        user,
    )

    return [(row["object_id"], row["object_version"], row["cid"]) for row in rows]


async def get_cached_chain_cids(
    redis_chain: async_redis.Redis,
    user: str,
) -> List[str]:
    """Get cached chain CIDs for a user, checking age requirement."""
    cache_key = f"pinned_cids:{user}"

    # Get cached data
    cached_data = await redis_chain.get(cache_key)
    if not cached_data:
        logger.info(f"User {user} does not have any cached chain profile")
        return []

    # Parse the cached data (expecting JSON with timestamp and cids)
    data = json.loads(cached_data)
    timestamp = data.get("timestamp", 0)
    current_time = time.time()

    if current_time - timestamp > 120:
        logger.info(f"User {user} cached chain profile is too old (age: {int(current_time - timestamp)}s), ignoring")
        return []

    raw_cids = data.get("cids", [])
    if not isinstance(raw_cids, list):
        logger.debug("Cached cids is not a list; ignoring")
        return []
    cids: List[str] = [str(c) for c in raw_cids]
    logger.debug(f"Retrieved {len(cids)} CIDs from cache for user {user}")
    return cids


async def check_user_cids(
    db: asyncpg.Connection,
    redis_chain: async_redis.Redis,
    redis_client: async_redis.Redis,
    user: str,
) -> None:
    """Check a single user's CIDs against their chain profile."""
    logger.debug(f"Checking CIDs for user {user}")

    db_cid_rows = await get_user_cids_from_db(db, user)
    if not db_cid_rows:
        logger.debug(f"User {user} has no uploaded objects with CIDs")
        return

    cid_to_object: Dict[str, Tuple[str, int]] = {}

    for object_id, object_version, cid in db_cid_rows:
        cid_to_object[cid] = (object_id, object_version)

    chain_cids = await get_cached_chain_cids(redis_chain, user)
    if not chain_cids:
        return

    db_cids_set = set(cid_to_object.keys())
    chain_cids_set = set(chain_cids)
    missing_cids = list(db_cids_set - chain_cids_set)

    logger.info(
        f"User {user}: S3={len(db_cids_set)} CIDs, Chain={len(chain_cids)} CIDs, Missing={len(missing_cids)} CIDs"
    )

    get_metrics_collector().set_pin_checker_missing_cids(user, len(missing_cids))

    if missing_cids:
        logger.info(f"Resubmitting {len(missing_cids)} missing CIDs for user {user}")

        missing_by_object: Dict[Tuple[str, int], List[str]] = {}
        for cid in missing_cids:
            obj_key = cid_to_object[cid]
            if obj_key not in missing_by_object:
                missing_by_object[obj_key] = []
            missing_by_object[obj_key].append(cid)

        for (object_id, object_version), cids in missing_by_object.items():
            request = SubstratePinningRequest(
                cids=cids,
                address=user,
                object_id=object_id,
                object_version=object_version,
            )
            await enqueue_substrate_request(request, redis_client)
            logger.info(f"Enqueued substrate request object_id={object_id} version={object_version} cids={len(cids)}")
    else:
        logger.info(f"All S3 CIDs for user {user} are present on chain")


async def get_all_users(
    db: asyncpg.Connection,
) -> List[str]:
    """Get all users from the database."""
    rows: List[asyncpg.Record] = await db.fetch(
        "SELECT DISTINCT main_account_id FROM buckets WHERE main_account_id IS NOT NULL"
    )
    user_ids: List[str] = [str(row["main_account_id"]) for row in rows]
    return user_ids


async def get_object_status_counts(
    db: asyncpg.Connection,
) -> dict[str, int]:
    """Get total count of objects by status across all users."""
    rows = await db.fetch(
        """
        SELECT ov.status, COUNT(*) as count
        FROM object_versions ov
        JOIN objects o ON o.object_id = ov.object_id AND o.current_object_version = ov.object_version
        WHERE ov.status IS NOT NULL
        GROUP BY ov.status
        """
    )

    return {str(row["status"]): int(row["count"]) for row in rows}


async def _wait_for_table(
    db: asyncpg.Connection, table: str, timeout_seconds: int = 120, poll_interval_seconds: float = 1.0
) -> None:
    """Wait until a given table exists. Raises after timeout."""
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            exists: Optional[str] = await db.fetchval("SELECT to_regclass($1)", f"public.{table}")
            if exists:
                logger.info(f"Schema ready: found table '{table}'")
                return
        except Exception as e:
            logger.debug(f"Error while checking for table {table}: {e}")
        await asyncio.sleep(poll_interval_seconds)
    raise TimeoutError(f"Timed out waiting for table '{table}' to exist")


async def _validate_redis_connection(
    redis_client: async_redis.Redis, name: str, timeout_seconds: int = 30, poll_interval_seconds: float = 1.0
) -> None:
    """Validate Redis connection by pinging it. Raises after timeout."""
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            await redis_client.ping()
            logger.info(f"Redis connection validated: {name}")
            return
        except Exception as e:
            logger.debug(f"Error while pinging Redis {name}: {e}")
        await asyncio.sleep(poll_interval_seconds)
    raise TimeoutError(f"Timed out waiting for Redis connection '{name}'")


async def run_chain_pin_checker_loop() -> None:
    """Main loop that checks user CIDs against chain data."""
    db = await asyncpg.connect(config.database_url)
    redis_chain = async_redis.from_url(config.redis_chain_url)
    redis_client = async_redis.from_url(config.redis_url)

    initialize_metrics_collector(redis_chain)

    logger.info("Starting chain pin checker service...")
    logger.info(f"Database: {config.database_url}")
    logger.info(f"Redis Chain: {config.redis_chain_url}")
    logger.info(f"Redis: {config.redis_url}")

    await _wait_for_table(db, "buckets", timeout_seconds=180)
    await _validate_redis_connection(redis_chain, "redis_chain", timeout_seconds=30)
    await _validate_redis_connection(redis_client, "redis", timeout_seconds=30)

    while True:
        users = await get_all_users(db)
        logger.info(f"Checking {len(users)} users for CID consistency")

        for user in users:
            await check_user_cids(db, redis_chain, redis_client, user)

        logger.info(f"Completed checking all {len(users)} users")

        status_counts = await get_object_status_counts(db)
        get_metrics_collector().set_object_status_counts(status_counts)
        logger.info(f"Object status counts: {status_counts}")

        await asyncio.sleep(config.pin_checker_loop_sleep)


if __name__ == "__main__":
    asyncio.run(run_chain_pin_checker_loop())
