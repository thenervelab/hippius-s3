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

import asyncpg
import redis.asyncio as async_redis
from dotenv import load_dotenv
from pydantic import BaseModel


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


class UserCidRecord(BaseModel):
    object_id: str
    object_version: int
    cid: str


async def get_user_cids_from_db(
    db: asyncpg.Connection,
    user: str,
) -> List[UserCidRecord]:
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

    return [
        UserCidRecord(object_id=str(row["object_id"]), object_version=row["object_version"], cid=row["cid"])
        for row in rows
    ]


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


async def should_move_to_dlq(
    db: asyncpg.Connection,
    cid: str,
    max_attempts: int,
) -> bool:
    """Check if a CID has reached max attempts and should move to DLQ.

    Returns True if we've already attempted max_attempts times.
    """
    row = await db.fetchrow(
        """
        SELECT pin_attempts
        FROM part_chunks
        WHERE cid = $1
        ORDER BY pin_attempts DESC
        LIMIT 1
        """,
        cid,
    )
    attempts = int(row["pin_attempts"]) if row else 0
    return attempts >= max_attempts


async def record_pin_attempt(
    db: asyncpg.Connection,
    cid: str,
) -> None:
    """Increment pin_attempts and update last_pinned_at for a CID."""
    await db.execute(
        """
        UPDATE part_chunks
        SET pin_attempts = pin_attempts + 1,
            last_pinned_at = NOW()
        WHERE cid = $1
        """,
        cid,
    )


async def reset_pin_attempts(
    db: asyncpg.Connection,
    cids: List[str],
) -> None:
    """Reset pin tracking for CIDs confirmed on chain."""
    if not cids:
        return
    await db.execute(
        """
        UPDATE part_chunks
        SET pin_attempts = 0,
            last_pinned_at = NULL
        WHERE cid = ANY($1::text[])
        """,
        cids,
    )


async def is_cid_in_dlq(cid: str) -> bool:
    """Check if CID is already in pinner DLQ."""
    from hippius_s3.redis_cache import get_cache_client

    redis_client = get_cache_client()
    all_entries_data = await redis_client.lrange("pinner:dlq", 0, -1)  # type: ignore[misc]
    all_entries: List[bytes] = list(all_entries_data) if all_entries_data else []
    for entry_json in all_entries:
        try:
            data = json.loads(entry_json)
            if data.get("cid") == cid:
                return True
        except Exception:
            continue
    return False


async def push_cid_to_dlq(
    db: asyncpg.Connection,
    cid: str,
    user: str,
    object_id: str,
    object_version: int,
    reason: str,
) -> None:
    """Push a CID to the pin checker DLQ."""
    from hippius_s3.dlq.pinner_dlq import PinnerDLQEntry
    from hippius_s3.dlq.pinner_dlq import PinnerDLQManager
    from hippius_s3.redis_cache import get_cache_client

    row = await db.fetchrow(
        "SELECT pin_attempts, last_pinned_at FROM part_chunks WHERE cid = $1 LIMIT 1",
        cid,
    )

    pin_attempts = int(row["pin_attempts"]) if row else 0
    last_pinned_at = float(row["last_pinned_at"].timestamp()) if row and row["last_pinned_at"] else None

    entry = PinnerDLQEntry(
        cid=cid,
        user=user,
        object_id=object_id,
        object_version=object_version,
        reason=reason,
        pin_attempts=pin_attempts,
        last_pinned_at=last_pinned_at,
    )

    dlq_manager = PinnerDLQManager(get_cache_client())
    await dlq_manager.push(entry)

    get_metrics_collector().increment_pinner_dlq_total(user)


async def check_user_cids(
    db: asyncpg.Connection,
    user: str,
) -> None:
    """Check a single user's CIDs against their chain profile."""
    from hippius_s3.redis_chain import get_chain_client

    logger.debug(f"Checking CIDs for user {user}")

    db_cid_records = await get_user_cids_from_db(db, user)
    if not db_cid_records:
        logger.debug(f"User {user} has no uploaded objects with CIDs")
        return

    cid_to_record: Dict[str, UserCidRecord] = {}

    for record in db_cid_records:
        cid_to_record[record.cid] = record

    chain_cids = await get_cached_chain_cids(get_chain_client(), user)
    if not chain_cids:
        return

    db_cids_set = set(cid_to_record.keys())
    chain_cids_set = set(chain_cids)

    confirmed_cids = list(db_cids_set & chain_cids_set)
    if confirmed_cids:
        await reset_pin_attempts(db, confirmed_cids)

        from hippius_s3.dlq.pinner_dlq import PinnerDLQManager
        from hippius_s3.redis_cache import get_cache_client

        dlq_manager = PinnerDLQManager(get_cache_client())
        cleaned_count = 0
        for cid in confirmed_cids:
            removed = await dlq_manager._find_and_remove_unlocked(cid)
            if removed:
                cleaned_count += 1

        if cleaned_count > 0:
            logger.info(
                f"Reset pin attempts for {len(confirmed_cids)} confirmed CIDs + cleaned {cleaned_count} from DLQ (user={user})"
            )
        else:
            logger.info(f"Reset pin attempts for {len(confirmed_cids)} confirmed CIDs (user={user})")

    missing_cids = list(db_cids_set - chain_cids_set)

    logger.info(
        f"User {user}: S3={len(db_cids_set)} CIDs, Chain={len(chain_cids)} CIDs, Missing={len(missing_cids)} CIDs"
    )

    get_metrics_collector().set_pin_checker_missing_cids(user, len(missing_cids))

    if not missing_cids:
        logger.info(f"All S3 CIDs for user {user} are present on chain")
        return

    eligible_cids = []
    dlq_cids = []

    for cid in missing_cids:
        if await should_move_to_dlq(db, cid, config.pin_checker_max_attempts):
            dlq_cids.append(cid)
        else:
            eligible_cids.append(cid)

    if dlq_cids:
        logger.warning(f"Moving {len(dlq_cids)} CIDs to DLQ (max attempts reached, user={user})")
        for cid in dlq_cids:
            if await is_cid_in_dlq(cid):
                logger.debug(f"CID {cid} already in DLQ, skipping")
                continue

            record = cid_to_record[cid]
            await push_cid_to_dlq(
                db=db,
                cid=cid,
                user=user,
                object_id=record.object_id,
                object_version=record.object_version,
                reason="max_pin_attempts_exceeded",
            )

    if eligible_cids:
        logger.info(f"Enqueuing {len(eligible_cids)} eligible CIDs for user {user}")

        missing_by_object: Dict[tuple[str, int], List[str]] = {}
        for cid in eligible_cids:
            record = cid_to_record[cid]
            obj_key = (record.object_id, record.object_version)
            if obj_key not in missing_by_object:
                missing_by_object[obj_key] = []
            missing_by_object[obj_key].append(cid)

        for (object_id, object_version), cids in missing_by_object.items():
            for cid in cids:
                await record_pin_attempt(db, cid)

            request = SubstratePinningRequest(
                cids=cids,
                address=user,
                object_id=str(object_id),
                object_version=object_version,
            )
            await enqueue_substrate_request(request)
            logger.info(f"Enqueued substrate request object_id={object_id} version={object_version} cids={len(cids)}")


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
    redis_queues_client = async_redis.from_url(config.redis_queues_url)

    from hippius_s3.queue import initialize_queue_client
    from hippius_s3.redis_cache import initialize_cache_client
    from hippius_s3.redis_chain import initialize_chain_client

    initialize_queue_client(redis_queues_client)
    initialize_chain_client(redis_chain)
    initialize_cache_client(redis_client)
    initialize_metrics_collector(redis_chain)

    logger.info("Starting chain pin checker service...")
    logger.info(f"Database: {config.database_url}")
    logger.info(f"Redis Chain: {config.redis_chain_url}")
    logger.info(f"Redis: {config.redis_url}")
    logger.info(f"Redis Queues: {config.redis_queues_url}")

    await _wait_for_table(db, "buckets", timeout_seconds=180)
    await _validate_redis_connection(redis_chain, "redis_chain", timeout_seconds=30)
    await _validate_redis_connection(redis_client, "redis", timeout_seconds=30)

    while True:
        users = await get_all_users(db)
        logger.info(f"Checking {len(users)} users for CID consistency")

        for user in users:
            await check_user_cids(db, user)

        logger.info(f"Completed checking all {len(users)} users")

        status_counts = await get_object_status_counts(db)
        get_metrics_collector().set_object_status_counts(status_counts)
        logger.info(f"Object status counts: {status_counts}")

        await asyncio.sleep(config.pin_checker_loop_sleep)


if __name__ == "__main__":
    asyncio.run(run_chain_pin_checker_loop())
