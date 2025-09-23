#!/usr/bin/env python3
import asyncio
import json
import logging
import sys
import time
from pathlib import Path
from typing import List

import asyncpg
import redis.asyncio as async_redis
from dotenv import load_dotenv


sys.path.insert(0, str(Path(__file__).parent.parent))

from hippius_s3.config import get_config
from workers.substrate import resubmit_substrate_pinning_request


load_dotenv()
config = get_config()

# Set logging level based on config
log_level = getattr(logging, config.log_level.upper(), logging.INFO)
logging.basicConfig(level=log_level, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


async def get_user_cids_from_db(
    db: asyncpg.Connection,
    user: str,
) -> List[str]:
    """Get all CIDs for a user from the database (both simple and multipart)."""
    user_cids = []

    # Get simple object CIDs for this user
    simple_objects = await db.fetch(
        """
        SELECT DISTINCT c.cid
        FROM objects o
        JOIN buckets b ON o.bucket_id = b.bucket_id
        JOIN cids c ON o.cid_id = c.id
        WHERE b.main_account_id = $1
        AND o.multipart = FALSE
        AND o.cid_id IS NOT NULL
        AND o.status = 'uploaded'
        AND o.created_at < NOW() - INTERVAL '1 hour'
    """,
        user,
    )

    for row in simple_objects:
        user_cids.append(row["cid"])  # noqa: PERF401

    # Get multipart object CIDs (main CID + parts CIDs) for this user
    multipart_objects = await db.fetch(
        """
        SELECT DISTINCT c.cid
        FROM objects o
        JOIN buckets b ON o.bucket_id = b.bucket_id
        JOIN cids c ON o.cid_id = c.id
        WHERE b.main_account_id = $1
        AND o.multipart = TRUE
        AND o.cid_id IS NOT NULL
        AND o.status = 'uploaded'
        AND o.created_at < NOW() - INTERVAL '1 hour'
    """,
        user,
    )

    user_cids.extend(row["cid"] for row in multipart_objects)

    # Get all parts CIDs for multipart uploads of this user
    parts_cids = await db.fetch(
        """
        SELECT DISTINCT c.cid
        FROM parts p
        JOIN objects o ON p.object_id = o.object_id
        JOIN buckets b ON o.bucket_id = b.bucket_id
        JOIN cids c ON p.cid_id = c.id
        WHERE b.main_account_id = $1
        AND p.cid_id IS NOT NULL
        AND o.status = 'uploaded'
        AND o.created_at < NOW() - INTERVAL '1 hour'
    """,
        user,
    )

    user_cids.extend(row["cid"] for row in parts_cids)

    return list(set(user_cids))  # Remove duplicates


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

    cids = data.get("cids", [])
    logger.debug(f"Retrieved {len(cids)} CIDs from cache for user {user}")
    return cids


async def check_user_cids(
    db: asyncpg.Connection,
    redis_chain: async_redis.Redis,
    user: str,
) -> None:
    """Check a single user's CIDs against their chain profile."""
    logger.debug(f"Checking CIDs for user {user}")

    # Get user's CIDs from database
    db_cids = await get_user_cids_from_db(db, user)
    if not db_cids:
        logger.debug(f"User {user} has no uploaded objects with CIDs")
        return

    # Get user's cached chain CIDs
    chain_cids = await get_cached_chain_cids(redis_chain, user)
    if not chain_cids:
        return  # Already logged in get_cached_chain_cids

    # Find missing CIDs (in DB but not on chain)
    db_cids_set = set(db_cids)
    chain_cids_set = set(chain_cids)
    missing_cids = list(db_cids_set - chain_cids_set)

    # Log CID comparison for this user
    logger.info(f"User {user}: S3={len(db_cids)} CIDs, Chain={len(chain_cids)} CIDs, Missing={len(missing_cids)} CIDs")

    if missing_cids:
        logger.info(f"Resubmitting {len(missing_cids)} missing CIDs for user {user}")
        await resubmit_substrate_pinning_request(
            user,
            missing_cids,
            config.resubmission_seed_phrase,
            config.substrate_url,
        )
    else:
        logger.info(f"All S3 CIDs for user {user} are present on chain")


async def get_all_users(
    db: asyncpg.Connection,
) -> List[str]:
    """Get all users from the database."""
    users = await db.fetch("SELECT DISTINCT main_account_id FROM buckets WHERE main_account_id IS NOT NULL")
    return [row["main_account_id"] for row in users]


async def run_chain_pin_checker_loop():
    """Main loop that checks user CIDs against chain data."""
    # Connect to database
    db = await asyncpg.connect(config.database_url)

    # Connect to redis-chain (identical to redis-accounts)
    redis_chain_url = config.redis_chain_url
    redis_chain = async_redis.from_url(redis_chain_url)

    logger.info("Starting chain pin checker service...")
    logger.info(f"Database: {config.database_url}")
    logger.info(f"Redis Chain: {redis_chain_url}")

    while True:
        # Get all users
        users = await get_all_users(db)
        logger.info(f"Checking {len(users)} users for CID consistency")

        # Process each user
        for user in users:
            await check_user_cids(db, redis_chain, user)

        logger.info(f"Completed checking all {len(users)} users")

        # Sleep before next iteration
        await asyncio.sleep(config.pin_checker_loop_sleep)


if __name__ == "__main__":
    asyncio.run(run_chain_pin_checker_loop())
