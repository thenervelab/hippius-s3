#!/usr/bin/env python3
import asyncio
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import List

import asyncpg
import redis.asyncio as async_redis
from dotenv import load_dotenv


sys.path.insert(0, str(Path(__file__).parent.parent))

from hippius_s3.config import get_config
from workers.substrate import get_all_pinned_cids
from workers.substrate import get_all_storage_requests


load_dotenv()
config = get_config()

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


async def get_all_unique_users(db: asyncpg.Connection,) -> List[str]:
    """Get all unique users from the database."""
    users = await db.fetch("""
        SELECT DISTINCT main_account_id
        FROM buckets
        WHERE main_account_id IS NOT NULL
        ORDER BY main_account_id
    """)
    return [row["main_account_id"] for row in users]


async def cache_user_chain_profile(redis_chain: async_redis.Redis, user: str, storage_requests_dict: dict,) -> None:
    """Download and cache chain profile data for a single user."""
    logger.debug(f"Caching chain profile for user {user}")

    # Get pinned CIDs from chain for this specific user
    pinned_cids = get_all_pinned_cids(user, config.substrate_url)

    # Get storage CIDs for this user from the pre-fetched dictionary
    storage_cids = storage_requests_dict.get(user, [])

    # Merge the lists and remove duplicates
    all_cids = list(set(pinned_cids + storage_cids))

    # Prepare cache data with timestamp
    cache_data = {
        "timestamp": time.time(),
        "cids": all_cids,
        "pinned_count": len(pinned_cids),
        "storage_count": len(storage_cids),
        "total_count": len(all_cids),
    }

    # Store in Redis with key format expected by chain_pin_checker
    cache_key = f"pinned_cids:{user}"

    # Set with TTL of 1 hour (3600 seconds)
    await redis_chain.setex(
        cache_key,
        600,  # 1 hour TTL
        json.dumps(cache_data),
    )

    logger.info(
        f"Cached {len(all_cids)} CIDs for user {user} (pinned: {len(pinned_cids)}, storage: {len(storage_cids)})"
    )


async def run_chain_profile_cacher_loop():
    """Main loop that downloads and caches substrate chain profiles for all users."""
    # Connect to database
    db = await asyncpg.connect(config.database_url)

    # Connect to redis-chain
    redis_chain_url = os.getenv("REDIS_CHAIN_URL", "redis://127.0.0.1:6381/0")
    redis_chain = async_redis.from_url(redis_chain_url)

    logger.info("Starting chain profile cacher service...")
    logger.info(f"Database: {config.database_url}")
    logger.info(f"Redis Chain: {redis_chain_url}")

    while True:
        # Get all unique users
        users = await get_all_unique_users(db)
        logger.info(f"Caching chain profiles for {len(users)} users")

        # Pre-fetch all storage requests once (more efficient than per-user calls)
        logger.info("Fetching all storage requests from chain...")
        storage_requests_dict = get_all_storage_requests(config.substrate_url)
        logger.info(f"Fetched storage requests for {len(storage_requests_dict)} users")

        # Process each user
        for user in users:
            await cache_user_chain_profile(redis_chain, user, storage_requests_dict)

        logger.info(f"Completed caching chain profiles for all {len(users)} users")

        # Sleep before next iteration
        await asyncio.sleep(config.cacher_loop_sleep)


if __name__ == "__main__":
    asyncio.run(run_chain_profile_cacher_loop())
