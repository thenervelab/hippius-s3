#!/usr/bin/env python3
import asyncio
import io
import json
import logging
import sys
import time
from pathlib import Path
from typing import List

import asyncpg
import httpx
import pandas as pd
import redis.asyncio as async_redis
from dotenv import load_dotenv


sys.path.insert(0, str(Path(__file__).parent.parent))

from hippius_s3.config import get_config
from hippius_s3.logging_config import setup_loki_logging
from workers.substrate import SubstrateClient
from workers.substrate import get_all_storage_requests


load_dotenv()
config = get_config()

setup_loki_logging(config, "chain-profile-cacher")
logger = logging.getLogger(__name__)


async def get_all_unique_users(
    db: asyncpg.Connection,
) -> List[str]:
    """Get all unique users from the database."""
    users = await db.fetch("""
        SELECT DISTINCT main_account_id
        FROM buckets
        WHERE main_account_id IS NOT NULL
        ORDER BY main_account_id
    """)
    return [row["main_account_id"] for row in users]


async def fetch_profile_content(http_client: httpx.AsyncClient, cid: str, gateway_url: str) -> dict:
    url = f"{gateway_url}/ipfs/{cid}"
    response = await http_client.get(url)

    if response.status_code == 200:
        try:
            df = pd.read_parquet(io.BytesIO(response.content))
            profile_data = df.to_dict(orient="records")
            logger.debug(f"Successfully fetched profile content for CID {cid}")
            return profile_data
        except Exception as e:
            if "ArrowInvalid" in str(type(e).__name__) or "Parquet magic bytes not found" in str(e):
                logger.info(f"Parquet read failed for {cid}, attempting JSON fallback")
                try:
                    profile_data = json.loads(response.content)
                    logger.debug(f"Successfully parsed profile as JSON for CID {cid}")
                    return profile_data
                except json.JSONDecodeError:
                    logger.warning(f"Failed to parse {cid} as both Parquet and JSON")
                    return {}
            logger.error(f"Unexpected error reading profile {cid}: {e}")
            return {}
    logger.warning(f"Failed to fetch profile {cid}: HTTP {response.status_code}")
    return {}


def extract_file_cids_from_profile(profile_data: dict) -> List[str]:
    """Extract individual file CIDs from user profile parquet data."""
    file_cids = []

    if isinstance(profile_data, list):
        for file_entry in profile_data:
            if isinstance(file_entry, dict):
                cid = file_entry.get("cid")
                if cid and isinstance(cid, str):
                    file_cids.append(cid)
                    logger.debug(f"Extracted file CID: {cid}")

    logger.debug(f"Extracted {len(file_cids)} file CIDs from profile")
    return file_cids


async def cache_user_chain_profile(
    redis_chain: async_redis.Redis,
    user: str,
    all_profiles: dict,
    storage_requests_dict: dict,
    http_client: httpx.AsyncClient,
    gateway_url: str,
) -> None:
    """Download and cache chain profile data for a single user."""
    logger.debug(f"Caching chain profile for user {user}")

    # Get profile CIDs from pre-fetched profiles dict
    profile_cids = all_profiles.get(user, [])

    # Download and parse each profile to extract file CIDs
    profile_file_cids = []
    for profile_cid in profile_cids:
        profile_data = await fetch_profile_content(http_client, profile_cid, gateway_url)
        if profile_data:
            file_cids = extract_file_cids_from_profile(profile_data)
            profile_file_cids.extend(file_cids)
            logger.debug(f"Profile {profile_cid} for {user}: extracted {len(file_cids)} file CIDs")

    # Get storage CIDs for this user from the pre-fetched dictionary
    storage_cids = storage_requests_dict.get(user, [])

    # Merge all CIDs: profile CIDs + file CIDs from profiles + storage CIDs
    all_cids = list(set(profile_cids + profile_file_cids + storage_cids))

    # Prepare cache data with timestamp
    cache_data = {
        "timestamp": time.time(),
        "cids": all_cids,
        "pinned_count": len(profile_cids),
        "profile_file_count": len(profile_file_cids),
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
        f"Cached {len(all_cids)} CIDs for user {user} (profile: {len(profile_cids)}, files: {len(profile_file_cids)}, storage: {len(storage_cids)})"
    )


async def run_chain_profile_cacher_loop():
    """Main loop that downloads and caches substrate chain profiles for all users."""
    # Connect to database
    db = await asyncpg.connect(config.database_url)

    redis_chain_url = config.redis_chain_url
    redis_chain = async_redis.from_url(redis_chain_url)
    http_client = httpx.AsyncClient(timeout=30.0)

    logger.info("Starting chain profile cacher service...")
    logger.info(f"Database: {config.database_url}")
    logger.info(f"Redis Chain: {redis_chain_url}")

    try:
        while True:
            # Get all unique users from S3 database
            users = await get_all_unique_users(db)
            logger.info(f"Found {len(users)} unique S3 users")
            users_set = set(users)

            # Pre-fetch all user profiles from chain
            logger.info("Fetching all user profiles from chain...")
            substrate_client = SubstrateClient(config.substrate_url)
            try:
                substrate_client.connect()
                all_profiles_raw = substrate_client.fetch_user_profiles()
                logger.info(f"Fetched profiles for {len(all_profiles_raw)} total substrate users")

                # Filter to only S3 users
                all_profiles = {user: cids for user, cids in all_profiles_raw.items() if user in users_set}
                logger.info(f"Filtered to {len(all_profiles)} S3 users with profiles")
            finally:
                substrate_client.close()

            # Pre-fetch storage requests from chain (filtered to S3 users only)
            logger.info(f"Fetching storage requests from chain for {len(users_set)} S3 users...")
            storage_requests_dict = await get_all_storage_requests(
                config.substrate_url,
                http_client,
                filter_users=users_set,
            )
            logger.info(f"Fetched storage requests for {len(storage_requests_dict)} S3 users")

            # Process each user
            for user in users:
                await cache_user_chain_profile(
                    redis_chain,
                    user,
                    all_profiles,
                    storage_requests_dict,
                    http_client,
                    config.ipfs_get_url,
                )

            logger.info(f"Completed caching chain profiles for all {len(users)} users")

            # Sleep before next iteration
            await asyncio.sleep(config.cacher_loop_sleep)

    finally:
        await http_client.aclose()
        await redis_chain.aclose()
        await db.close()


if __name__ == "__main__":
    asyncio.run(run_chain_profile_cacher_loop())
