#!/usr/bin/env python3
import asyncio
import json
import logging
import sys
from pathlib import Path
from typing import Dict
from typing import Optional

import redis.asyncio as async_redis
from dotenv import load_dotenv
from hippius_sdk.substrate import SubstrateClient


sys.path.append(str(Path(__file__).parent.parent))

from hippius_s3.config import get_config


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()


class SubstrateCacher:
    """Fetches and caches substrate data in Redis."""

    def __init__(self, substrate_url: str, redis_url: str):
        self.substrate_url = substrate_url
        self.redis_url = redis_url
        self.redis_client: Optional[async_redis.Redis] = None
        self.substrate_client: Optional[SubstrateClient] = None

    async def connect(self) -> None:
        """Connect to Redis and Substrate."""
        self.redis_client = async_redis.from_url(self.redis_url)
        await self.redis_client.ping()
        logger.debug("Connected to Redis")

        self.substrate_client = SubstrateClient(
            url=self.substrate_url,
            password="",
            account_name=None,
        )
        self.substrate_client.connect()
        logger.debug("Connected to Substrate")

    async def disconnect(self) -> None:
        """Disconnect from Redis and Substrate."""
        if self.redis_client:
            await self.redis_client.aclose()

        if self.substrate_client:
            # SubstrateClient doesn't have disconnect method
            self.substrate_client = None

    async def fetch_free_credits(self) -> Dict[str, int]:
        """Fetch all free credits from substrate storage."""
        logger.debug("Fetching all free credits")

        # Debug: try to find the correct pallet name by testing common variations
        pallet_names = ["credits", "Credits", "CREDITS", "credit", "Credit", "CREDIT"]
        storage_names = ["freeCredits", "FreeCredits", "free_credits", "FREE_CREDITS"]

        storage_map = None

        for pallet in pallet_names:
            for storage in storage_names:
                try:
                    storage_map = self.substrate_client._substrate.query_map(
                        module=pallet,
                        storage_function=storage,
                    )
                    logger.info(f"Found credits storage at {pallet}.{storage}")
                    break
                except Exception as e:
                    logger.debug(f"Failed {pallet}.{storage}: {e}")
                    continue
            if storage_map:
                break

        if not storage_map:
            raise RuntimeError("Could not find credits storage in substrate - tried all common variations")

        result = {}
        for key, value in storage_map:
            if hasattr(key, "value"):
                account_id = key.value[0] if isinstance(key.value, list) else key.value
            else:
                account_id = str(key)

            credits = value.value if hasattr(value, "value") else value

            result[account_id] = credits

        logger.debug(f"Fetched {len(result)} free credits entries")
        return result

    async def fetch_subaccount_roles(self) -> Dict[str, str]:
        """Fetch all subaccount roles from substrate storage."""
        logger.debug("Fetching subaccount roles")

        # Try to find the correct pallet name for subaccount roles
        pallet_names = ["SubAccount"]
        storage_names = ["SubaccountRole", "subaccountRole", "SubAccountRole", "Role", "role"]

        storage_map = None
        for pallet in pallet_names:
            for storage in storage_names:
                try:
                    storage_map = self.substrate_client._substrate.query_map(module=pallet, storage_function=storage)
                    logger.info(f"Found subaccount roles at {pallet}.{storage}")
                    break
                except Exception as e:
                    logger.debug(f"Failed {pallet}.{storage}: {e}")
                    continue
            if storage_map:
                break

        if not storage_map:
            raise RuntimeError("Could not find subaccount roles storage in substrate")

        result = {}
        for key, value in storage_map:
            if hasattr(key, "value"):
                subaccount_id = key.value[0] if isinstance(key.value, list) else key.value
            else:
                subaccount_id = str(key)

            role = value.value if hasattr(value, "value") else value

            result[subaccount_id] = role

        logger.debug(f"Fetched {len(result)} subaccount role entries")
        return result

    async def fetch_subaccount_mappings(self) -> Dict[str, str]:
        """Fetch all subaccount mappings from substrate storage."""
        logger.debug("Fetching subaccount mappings")

        # Try to find the correct pallet name for subaccount mappings
        pallet_names = ["SubAccount"]
        storage_names = ["SubAccount"]

        storage_map = None
        for pallet in pallet_names:
            for storage in storage_names:
                try:
                    storage_map = self.substrate_client._substrate.query_map(module=pallet, storage_function=storage)
                    logger.info(f"Found subaccount mappings at {pallet}.{storage}")
                    break
                except Exception as e:
                    logger.debug(f"Failed {pallet}.{storage}: {e}")
                    continue
            if storage_map:
                break

        if not storage_map:
            raise RuntimeError("Could not find subaccount mappings storage in substrate")

        result = {}
        for key, value in storage_map:
            if hasattr(key, "value"):
                subaccount_id = key.value[0] if isinstance(key.value, list) else key.value
            else:
                subaccount_id = str(key)

            main_account_id = value.value if hasattr(value, "value") else value

            result[subaccount_id] = main_account_id

        logger.debug(f"Fetched {len(result)} subaccount mapping entries")
        return result

    async def cache_subaccount_data(
        self, main_account_credits: Dict[str, int], subaccount_roles: Dict[str, str], subaccount_to_main: Dict[str, str]
    ) -> None:
        """
        Cache subaccount data in Redis.

        For each subaccount, stores a JSON object with:
        - subaccount_id
        - main_account_id
        - role (Upload, UploadDelete)
        - free_credits (from main account)
        """
        cached_count = 0
        pipeline = self.redis_client.pipeline()

        for subaccount, main_account in subaccount_to_main.items():
            role = subaccount_roles.get(subaccount, "Unknown")
            free_credits = main_account_credits.get(main_account, 0)

            cache_data = {
                "subaccount_id": subaccount,
                "main_account_id": main_account,
                "role": role,
                "free_credits": free_credits,
                "has_credits": free_credits > 0,
            }

            cache_key = f"hippius_subaccount_cache:{subaccount}"

            await pipeline.setex(
                cache_key,
                600,  # 10 minutes TTL (cacher runs every 5 minutes)
                json.dumps(cache_data),
            )
            cached_count += 1

            if cached_count % 100 == 0:
                await pipeline.execute()
                pipeline = self.redis_client.pipeline()
                logger.debug(f"Cached {cached_count} subaccounts so far...")

        if cached_count % 100 != 0:
            await pipeline.execute()

        logger.debug(f"Cached {cached_count} subaccounts in Redis")

    async def cache_main_account_credits(self, main_account_credits: Dict[str, int]) -> None:
        """Cache main account credits separately for quick lookups."""
        pipeline = self.redis_client.pipeline()
        cached_count = 0

        for main_account, credits in main_account_credits.items():
            cache_key = f"hippius_main_account_credits:{main_account}"
            cache_data = {"main_account_id": main_account, "free_credits": credits, "has_credits": credits > 0}

            await pipeline.setex(
                cache_key,
                600,  # 10 minutes TTL (cacher runs every 5 minutes)
                json.dumps(cache_data),
            )
            cached_count += 1

        await pipeline.execute()
        logger.debug(f"Cached {cached_count} main account credits in Redis")

    async def run_cache_update(self) -> None:
        """Main function to fetch and cache all substrate data."""
        await self.connect()

        main_account_credits = await self.fetch_free_credits()
        logger.info(f"Successfully fetched {len(main_account_credits)} account credits")

        subaccount_role_map = await self.fetch_subaccount_roles()
        subaccount_to_main = await self.fetch_subaccount_mappings()

        logger.debug(
            f"Data summary: {len(main_account_credits)} credits, "
            f"{len(subaccount_role_map)} roles, {len(subaccount_to_main)} mappings"
        )

        await self.cache_subaccount_data(main_account_credits, subaccount_role_map, subaccount_to_main)
        await self.cache_main_account_credits(main_account_credits)

        await self.disconnect()


async def main():
    """Entry point for the cacher script."""
    try:
        config = get_config()

        cacher = SubstrateCacher(substrate_url=config.substrate_url, redis_url=config.redis_url)

        await cacher.run_cache_update()
        logger.info("Cache update completed successfully")

    except Exception as e:
        logger.exception(f"Failed to run cache update: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
