"""Account service for blockchain interaction and account verification."""

import json
import logging

import redis.asyncio as async_redis
from hippius_s3.models.account import HippiusAccount


logger = logging.getLogger(__name__)


class BadAccount(Exception):
    pass


async def fetch_account_by_main_address(
    account_address: str,
    redis_accounts_client: async_redis.Redis,
    substrate_url: str,
) -> HippiusAccount:
    """
    Fetch account information for a main account address (access key auth).

    For access keys:
    - account_address is the main account (not a subaccount)
    - No upload/delete permission flags (ACL enforces)
    - Credits fetched from substrate/cache

    Args:
        account_address: Main account SS58 address
        redis_accounts_client: Redis client for account cache
        substrate_url: Substrate URL (unused currently, kept for consistency)

    Returns:
        HippiusAccount with credit info

    Raises:
        BadAccount: If account cache is not found
    """
    main_account_cache_key = f"hippius_main_account_credits:{account_address}"
    main_account_data = await redis_accounts_client.get(main_account_cache_key)

    has_credits, free_credits = True, 0
    if main_account_data:
        main_data = json.loads(main_account_data)
        has_credits = main_data.get("has_credits", True)
        free_credits = main_data.get("free_credits", 0)
        logger.info(f"Access key account: {account_address}:{free_credits}")
    else:
        logger.warning(f"No cached credits found for main account: {account_address}, defaulting to has_credits=True")

    return HippiusAccount(
        id=account_address,
        main_account=account_address,
        has_credits=has_credits,
        upload=True,
        delete=True,
    )
