"""Account service for blockchain interaction and account verification."""

import asyncio
import json
import logging

import redis.asyncio as async_redis
from mnemonic import Mnemonic

from gateway.substrate_client import SubstrateClient
from hippius_s3.models.account import HippiusAccount


logger = logging.getLogger(__name__)


class MainAccountError(Exception):
    pass


class InvalidSeedPhraseError(Exception):
    pass


class BadAccount(Exception):
    pass


async def get_subaccount_id_from_seed(seed_phrase: str, substrate_url: str) -> str | None:
    """Get subaccount ID from seed phrase using Substrate client in a separate thread.

    Returns:
        str: The subaccount ID if this is a valid subaccount seed phrase
        None: If this is a main account seed phrase (not allowed for S3)
    """

    def _connect_substrate() -> str | None:
        client = SubstrateClient(
            seed_phrase=seed_phrase,
            url=substrate_url,
        )
        client.connect(seed_phrase=seed_phrase)
        if client._account_address is None:
            return None
        if client.is_main_account(
            account_id=client._account_address,
            seed_phrase=seed_phrase,
        ):
            return None
        return client._account_address

    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _connect_substrate)


async def fetch_account(
    seed_phrase: str,
    redis_accounts_client: async_redis.Redis,
    substrate_url: str,
) -> HippiusAccount:
    """Fetch account information with full caching logic handled internally."""
    m = Mnemonic("english")
    if not m.check(seed_phrase):
        raise InvalidSeedPhraseError("Invalid seed phrase format")

    seed_cache_key = f"hippius_seed_to_subaccount:{hash(seed_phrase)}"
    cached_subaccount_id = await redis_accounts_client.get(seed_cache_key)

    if cached_subaccount_id:
        cached_value = cached_subaccount_id.decode()
        if cached_value == "False":
            raise MainAccountError("Please use a Hippius subaccount seed phrase for S3 operations")
        subaccount_id = cached_value
    else:
        subaccount_id = await get_subaccount_id_from_seed(
            seed_phrase,
            substrate_url,
        )

        if subaccount_id is None:
            await redis_accounts_client.set(seed_cache_key, "False")
            logger.error("Cached main account detection (False)")
            raise MainAccountError("Please use a Hippius subaccount seed phrase for S3 operations")

        await redis_accounts_client.set(seed_cache_key, subaccount_id)

    cache_key = f"hippius_subaccount_cache:{subaccount_id}"
    cached_data = await redis_accounts_client.get(cache_key)

    if cached_data:
        data = json.loads(cached_data)

        role = data.get("role", "Unknown")
        upload = role in ["Upload", "UploadDelete"]
        delete = role == "UploadDelete"
        main_account_id = data["main_account_id"]

        has_credits, free_credits = False, 0
        main_account_cache_key = f"hippius_main_account_credits:{main_account_id}"
        main_account_data = await redis_accounts_client.get(main_account_cache_key)
        if main_account_data:
            main_data = json.loads(main_account_data)
            has_credits = main_data.get("has_credits", False)
            free_credits = main_data.get("free_credits", 0)

        logger.info(f"{subaccount_id}:{main_account_id}:{free_credits}")

        return HippiusAccount(
            id=subaccount_id,
            main_account=main_account_id,
            has_credits=has_credits,
            upload=upload,
            delete=delete,
        )

    logger.error(f"No account cache found for subaccount {subaccount_id}")
    raise BadAccount("Your account does not exist")


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

    has_credits, free_credits = False, 0
    if main_account_data:
        main_data = json.loads(main_account_data)
        has_credits = main_data.get("has_credits", False)
        free_credits = main_data.get("free_credits", 0)
        logger.info(f"Access key account: {account_address}:{free_credits}")
    else:
        logger.warning(f"No cached credits found for main account: {account_address}")

    return HippiusAccount(
        id=account_address,
        main_account=account_address,
        has_credits=has_credits,
        upload=True,
        delete=True,
    )
