#!/usr/bin/env python3
"""
Substrate blockchain interaction module for fetching user profiles and storage requests.
Extracted from example.py and adapted for the chain pin checker system.
"""

import asyncio
import binascii
import logging
import random
from contextlib import suppress
from typing import Dict
from typing import List
from typing import Optional

from hippius_sdk.errors import HippiusSubstrateError
from hippius_sdk.substrate import FileInput
from hippius_sdk.substrate import SubstrateClient as HippiusSubstrateClient
from substrateinterface import SubstrateInterface

from hippius_s3.config import get_config


logger = logging.getLogger(__name__)


class SubstrateClient:
    """Client for interacting with substrate blockchain to fetch user data."""

    def __init__(
        self,
        substrate_url: str,
    ):
        self.substrate_url = substrate_url
        self.substrate = None

    def connect(self):
        """Connect to substrate blockchain."""
        self.substrate = SubstrateInterface(url=self.substrate_url, use_remote_preset=True)
        logger.info(f"Connected to substrate at {self.substrate_url}")

    def close(self):
        """Close substrate connection."""
        if self.substrate:
            self.substrate.close()

    def hex_to_cid(
        self,
        hex_string: str,
    ) -> str:
        """Convert hex-encoded string to IPFS CID."""
        # If it's already a valid CID string, return as-is
        if isinstance(hex_string, str) and (hex_string.startswith("Qm") or hex_string.startswith("b")):
            return hex_string

        # Convert hex string to bytes then to ASCII string (CID)
        if isinstance(hex_string, str) and len(hex_string) % 2 == 0:
            try:
                byte_data = binascii.unhexlify(hex_string)
                # Convert bytes to string (this should give us the CID)
                cid_string = byte_data.decode("utf-8", errors="ignore")

                # Validate it looks like a CID
                if cid_string and (cid_string.startswith("Qm") or cid_string.startswith("b")):
                    logger.debug(f"Successfully converted hex to CID: {cid_string}")
                    return cid_string
                logger.warning(f"Hex decoded to non-CID string: {cid_string}")
                return cid_string  # Return anyway, might still be valid

            except Exception as decode_error:
                logger.warning(f"Error decoding hex {hex_string}: {decode_error}")

        # Fallback: return original data as string
        return str(hex_string)

    def fetch_user_profiles(self) -> Dict[str, List[str]]:
        """
        Fetch user profiles from substrate UserProfile storage.
        Returns dict mapping account_id -> list of profile CIDs.
        """
        try:
            logger.debug("Fetching user profiles from IpfsPallet::UserProfile...")

            # Query the storage map
            result = self.substrate.query_map(module="IpfsPallet", storage_function="UserProfile")

            profiles = {}
            for key, value in result:
                # Extract account and CID
                account = str(key.value) if hasattr(key, "value") else str(key)
                cid = str(value.value) if hasattr(value, "value") else str(value)

                if account and cid:
                    if account not in profiles:
                        profiles[account] = []
                    profiles[account].append(cid)

            logger.info(
                f"Fetched profiles for {len(profiles)} users with total {sum(len(v) for v in profiles.values())} profile CIDs"
            )
            return profiles

        except Exception as e:
            logger.error(f"Error fetching user profiles: {e}")
            return {}

    def fetch_user_storage_requests(self) -> Dict[str, List[str]]:
        """
        Fetch user storage requests from substrate UserStorageRequests storage.
        Returns dict mapping account_id -> list of storage CIDs.
        """
        try:
            logger.debug("Fetching user storage requests from IpfsPallet::UserStorageRequests...")

            # Query the storage double map
            result = self.substrate.query_map(module="IpfsPallet", storage_function="UserStorageRequests")

            storage_requests = {}
            for key, _value in result:
                try:
                    # Handle double map key (owner_account_id, file_hash)
                    if isinstance(key, (tuple, list)) and len(key) >= 2:
                        account = str(key[0].value) if hasattr(key[0], "value") else str(key[0])
                        file_hash_hex = str(key[1].value) if hasattr(key[1], "value") else str(key[1])

                        # Convert hex-encoded file_hash to CID string
                        cid = self.hex_to_cid(file_hash_hex) if file_hash_hex else None

                        if account and cid:
                            if account not in storage_requests:
                                storage_requests[account] = []
                            storage_requests[account].append(cid)
                            logger.debug(f"Storage request: {account} -> {cid}")

                except Exception as e:
                    logger.warning(f"Error processing storage request key {key}: {e}")
                    continue

            total_requests = sum(len(v) for v in storage_requests.values())
            logger.info(
                f"Fetched storage requests for {len(storage_requests)} users with total {total_requests} request CIDs"
            )
            return storage_requests

        except Exception as e:
            logger.error(f"Error fetching user storage requests: {e}")
            return {}


def get_all_pinned_cids(
    user: str,
    substrate_url: str,
) -> List[str]:
    """Get all pinned CIDs for a specific user from substrate chain."""
    client = SubstrateClient(substrate_url)

    try:
        client.connect()

        # Get user profiles
        user_profiles = client.fetch_user_profiles()
        user_cids = user_profiles.get(user, [])

        logger.debug(f"Found {len(user_cids)} pinned CIDs for user {user}")
        return user_cids

    except Exception as e:
        logger.error(f"Error getting pinned CIDs for user {user}: {e}")
        return []
    finally:
        client.close()


async def get_all_storage_requests(
    substrate_url: str,
    http_client,
    filter_users: set = None,
) -> Dict[str, List[str]]:
    """Get all storage request CIDs for specified users from substrate chain.

    Args:
        substrate_url: Substrate node URL
        http_client: HTTP client for IPFS requests
        filter_users: Optional set of user addresses to filter. If None, fetches for all users.
    """
    client = SubstrateClient(substrate_url)

    try:
        client.connect()

        # Query the storage double map directly
        result = client.substrate.query_map(module="IpfsPallet", storage_function="UserStorageRequests")

        storage_requests = {}
        for key, _value in result:
            try:
                # Handle double map key (owner_account_id, file_hash)
                if isinstance(key, (tuple, list)) and len(key) >= 2:
                    account = str(key[0].value) if hasattr(key[0], "value") else str(key[0])

                    # Skip if filtering is enabled and user not in filter set
                    if filter_users is not None and account not in filter_users:
                        continue

                    file_hash_hex = str(key[1].value) if hasattr(key[1], "value") else str(key[1])

                    # Convert hex-encoded file_hash to CID string
                    storage_request_cid = _hex_to_cid(file_hash_hex) if file_hash_hex else None

                    if account and storage_request_cid:
                        if account not in storage_requests:
                            storage_requests[account] = []

                        # Download and parse the storage request JSON to extract individual CIDs
                        try:
                            gateway_url = "https://get.hippius.network"
                            url = f"{gateway_url}/ipfs/{storage_request_cid}"
                            response = await http_client.get(url)

                            if response.status_code == 200:
                                storage_data = response.json()
                                if isinstance(storage_data, list):
                                    for obj in storage_data:
                                        if isinstance(obj, dict) and "cid" in obj:
                                            storage_requests[account].append(obj["cid"])
                                    logger.debug(
                                        f"Storage request {storage_request_cid}: extracted {len(storage_data)} CIDs for {account}"
                                    )
                            else:
                                logger.warning(
                                    f"Failed to fetch storage request {storage_request_cid} for {account} from {url}: HTTP {response.status_code}"
                                )
                        except Exception as e:
                            logger.warning(
                                f"Error fetching storage request {storage_request_cid} for {account} from {url}: {e}"
                            )

            except Exception as e:
                logger.warning(f"Error processing storage request key {key}: {e}")
                continue

        logger.info(
            f"Fetched storage requests for {len(storage_requests)} users with total {sum(len(cids) for cids in storage_requests.values())} request CIDs"
        )
        return storage_requests

    except Exception as e:
        logger.error(f"Error getting storage requests: {e}")
        return {}
    finally:
        client.close()


def _hex_to_cid(hex_string: str) -> str:
    """Convert hex-encoded string to IPFS CID."""
    try:
        # If it's already a valid CID string, return as-is
        if isinstance(hex_string, str) and (hex_string.startswith("Qm") or hex_string.startswith("b")):
            return hex_string

        # Convert hex string to bytes then to ASCII string (CID)
        if hex_string.startswith("0x"):
            hex_string = hex_string[2:]

        # Convert hex to bytes, then decode as UTF-8
        data = bytes.fromhex(hex_string)
        return data.decode("utf-8", errors="ignore").strip()

    except Exception as e:
        logger.warning(f"Error converting to CID: {hex_string}, error: {e}")
        return str(hex_string)


async def submit_storage_request(
    cids: List[str],
    seed_phrase: str,
    substrate_url: str,
) -> str:
    """Submit a storage request to substrate for a list of CIDs with retry and timeouts."""
    if not cids:
        raise ValueError("CID list cannot be empty")

    # Create FileInput objects from CIDs
    files = [FileInput(file_hash=cid, file_name=f"s3-{cid}") for cid in cids]

    config = get_config()

    def _backoff_ms(attempt: int) -> float:
        base = getattr(config, "substrate_retry_base_ms", 500)
        max_ms = getattr(config, "substrate_retry_max_ms", 5000)
        exp = base * (2 ** max(0, attempt - 1))
        jitter = random.uniform(0, exp * 0.1)
        return float(min(exp + jitter, max_ms))

    # Use Optional for broad Python compatibility (avoid PEP604 union here)
    last_exc: Optional[Exception] = None
    for attempt in range(1, int(getattr(config, "substrate_max_retries", 3)) + 1):
        # Fresh client per attempt to avoid hung WS
        substrate_client = HippiusSubstrateClient(
            url=substrate_url,
            seed_phrase=seed_phrase,
        )
        try:
            # Enforce call timeout
            tx_hash = await asyncio.wait_for(
                substrate_client.storage_request(
                    files=files,
                    miner_ids=[],
                    seed_phrase=seed_phrase,
                ),
                timeout=float(getattr(config, "substrate_call_timeout_seconds", 20.0)),
            )
            if not tx_hash or tx_hash == "0x" or len(tx_hash) < 10:
                raise HippiusSubstrateError(
                    f"Invalid transaction hash received: {tx_hash}. This might indicate insufficient credits or transaction failure."
                )
            logger.info(f"Successfully submitted storage request with transaction: {tx_hash} (attempt {attempt})")
            return tx_hash
        except (asyncio.TimeoutError, Exception) as e:  # treat all errors as retryable up to max
            last_exc = e
            # Best-effort close to avoid hung websockets; SDK lacks public close()
            with suppress(Exception):
                if hasattr(substrate_client, "_substrate") and substrate_client._substrate:
                    substrate_client._substrate.close()
            if attempt >= int(getattr(config, "substrate_max_retries", 3)):
                logger.error(f"Substrate submit failed after {attempt} attempts: {e}")
                raise
            backoff = _backoff_ms(attempt)
            logger.warning(f"Substrate submit failed (attempt {attempt}), retrying in {backoff:.0f}ms: {e}")
            await asyncio.sleep(backoff / 1000.0)

    # Should not reach here, but raise last exception defensively
    if last_exc:
        raise last_exc
    raise HippiusSubstrateError("Unknown substrate submission failure")
