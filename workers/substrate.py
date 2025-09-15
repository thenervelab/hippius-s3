#!/usr/bin/env python3
"""
Substrate blockchain interaction module for fetching user profiles and storage requests.
Extracted from example.py and adapted for the chain pin checker system.
"""

import binascii
import logging
from typing import Dict
from typing import List

from hippius_sdk.errors import HippiusSubstrateError
from hippius_sdk.substrate import FileInput
from hippius_sdk.substrate import SubstrateClient as HippiusSubstrateClient
from substrateinterface import SubstrateInterface


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
            for key, value in result:
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


def get_all_storage_requests(
    substrate_url: str,
) -> Dict[str, List[str]]:
    """Get all storage request CIDs for all users from substrate chain."""
    client = SubstrateClient(substrate_url)

    try:
        client.connect()

        # Get all user storage requests
        storage_requests = client.fetch_user_storage_requests()

        logger.info(f"Found storage requests for {len(storage_requests)} users")
        return storage_requests

    except Exception as e:
        logger.error(f"Error getting storage requests: {e}")
        return {}
    finally:
        client.close()


async def submit_storage_request(
    cids: List[str],
    seed_phrase: str,
    substrate_url: str,
) -> str:
    """Submit a storage request to substrate for a list of CIDs."""
    if not cids:
        raise ValueError("CID list cannot be empty")

    # Create FileInput objects from CIDs
    files = [FileInput(file_hash=cid, file_name=f"file_{cid[:8]}.data") for cid in cids]

    # Create substrate client for this request
    substrate_client = HippiusSubstrateClient(
        url=substrate_url,
        seed_phrase=seed_phrase,
    )

    # Submit storage request
    tx_hash = await substrate_client.storage_request(
        files=files,
        miner_ids=[],
        seed_phrase=seed_phrase,
    )

    logger.info(f"Submitted storage request for {len(cids)} CIDs")
    logger.debug(f"Substrate call result: {tx_hash}")

    if not tx_hash or tx_hash == "0x" or len(tx_hash) < 10:
        logger.error(f"Invalid transaction hash received: {tx_hash}")
        raise HippiusSubstrateError(
            f"Invalid transaction hash received: {tx_hash}. "
            "This might indicate insufficient credits or transaction failure."
        )

    logger.info(f"Successfully submitted storage request with transaction: {tx_hash}")
    return tx_hash


async def submit_storage_request_for_user(
    user: str,
    cids: List[str],
    seed_phrase: str,
    substrate_url: str,
) -> str:
    """Submit a storage request to substrate for a specific user's missing CIDs."""
    if not cids:
        raise ValueError("CID list cannot be empty")

    # Create FileInput objects from CIDs
    files = [FileInput(file_hash=cid, file_name=f"resubmit_{cid[:8]}.data") for cid in cids]

    # Create substrate client for this request
    substrate_client = HippiusSubstrateClient(
        url=substrate_url,
        seed_phrase=seed_phrase,
    )

    # Submit storage request
    tx_hash = await substrate_client.storage_request(
        files=files,
        miner_ids=[],
        seed_phrase=seed_phrase,
        pallet_name="IpfsPallet",
        fn_name="submit_storage_request_for_user",
        extra_params={
            "user": user,
        },
    )

    logger.info(f"Submitted storage request for user {user} with {len(cids)} CIDs")
    logger.debug(f"Substrate call result: {tx_hash}")

    if not tx_hash or tx_hash == "0x" or len(tx_hash) < 10:
        logger.error(f"Invalid transaction hash received: {tx_hash}")
        raise HippiusSubstrateError(
            f"Invalid transaction hash received: {tx_hash}. "
            "This might indicate insufficient credits or transaction failure."
        )

    logger.info(f"Successfully submitted storage request for user {user} with transaction: {tx_hash}")
    return tx_hash


async def resubmit_substrate_pinning_request(
    user: str,
    cids: List[str],
    seed_phrase: str,
    substrate_url: str,
) -> None:
    """Resubmit pinning requests for missing CIDs."""
    if not cids:
        logger.info(f"No missing CIDs to resubmit for user {user}")
        return

    logger.info(
        f"Resubmitting pinning for user {user} with {len(cids)} CIDs: {cids[:5]}{'...' if len(cids) > 5 else ''}"
    )

    # Call the substrate submission function
    tx_hash = await submit_storage_request_for_user(
        user,
        cids,
        seed_phrase,
        substrate_url,
    )
    logger.info(f"Successfully resubmitted CIDs for user {user} with transaction: {tx_hash}")
