import hashlib
import logging
import tempfile
from pathlib import Path
from typing import Dict
from typing import Optional
from typing import Union

import redis.asyncio as async_redis

from hippius_sdk.client import HippiusClient

from hippius_s3.config import Config


logger = logging.getLogger(__name__)


class IPFSService:
    """Service for interacting with IPFS through Hippius SDK."""

    def __init__(self, config: Config, redis_client: Optional[async_redis.Redis] = None):
        """Initialize the IPFS service."""
        self.config = config
        self.client = HippiusClient(
            ipfs_gateway=config.ipfs_get_url,
            ipfs_api_url=config.ipfs_store_url,
            substrate_url=config.substrate_url,
            encrypt_by_default=False,
        )
        self.redis_client = redis_client
        logger.info(
            f"IPFS service initialized: IPFS={config.ipfs_get_url} {config.ipfs_store_url}, Substrate={config.substrate_url}"
        )

    async def upload_file(
        self,
        file_data: bytes,
        file_name: str,
        content_type: str,
        encrypt: bool = False,
        seed_phrase: Optional[str] = None,
    ) -> Dict[str, Union[str, int]]:
        """
        Upload file data to IPFS (legacy method).

        NOTE: This method is deprecated for regular uploads. Use client.s3_publish() instead
        for full IPFS upload + pinning + blockchain publishing.

        This method is still used for multipart upload parts, which only need IPFS upload
        without blockchain publishing (the final concatenated file gets published).

        Args:
            file_data: Binary file data
            file_name: Name of the file
            content_type: MIME type of the file
            encrypt: Whether to encrypt the file
            seed_phrase: Seed phrase to use for blockchain operations

        Returns:
            Dict containing IPFS CID and file information
        """
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_path = temp_file.name
            temp_file.write(file_data)

        try:
            result = await self.client.upload_file(
                temp_path,
                encrypt=encrypt,
                seed_phrase=seed_phrase,
            )
            pinning_status = await self.client.pin(
                result["cid"],
                seed_phrase=seed_phrase,
            )

            return {
                "cid": result["cid"],
                "file_name": file_name,
                "content_type": content_type,
                "size_bytes": result["size_bytes"],
                "encrypted": result.get("encrypted", False),
                "pinning_status": pinning_status,
            }
        finally:
            if Path(temp_path).exists():
                Path(temp_path).unlink()

    async def download_file(
        self,
        cid: str,
        subaccount_id: str,
        bucket_name: str,
        decrypt: bool = True,
        max_retries: int = 1,
        retry_delay: int = 2,
        seed_phrase: Optional[str] = None,
        streaming: bool = False,
    ):
        """
        Download file data from IPFS with automatic decryption using s3_download.

        Args:
            cid: IPFS content identifier
            decrypt: Whether to attempt automatic decryption (default: True since all files are encrypted)
            streaming: If True, return streaming HTTP response; if False, return bytes
            seed_phrase: Seed phrase to use for decryption key retrieval

        Returns:
            Binary file data (if streaming=False) or streaming HTTP response (if streaming=True)
        """
        if streaming:
            # Return streaming HTTP response directly from s3_download
            return await self.client.s3_download(
                cid=cid,
                subaccount_id=subaccount_id,
                bucket_name=bucket_name,
                auto_decrypt=decrypt,
                download_node=self.config.ipfs_store_url,
                streaming=True,
            )
        # Use return_bytes mode instead of temp file to avoid file I/O issues
        try:
            return await self.client.s3_download(
                cid=cid,
                subaccount_id=subaccount_id,
                bucket_name=bucket_name,
                auto_decrypt=decrypt,
                download_node=self.config.ipfs_store_url,
                return_bytes=True,
            )

        except Exception as e:
            logger.exception(f"Failed to download file with CID {cid}: {e}")
            raise RuntimeError(f"Failed to download file with CID {cid}") from e

    async def delete_file(
        self,
        cid: str,
        seed_phrase: Optional[str] = None,
        unpin=False,
    ) -> Dict[str, Union[bool, str]]:
        """
        Delete file from IPFS.

        Args:
            cid: IPFS content identifier
            seed_phrase: Seed phrase to use for blockchain operations
            unpin: whether to unpin the file as well.

        Returns:
            Dict containing deletion status
        """
        try:
            return {
                "deleted": await self.client.ipfs_client.delete_file(
                    cid,
                    cancel_from_blockchain=True,
                    seed_phrase=seed_phrase,
                    unpin=unpin,
                ),
                "cid": cid,
                "message": "File successfully deleted from IPFS",
            }
        except Exception as e:
            logger.error(f"Error deleting file from IPFS: {e}")
            return {
                "deleted": False,
                "cid": cid,
                "error": str(e),
            }

    async def check_file_exists(self, cid: str, seed_phrase: Optional[str] = None) -> bool:
        """
        Check if a file exists in IPFS.

        Args:
            cid: IPFS content identifier
            seed_phrase: Seed phrase to use for blockchain operations

        Returns:
            True if the file exists, False otherwise
        """
        try:
            result = await self.client.exists(cid, seed_phrase=seed_phrase)
            return bool(result.get("exists", False))
        except Exception as e:
            logger.error(f"Error checking if file exists in IPFS: {e}")
            return False

    async def check_cid_type(self, cid: str, seed_phrase: Optional[str] = None) -> str:
        """
        Check if a CID refers to a file or a directory in IPFS.

        Args:
            cid: IPFS content identifier
            seed_phrase: Seed phrase to use for blockchain operations

        Returns:
            'file' if the CID is a file, 'directory' if it's a directory,
            or 'unknown' if the check failed
        """
        # Get the detailed result from ls to check if it's a directory
        ls_result = await self.client.ipfs_client.ls(cid, seed_phrase=seed_phrase)

        # Analyze the response to determine if it's a file or directory
        # The 'Objects' list will contain entries with 'Links'
        # If it's a directory, there will be Links with their own Hashes
        objects = ls_result.get("Objects", [])

        if not objects:
            raise ValueError(f"Objects field empty when ls-ing cid={cid}")

        # Get the first (and usually only) object
        first_object = objects[0]
        links = first_object.get("Links", [])

        # If there are links, it's a directory
        if links:
            logger.info(f"CID {cid} is a directory with {len(links)} items")
            return "directory"

        logger.info(f"CID {cid} is a file")
        return "file"

    async def upload_part(
        self, file_data: bytes, part_number: int, encrypt: bool = True, seed_phrase: Optional[str] = None
    ) -> Dict[str, Union[str, int]]:
        """
        Upload a part of a multipart upload to IPFS.

        Args:
            file_data: Binary file data for the part
            part_number: Part number (1-10000)
            seed_phrase: Seed phrase to use for blockchain operations

        Returns:
            Dict containing IPFS CID, ETag, and file information
        """
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_path = temp_file.name
            temp_file.write(file_data)

        try:
            # If encryption is requested, create a new client with encryption key
            if encrypt and seed_phrase:
                # Derive encryption key from seed phrase
                key_material = hashlib.sha256(seed_phrase.encode("utf-8")).digest()
                client = HippiusClient(
                    ipfs_gateway=self.config.ipfs_get_url,
                    ipfs_api_url=self.config.ipfs_store_url,
                    substrate_url=self.config.substrate_url,
                    encrypt_by_default=False,
                    encryption_key=key_material,
                )
                # Upload the part to IPFS with encryption
                result = await client.upload_file(
                    temp_path,
                    encrypt=encrypt,
                    seed_phrase=seed_phrase,
                )
            else:
                # Upload the part to IPFS without encryption
                result = await self.client.upload_file(
                    temp_path,
                    encrypt=encrypt,
                    seed_phrase=seed_phrase,
                )

            # Calculate ETag (MD5 hash) for the part, similar to S3
            md5_hash = hashlib.md5(file_data).hexdigest()
            etag = f"{md5_hash}-{part_number}"

            # Pin the CID to ensure it stays available
            await self.client.pin(
                result["cid"],
                seed_phrase=seed_phrase,
            )

            return {
                "cid": result["cid"],
                "size_bytes": result["size_bytes"],
                "etag": etag,
                "part_number": part_number,
            }
        finally:
            # Clean up the temporary file
            if Path(temp_path).exists():
                Path(temp_path).unlink()
