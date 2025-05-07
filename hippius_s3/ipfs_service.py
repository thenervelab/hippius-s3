import hashlib
import logging
import tempfile
from pathlib import Path
from typing import Dict
from typing import List
from typing import Tuple
from typing import Union
from typing import cast

import aiofiles
from hippius_sdk.client import HippiusClient

from hippius_s3.config import Config


logger = logging.getLogger(__name__)


class IPFSService:
    """Service for interacting with IPFS through Hippius SDK."""

    def __init__(self, config: Config):
        """Initialize the IPFS service."""
        self.config = config
        self.client = HippiusClient(
            ipfs_api_url=config.ipfs_service_url,
            encrypt_by_default=False,
        )
        logger.info(f"IPFS service initialized with API URL: {config.ipfs_service_url}")

    async def upload_file(
        self, file_data: bytes, file_name: str, content_type: str, encrypt: bool = False
    ) -> Dict[str, Union[str, int]]:
        """
        Upload file data to IPFS.

        Args:
            file_data: Binary file data
            file_name: Name of the file
            content_type: MIME type of the file
            encrypt: Whether to encrypt the file

        Returns:
            Dict containing IPFS CID and file information
        """
        # Create a temporary file with the data
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_path = temp_file.name
            temp_file.write(file_data)

        try:
            # Upload the file to IPFS
            result = await self.client.upload_file(temp_path, encrypt=encrypt)

            # Pin the CID to ensure it stays available
            await self.client.pin(result["cid"])

            return {
                "cid": result["cid"],
                "file_name": file_name,
                "content_type": content_type,
                "size_bytes": result["size_bytes"],
                "encrypted": result.get("encrypted", False),
            }
        finally:
            # Clean up the temporary file
            if Path(temp_path).exists():
                Path(temp_path).unlink()

    async def download_file(self, cid: str, decrypt: bool = False) -> bytes:
        """
        Download file data from IPFS.

        Args:
            cid: IPFS content identifier
            decrypt: Whether to decrypt the file (if it was encrypted)

        Returns:
            Binary file data
        """
        # Create a temporary file path for the download
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_path = temp_file.name

        try:
            # Download the file from IPFS
            await self.client.download_file(cid, temp_path, decrypt=decrypt)

            # Read the file data
            async with aiofiles.open(temp_path, "rb") as f:
                content = await f.read()
                return cast(bytes, content)
        finally:
            # Clean up the temporary file
            if Path(temp_path).exists():
                Path(temp_path).unlink()

    async def delete_file(self, cid: str) -> Dict[str, Union[bool, str]]:
        """
        Delete file from IPFS.

        Args:
            cid: IPFS content identifier

        Returns:
            Dict containing deletion status
        """
        try:
            return {
                "deleted": await self.client.delete_file(cid, cancel_from_blockchain=True),
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

    async def check_file_exists(self, cid: str) -> bool:
        """
        Check if a file exists in IPFS.

        Args:
            cid: IPFS content identifier

        Returns:
            True if the file exists, False otherwise
        """
        try:
            result = await self.client.exists(cid)
            return bool(result.get("exists", False))
        except Exception as e:
            logger.error(f"Error checking if file exists in IPFS: {e}")
            return False

    async def upload_part(self, file_data: bytes, part_number: int) -> Dict[str, Union[str, int]]:
        """
        Upload a part of a multipart upload to IPFS.

        Args:
            file_data: Binary file data for the part
            part_number: Part number (1-10000)

        Returns:
            Dict containing IPFS CID, ETag, and file information
        """
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_path = temp_file.name
            temp_file.write(file_data)

        try:
            # Upload the part to IPFS
            result = await self.client.upload_file(temp_path, encrypt=False)

            # Calculate ETag (MD5 hash) for the part, similar to S3
            md5_hash = hashlib.md5(file_data).hexdigest()
            etag = f"{md5_hash}-{part_number}"

            # Pin the CID to ensure it stays available
            await self.client.pin(result["cid"])

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

    async def concatenate_parts(
        self, parts: List[Dict[str, Union[str, int]]], content_type: str, file_name: str
    ) -> Dict[str, Union[str, int]]:
        """
        Concatenate multiple file parts into a single file in IPFS.

        This is used to complete a multipart upload.

        Args:
            parts: List of part information including CIDs
            content_type: MIME type of the final file
            file_name: Name of the final file

        Returns:
            Dict containing IPFS CID and file information for the complete file
        """
        # Create a temporary directory for working with parts
        temp_dir = tempfile.mkdtemp()
        output_path = Path(temp_dir) / "complete_file"
        part_files: List[Tuple[int, Path]] = []

        try:
            # Download all parts to temporary files
            part_files = []  # Reset the list
            total_size = 0

            for part_info in parts:
                cid = part_info["ipfs_cid"]
                part_number = part_info["part_number"]
                part_path = Path(temp_dir) / f"part_{part_number}"

                # Download the part from IPFS
                await self.client.download_file(cid, part_path)
                part_files.append((int(part_number), part_path))
                total_size += int(part_info["size_bytes"])

            # Sort parts by part number
            part_files.sort(key=lambda x: x[0])

            # Concatenate the parts into a single file
            async with aiofiles.open(output_path, "wb") as output_file:
                for _, part_path in part_files:
                    async with aiofiles.open(part_path, "rb") as part_file:
                        await output_file.write(await part_file.read())

            # Upload the complete file to IPFS
            result = await self.client.upload_file(output_path, encrypt=False)

            # Pin the CID to ensure it stays available
            await self.client.pin(result["cid"])

            # Calculate the combined ETag (required for S3 compatibility)
            # For multipart uploads, S3 uses a special ETag format
            etags = [str(part_info["etag"]).split("-")[0] for part_info in parts]
            combined_etag = hashlib.md5(("".join(etags)).encode()).hexdigest()
            final_etag = f"{combined_etag}-{len(parts)}"

            return {
                "cid": result["cid"],
                "file_name": file_name,
                "content_type": content_type,
                "size_bytes": total_size,
                "etag": final_etag,
                "encrypted": False,
            }
        finally:
            # Clean up temporary files
            for _, part_path in part_files:
                if part_path.exists():
                    part_path.unlink()

            if output_path.exists():
                output_path.unlink()

            if Path(temp_dir).exists():
                Path(temp_dir).rmdir()
