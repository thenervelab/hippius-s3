import asyncio
import hashlib
import logging
import shutil
import tempfile
from pathlib import Path
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union

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
            ipfs_gateway=config.ipfs_get_url,
            ipfs_api_url=config.ipfs_store_url,
            encrypt_by_default=False,
        )
        logger.info(f"IPFS service initialized: {config.ipfs_get_url} {config.ipfs_store_url}")

    async def upload_file(
        self,
        file_data: bytes,
        file_name: str,
        content_type: str,
        encrypt: bool = False,
        seed_phrase: Optional[str] = None,
    ) -> Dict[str, Union[str, int]]:
        """
        Upload file data to IPFS.

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
            result = await self.client.upload_file(temp_path, encrypt=encrypt, seed_phrase=seed_phrase)
            pinning_status = await self.client.pin(result["cid"], seed_phrase=seed_phrase)

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
        decrypt: bool = False,
        max_retries: int = 1,
        retry_delay: int = 2,
        seed_phrase: Optional[str] = None,
    ) -> bytes:
        """
        Download file data from IPFS with retry logic.

        Args:
            cid: IPFS content identifier
            decrypt: Whether to decrypt the file (if it was encrypted)
            max_retries: Maximum number of download retry attempts
            retry_delay: Delay in seconds between retry attempts
            seed_phrase: Seed phrase to use for blockchain operations

        Returns:
            Binary file data
        """
        last_exception = None
        for attempt in range(1, max_retries + 1):
            temp_dir = tempfile.mkdtemp()
            temp_path = Path(temp_dir) / "downloaded_file"

            try:
                await self.client.download_file(
                    cid,
                    str(temp_path),
                    decrypt=decrypt,
                    seed_phrase=seed_phrase,
                    skip_directory_check=True,
                )

                if temp_path.is_dir():
                    logger.debug(f"Directory detected at {temp_path}")
                    for file in temp_path.iterdir():
                        logger.debug(f"Directory contains file: {file}")

                async with aiofiles.open(temp_path, "rb") as f:
                    data = await f.read()
                    return bytes(data)

            except Exception as e:
                last_exception = e
                logger.exception(f"Download attempt {attempt} failed: {e}")
                if attempt < max_retries:
                    logger.info(f"Retrying in {retry_delay} seconds...")
                    await asyncio.sleep(retry_delay)
                    retry_delay *= 2
            finally:
                shutil.rmtree(temp_dir, ignore_errors=True)

        if last_exception:
            raise RuntimeError(f"Failed to download file with CID {cid}") from last_exception

        raise RuntimeError(f"Failed to download file with CID {cid} after {max_retries} attempts")

    async def _download_directory(self, cid: str, decrypt: bool = False, seed_phrase: Optional[str] = None) -> bytes:
        """
        Handle downloading content from an IPFS directory CID.

        Args:
            cid: IPFS content identifier for a directory
            decrypt: Whether to decrypt the files (if they were encrypted)
            seed_phrase: Seed phrase to use for blockchain operations

        Returns:
            Binary data from the first file in the directory
        """
        logger.info(f"Handling directory CID: {cid}")

        # Get the directory listing to find files
        ls_result = await self.client.ipfs_client.ls(cid, seed_phrase=seed_phrase)
        objects = ls_result.get("Objects", [])

        if not objects or not objects[0].get("Links"):
            raise ValueError(f"No valid content found in directory CID: {cid}")

        # Get links from the first object
        links = objects[0].get("Links", [])

        if not links:
            raise ValueError(f"No links found in directory CID: {cid}")

        # Get the first file in the directory
        first_link = links[0]
        file_cid = first_link.get("Hash")

        if not file_cid:
            raise ValueError("No file CID found in directory listing")

        logger.info(f"Using first file from directory: {first_link.get('Name', 'unnamed')} (CID: {file_cid})")

        return await self.download_file(file_cid, decrypt, seed_phrase=seed_phrase)

    async def delete_file(self, cid: str, seed_phrase: Optional[str] = None) -> Dict[str, Union[bool, str]]:
        """
        Delete file from IPFS.

        Args:
            cid: IPFS content identifier
            seed_phrase: Seed phrase to use for blockchain operations

        Returns:
            Dict containing deletion status
        """
        try:
            return {
                "deleted": await self.client.delete_file(cid, cancel_from_blockchain=True, seed_phrase=seed_phrase),
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
        self, file_data: bytes, part_number: int, seed_phrase: Optional[str] = None
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
            # Upload the part to IPFS
            result = await self.client.upload_file(temp_path, encrypt=False, seed_phrase=seed_phrase)

            # Calculate ETag (MD5 hash) for the part, similar to S3
            md5_hash = hashlib.md5(file_data).hexdigest()
            etag = f"{md5_hash}-{part_number}"

            # Pin the CID to ensure it stays available
            await self.client.pin(result["cid"], seed_phrase=seed_phrase)

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
        self,
        parts: List[Dict[str, Union[str, int]]],
        content_type: str,
        file_name: str,
        seed_phrase: Optional[str] = None,
    ) -> Dict[str, Union[str, int]]:
        """
        Concatenate multiple file parts into a single file in IPFS.

        This is used to complete a multipart upload.

        Args:
            parts: List of part information including CIDs
            content_type: MIME type of the final file
            file_name: Name of the final file
            seed_phrase: Seed phrase to use for blockchain operations

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
                part_size = int(part_info["size_bytes"])

                logger.info(f"Processing part {part_number}: CID={cid}, Expected size={part_size} bytes")

                # Download the part from IPFS
                try:
                    await self.client.download_file(
                        cid,
                        part_path,
                        decrypt=False,
                        seed_phrase=seed_phrase,
                        skip_directory_check=True,
                    )
                    actual_size = part_path.stat().st_size
                    logger.info(f"Downloaded part {part_number}: CID={cid}, Actual size={actual_size} bytes")

                    # Verify size matches expected
                    if actual_size != part_size:
                        logger.warning(
                            f"Size mismatch for part {part_number}: Expected={part_size}, Actual={actual_size}"
                        )

                    part_files.append((int(part_number), part_path))
                    total_size += part_size
                except Exception as e:
                    logger.error(f"Failed to download part {part_number}: {e}")
                    raise

            # Sort parts by part number - CRITICAL for correct concatenation
            part_files.sort(key=lambda x: x[0])
            logger.info(f"Sorted part files by part number: {[(pnum, str(p)) for pnum, p in part_files]}")

            # Concatenate the parts into a single file
            logger.info(f"Concatenating parts in order: {[pn for pn, _ in part_files]}")

            # Ensure all part files exist before concatenating
            for part_num, part_path in part_files:
                if not part_path.exists():
                    logger.error(f"Part file missing: part_number={part_num}, path={part_path}")
                else:
                    logger.info(f"Part file exists: part_number={part_num}, size={part_path.stat().st_size} bytes")

            # Perform the concatenation
            async with aiofiles.open(output_path, "wb") as output_file:
                total_written = 0
                for part_num, part_path in part_files:
                    async with aiofiles.open(part_path, "rb") as part_file:
                        part_data = await part_file.read()
                        part_size = len(part_data)
                        await output_file.write(part_data)
                        total_written += part_size
                        logger.info(f"Wrote part {part_num} with {part_size} bytes, total now: {total_written} bytes")

            # Calculate MD5 hash of the final concatenated file for debugging
            async with aiofiles.open(output_path, "rb") as f:
                concatenated_data = await f.read()
                concatenated_md5 = hashlib.md5(concatenated_data).hexdigest()
                concatenated_size = len(concatenated_data)

            logger.info(f"Concatenated file MD5 hash: {concatenated_md5}")
            logger.info(f"Concatenated file size: {concatenated_size} bytes")

            # Upload the complete file to IPFS
            try:
                result = await self.client.upload_file(output_path, encrypt=False, seed_phrase=seed_phrase)
                logger.info(
                    f"Uploaded concatenated file to IPFS: CID={result['cid']}, Size={result['size_bytes']} bytes"
                )

                # Verify that the uploaded size matches our calculated size
                if result["size_bytes"] != concatenated_size:
                    logger.warning(
                        f"Size mismatch in uploaded file: Expected={concatenated_size}, Actual={result['size_bytes']}"
                    )

                # Pin the CID to ensure it stays available
                await self.client.pin(result["cid"], seed_phrase=seed_phrase)
                logger.info(f"Pinned CID: {result['cid']}")
            except Exception as e:
                logger.error(f"Failed to upload concatenated file: {e}")
                raise

            # Calculate the combined ETag (required for S3 compatibility)
            # For multipart uploads, S3 uses a special ETag format

            # First, download the concatenated file we just uploaded to verify integrity
            verification_path = Path(temp_dir) / "verification_download"
            await self.client.download_file(
                result["cid"],
                verification_path,
                decrypt=False,
                seed_phrase=seed_phrase,
                skip_directory_check=True,
            )

            # Calculate MD5 of the downloaded concatenated file
            async with aiofiles.open(verification_path, "rb") as f:
                verification_data = await f.read()
                verification_md5 = hashlib.md5(verification_data).hexdigest()
            logger.info(f"Downloaded concatenated file md5: {verification_md5}")

            # Calculate individual MD5s for each part
            part_md5s = []
            for part_num, part_path in part_files:
                async with aiofiles.open(part_path, "rb") as f:
                    part_data = await f.read()
                    part_md5 = hashlib.md5(part_data).hexdigest()
                    part_md5s.append(part_md5)
                    logger.info(f"Part {part_num} md5: {part_md5}")

            # Now continue with standard ETag calculation
            etags = [str(part_info["etag"]).split("-")[0] for part_info in parts]
            combined_etag = hashlib.md5(("".join(etags)).encode()).hexdigest()
            final_etag = f"{combined_etag}-{len(parts)}"

            # Log information about concatenation
            logger.info(f"Concatenated {len(parts)} parts into single file")
            logger.info(f"Total size: {total_size} bytes, CID: {result['cid']}")
            for part_num, _ in part_files:
                logger.info(f"Part {part_num} included in concatenation")

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

            # Clean up temp directory recursively
            if Path(temp_dir).exists():
                import shutil

                shutil.rmtree(temp_dir, ignore_errors=True)
                logger.info(f"Cleaned up temp directory: {temp_dir}")
