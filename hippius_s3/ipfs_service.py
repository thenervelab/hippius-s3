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
            ipfs_gateway=config.ipfs_get_url,
            ipfs_api_url=config.ipfs_store_url,
            encrypt_by_default=False,
        )
        logger.info(f"IPFS service initialized: {config.ipfs_get_url} {config.ipfs_store_url}")

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
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_path = temp_file.name
            temp_file.write(file_data)

        try:
            result = await self.client.upload_file(
                temp_path,
                encrypt=encrypt,
            )
            pinning_status = await self.client.pin(result["cid"])

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

    async def download_file(self, cid: str, decrypt: bool = False, max_retries: int = 3, retry_delay: int = 2) -> bytes:
        """
        Download file data from IPFS with retry logic.

        Args:
            cid: IPFS content identifier
            decrypt: Whether to decrypt the file (if it was encrypted)
            max_retries: Maximum number of download retry attempts
            retry_delay: Delay in seconds between retry attempts

        Returns:
            Binary file data
        """
        # Create a temporary file path for the download
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_path = temp_file.name

        try:
            # Implement retry logic for IPFS downloads
            attempt = 0
            last_exception = None

            while attempt < max_retries:
                attempt += 1
                try:
                    # Download the file from IPFS
                    await self.client.download_file(
                        cid,
                        temp_path,
                        decrypt=decrypt,
                    )

                    # Read the file data
                    async with aiofiles.open(temp_path, "rb") as f:
                        content = await f.read()
                        logger.info(f"Successfully downloaded file with CID {cid} (attempt {attempt})")
                        return cast(bytes, content)

                except Exception as e:
                    last_exception = e
                    logger.warning(f"Download attempt {attempt} failed: {e}")

                    if attempt < max_retries:
                        logger.info(f"Retrying in {retry_delay} seconds...")
                        import asyncio

                        await asyncio.sleep(retry_delay)
                        # Double the delay for exponential backoff
                        retry_delay *= 2

            # If we get here, all retries have failed
            logger.error(
                f"Failed to download file with CID {cid} after {max_retries} attempts. Last error: {last_exception}"
            )
            raise last_exception or RuntimeError(f"Failed to download file with CID {cid} after {max_retries} attempts")

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
                part_size = int(part_info["size_bytes"])

                logger.info(f"Processing part {part_number}: CID={cid}, Expected size={part_size} bytes")

                # Download the part from IPFS
                try:
                    await self.client.download_file(cid, part_path)
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
                result = await self.client.upload_file(output_path, encrypt=False)
                logger.info(
                    f"Uploaded concatenated file to IPFS: CID={result['cid']}, Size={result['size_bytes']} bytes"
                )

                # Verify that the uploaded size matches our calculated size
                if result["size_bytes"] != concatenated_size:
                    logger.warning(
                        f"Size mismatch in uploaded file: Expected={concatenated_size}, Actual={result['size_bytes']}"
                    )

                # Pin the CID to ensure it stays available
                await self.client.pin(result["cid"])
                logger.info(f"Pinned CID: {result['cid']}")
            except Exception as e:
                logger.error(f"Failed to upload concatenated file: {e}")
                raise

            # Calculate the combined ETag (required for S3 compatibility)
            # For multipart uploads, S3 uses a special ETag format

            # First, download the concatenated file we just uploaded to verify integrity
            verification_path = Path(temp_dir) / "verification_download"
            await self.client.download_file(result["cid"], verification_path)

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
