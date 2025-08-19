import hashlib
import logging
import tempfile
from pathlib import Path
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union

import aiofiles
import redis.asyncio as async_redis
from hippius_sdk.client import HippiusClient

from hippius_s3.config import Config
from hippius_s3.queue import enqueue_upload_request


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
    ) -> bytes:
        """
        Download file data from IPFS with automatic decryption using s3_download.

        This method now uses s3_download internally for better key management and
        automatic decryption while maintaining backward compatibility.

        Args:
            cid: IPFS content identifier
            decrypt: Whether to attempt automatic decryption (default: True since all files are encrypted)
            max_retries: Maximum number of download retry attempts (currently not used by s3_download)
            retry_delay: Delay in seconds between retry attempts (currently not used by s3_download)
            seed_phrase: Seed phrase to use for decryption key retrieval

        Returns:
            Binary file data (decrypted if decrypt=True and file was encrypted)
        """
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_path = temp_file.name

        try:
            await self.client.s3_download(
                cid=cid,
                output_path=temp_path,
                subaccount_id=subaccount_id,
                bucket_name=bucket_name,
                auto_decrypt=decrypt,
                download_node=self.config.ipfs_store_url,
            )

            # Read file and return bytes (maintains existing interface)
            async with aiofiles.open(temp_path, "rb") as f:
                data = await f.read()
                return bytes(data)

        except Exception as e:
            # Maintain compatibility with existing error handling
            logger.exception(f"Failed to download file with CID {cid}: {e}")
            raise RuntimeError(f"Failed to download file with CID {cid}") from e
        finally:
            # Clean up temporary file
            if Path(temp_path).exists():
                Path(temp_path).unlink()

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
            result = await self.client.upload_file(
                temp_path,
                encrypt=False,
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

    async def concatenate_parts(
        self,
        parts: List[Dict[str, Union[str, int]]],
        content_type: str,
        file_name: str,
        subaccount_id: str,
        bucket_name: str,
        is_public_bucket: bool = False,
        seed_phrase: Optional[str] = None,
    ) -> Dict[str, Union[str, int]]:
        """
        Concatenate multiple file parts into a single file in IPFS.

        This is used to complete a multipart upload.

        Args:
            parts: List of part information including CIDs
            content_type: MIME type of the final file
            file_name: Name of the final file
            subaccount_id: the api key
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
                cid = str(part_info["ipfs_cid"])
                part_number = part_info["part_number"]
                part_path = Path(temp_dir) / f"part_{part_number}"
                part_size = int(part_info["size_bytes"])

                logger.info(f"Processing part {part_number}: CID={cid}, Expected size={part_size} bytes")

                # Download the part from IPFS (parts are unencrypted)
                try:
                    part_data = await self.download_file(
                        cid=cid,
                        subaccount_id=subaccount_id,
                        bucket_name=bucket_name,
                        decrypt=False,
                    )

                    # Write part data to file
                    async with aiofiles.open(part_path, "wb") as f:
                        await f.write(part_data)

                    actual_size = len(part_data)
                    logger.info(f"Downloaded part {part_number}: CID={cid}, Size={actual_size} bytes")

                    # Verify size matches expected if provided
                    if 0 < part_size != actual_size:
                        logger.warning(f"Part {part_number} size mismatch: Expected={part_size}, Actual={actual_size}")

                    part_files.append((int(part_number), part_path))
                    total_size += actual_size
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

            # Upload the complete file to IPFS and publish to blockchain using s3_publish
            try:
                logger.info(
                    f"Using seed phrase for multipart s3_publish (length: {len(seed_phrase.split()) if seed_phrase else 0} words)"
                )

                # Only encrypt if bucket is private
                should_encrypt = not is_public_bucket

                s3_result = await self.client.s3_publish(
                    file_path=str(output_path),
                    encrypt=should_encrypt,
                    seed_phrase=seed_phrase,
                    subaccount_id=subaccount_id,
                    bucket_name=bucket_name,
                    store_node=self.config.ipfs_store_url,
                    pin_node=self.config.ipfs_store_url,
                    substrate_url=self.config.substrate_url,
                    publish=False,
                )
                logger.info(f"Published concatenated file: CID={s3_result.cid}, Size={s3_result.size_bytes} bytes")

                # Queue the upload request for pinning
                await enqueue_upload_request(self.redis_client, s3_result, seed_phrase)

                # Verify that the uploaded size matches our calculated size
                if s3_result.size_bytes != concatenated_size:
                    logger.warning(
                        f"Size mismatch in uploaded file: Expected={concatenated_size}, Actual={s3_result.size_bytes}"
                    )

                # Store the result for later use
                result = {
                    "cid": s3_result.cid,
                    "size_bytes": s3_result.size_bytes,
                }

                # Delete the individual part CIDs since they're no longer needed
                # The final concatenated file is now pinned instead
                for part_info in parts:
                    try:
                        await self.client.ipfs_client.delete_file(
                            part_info["ipfs_cid"],
                            cancel_from_blockchain=False,
                            seed_phrase=seed_phrase,
                            unpin=False,
                        )
                        logger.debug(f"Deleted part CID: {part_info['ipfs_cid']}")
                    except Exception as e:  # noqa: PERF203
                        logger.error(
                            f"Failed to delete part {part_info['ipfs_cid']}: {e}"
                        )  # Don't fail the whole operation if deletion fails

            except Exception as e:
                logger.error(f"Failed to publish concatenated file: {e}")
                raise

            # Calculate the combined ETag (required for S3 compatibility)
            # For multipart uploads, S3 uses a special ETag format

            # Verify integrity by downloading the final concatenated file
            verification_data = await self.download_file(
                cid=result["cid"],
                subaccount_id=subaccount_id,
                bucket_name=bucket_name,
                decrypt=False,
            )

            verification_md5 = hashlib.md5(verification_data).hexdigest()
            logger.info(f"Verification: downloaded file MD5={verification_md5}, size={len(verification_data)} bytes")

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
                "encrypted": should_encrypt,
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
