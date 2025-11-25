import asyncio
import base64
import hashlib
import logging
import random
import tempfile
import time
from pathlib import Path
from typing import AsyncIterator
from typing import Dict
from typing import Optional
from typing import Union

import asyncpg
import httpx
import nacl.secret
from hippius_sdk.client import HippiusClient
from pydantic import BaseModel

from hippius_s3.config import get_config


logger = logging.getLogger(__name__)
config = get_config()


class S3Download(BaseModel):
    """Result model for s3_download method."""

    cid: str
    elapsed: float
    decrypted: bool
    data: bytes
    size_bytes: int


async def get_encryption_key(identifier: str) -> str:
    """Get the most recent encryption key for an identifier.

    Returns the newest key first for the fast path.
    Use get_all_encryption_keys() if you need to try multiple keys.
    """
    hashed_identifier = hashlib.sha256(identifier.encode("utf-8")).hexdigest()
    conn = await asyncpg.connect(config.encryption_database_url)
    try:
        result = await conn.fetchrow(
            """
            SELECT encryption_key_b64
            FROM encryption_keys
            WHERE subaccount_id = $1
            ORDER BY created_at DESC
            LIMIT 1
        """,
            hashed_identifier,
        )
        return str(result["encryption_key_b64"])
    finally:
        await conn.close()


async def get_all_encryption_keys(identifier: str) -> list[str]:
    """Get all encryption keys for an identifier, ordered by newest first.

    Use this when decryption fails with the primary key and you need to try fallbacks.
    """
    hashed_identifier = hashlib.sha256(identifier.encode("utf-8")).hexdigest()
    conn = await asyncpg.connect(config.encryption_database_url)
    try:
        results = await conn.fetch(
            """
            SELECT encryption_key_b64
            FROM encryption_keys
            WHERE subaccount_id = $1
            ORDER BY created_at DESC
        """,
            hashed_identifier,
        )
        return [str(row["encryption_key_b64"]) for row in results]
    finally:
        await conn.close()


async def _stream_cid(cid: str) -> AsyncIterator[bytes]:
    download_url = f"{config.ipfs_store_url.rstrip('/')}/api/v0/cat?arg={cid}"

    async with httpx.AsyncClient(timeout=config.httpx_ipfs_api_timeout) as client:  # noqa: SIM117
        async with client.stream(
            "POST",
            download_url,
        ) as response:
            response.raise_for_status()

            async for chunk in response.aiter_bytes(chunk_size=8192):
                yield chunk


async def s3_download(
    cid: str,
    account_address: str,
    bucket_name: str,
    decrypt: bool,
) -> S3Download:
    start_time = time.time()

    raw_data = bytearray()
    async for chunk in _stream_cid(cid):
        raw_data.extend(chunk)
    raw_data_bytes = bytes(raw_data)

    if decrypt:
        identifier = f"{account_address}:{bucket_name}"
        encryption_key_b64 = await get_encryption_key(identifier)
        encryption_key = base64.b64decode(encryption_key_b64)

        box = nacl.secret.SecretBox(encryption_key)
        data = box.decrypt(raw_data_bytes)

        logger.info(f"Decrypted {len(data)} bytes for {cid=} {bucket_name=} {account_address=}")
    else:
        data = raw_data_bytes
        logger.info(f"Public chunk, no encryption {len(data)} bytes {cid=} {bucket_name=} {account_address=}")

    size_bytes = len(data)
    elapsed_time = time.time() - start_time

    return S3Download(
        cid=cid,
        data=data,
        decrypted=decrypt,
        elapsed=elapsed_time,
        size_bytes=size_bytes,
    )


async def upload_to_ipfs(
    *,
    file_data: bytes,
    file_name: str,
    content_type: str,
    client: HippiusClient,
    encrypt: bool = False,
    seed_phrase: Optional[str] = None,
) -> Dict[str, Union[str, int]]:
    """
    Upload file data to IPFS (legacy method).

    NOTE: This method is deprecated for regular uploads. Use client.s3_publish() instead
    for full IPFS upload + pinning + blockchain publishing.

    This method is still used for multipart upload parts, which only need IPFS upload
    without blockchain publishing (the final concatenated file gets published).

    TODO: Remove this function once multipart upload flow is refactored to use publish path.

    Args:
        file_data: Binary file data
        file_name: Name of the file
        content_type: MIME type of the file
        client: HippiusClient instance for IPFS operations
        encrypt: Whether to encrypt the file
        seed_phrase: Seed phrase to use for blockchain operations

    Returns:
        Dict containing IPFS CID and file information
    """
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_path = temp_file.name
        temp_file.write(file_data)

    try:

        def _compute_backoff_ms(attempt: int) -> float:
            base = getattr(config, "ipfs_retry_base_ms", 500)
            max_ms = getattr(config, "ipfs_retry_max_ms", 5000)
            exp = base * (2 ** max(0, attempt - 1))
            jitter = random.uniform(0, exp * 0.1)
            return float(min(exp + jitter, max_ms))

        for attempt in range(1, int(getattr(config, "ipfs_max_retries", 3)) + 1):
            try:
                if encrypt and seed_phrase:
                    key_material = hashlib.sha256(seed_phrase.encode("utf-8")).digest()
                    enc_client = HippiusClient(
                        ipfs_api_url=config.ipfs_store_url,
                        api_url=config.hippius_api_base_url,
                        encrypt_by_default=False,
                        encryption_key=key_material,
                    )
                    result = await enc_client.upload_file(
                        temp_path,
                        encrypt=encrypt,
                        hippius_key=config.hippius_service_key,
                        pin=False,
                    )
                else:
                    result = await client.upload_file(
                        temp_path,
                        encrypt=encrypt,
                        hippius_key=config.hippius_service_key,
                        pin=False,
                    )
                break
            except Exception as e:
                if attempt >= int(getattr(config, "ipfs_max_retries", 3)):
                    logger.exception(f"IPFS upload failed after {attempt} attempts: {e}")
                    raise
                backoff_ms = _compute_backoff_ms(attempt)
                logger.warning(f"IPFS upload failed (attempt {attempt}), retrying in {backoff_ms:.0f}ms: {e}")
                await asyncio.sleep(backoff_ms / 1000.0)

        return {
            "cid": result["cid"],
            "file_name": file_name,
            "content_type": content_type,
            "size_bytes": result["size_bytes"],
            "encrypted": result.get("encrypted", False),
        }
    finally:
        if Path(temp_path).exists():
            Path(temp_path).unlink()
