import base64
import hashlib
import logging
import time
from typing import AsyncIterator

import asyncpg
import httpx
import nacl.secret
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
