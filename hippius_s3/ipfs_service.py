import asyncio
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

PENDING_CID_SENTINELS: set[str] = {"", "none", "pending"}


class PendingCIDError(ValueError):
    """Raised when a placeholder CID (e.g. 'pending') is used for an IPFS fetch."""


def _normalize_cid(cid: str) -> str:
    return str(cid or "").strip()


def _ensure_concrete_cid(cid: str) -> str:
    cid_norm = _normalize_cid(cid)
    # Placeholder detection is case-insensitive, but returned CID must preserve original casing.
    if cid_norm.lower() in PENDING_CID_SENTINELS:
        raise PendingCIDError(f"Refusing to download placeholder CID: {cid!r}")
    return cid_norm


class S3Download(BaseModel):
    """Result model for s3_download method."""

    cid: str
    elapsed: float
    decrypted: bool
    data: bytes
    size_bytes: int


def _parse_csv_urls(value: str | None) -> list[str]:
    raw = str(value or "")
    out: list[str] = []
    for part in raw.split(","):
        u = part.strip().strip('"').strip("'")
        if not u:
            continue
        out.append(u.rstrip("/"))
    # Preserve order but de-dup
    deduped: list[str] = []
    seen: set[str] = set()
    for u in out:
        if u in seen:
            continue
        seen.add(u)
        deduped.append(u)
    return deduped


def _ipfs_api_candidates() -> list[str]:
    """
    Return IPFS API base URLs to try, in order.

    - Prefer HIPPIUS_IPFS_API_URLS (CSV) when set.
    """
    return _parse_csv_urls(getattr(config, "ipfs_api_urls", "") or "")


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
    cid_norm = _ensure_concrete_cid(cid)
    candidates = _ipfs_api_candidates()
    if not candidates:
        raise RuntimeError("No IPFS API URL configured (HIPPIUS_IPFS_API_URLS)")

    last_err: Exception | None = None
    yielded_any = False
    async with httpx.AsyncClient(timeout=config.httpx_ipfs_api_timeout) as client:  # noqa: SIM117
        for idx, base in enumerate(candidates, 1):
            download_url = f"{base}/api/v0/cat"
            try:
                logger.debug(
                    "IPFS cat attempt %s/%s cid=%s base=%s",
                    idx,
                    len(candidates),
                    cid_norm,
                    base,
                )
                async with client.stream("POST", download_url, params={"arg": cid_norm}) as response:
                    response.raise_for_status()
                    async for chunk in response.aiter_bytes(chunk_size=8192):
                        yielded_any = True
                        yield chunk
                return
            except Exception as e:
                last_err = e
                if yielded_any:
                    # Correctness: once we've yielded any bytes, we must not fail over to another node,
                    # otherwise the consumer will see duplicated/restarted stream content.
                    logger.error(
                        "IPFS cat failed after yielding bytes (no failover) cid=%s base=%s err=%s",
                        cid_norm,
                        base,
                        repr(e),
                    )
                    raise

                # Safe to fail over before any bytes have been yielded.
                logger.warning(
                    "IPFS cat failed attempt %s/%s (will failover) cid=%s base=%s err=%s",
                    idx,
                    len(candidates),
                    cid_norm,
                    base,
                    repr(e),
                )
                continue

    assert last_err is not None
    raise last_err


async def s3_download(
    cid: str,
    account_address: str,
    bucket_name: str,
    decrypt: bool,
) -> S3Download:
    # Fast-fail placeholder CIDs so we don't spam IPFS with arg=pending
    _ensure_concrete_cid(cid)
    start_time = time.time()
    try:
        raw_data = bytearray()
        async for chunk in _stream_cid(cid):
            raw_data.extend(chunk)
        raw_data_bytes = bytes(raw_data)
    except asyncio.TimeoutError:
        elapsed = time.time() - start_time
        raise TimeoutError(f"IPFS download timeout after {elapsed:.1f}s for CID {cid}") from None

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
