from __future__ import annotations

import logging
from typing import Optional

from hippius_s3.config import get_config
from hippius_s3.services.crypto_service import CryptoService
from hippius_s3.storage_version import require_supported_storage_version


logger = logging.getLogger(__name__)


async def decrypt_chunk(
    cbytes: bytes,
    *,
    object_id: str,
    part_number: int,
    chunk_index: int,
    address: str,
    bucket_name: str,
) -> bytes:
    """Decrypt a ciphertext chunk.

    Hippius S3 now assumes v4+ objects everywhere; legacy (< v3) decrypt paths are removed.
    """
    from hippius_s3.services.key_service import get_or_create_encryption_key_bytes

    if not address or not bucket_name:
        raise RuntimeError("missing_address_or_bucket")
    cfg = get_config()
    try:
        key_bytes = await get_or_create_encryption_key_bytes(
            main_account_id=address,
            bucket_name=bucket_name,
        )
        return CryptoService.decrypt_chunk(
            cbytes,
            seed_phrase="",
            object_id=object_id,
            part_number=int(part_number),
            chunk_index=int(chunk_index),
            key=key_bytes,
        )
    except Exception:
        logger.error(
            "decrypt_failed key_debug dsn=%s addr=%s bucket=%s object_id=%s part=%s chunk=%s",
            getattr(cfg, "encryption_database_url", ""),
            address,
            bucket_name,
            object_id,
            str(part_number),
            str(chunk_index),
        )
        raise


async def decrypt_chunk_if_needed(
    should_decrypt: bool,
    cbytes: bytes,
    *,
    object_id: str,
    part_number: int,
    chunk_index: int,
    address: str,
    bucket_name: str,
    storage_version: int,
) -> bytes:
    """Optionally decrypt a chunk based on caller context.

    Note: Hippius S3 only supports storage_version>=4. We still validate the version
    here so callers can't accidentally stream legacy objects.
    """
    _ = require_supported_storage_version(int(storage_version))
    if not should_decrypt:
        return cbytes
    return await decrypt_chunk(
        cbytes,
        object_id=object_id,
        part_number=int(part_number),
        chunk_index=int(chunk_index),
        address=address,
        bucket_name=bucket_name,
    )


def maybe_slice(pt: bytes, start: Optional[int], end_excl: Optional[int]) -> bytes:
    if start is None or end_excl is None:
        return pt
    return pt[int(start) : int(end_excl)]
