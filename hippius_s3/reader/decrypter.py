from __future__ import annotations

from typing import Optional

from hippius_s3.services.crypto_service import CryptoService
from hippius_s3.storage_version import require_supported_storage_version


async def decrypt_chunk_if_needed(
    cbytes: bytes,
    *,
    object_id: str,
    part_number: int,
    chunk_index: int,
    address: str = "",
    bucket_name: str = "",
    storage_version: int,
    key_bytes: bytes | None,
    suite_id: str | None,
    bucket_id: str,
    upload_id: str,
) -> bytes:
    _ = require_supported_storage_version(int(storage_version))
    if key_bytes is None:
        raise RuntimeError("decrypt_key_missing")

    return CryptoService.decrypt_chunk(
        cbytes,
        seed_phrase="",
        object_id=object_id,
        part_number=int(part_number),
        chunk_index=int(chunk_index),
        key=key_bytes,
        suite_id=suite_id,
        bucket_id=str(bucket_id or ""),
        upload_id=str(upload_id or ""),
    )


def maybe_slice(pt: bytes, start: Optional[int], end_excl: Optional[int]) -> bytes:
    if start is None or end_excl is None:
        return pt
    return pt[int(start) : int(end_excl)]
