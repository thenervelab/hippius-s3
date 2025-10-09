from __future__ import annotations

from typing import Optional

from hippius_s3.services.crypto_service import CryptoService


async def decrypt_chunk_if_needed(
    should_decrypt: bool,
    cbytes: bytes,
    *,
    seed_phrase: str,
    object_id: str,
    part_number: int,
    chunk_index: int,
    address: str,
    bucket_name: str,
    storage_version: int,
) -> bytes:
    if not should_decrypt:
        return cbytes
    # Prefer indicated storage_version but fall back to alternate on failure when allowed
    if int(storage_version) == 2:
        try:
            return CryptoService.decrypt_chunk(
                cbytes,
                seed_phrase=seed_phrase,
                object_id=object_id,
                part_number=int(part_number),
                chunk_index=int(chunk_index),
            )
        except Exception:
            from hippius_s3.config import get_config as _get_cfg

            cfg = _get_cfg()
            if not cfg.enable_legacy_sdk_compat:
                raise
            from hippius_s3.legacy.sdk_compat import decrypt_part_ciphertext  # local import

            if not address or not bucket_name:
                raise
            return await decrypt_part_ciphertext(ciphertext=cbytes, address=address, bucket_name=bucket_name)
    else:
        from hippius_s3.config import get_config as _get_cfg

        cfg = _get_cfg()
        try:
            if not cfg.enable_legacy_sdk_compat:
                raise RuntimeError("legacy_disabled")
            from hippius_s3.legacy.sdk_compat import decrypt_part_ciphertext  # local import

            if not address or not bucket_name:
                raise RuntimeError("legacy_key_missing")
            return await decrypt_part_ciphertext(ciphertext=cbytes, address=address, bucket_name=bucket_name)
        except Exception:
            return CryptoService.decrypt_chunk(
                cbytes,
                seed_phrase=seed_phrase,
                object_id=object_id,
                part_number=int(part_number),
                chunk_index=int(chunk_index),
            )


def maybe_slice(pt: bytes, start: Optional[int], end_excl: Optional[int]) -> bytes:
    if start is None or end_excl is None:
        return pt
    return pt[int(start) : int(end_excl)]
