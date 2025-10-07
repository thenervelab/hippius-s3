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
) -> bytes:
    if not should_decrypt:
        return cbytes
    try:
        return CryptoService.decrypt_chunk(
            cbytes,
            seed_phrase=seed_phrase,
            object_id=object_id,
            part_number=int(part_number),
            chunk_index=int(chunk_index),
        )
    except Exception:
        # Fallback to SDK legacy key (account+bucket scoped) for single-chunk legacy ciphertext
        from hippius_s3.config import get_config

        cfg = get_config()
        if not cfg.enable_legacy_sdk_compat:
            raise
        from hippius_s3.legacy.sdk_compat import decrypt_part_ciphertext  # local import

        if not address or not bucket_name:
            raise
        return await decrypt_part_ciphertext(ciphertext=cbytes, address=address, bucket_name=bucket_name)


def maybe_slice(pt: bytes, start: Optional[int], end_excl: Optional[int]) -> bytes:
    if start is None or end_excl is None:
        return pt
    return pt[int(start) : int(end_excl)]
