from __future__ import annotations

from typing import Optional

from hippius_s3.services.crypto_service import CryptoService


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
    if not should_decrypt:
        return cbytes
    # v2+ (modern): decrypt using per-bucket key from SDK key storage. No seed fallback.
    if int(storage_version) >= 2:
        from hippius_s3.services.key_service import get_or_create_encryption_key_bytes

        if not address or not bucket_name:
            raise RuntimeError("missing_address_or_bucket")
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

    # v1: legacy SDK compatibility decrypt (whole-part compatibility helper)
    from hippius_s3.legacy.sdk_compat import decrypt_part_ciphertext  # local import

    if not address or not bucket_name:
        raise RuntimeError("legacy_key_missing")
    return await decrypt_part_ciphertext(ciphertext=cbytes, address=address, bucket_name=bucket_name)


def maybe_slice(pt: bytes, start: Optional[int], end_excl: Optional[int]) -> bytes:
    if start is None or end_excl is None:
        return pt
    return pt[int(start) : int(end_excl)]
