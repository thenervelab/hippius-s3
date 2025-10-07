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
        try:
            import base64 as _b64

            import nacl.secret as _secret

            from hippius_s3.ipfs_service import get_encryption_key  # local import to avoid cycles

            key_b64 = None
            if address and bucket_name:
                key_b64 = await get_encryption_key(f"{address}:{bucket_name}")
            if not key_b64:
                raise RuntimeError("legacy_key_missing")
            key = _b64.b64decode(key_b64)
            box = _secret.SecretBox(key)
            return bytes(box.decrypt(cbytes))
        except Exception as _:
            # Re-raise original
            raise


def maybe_slice(pt: bytes, start: Optional[int], end_excl: Optional[int]) -> bytes:
    if start is None or end_excl is None:
        return pt
    return pt[int(start) : int(end_excl)]
