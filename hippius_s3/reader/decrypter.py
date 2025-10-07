from __future__ import annotations

from typing import Optional

from hippius_s3.services.crypto_service import CryptoService


def decrypt_chunk_if_needed(
    should_decrypt: bool,
    cbytes: bytes,
    *,
    seed_phrase: str,
    object_id: str,
    part_number: int,
    chunk_index: int,
) -> bytes:
    if not should_decrypt:
        return cbytes
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
