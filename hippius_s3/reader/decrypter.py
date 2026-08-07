from __future__ import annotations

from typing import Optional

from cryptography.exceptions import InvalidTag
from nacl.exceptions import CryptoError

from hippius_s3.services.crypto_pool import run_crypto
from hippius_s3.services.crypto_service import CryptoService
from hippius_s3.storage_version import require_supported_storage_version


# The two ways stored ciphertext can turn out not to be the chunk that was asked for: the AEAD
# tag does not authenticate (InvalidTag), or the body is too short to even hold a nonce and tag
# (CryptoError("ciphertext_too_short") — the shape a version-skewed peer serves). Named here
# because the decrypter is what defines "these bytes are unusable"; the streamer acts on it.
CIPHERTEXT_UNUSABLE: tuple[type[BaseException], ...] = (InvalidTag, CryptoError)


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

    # RD-2: offload the AES-GCM decrypt to the dedicated crypto pool so a ~4 MiB decrypt doesn't
    # head-of-line-block every other request on this worker. cryptography releases the GIL, so this
    # yields real parallelism; the streamer still awaits chunks in plan order, so emit order is
    # unchanged and an auth-tag failure still propagates to break the stream.
    return await run_crypto(
        CryptoService.decrypt_chunk,
        cbytes,
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
