from __future__ import annotations

import logging

from hippius_s3.config import get_config


logger = logging.getLogger(__name__)


# TODO: Remove this entire module once all objects are migrated to new encryption format (storage_version >= 3).
# This module provides backward compatibility for legacy SDK encryption (whole-ciphertext decryption).
# It is used by hippius_s3/reader/decrypter.py when enable_legacy_sdk_compat=true.
# Once migration is complete, set LEGACY_SDK_COMPAT=false and delete this file.


async def get_sdk_key_b64(address: str, bucket_name: str) -> str:
    from hippius_s3.ipfs_service import get_encryption_key  # local import to avoid cycles

    return await get_encryption_key(f"{address}:{bucket_name}")


async def get_all_sdk_keys_b64(address: str, bucket_name: str) -> list[str]:
    from hippius_s3.ipfs_service import get_all_encryption_keys

    return await get_all_encryption_keys(f"{address}:{bucket_name}")


async def decrypt_whole_ciphertext(
    *,
    ciphertext: bytes,
    address: str,
    bucket_name: str,
) -> bytes:
    cfg = get_config()
    if not cfg.enable_legacy_sdk_compat:
        raise RuntimeError("legacy_compat_disabled")
    import base64 as _b64

    import nacl.exceptions
    import nacl.secret as _secret

    identifier = f"{address}:{bucket_name}"

    # Fast path: Try the most recent key first
    key_b64 = await get_sdk_key_b64(address, bucket_name)
    key = _b64.b64decode(key_b64)
    box = _secret.SecretBox(key)

    try:
        return bytes(box.decrypt(ciphertext))
    except nacl.exceptions.CryptoError:
        logger.warning(f"Primary key failed for {identifier}, trying fallback keys...")

    # Slow path: Try all available keys
    all_keys_b64 = await get_all_sdk_keys_b64(address, bucket_name)

    if len(all_keys_b64) <= 1:
        logger.warning(
            f"Decryption failed for {identifier} and no fallback keys available; returning plaintext fallback"
        )
        return ciphertext

    for idx, fallback_key_b64 in enumerate(all_keys_b64[1:], start=1):
        try:
            fallback_key = _b64.b64decode(fallback_key_b64)
            fallback_box = _secret.SecretBox(fallback_key)
            result = bytes(fallback_box.decrypt(ciphertext))
            logger.warning(
                f"Decryption succeeded with fallback key #{idx} for {identifier} "
                f"(tried {idx + 1}/{len(all_keys_b64)} keys)"
            )
            return result
        except nacl.exceptions.CryptoError:
            continue

    logger.warning(
        f"Decryption failed for {identifier} with all {len(all_keys_b64)} available keys; returning plaintext fallback"
    )
    return ciphertext


async def decrypt_part_ciphertext(
    *,
    ciphertext: bytes,
    address: str,
    bucket_name: str,
) -> bytes:
    # Alias retained for clarity
    return await decrypt_whole_ciphertext(ciphertext=ciphertext, address=address, bucket_name=bucket_name)
