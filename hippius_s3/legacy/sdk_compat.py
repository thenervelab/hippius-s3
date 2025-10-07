from __future__ import annotations

from hippius_s3.config import get_config


async def get_sdk_key_b64(address: str, bucket_name: str) -> str:
    from hippius_s3.ipfs_service import get_encryption_key  # local import to avoid cycles

    return await get_encryption_key(f"{address}:{bucket_name}")


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

    import nacl.secret as _secret

    key_b64 = await get_sdk_key_b64(address, bucket_name)
    key = _b64.b64decode(key_b64)
    box = _secret.SecretBox(key)
    return bytes(box.decrypt(ciphertext))


async def decrypt_part_ciphertext(
    *,
    ciphertext: bytes,
    address: str,
    bucket_name: str,
) -> bytes:
    # Alias retained for clarity
    return await decrypt_whole_ciphertext(ciphertext=ciphertext, address=address, bucket_name=bucket_name)
