from __future__ import annotations

import base64

from hippius_sdk.key_storage import generate_and_store_key_for_subaccount
from hippius_sdk.key_storage import get_key_for_subaccount
from hippius_sdk.key_storage import is_key_storage_enabled


async def get_or_create_encryption_key_bytes(
    *,
    subaccount_id: str,
    bucket_name: str,
) -> bytes:
    """Resolve encryption key bytes for a given subaccount + bucket.

    Uses SDK key storage exclusively. If unavailable or key cannot be created,
    raises an error. No seed-based fallback.
    """
    combined_id = f"{subaccount_id}:{bucket_name}"

    if not is_key_storage_enabled():
        raise RuntimeError("encryption_key_unavailable")

    key_b64 = await get_key_for_subaccount(combined_id)
    if not key_b64:
        key_b64 = await generate_and_store_key_for_subaccount(combined_id)
    if not key_b64:
        raise RuntimeError("encryption_key_unavailable")
    return base64.b64decode(key_b64)
