from __future__ import annotations


MIN_SUPPORTED_STORAGE_VERSION = 4


def require_supported_storage_version(storage_version: int) -> int:
    """Validate storage_version is supported by this deployment.

    Hippius S3 has dropped support for legacy storage versions (< v4).
    """
    sv = int(storage_version)
    if sv < MIN_SUPPORTED_STORAGE_VERSION:
        raise RuntimeError(f"unsupported_storage_version:{sv}")
    return sv
