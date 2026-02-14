from __future__ import annotations


MIN_SUPPORTED_STORAGE_VERSION = 5


class UnsupportedStorageVersionError(RuntimeError):
    def __init__(self, storage_version: int) -> None:
        super().__init__(f"unsupported_storage_version:{int(storage_version)}")
        self.storage_version = int(storage_version)


def require_supported_storage_version(storage_version: int) -> int:
    """Validate storage_version is supported by this deployment.

    Hippius S3 has dropped support for storage versions < v5.
    """
    sv = int(storage_version)
    if sv < MIN_SUPPORTED_STORAGE_VERSION:
        raise UnsupportedStorageVersionError(sv)
    return sv
