from .dual_fs_store import DualFileSystemPartsStore
from .fs_store import FileSystemPartsStore
from .notifier import ChunkNotifier
from .object_parts import RedisObjectPartsCache


def create_fs_store(config: object, *, on_promote: object = None) -> FileSystemPartsStore:
    """Build the parts store: dual-tier when a fallback pool is configured, else single.

    `on_promote` records a promoted chunk's residency so the local evictor owns the copy.
    Promotion stays off unless BOTH the flag is set and a recorder is supplied — an
    unrecorded promotion is a copy no evictor can reclaim, so the two travel together
    rather than being independently switchable.
    """
    fallback_dir = getattr(config, "object_cache_fallback_dir", "")
    cache_dir = getattr(config, "object_cache_dir", "")
    if fallback_dir:
        promote = bool(getattr(config, "object_cache_promote_on_read", False)) and on_promote is not None
        return DualFileSystemPartsStore(cache_dir, fallback_dir, promote=promote, on_promote=on_promote)  # type: ignore[arg-type]
    return FileSystemPartsStore(cache_dir)


__all__ = [
    "ChunkNotifier",
    "RedisObjectPartsCache",
    "FileSystemPartsStore",
    "DualFileSystemPartsStore",
    "create_fs_store",
]
