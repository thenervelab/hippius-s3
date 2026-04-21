from .dual_fs_store import DualFileSystemPartsStore
from .fs_store import FileSystemPartsStore
from .notifier import ChunkNotifier
from .object_parts import RedisObjectPartsCache


def create_fs_store(config: object) -> FileSystemPartsStore:
    fallback_dir = getattr(config, "object_cache_fallback_dir", "")
    cache_dir = getattr(config, "object_cache_dir", "")
    if fallback_dir:
        return DualFileSystemPartsStore(cache_dir, fallback_dir)
    return FileSystemPartsStore(cache_dir)


__all__ = [
    "ChunkNotifier",
    "RedisObjectPartsCache",
    "FileSystemPartsStore",
    "DualFileSystemPartsStore",
    "create_fs_store",
]
