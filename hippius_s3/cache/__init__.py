from .download_chunks import DownloadChunksCache
from .download_chunks import NullDownloadChunksCache
from .download_chunks import RedisDownloadChunksCache
from .dual_fs_store import DualFileSystemPartsStore
from .fs_store import FileSystemPartsStore
from .object_parts import NullObjectPartsCache
from .object_parts import ObjectPartsCache
from .object_parts import RedisObjectPartsCache
from .object_parts import RedisUploadPartsCache


def create_fs_store(config: object) -> FileSystemPartsStore:
    fallback_dir = getattr(config, "object_cache_fallback_dir", "")
    cache_dir = getattr(config, "object_cache_dir", "")
    if fallback_dir:
        return DualFileSystemPartsStore(cache_dir, fallback_dir)
    return FileSystemPartsStore(cache_dir)


__all__ = [
    "ObjectPartsCache",
    "RedisObjectPartsCache",
    "RedisUploadPartsCache",
    "NullObjectPartsCache",
    "DownloadChunksCache",
    "RedisDownloadChunksCache",
    "NullDownloadChunksCache",
    "FileSystemPartsStore",
    "DualFileSystemPartsStore",
    "create_fs_store",
]
