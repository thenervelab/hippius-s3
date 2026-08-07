from hippius_s3.fs_pressure import FreeSpaceGate
from hippius_s3.fs_pressure import validate_promotion_band

from .dual_fs_store import DualFileSystemPartsStore
from .dual_fs_store import LocalReadRecorder
from .dual_fs_store import PeerFetcher
from .dual_fs_store import PromotionRecorder
from .fs_store import FileSystemPartsStore
from .notifier import ChunkNotifier
from .object_parts import RedisObjectPartsCache


def create_fs_store(
    config: object,
    *,
    on_promote: PromotionRecorder | None = None,
    peer_fetch: PeerFetcher | None = None,
    on_local_read: LocalReadRecorder | None = None,
) -> FileSystemPartsStore:
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
        space_gate = None
        if promote:
            floor = float(getattr(config, "promote_min_free_ratio", 0.175))
            # Fail fast on a threshold set that cannot recover. Every one of these is settable
            # per-deployment, and a bad combination is SILENT: the read tier simply stops
            # warming, with nothing in the logs saying why. Better a refused startup than a
            # cache that quietly never fills.
            validate_promotion_band(
                promote_min_free_ratio=floor,
                fs_cache_min_free_ratio=float(getattr(config, "fs_cache_min_free_ratio", 0.08)),
            )
            # Built whenever promotion is on: an ungated promoter is an unthrottled writer on
            # the disk ingest depends on, so the two are not independently switchable either.
            space_gate = FreeSpaceGate(cache_dir, floor)
        return DualFileSystemPartsStore(
            cache_dir,
            fallback_dir,
            promote=promote,
            on_promote=on_promote,
            peer_fetch=peer_fetch,
            space_gate=space_gate,
            on_local_read=on_local_read,
        )
    return FileSystemPartsStore(cache_dir)


__all__ = [
    "ChunkNotifier",
    "RedisObjectPartsCache",
    "FileSystemPartsStore",
    "DualFileSystemPartsStore",
    "create_fs_store",
]
