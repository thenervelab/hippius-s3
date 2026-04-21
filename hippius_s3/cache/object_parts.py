"""Parts cache facade — FS-backed chunks with Redis pub/sub notifications.

Previously backed by Redis (hence the class name `RedisObjectPartsCache`).
After the FS-cache migration, chunk and meta I/O is delegated to
`FileSystemPartsStore` and the only Redis use is the pub/sub channel
for chunk-ready notifications (isolated in `ChunkNotifier`).

The class name is kept for now to minimize call-site churn; a follow-up PR
may rename to `PartsCache`.
"""

from __future__ import annotations

from typing import Any
from typing import Optional
from typing import Protocol

from .fs_store import FileSystemPartsStore
from .notifier import ChunkNotifier
from .notifier import build_chunk_key


# Lazy monitoring import: avoid pulling opentelemetry at import-time
class _MetricsLike(Protocol):
    def record_cache_operation(self, hit: bool, operation: str) -> None: ...


def _get_metrics_collector() -> _MetricsLike:
    try:
        from hippius_s3.monitoring import get_metrics_collector

        return get_metrics_collector()
    except Exception:

        class _Noop:
            def record_cache_operation(self, hit: bool, operation: str) -> None:
                return None

        return _Noop()


def _get_config_value(name: str, default: int) -> int:
    try:
        from hippius_s3.config import get_config

        cfg = get_config()
        if name == "object_chunk_size_bytes":
            return cfg.object_chunk_size_bytes
        if name == "cache_ttl_seconds":
            return cfg.cache_ttl_seconds
        return default
    except Exception:
        return default


DEFAULT_OBJ_PART_TTL_SECONDS = 1800


class RedisObjectPartsCache:
    """FS-backed parts cache with Redis pub/sub notifications.

    Composes `FileSystemPartsStore` for all chunk/meta I/O and `ChunkNotifier`
    for the wait/notify pub/sub pattern that coordinates streamers with
    download workers.

    The `redis_client` param is retained for backward compatibility (some
    callers reach `.redis` for in-progress flag cleanup); it's only used
    for those legacy operations and may be removed once those are cleaned up.
    """

    def __init__(
        self,
        redis_client: Any,
        queues_client: Any = None,
        download_cache_client: Any = None,
        fs_store: Optional[FileSystemPartsStore] = None,
    ) -> None:
        # `redis_client` retained for a few callers that access `.redis.delete`
        # directly (in-progress flags). Not used for chunk/meta storage.
        self.redis = redis_client
        # `queues_client` is the pub/sub transport. Fall back to redis_client
        # only for tests that pass a single mock.
        self._notifier = ChunkNotifier(queues_client or redis_client)
        # `download_cache_client` accepted for backward compat with existing
        # call sites — completely ignored; FS is the only chunk cache now.
        del download_cache_client
        self._fs = fs_store

    # ---- key builders (used by some callers for pub/sub / diagnostics) ----

    def build_key(self, object_id: str, object_version: int, part_number: int) -> str:
        return f"obj:{object_id}:v:{int(object_version)}:part:{int(part_number)}"

    def build_chunk_key(self, object_id: str, object_version: int, part_number: int, chunk_index: int) -> str:
        return build_chunk_key(object_id, object_version, part_number, chunk_index)

    def build_meta_key(self, object_id: str, object_version: int, part_number: int) -> str:
        return f"obj:{object_id}:v:{int(object_version)}:part:{int(part_number)}:meta"

    # ---- FS store accessor ----

    @property
    def fs(self) -> FileSystemPartsStore:
        if self._fs is None:
            raise RuntimeError("fs_store is required but not configured")
        return self._fs

    # ---- chunked API (FS-backed) ----

    async def get_chunk(
        self, object_id: str, object_version: int, part_number: int, chunk_index: int
    ) -> Optional[bytes]:
        data = await self.fs.get_chunk(object_id, int(object_version), int(part_number), int(chunk_index))
        _get_metrics_collector().record_cache_operation(hit=data is not None, operation="get_chunk")
        return data

    async def set_chunk(
        self,
        object_id: str,
        object_version: int,
        part_number: int,
        chunk_index: int,
        data: bytes,
        *,
        ttl: int = DEFAULT_OBJ_PART_TTL_SECONDS,
    ) -> None:
        # ttl kept in signature for backward compat — ignored (no TTL on FS).
        del ttl
        await self.fs.set_chunk(object_id, int(object_version), int(part_number), int(chunk_index), data)

    async def chunk_exists(self, object_id: str, object_version: int, part_number: int, chunk_index: int) -> bool:
        exists = await self.fs.chunk_exists(object_id, int(object_version), int(part_number), int(chunk_index))
        _get_metrics_collector().record_cache_operation(hit=exists, operation="chunk_exists")
        return exists

    async def chunks_exist_batch(
        self, object_id: str, object_version: int, checks: list[tuple[int, int]]
    ) -> list[bool]:
        return await self.fs.chunks_exist_batch(object_id, int(object_version), checks)

    async def set_chunks(
        self,
        object_id: str,
        object_version: int,
        part_number: int,
        chunks: list[bytes],
        *,
        ttl: int = DEFAULT_OBJ_PART_TTL_SECONDS,
        start_index: int = 0,
    ) -> None:
        del ttl
        for i, data in enumerate(chunks, start=start_index):
            await self.fs.set_chunk(object_id, int(object_version), int(part_number), int(i), data)

    # ---- metadata API (FS-backed) ----

    async def set_meta(
        self,
        object_id: str,
        object_version: int,
        part_number: int,
        *,
        chunk_size: int,
        num_chunks: int,
        size_bytes: int,
        ttl: int = DEFAULT_OBJ_PART_TTL_SECONDS,
    ) -> None:
        del ttl
        await self.fs.set_meta(
            object_id,
            int(object_version),
            int(part_number),
            chunk_size=int(chunk_size),
            num_chunks=int(num_chunks),
            size_bytes=int(size_bytes),
        )

    async def get_meta(self, object_id: str, object_version: int, part_number: int) -> Optional[dict]:
        return await self.fs.get_meta(object_id, int(object_version), int(part_number))

    # ---- whole-part legacy API (assembled from FS chunks) ----

    async def get(self, object_id: str, object_version: int, part_number: int) -> Optional[bytes]:
        meta = await self.fs.get_meta(object_id, int(object_version), int(part_number))
        if not meta:
            _get_metrics_collector().record_cache_operation(hit=False, operation="get")
            return None
        num_chunks = int(meta.get("num_chunks", 0))
        if num_chunks <= 0:
            # Empty part: return empty bytes
            _get_metrics_collector().record_cache_operation(hit=True, operation="get")
            return b""
        chunks: list[bytes] = []
        for i in range(num_chunks):
            c = await self.fs.get_chunk(object_id, int(object_version), int(part_number), i)
            if c is None:
                _get_metrics_collector().record_cache_operation(hit=False, operation="get")
                return None
            chunks.append(c)
        _get_metrics_collector().record_cache_operation(hit=True, operation="get")
        return b"".join(chunks)

    async def set(
        self,
        object_id: str,
        object_version: int,
        part_number: int,
        data: bytes,
        *,
        ttl: int = DEFAULT_OBJ_PART_TTL_SECONDS,
    ) -> None:
        del ttl
        chunk_size = _get_config_value("object_chunk_size_bytes", 4 * 1024 * 1024)
        total = len(data) if isinstance(data, (bytes, bytearray)) else 0
        if total == 0:
            await self.fs.set_meta(
                object_id,
                int(object_version),
                int(part_number),
                chunk_size=chunk_size,
                num_chunks=0,
                size_bytes=0,
            )
            return
        num_chunks = (total + chunk_size - 1) // chunk_size
        for i in range(num_chunks):
            start = i * chunk_size
            end = min(start + chunk_size, total)
            await self.fs.set_chunk(object_id, int(object_version), int(part_number), i, data[start:end])
        # Write meta LAST so readers don't see partially-written parts.
        await self.fs.set_meta(
            object_id,
            int(object_version),
            int(part_number),
            chunk_size=chunk_size,
            num_chunks=num_chunks,
            size_bytes=total,
        )

    async def exists(self, object_id: str, object_version: int, part_number: int) -> bool:
        meta = await self.fs.get_meta(object_id, int(object_version), int(part_number))
        return meta is not None

    async def strlen(self, object_id: str, object_version: int, part_number: int) -> int:
        meta = await self.fs.get_meta(object_id, int(object_version), int(part_number))
        if not meta:
            return 0
        return int(meta.get("size_bytes", 0))

    async def expire(
        self,
        object_id: str,
        object_version: int,
        part_number: int,
        *,
        ttl: int = DEFAULT_OBJ_PART_TTL_SECONDS,
    ) -> None:
        """Refresh the 'recently used' marker for a part.

        Previously extended Redis TTL; now touches FS mtime/atime so the
        janitor's age-based GC treats the part as freshly accessed.
        """
        del ttl
        await self.fs.touch_part(object_id, int(object_version), int(part_number))

    # ---- pub/sub API ----

    async def wait_for_chunk(self, object_id: str, object_version: int, part_number: int, chunk_index: int) -> bytes:
        timeout = _get_config_value("cache_ttl_seconds", DEFAULT_OBJ_PART_TTL_SECONDS)
        return await self._notifier.wait_for_chunk(
            object_id,
            int(object_version),
            int(part_number),
            int(chunk_index),
            fetch_fn=self.fs.get_chunk,
            timeout=float(timeout),
        )

    async def notify_chunk(self, object_id: str, object_version: int, part_number: int, chunk_index: int) -> None:
        await self._notifier.notify(object_id, int(object_version), int(part_number), int(chunk_index))

    # ---- legacy shim: download cache write is now just a chunk write ----

    async def set_download_chunk(
        self,
        object_id: str,
        object_version: int,
        part_number: int,
        chunk_index: int,
        data: bytes,
        *,
        ttl: int = 300,
    ) -> None:
        """Alias for set_chunk — kept for backward compat with callers that
        used to distinguish short-lived download chunks from long-lived
        upload chunks. Now everything is just an FS write.
        """
        del ttl
        await self.fs.set_chunk(object_id, int(object_version), int(part_number), int(chunk_index), data)
