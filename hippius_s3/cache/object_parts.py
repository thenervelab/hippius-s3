from __future__ import annotations

import asyncio
import json as _json
from contextlib import asynccontextmanager
from typing import Any
from typing import AsyncIterator
from typing import Optional
from typing import Protocol


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


# Lazy-config: avoid importing application config at module import time.
# Use a conservative default TTL and resolve real config only when needed inside methods.
DEFAULT_OBJ_PART_TTL_SECONDS = 1800


def _get_config_value(name: str, default: int) -> int:
    try:
        # Local import to avoid triggering httpx and other deps at module import time
        from hippius_s3.config import get_config

        cfg = get_config()
        if name == "object_chunk_size_bytes":
            return cfg.object_chunk_size_bytes
        if name == "cache_ttl_seconds":
            return cfg.cache_ttl_seconds
        # Unknown config keys must not be accessed dynamically.
        return default
    except Exception:
        return default


class ObjectPartsCache(Protocol):
    def build_key(self, object_id: str, object_version: int, part_number: int) -> str: ...

    # Back-compat whole-part API
    async def get(self, object_id: str, object_version: int, part_number: int) -> Optional[bytes]: ...

    async def set(
        self,
        object_id: str,
        object_version: int,
        part_number: int,
        data: bytes,
        *,
        ttl: int = DEFAULT_OBJ_PART_TTL_SECONDS,
    ) -> None: ...

    async def exists(self, object_id: str, object_version: int, part_number: int) -> bool: ...

    async def strlen(self, object_id: str, object_version: int, part_number: int) -> int: ...

    async def expire(
        self, object_id: str, object_version: int, part_number: int, *, ttl: int = DEFAULT_OBJ_PART_TTL_SECONDS
    ) -> None: ...

    # New chunked API
    def build_chunk_key(self, object_id: str, object_version: int, part_number: int, chunk_index: int) -> str: ...

    async def get_chunk(
        self, object_id: str, object_version: int, part_number: int, chunk_index: int
    ) -> Optional[bytes]: ...

    async def set_chunk(
        self,
        object_id: str,
        object_version: int,
        part_number: int,
        chunk_index: int,
        data: bytes,
        *,
        ttl: int = DEFAULT_OBJ_PART_TTL_SECONDS,
    ) -> None: ...

    async def chunk_exists(self, object_id: str, object_version: int, part_number: int, chunk_index: int) -> bool: ...

    async def chunks_exist_batch(
        self, object_id: str, object_version: int, checks: list[tuple[int, int]]
    ) -> list[bool]: ...

    async def set_chunks(
        self,
        object_id: str,
        object_version: int,
        part_number: int,
        chunks: list[bytes],
        *,
        ttl: int = DEFAULT_OBJ_PART_TTL_SECONDS,
    ) -> None: ...

    # Pub/sub notification API
    async def wait_for_chunk(
        self, object_id: str, object_version: int, part_number: int, chunk_index: int
    ) -> bytes: ...

    async def notify_chunk(self, object_id: str, object_version: int, part_number: int, chunk_index: int) -> None: ...

    # Metadata API
    def build_meta_key(self, object_id: str, object_version: int, part_number: int) -> str: ...

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
    ) -> None: ...

    async def get_meta(self, object_id: str, object_version: int, part_number: int) -> Optional[dict]: ...


class RedisObjectPartsCache:
    def __init__(self, redis_client: Any, queues_client: Any = None) -> None:
        self.redis = redis_client
        self._queues_client = queues_client or redis_client

    def build_key(self, object_id: str, object_version: int, part_number: int) -> str:
        return f"obj:{object_id}:v:{int(object_version)}:part:{int(part_number)}"

    def build_chunk_key(self, object_id: str, object_version: int, part_number: int, chunk_index: int) -> str:
        return f"obj:{object_id}:v:{int(object_version)}:part:{int(part_number)}:chunk:{int(chunk_index)}"

    def build_meta_key(self, object_id: str, object_version: int, part_number: int) -> str:
        return f"obj:{object_id}:v:{int(object_version)}:part:{int(part_number)}:meta"

    async def get(self, object_id: str, object_version: int, part_number: int) -> Optional[bytes]:
        # Assemble from chunked entries using meta
        try:
            meta_raw = await self.redis.get(self.build_meta_key(object_id, object_version, part_number))
            if not meta_raw:
                _get_metrics_collector().record_cache_operation(hit=False, operation="get")
                return None
            meta = _json.loads(meta_raw)
            num_chunks = int(meta.get("num_chunks", 0))
            if num_chunks <= 0:
                _get_metrics_collector().record_cache_operation(hit=False, operation="get")
                return None
            chunks: list[bytes] = []
            for i in range(num_chunks):
                c = await self.redis.get(self.build_chunk_key(object_id, object_version, part_number, i))
                if not isinstance(c, bytes):
                    _get_metrics_collector().record_cache_operation(hit=False, operation="get")
                    return None
                chunks.append(c)
            _get_metrics_collector().record_cache_operation(hit=True, operation="get")
            return b"".join(chunks)
        except Exception:
            _get_metrics_collector().record_cache_operation(hit=False, operation="get")
            return None

    async def set(
        self,
        object_id: str,
        object_version: int,
        part_number: int,
        data: bytes,
        *,
        ttl: int = DEFAULT_OBJ_PART_TTL_SECONDS,
    ) -> None:
        # Split into fixed-size chunks and store meta first (for cheap readiness checks), then chunk keys
        chunk_size = _get_config_value("object_chunk_size_bytes", 4 * 1024 * 1024)
        total = len(data) if isinstance(data, (bytes, bytearray)) else 0
        if total == 0:
            # Still write empty meta with zero chunks for consistency
            await self.set_meta(
                object_id,
                object_version,
                part_number,
                chunk_size=chunk_size,
                num_chunks=0,
                size_bytes=0,
                ttl=ttl,
            )
            return
        num_chunks = (total + chunk_size - 1) // chunk_size
        # Write meta first to signal readiness before chunks are fully written
        await self.set_meta(
            object_id,
            object_version,
            part_number,
            chunk_size=chunk_size,
            num_chunks=num_chunks,
            size_bytes=total,
            ttl=ttl,
        )
        # Then write chunk data — pipeline all setex calls into a single round-trip
        ttl_val = int(ttl if ttl is not None else _get_config_value("cache_ttl_seconds", DEFAULT_OBJ_PART_TTL_SECONDS))
        async with self.redis.pipeline(transaction=False) as pipe:
            for i in range(num_chunks):
                start = i * chunk_size
                end = min(start + chunk_size, total)
                key = self.build_chunk_key(object_id, object_version, part_number, i)
                pipe.setex(key, ttl_val, data[start:end])
            await pipe.execute()

    async def exists(
        self,
        object_id: str,
        object_version: int,
        part_number: int,
    ) -> bool:
        return bool(
            await self.redis.exists(
                self.build_meta_key(object_id, object_version, part_number),
            ),
        )

    async def strlen(
        self,
        object_id: str,
        object_version: int,
        part_number: int,
    ) -> int:
        try:
            meta_raw = await self.redis.get(
                self.build_meta_key(
                    object_id,
                    object_version,
                    part_number,
                )
            )
            if not meta_raw:
                return 0
            meta = _json.loads(meta_raw)
            return int(meta.get("size_bytes", 0))
        except Exception:
            return 0

    async def expire(
        self,
        object_id: str,
        object_version: int,
        part_number: int,
        *,
        ttl: int = DEFAULT_OBJ_PART_TTL_SECONDS,
    ) -> None:
        """Set TTL on meta and all chunk keys for this part to prevent orphaned chunk bytes."""
        ttl_val = int(ttl if ttl is not None else _get_config_value("cache_ttl_seconds", DEFAULT_OBJ_PART_TTL_SECONDS))
        # Build deterministic key list from meta instead of scanning the keyspace
        keys: list[Any] = [self.build_meta_key(object_id, object_version, part_number)]
        meta = await self.get_meta(object_id, object_version, part_number)
        if meta:
            num_chunks = int(meta.get("num_chunks", 0))
            keys.extend(self.build_chunk_key(object_id, object_version, part_number, i) for i in range(num_chunks))
        async with self.redis.pipeline(transaction=False) as pipe:
            for key in keys:
                pipe.expire(key, ttl_val)
            await pipe.execute()

    # Note: base part policy is 1-based; callers should use get/set directly with part_number=1

    # Chunked API
    async def get_chunk(
        self, object_id: str, object_version: int, part_number: int, chunk_index: int
    ) -> Optional[bytes]:
        result = await self.redis.get(
            self.build_chunk_key(
                object_id,
                object_version,
                part_number,
                chunk_index,
            )
        )
        is_hit = isinstance(result, bytes)
        _get_metrics_collector().record_cache_operation(
            hit=is_hit,
            operation="get_chunk",
        )
        return result if is_hit else None

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
        await self.redis.setex(
            self.build_chunk_key(
                object_id,
                object_version,
                part_number,
                chunk_index,
            ),
            int(
                ttl if ttl is not None else _get_config_value("cache_ttl_seconds", DEFAULT_OBJ_PART_TTL_SECONDS),
            ),
            data,
        )

    async def chunk_exists(
        self,
        object_id: str,
        object_version: int,
        part_number: int,
        chunk_index: int,
    ) -> bool:
        exists = bool(
            await self.redis.exists(
                self.build_chunk_key(object_id, object_version, part_number, chunk_index),
            )
        )
        _get_metrics_collector().record_cache_operation(
            hit=exists,
            operation="chunk_exists",
        )
        return exists

    async def set_chunks(
        self,
        object_id: str,
        object_version: int,
        part_number: int,
        chunks: list[bytes],
        *,
        ttl: int = DEFAULT_OBJ_PART_TTL_SECONDS,
    ) -> None:
        ttl_val = int(ttl if ttl is not None else _get_config_value("cache_ttl_seconds", DEFAULT_OBJ_PART_TTL_SECONDS))
        async with self.redis.pipeline(transaction=False) as pipe:
            for i, data in enumerate(chunks):
                key = self.build_chunk_key(object_id, int(object_version), int(part_number), int(i))
                pipe.setex(key, ttl_val, data)
            await pipe.execute()

    @asynccontextmanager
    async def _subscribe(self, channel: str) -> AsyncIterator[Any]:
        pubsub = self._queues_client.pubsub()
        await pubsub.subscribe(channel)
        try:
            yield pubsub
        finally:
            await pubsub.unsubscribe(channel)
            await pubsub.aclose()

    async def chunks_exist_batch(
        self,
        object_id: str,
        object_version: int,
        checks: list[tuple[int, int]],
    ) -> list[bool]:
        """Check existence of multiple chunks in a single Redis pipeline round trip."""
        if not checks:
            return []
        keys = [self.build_chunk_key(object_id, int(object_version), int(pn), int(ci)) for pn, ci in checks]
        async with self.redis.pipeline(transaction=False) as pipe:
            for key in keys:
                pipe.exists(key)
            results = await pipe.execute()
        return [bool(r) for r in results]

    async def wait_for_chunk(self, object_id: str, object_version: int, part_number: int, chunk_index: int) -> bytes:
        timeout = _get_config_value("cache_ttl_seconds", DEFAULT_OBJ_PART_TTL_SECONDS)

        # Fast path: chunk already cached — skip pubsub subscribe entirely
        c = await self.get_chunk(object_id, int(object_version), int(part_number), int(chunk_index))
        if c is not None:
            return c

        # Slow path: subscribe and wait for worker to populate cache
        chunk_key = self.build_chunk_key(
            object_id,
            int(object_version),
            int(part_number),
            int(chunk_index),
        )
        channel = f"notify:{chunk_key}"

        async with self._subscribe(channel) as pubsub:
            # Re-check after subscribe (race safety: worker may have finished between our check and subscribe)
            c = await self.get_chunk(
                object_id,
                int(object_version),
                int(part_number),
                int(chunk_index),
            )
            if c is not None:
                return c

            # Wait for notification with timeout to avoid hanging forever if the worker crashes
            async def _listen() -> None:
                async for msg in pubsub.listen():
                    if msg["type"] == "message":
                        return

            await asyncio.wait_for(_listen(), timeout=timeout)

        # Fetch the chunk — verify it wasn't evicted between notification and read
        data = await self.get_chunk(
            object_id,
            int(object_version),
            int(part_number),
            int(chunk_index),
        )
        if data is None:
            raise RuntimeError(f"Chunk evicted after pub/sub notification: {chunk_key}")
        return data

    async def notify_chunk(
        self,
        object_id: str,
        object_version: int,
        part_number: int,
        chunk_index: int,
    ) -> None:
        chunk_key = self.build_chunk_key(
            object_id,
            int(object_version),
            int(part_number),
            int(chunk_index),
        )
        await self._queues_client.publish(f"notify:{chunk_key}", "1")

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
        payload = {"chunk_size": int(chunk_size), "num_chunks": int(num_chunks), "size_bytes": int(size_bytes)}
        await self.redis.setex(
            self.build_meta_key(object_id, object_version, part_number),
            int(ttl if ttl is not None else _get_config_value("cache_ttl_seconds", DEFAULT_OBJ_PART_TTL_SECONDS)),
            _json.dumps(payload),
        )

    async def get_meta(self, object_id: str, object_version: int, part_number: int) -> Optional[dict]:
        raw = await self.redis.get(self.build_meta_key(object_id, object_version, part_number))
        if not raw:
            return None
        try:
            return dict(_json.loads(raw))
        except Exception:
            return None


class RedisUploadPartsCache:
    """Cache for in-flight multipart uploads keyed by upload_id."""

    def __init__(self, redis_client: Any) -> None:
        self.redis = redis_client

    def build_key(self, upload_id: str, part_number: int) -> str:
        return f"obj:{upload_id}:part:{int(part_number)}"

    async def get(self, upload_id: str, part_number: int) -> Optional[bytes]:
        result = await self.redis.get(self.build_key(upload_id, part_number))
        return result if isinstance(result, bytes) else None

    async def set(
        self, upload_id: str, part_number: int, data: bytes, *, ttl: int = DEFAULT_OBJ_PART_TTL_SECONDS
    ) -> None:
        await self.redis.setex(self.build_key(upload_id, part_number), ttl, data)

    async def delete(self, upload_id: str, part_number: int) -> None:
        await self.redis.delete(self.build_key(upload_id, part_number))


class NullObjectPartsCache:
    def build_key(self, object_id: str, object_version: int, part_number: int) -> str:
        return f"obj:{object_id}:v:{int(object_version)}:part:{int(part_number)}"

    async def get(self, object_id: str, object_version: int, part_number: int) -> Optional[bytes]:
        return None

    async def set(
        self,
        object_id: str,
        object_version: int,
        part_number: int,
        data: bytes,
        *,
        ttl: int = DEFAULT_OBJ_PART_TTL_SECONDS,
    ) -> None:
        return None

    async def exists(self, object_id: str, object_version: int, part_number: int) -> bool:
        return False

    async def strlen(self, object_id: str, object_version: int, part_number: int) -> int:
        return 0

    async def expire(
        self, object_id: str, object_version: int, part_number: int, *, ttl: int = DEFAULT_OBJ_PART_TTL_SECONDS
    ) -> None:
        return None

    async def set_chunks(
        self,
        object_id: str,
        object_version: int,
        part_number: int,
        chunks: list[bytes],
        *,
        ttl: int = DEFAULT_OBJ_PART_TTL_SECONDS,
    ) -> None:
        return None

    async def chunks_exist_batch(
        self, object_id: str, object_version: int, checks: list[tuple[int, int]]
    ) -> list[bool]:
        return [False] * len(checks)

    async def wait_for_chunk(self, object_id: str, object_version: int, part_number: int, chunk_index: int) -> bytes:
        raise NotImplementedError("NullObjectPartsCache does not support wait_for_chunk")

    async def notify_chunk(self, object_id: str, object_version: int, part_number: int, chunk_index: int) -> None:
        return None

    # Base helpers removed; use get/set directly
