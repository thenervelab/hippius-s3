from __future__ import annotations

import json as _json
from typing import Any
from typing import Optional
from typing import Protocol

from hippius_s3.config import get_config
from hippius_s3.monitoring import get_metrics_collector


DEFAULT_OBJ_PART_TTL_SECONDS = get_config().cache_ttl_seconds


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
    def __init__(self, redis_client: Any) -> None:
        self.redis = redis_client

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
                get_metrics_collector().record_cache_operation(hit=False, operation="get")
                return None
            meta = _json.loads(meta_raw)
            num_chunks = int(meta.get("num_chunks", 0))
            if num_chunks <= 0:
                get_metrics_collector().record_cache_operation(hit=False, operation="get")
                return None
            chunks: list[bytes] = []
            for i in range(num_chunks):
                c = await self.redis.get(self.build_chunk_key(object_id, object_version, part_number, i))
                if not isinstance(c, bytes):
                    get_metrics_collector().record_cache_operation(hit=False, operation="get")
                    return None
                chunks.append(c)
            get_metrics_collector().record_cache_operation(hit=True, operation="get")
            return b"".join(chunks)
        except Exception:
            get_metrics_collector().record_cache_operation(hit=False, operation="get")
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
        try:
            chunk_size = int(get_config().object_chunk_size_bytes)
        except Exception:
            chunk_size = 4 * 1024 * 1024
        total = len(data) if isinstance(data, (bytes, bytearray)) else 0
        if total == 0:
            # Still write empty meta with zero chunks for consistency
            await self.set_meta(
                object_id, object_version, part_number, chunk_size=chunk_size, num_chunks=0, size_bytes=0, ttl=ttl
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
        # Then write chunk data
        for i in range(num_chunks):
            start = i * chunk_size
            end = min(start + chunk_size, total)
            await self.set_chunk(object_id, object_version, part_number, i, data[start:end], ttl=ttl)

    async def exists(self, object_id: str, object_version: int, part_number: int) -> bool:
        return bool(await self.redis.exists(self.build_meta_key(object_id, object_version, part_number)))

    async def strlen(self, object_id: str, object_version: int, part_number: int) -> int:
        try:
            meta_raw = await self.redis.get(self.build_meta_key(object_id, object_version, part_number))
            if not meta_raw:
                return 0
            meta = _json.loads(meta_raw)
            return int(meta.get("size_bytes", 0))
        except Exception:
            return 0

    async def expire(
        self, object_id: str, object_version: int, part_number: int, *, ttl: int = DEFAULT_OBJ_PART_TTL_SECONDS
    ) -> None:
        """Set TTL on meta and all chunk keys for this part to prevent orphaned chunk bytes."""
        await self.redis.expire(self.build_meta_key(object_id, object_version, part_number), ttl)
        # Also expire all chunk keys for this part (scan pattern to catch all indices)
        pattern = f"obj:{object_id}:v:{int(object_version)}:part:{int(part_number)}:chunk:*"
        cursor = 0
        while True:
            cursor, keys = await self.redis.scan(cursor, match=pattern, count=100)
            if keys:
                # Expire all found chunk keys
                for key in keys:
                    await self.redis.expire(key, ttl)
            if cursor == 0:
                break

    # Note: base part policy is 1-based; callers should use get/set directly with part_number=1

    # Chunked API
    async def get_chunk(
        self, object_id: str, object_version: int, part_number: int, chunk_index: int
    ) -> Optional[bytes]:
        result = await self.redis.get(self.build_chunk_key(object_id, object_version, part_number, chunk_index))
        is_hit = isinstance(result, bytes)
        get_metrics_collector().record_cache_operation(
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
        await self.redis.setex(self.build_chunk_key(object_id, object_version, part_number, chunk_index), ttl, data)

    async def chunk_exists(self, object_id: str, object_version: int, part_number: int, chunk_index: int) -> bool:
        exists = bool(
            await self.redis.exists(self.build_chunk_key(object_id, object_version, part_number, chunk_index))
        )
        get_metrics_collector().record_cache_operation(
            hit=exists,
            operation="chunk_exists",
        )
        return exists

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
        await self.redis.setex(self.build_meta_key(object_id, object_version, part_number), ttl, _json.dumps(payload))

    async def get_meta(self, object_id: str, object_version: int, part_number: int) -> Optional[dict]:
        raw = await self.redis.get(self.build_meta_key(object_id, object_version, part_number))
        if not raw:
            return None
        try:
            return dict(_json.loads(raw))  # type: ignore[arg-type]
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
    def build_key(self, object_id: str, object_version: int, part_number: int) -> str:  # type: ignore[override]
        return f"obj:{object_id}:v:{int(object_version)}:part:{int(part_number)}"

    async def get(self, object_id: str, object_version: int, part_number: int) -> Optional[bytes]:  # type: ignore[override]
        return None

    async def set(
        self,
        object_id: str,
        object_version: int,
        part_number: int,
        data: bytes,
        *,
        ttl: int = DEFAULT_OBJ_PART_TTL_SECONDS,
    ) -> None:  # type: ignore[override]
        return None

    async def exists(self, object_id: str, object_version: int, part_number: int) -> bool:  # type: ignore[override]
        return False

    async def strlen(self, object_id: str, object_version: int, part_number: int) -> int:  # type: ignore[override]
        return 0

    async def expire(
        self, object_id: str, object_version: int, part_number: int, *, ttl: int = DEFAULT_OBJ_PART_TTL_SECONDS
    ) -> None:  # type: ignore[override]
        return None

    # Base helpers removed; use get/set directly
