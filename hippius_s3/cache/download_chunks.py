from __future__ import annotations

from typing import Any
from typing import Optional
from typing import Protocol


DEFAULT_DL_TTL_SECONDS = 300


class DownloadChunksCache(Protocol):
    def build_key(self, object_id: str, request_id: str, part_number: int) -> str: ...

    async def get(self, object_id: str, request_id: str, part_number: int) -> Optional[bytes]: ...
    async def set(
        self, object_id: str, request_id: str, part_number: int, data: bytes, *, ttl: int = DEFAULT_DL_TTL_SECONDS
    ) -> None: ...
    async def exists(self, object_id: str, request_id: str, part_number: int) -> bool: ...
    async def expire(
        self, object_id: str, request_id: str, part_number: int, *, ttl: int = DEFAULT_DL_TTL_SECONDS
    ) -> None: ...


class RedisDownloadChunksCache:
    def __init__(self, redis_client: Any) -> None:
        self.redis = redis_client

    def build_key(self, object_id: str, request_id: str, part_number: int) -> str:
        return f"dl:{object_id}:{request_id}:{int(part_number)}"

    async def get(self, object_id: str, request_id: str, part_number: int) -> Optional[bytes]:
        result = await self.redis.get(self.build_key(object_id, request_id, part_number))
        return result if isinstance(result, bytes) else None

    async def set(
        self, object_id: str, request_id: str, part_number: int, data: bytes, *, ttl: int = DEFAULT_DL_TTL_SECONDS
    ) -> None:
        await self.redis.set(self.build_key(object_id, request_id, part_number), data, ex=ttl)

    async def exists(self, object_id: str, request_id: str, part_number: int) -> bool:
        return bool(await self.redis.exists(self.build_key(object_id, request_id, part_number)))

    async def expire(
        self, object_id: str, request_id: str, part_number: int, *, ttl: int = DEFAULT_DL_TTL_SECONDS
    ) -> None:
        await self.redis.expire(self.build_key(object_id, request_id, part_number), ttl)


class NullDownloadChunksCache:
    def build_key(self, object_id: str, request_id: str, part_number: int) -> str:
        return f"dl:{object_id}:{request_id}:{int(part_number)}"

    async def get(self, object_id: str, request_id: str, part_number: int) -> Optional[bytes]:
        return None

    async def set(
        self, object_id: str, request_id: str, part_number: int, data: bytes, *, ttl: int = DEFAULT_DL_TTL_SECONDS
    ) -> None:
        return None

    async def exists(self, object_id: str, request_id: str, part_number: int) -> bool:
        return False

    async def expire(
        self, object_id: str, request_id: str, part_number: int, *, ttl: int = DEFAULT_DL_TTL_SECONDS
    ) -> None:
        return None
