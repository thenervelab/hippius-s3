from __future__ import annotations

from typing import Any
from typing import Protocol
from typing import runtime_checkable

from hippius_s3.cache import RedisDownloadChunksCache
from hippius_s3.cache import RedisObjectPartsCache


@runtime_checkable
class ObjectPartsCache(Protocol):
    def build_key(self, object_id: str, part_number: int) -> str: ...
    async def get(self, object_id: str, part_number: int) -> bytes | None: ...
    async def set(self, object_id: str, part_number: int, data: bytes, ttl: int | None = None) -> None: ...
    async def exists(self, object_id: str, part_number: int) -> bool: ...
    async def expire(self, object_id: str, part_number: int, seconds: int) -> None: ...
    async def strlen(self, object_id: str, part_number: int) -> int: ...


@runtime_checkable
class DownloadChunksCache(Protocol):
    def build_key(self, object_id: str, request_id: str, part_number: int) -> str: ...
    async def get(self, object_id: str, request_id: str, part_number: int) -> bytes | None: ...
    async def set(
        self,
        object_id: str,
        request_id: str,
        part_number: int,
        data: bytes,
        ttl: int | None = None,
    ) -> None: ...
    async def exists(self, object_id: str, request_id: str, part_number: int) -> bool: ...
    async def expire(self, object_id: str, request_id: str, part_number: int, seconds: int) -> None: ...


class RedisObjectPartsCacheAdapter(ObjectPartsCache):
    def __init__(self, redis_client: Any) -> None:
        self._delegate = RedisObjectPartsCache(redis_client)

    def build_key(self, object_id: str, part_number: int) -> str:
        return self._delegate.build_key(object_id, part_number)

    async def get(self, object_id: str, part_number: int) -> bytes | None:
        return await self._delegate.get(object_id, part_number)

    async def set(self, object_id: str, part_number: int, data: bytes, ttl: int | None = None) -> None:
        if ttl is not None:
            await self._delegate.set(object_id, part_number, data, ttl=ttl)
        else:
            await self._delegate.set(object_id, part_number, data)

    async def exists(self, object_id: str, part_number: int) -> bool:
        return await self._delegate.exists(object_id, part_number)

    async def expire(self, object_id: str, part_number: int, seconds: int) -> None:
        await self._delegate.expire(object_id, part_number, ttl=seconds)

    async def strlen(self, object_id: str, part_number: int) -> int:
        return await self._delegate.strlen(object_id, part_number)


class RedisDownloadChunksCacheAdapter(DownloadChunksCache):
    def __init__(self, redis_client: Any) -> None:
        self._delegate = RedisDownloadChunksCache(redis_client)

    def build_key(self, object_id: str, request_id: str, part_number: int) -> str:
        return self._delegate.build_key(object_id, request_id, part_number)

    async def get(self, object_id: str, request_id: str, part_number: int) -> bytes | None:
        return await self._delegate.get(object_id, request_id, part_number)

    async def set(
        self,
        object_id: str,
        request_id: str,
        part_number: int,
        data: bytes,
        ttl: int | None = None,
    ) -> None:
        if ttl is not None:
            await self._delegate.set(object_id, request_id, part_number, data, ttl=ttl)
        else:
            await self._delegate.set(object_id, request_id, part_number, data)

    async def exists(self, object_id: str, request_id: str, part_number: int) -> bool:
        return await self._delegate.exists(object_id, request_id, part_number)

    async def expire(self, object_id: str, request_id: str, part_number: int, seconds: int) -> None:
        await self._delegate.expire(object_id, request_id, part_number, ttl=seconds)
