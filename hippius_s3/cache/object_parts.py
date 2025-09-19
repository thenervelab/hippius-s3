from __future__ import annotations

from typing import Any
from typing import Optional
from typing import Protocol


DEFAULT_OBJ_PART_TTL_SECONDS = 1800


class ObjectPartsCache(Protocol):
    def build_key(self, object_id: str, part_number: int) -> str: ...

    async def get(self, object_id: str, part_number: int) -> Optional[bytes]: ...
    async def set(
        self, object_id: str, part_number: int, data: bytes, *, ttl: int = DEFAULT_OBJ_PART_TTL_SECONDS
    ) -> None: ...
    async def exists(self, object_id: str, part_number: int) -> bool: ...
    async def strlen(self, object_id: str, part_number: int) -> int: ...
    async def expire(self, object_id: str, part_number: int, *, ttl: int = DEFAULT_OBJ_PART_TTL_SECONDS) -> None: ...

    async def read_base_for_append(self, object_id: str) -> Optional[bytes]: ...
    async def write_base_for_append(
        self, object_id: str, data: bytes, *, ttl: int = DEFAULT_OBJ_PART_TTL_SECONDS
    ) -> None: ...


class RedisObjectPartsCache:
    def __init__(self, redis_client: Any) -> None:
        self.redis = redis_client

    def build_key(self, object_id: str, part_number: int) -> str:
        return f"obj:{object_id}:part:{int(part_number)}"

    async def get(self, object_id: str, part_number: int) -> Optional[bytes]:
        result = await self.redis.get(self.build_key(object_id, part_number))
        return result if isinstance(result, bytes) else None

    async def set(
        self, object_id: str, part_number: int, data: bytes, *, ttl: int = DEFAULT_OBJ_PART_TTL_SECONDS
    ) -> None:
        await self.redis.setex(self.build_key(object_id, part_number), ttl, data)

    async def exists(self, object_id: str, part_number: int) -> bool:
        return bool(await self.redis.exists(self.build_key(object_id, part_number)))

    async def strlen(self, object_id: str, part_number: int) -> int:
        return int(await self.redis.strlen(self.build_key(object_id, part_number)))

    async def expire(self, object_id: str, part_number: int, *, ttl: int = DEFAULT_OBJ_PART_TTL_SECONDS) -> None:
        await self.redis.expire(self.build_key(object_id, part_number), ttl)

    async def read_base_for_append(self, object_id: str) -> Optional[bytes]:
        # Prefer part 0, then 1. Do not scan.
        data = await self.get(object_id, 0)
        if data:
            return data
        return await self.get(object_id, 1)

    async def write_base_for_append(
        self, object_id: str, data: bytes, *, ttl: int = DEFAULT_OBJ_PART_TTL_SECONDS
    ) -> None:
        # Write to part 1 for compatibility with current append flow
        await self.set(object_id, 1, data, ttl=ttl)


class NullObjectPartsCache:
    def build_key(self, object_id: str, part_number: int) -> str:  # type: ignore[override]
        return f"obj:{object_id}:part:{int(part_number)}"

    async def get(self, object_id: str, part_number: int) -> Optional[bytes]:  # type: ignore[override]
        return None

    async def set(
        self, object_id: str, part_number: int, data: bytes, *, ttl: int = DEFAULT_OBJ_PART_TTL_SECONDS
    ) -> None:  # type: ignore[override]
        return None

    async def exists(self, object_id: str, part_number: int) -> bool:  # type: ignore[override]
        return False

    async def strlen(self, object_id: str, part_number: int) -> int:  # type: ignore[override]
        return 0

    async def expire(self, object_id: str, part_number: int, *, ttl: int = DEFAULT_OBJ_PART_TTL_SECONDS) -> None:  # type: ignore[override]
        return None

    async def read_base_for_append(self, object_id: str) -> Optional[bytes]:  # type: ignore[override]
        return None

    async def write_base_for_append(
        self, object_id: str, data: bytes, *, ttl: int = DEFAULT_OBJ_PART_TTL_SECONDS
    ) -> None:  # type: ignore[override]
        return None
