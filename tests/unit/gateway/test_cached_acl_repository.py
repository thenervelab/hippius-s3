import json
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from redis.exceptions import RedisError

from gateway.repositories.cached_acl_repository import CachedACLRepository
from hippius_s3.models.acl import ACL
from hippius_s3.models.acl import Grant
from hippius_s3.models.acl import Grantee
from hippius_s3.models.acl import GranteeType
from hippius_s3.models.acl import Owner
from hippius_s3.models.acl import Permission


@pytest.fixture
def mock_base_repo() -> Any:
    repo = MagicMock()
    repo.get_bucket_acl = AsyncMock()
    repo.get_object_acl = AsyncMock()
    repo.set_bucket_acl = AsyncMock()
    repo.set_object_acl = AsyncMock()
    repo.delete_bucket_acl = AsyncMock()
    repo.delete_object_acl = AsyncMock()
    return repo


@pytest.fixture
def mock_redis() -> Any:
    redis = MagicMock()
    redis.get = AsyncMock()
    redis.setex = AsyncMock()
    redis.delete = AsyncMock()
    redis.scan_iter = MagicMock()
    return redis


@pytest.fixture
def cached_repo(mock_base_repo: Any, mock_redis: Any) -> CachedACLRepository:
    return CachedACLRepository(mock_base_repo, mock_redis, ttl_seconds=600)


@pytest.fixture
def sample_acl() -> ACL:
    owner = Owner(id="account123", display_name="Test User")
    grants = [
        Grant(
            grantee=Grantee(type=GranteeType.CANONICAL_USER, id="account123"),
            permission=Permission.FULL_CONTROL,
        )
    ]
    return ACL(owner=owner, grants=grants)


class TestCacheKeys:
    def test_bucket_acl_key_format(self, cached_repo: CachedACLRepository) -> None:
        key = cached_repo._bucket_acl_key("my-bucket")
        assert key == "hippius_acl:bucket:my-bucket"

    def test_object_acl_key_format(self, cached_repo: CachedACLRepository) -> None:
        key = cached_repo._object_acl_key("my-bucket", "path/to/object.txt")
        assert key == "hippius_acl:object:my-bucket:path/to/object.txt"


class TestGetBucketACL:
    @pytest.mark.asyncio
    async def test_cache_hit_returns_cached_acl(
        self, cached_repo: CachedACLRepository, mock_redis: Any, mock_base_repo: Any, sample_acl: ACL
    ) -> None:
        acl_json = json.dumps(sample_acl.to_dict())
        mock_redis.get.return_value = acl_json

        result = await cached_repo.get_bucket_acl("my-bucket")

        assert result is not None
        assert result.owner.id == "account123"
        mock_redis.get.assert_called_once_with("hippius_acl:bucket:my-bucket")
        mock_base_repo.get_bucket_acl.assert_not_called()

    @pytest.mark.asyncio
    async def test_cache_miss_fetches_from_db_and_caches(
        self, cached_repo: CachedACLRepository, mock_redis: Any, mock_base_repo: Any, sample_acl: ACL
    ) -> None:
        mock_redis.get.return_value = None
        mock_base_repo.get_bucket_acl.return_value = sample_acl

        result = await cached_repo.get_bucket_acl("my-bucket")

        assert result is not None
        assert result.owner.id == "account123"
        mock_redis.get.assert_called_once()
        mock_base_repo.get_bucket_acl.assert_called_once_with("my-bucket")
        mock_redis.setex.assert_called_once()
        args = mock_redis.setex.call_args[0]
        assert args[0] == "hippius_acl:bucket:my-bucket"
        assert args[1] == 600

    @pytest.mark.asyncio
    async def test_cache_miss_none_does_not_cache(
        self, cached_repo: CachedACLRepository, mock_redis: Any, mock_base_repo: Any
    ) -> None:
        mock_redis.get.return_value = None
        mock_base_repo.get_bucket_acl.return_value = None

        result = await cached_repo.get_bucket_acl("my-bucket")

        assert result is None
        mock_redis.setex.assert_not_called()


class TestGetObjectACL:
    @pytest.mark.asyncio
    async def test_cache_hit_returns_cached_acl(
        self, cached_repo: CachedACLRepository, mock_redis: Any, mock_base_repo: Any, sample_acl: ACL
    ) -> None:
        acl_json = json.dumps(sample_acl.to_dict())
        mock_redis.get.return_value = acl_json

        result = await cached_repo.get_object_acl("my-bucket", "my-object")

        assert result is not None
        assert result.owner.id == "account123"
        mock_redis.get.assert_called_once_with("hippius_acl:object:my-bucket:my-object")
        mock_base_repo.get_object_acl.assert_not_called()

    @pytest.mark.asyncio
    async def test_cache_miss_fetches_from_db_and_caches(
        self, cached_repo: CachedACLRepository, mock_redis: Any, mock_base_repo: Any, sample_acl: ACL
    ) -> None:
        mock_redis.get.return_value = None
        mock_base_repo.get_object_acl.return_value = sample_acl

        result = await cached_repo.get_object_acl("my-bucket", "my-object")

        assert result is not None
        assert result.owner.id == "account123"
        mock_redis.get.assert_called_once()
        mock_base_repo.get_object_acl.assert_called_once_with("my-bucket", "my-object")
        mock_redis.setex.assert_called_once()
        args = mock_redis.setex.call_args[0]
        assert args[0] == "hippius_acl:object:my-bucket:my-object"
        assert args[1] == 600


class TestSetBucketACL:
    @pytest.mark.asyncio
    async def test_set_bucket_acl_writes_and_invalidates(
        self, cached_repo: CachedACLRepository, mock_redis: Any, mock_base_repo: Any, sample_acl: ACL
    ) -> None:
        await cached_repo.set_bucket_acl("my-bucket", "owner123", sample_acl)

        mock_base_repo.set_bucket_acl.assert_called_once_with("my-bucket", "owner123", sample_acl)
        mock_redis.delete.assert_called_once_with("hippius_acl:bucket:my-bucket")


class TestSetObjectACL:
    @pytest.mark.asyncio
    async def test_set_object_acl_writes_and_invalidates(
        self, cached_repo: CachedACLRepository, mock_redis: Any, mock_base_repo: Any, sample_acl: ACL
    ) -> None:
        await cached_repo.set_object_acl("my-bucket", "my-object", "owner123", sample_acl)

        mock_base_repo.set_object_acl.assert_called_once_with("my-bucket", "my-object", "owner123", sample_acl)
        mock_redis.delete.assert_called_once_with("hippius_acl:object:my-bucket:my-object")


class TestDeleteACL:
    @pytest.mark.asyncio
    async def test_delete_bucket_acl_deletes_and_invalidates(
        self, cached_repo: CachedACLRepository, mock_redis: Any, mock_base_repo: Any
    ) -> None:
        await cached_repo.delete_bucket_acl("my-bucket")

        mock_base_repo.delete_bucket_acl.assert_called_once_with("my-bucket")
        mock_redis.delete.assert_called_once_with("hippius_acl:bucket:my-bucket")

    @pytest.mark.asyncio
    async def test_delete_object_acl_deletes_and_invalidates(
        self, cached_repo: CachedACLRepository, mock_redis: Any, mock_base_repo: Any
    ) -> None:
        await cached_repo.delete_object_acl("my-bucket", "my-object")

        mock_base_repo.delete_object_acl.assert_called_once_with("my-bucket", "my-object")
        mock_redis.delete.assert_called_once_with("hippius_acl:object:my-bucket:my-object")


class TestInvalidation:
    @pytest.mark.asyncio
    async def test_invalidate_bucket_acl_deletes_key(self, cached_repo: CachedACLRepository, mock_redis: Any) -> None:
        mock_redis.delete.return_value = 1

        await cached_repo.invalidate_bucket_acl("my-bucket")

        mock_redis.delete.assert_called_once_with("hippius_acl:bucket:my-bucket")

    @pytest.mark.asyncio
    async def test_invalidate_object_acl_deletes_key(self, cached_repo: CachedACLRepository, mock_redis: Any) -> None:
        mock_redis.delete.return_value = 1

        await cached_repo.invalidate_object_acl("my-bucket", "my-object")

        mock_redis.delete.assert_called_once_with("hippius_acl:object:my-bucket:my-object")

    @pytest.mark.asyncio
    async def test_invalidate_all_bucket_objects_scans_and_deletes(
        self, cached_repo: CachedACLRepository, mock_redis: Any
    ) -> None:
        class AsyncIterator:
            def __init__(self, items: list[str]):
                self.items = items
                self.index = 0

            def __aiter__(self) -> "AsyncIterator":
                return self

            async def __anext__(self) -> str:
                if self.index >= len(self.items):
                    raise StopAsyncIteration
                item = self.items[self.index]
                self.index += 1
                return item

        mock_redis.scan_iter.return_value = AsyncIterator(
            ["hippius_acl:object:my-bucket:obj1", "hippius_acl:object:my-bucket:obj2"]
        )
        mock_redis.delete.return_value = 2

        await cached_repo.invalidate_all_bucket_objects("my-bucket")

        mock_redis.scan_iter.assert_called_once()
        mock_redis.delete.assert_called_once()
        args = mock_redis.delete.call_args[0]
        assert len(args) == 2


class TestRedisOutageFallsBackToDB:
    """A6: a redis-acl outage must not 500 permission checks — the cache falls through to the DB
    (the ACL source of truth). Reads fall back; writes/invalidations are best-effort."""

    @pytest.mark.asyncio
    async def test_bucket_read_falls_back_to_db_on_redis_error(
        self, cached_repo: CachedACLRepository, mock_redis: Any, mock_base_repo: Any, sample_acl: ACL
    ) -> None:
        mock_redis.get.side_effect = RedisError("redis-acl down")
        mock_base_repo.get_bucket_acl.return_value = sample_acl

        result = await cached_repo.get_bucket_acl("my-bucket")

        assert result is sample_acl  # authoritative DB grants returned, no 500
        mock_base_repo.get_bucket_acl.assert_awaited_once_with("my-bucket")

    @pytest.mark.asyncio
    async def test_object_read_falls_back_to_db_on_redis_error(
        self, cached_repo: CachedACLRepository, mock_redis: Any, mock_base_repo: Any, sample_acl: ACL
    ) -> None:
        mock_redis.get.side_effect = RedisError("redis-acl down")
        mock_base_repo.get_object_acl.return_value = sample_acl

        result = await cached_repo.get_object_acl("my-bucket", "k")

        assert result is sample_acl
        mock_base_repo.get_object_acl.assert_awaited_once_with("my-bucket", "k")

    @pytest.mark.asyncio
    async def test_write_failure_does_not_fail_the_lookup(
        self, cached_repo: CachedACLRepository, mock_redis: Any, mock_base_repo: Any, sample_acl: ACL
    ) -> None:
        mock_redis.get.return_value = None  # cache miss → DB read → cache write (which explodes)
        mock_redis.setex.side_effect = RedisError("redis-acl down")
        mock_base_repo.get_bucket_acl.return_value = sample_acl

        result = await cached_repo.get_bucket_acl("my-bucket")

        assert result is sample_acl  # the SETEX failure is swallowed; the DB value is returned

    @pytest.mark.asyncio
    async def test_invalidation_does_not_raise_on_redis_error(
        self, cached_repo: CachedACLRepository, mock_redis: Any
    ) -> None:
        mock_redis.delete.side_effect = RedisError("redis-acl down")
        # must not raise — the DB write already committed upstream
        await cached_repo.invalidate_bucket_acl("my-bucket")
        await cached_repo.invalidate_object_acl("my-bucket", "k")

    @pytest.mark.asyncio
    async def test_db_failure_during_outage_propagates_never_fails_open(
        self, cached_repo: CachedACLRepository, mock_redis: Any, mock_base_repo: Any
    ) -> None:
        # SECURITY guard: only RedisError is caught. If redis is down AND the authoritative DB read
        # ALSO fails, the DB exception must PROPAGATE (→ 500 = deny), never be swallowed into a
        # permissive default. This test pins the boundary so a future widened `except` can't fail open.
        mock_redis.get.side_effect = RedisError("redis-acl down")
        mock_base_repo.get_bucket_acl.side_effect = RuntimeError("acl DB unavailable")

        with pytest.raises(RuntimeError, match="acl DB unavailable"):
            await cached_repo.get_bucket_acl("my-bucket")

        mock_base_repo.get_object_acl.side_effect = RuntimeError("acl DB unavailable")
        with pytest.raises(RuntimeError, match="acl DB unavailable"):
            await cached_repo.get_object_acl("my-bucket", "k")
