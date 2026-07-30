"""Fix 3: ACLService.get_bucket_owner_and_id is Redis-cached (hits only)."""

from typing import Any

import pytest
from redis.exceptions import ConnectionError as RedisConnectionError

from gateway.services.acl_service import ACLService
from gateway.services.acl_service import BucketLookup


class FakePool:
    def __init__(self, row: Any) -> None:
        self.row = row
        self.fetchrow_calls = 0

    async def fetchrow(self, _query: str, *_args: Any) -> Any:
        self.fetchrow_calls += 1
        return self.row


class FakeRedis:
    def __init__(self) -> None:
        self.store: dict[str, Any] = {}
        self.setex_calls = 0

    async def get(self, key: str) -> Any:
        return self.store.get(key)

    async def setex(self, key: str, _ttl: int, value: Any) -> None:
        self.setex_calls += 1
        self.store[key] = value

    async def delete(self, key: str) -> int:
        return 1 if self.store.pop(key, None) is not None else 0


ROW = {"main_account_id": "owner-1", "bucket_id": "b-1", "is_cache_warm": True}


@pytest.mark.asyncio
async def test_lookup_cached_after_first_query() -> None:
    pool = FakePool(ROW)
    redis = FakeRedis()
    svc = ACLService(pool, redis_client=redis, cache_ttl=600)

    first = await svc.get_bucket_owner_and_id("bkt")
    assert first == BucketLookup(owner_id="owner-1", bucket_id="b-1", is_cache_warm=True)
    assert pool.fetchrow_calls == 1
    assert redis.setex_calls == 1

    second = await svc.get_bucket_owner_and_id("bkt")
    assert second == first
    assert pool.fetchrow_calls == 1, "second lookup must be served from Redis, not the DB"


@pytest.mark.asyncio
async def test_missing_bucket_not_negatively_cached() -> None:
    pool = FakePool(None)
    redis = FakeRedis()
    svc = ACLService(pool, redis_client=redis, cache_ttl=600)

    assert await svc.get_bucket_owner_and_id("nope") is None
    assert redis.setex_calls == 0, "a non-existent bucket must not be cached"
    # No negative cache → a freshly-created bucket is visible on the next request.
    assert await svc.get_bucket_owner_and_id("nope") is None
    assert pool.fetchrow_calls == 2


@pytest.mark.asyncio
async def test_no_redis_uses_db_directly() -> None:
    pool = FakePool(ROW)
    svc = ACLService(pool, redis_client=None)

    result = await svc.get_bucket_owner_and_id("bkt")
    assert result is not None
    assert result.bucket_id == "b-1"
    assert pool.fetchrow_calls == 1


class ExplodingRedis:
    """Simulates a redis-acl outage: every op raises a RedisError."""

    async def get(self, _key: str) -> Any:
        raise RedisConnectionError("redis down")

    async def setex(self, _key: str, _ttl: int, _value: Any) -> None:
        raise RedisConnectionError("redis down")

    async def delete(self, _key: str) -> int:
        raise RedisConnectionError("redis down")


@pytest.mark.asyncio
async def test_redis_outage_falls_back_to_db() -> None:
    """The cache is a pure optimization: a Redis outage must transparently fall back to the DB so
    bucket resolution (and master-token traffic) keeps working."""
    pool = FakePool(ROW)
    svc = ACLService(pool, redis_client=ExplodingRedis(), cache_ttl=600)

    result = await svc.get_bucket_owner_and_id("bkt")
    assert result is not None
    assert result.bucket_id == "b-1"
    assert pool.fetchrow_calls == 1


@pytest.mark.asyncio
async def test_invalidate_bucket_meta_forces_refetch() -> None:
    pool = FakePool(ROW)
    redis = FakeRedis()
    svc = ACLService(pool, redis_client=redis, cache_ttl=600)

    await svc.get_bucket_owner_and_id("bkt")
    assert pool.fetchrow_calls == 1

    await svc.invalidate_bucket_meta("bkt")
    assert redis.store == {}

    await svc.get_bucket_owner_and_id("bkt")
    assert pool.fetchrow_calls == 2, "invalidated entry must be re-read from the DB"


@pytest.mark.asyncio
async def test_invalidate_bucket_meta_survives_redis_outage() -> None:
    """DeleteBucket has already committed upstream by the time we purge; a redis-acl blip must not
    raise out of the invalidation path and turn a 204 into a 500."""
    svc = ACLService(FakePool(ROW), redis_client=ExplodingRedis(), cache_ttl=600)

    await svc.invalidate_bucket_meta("bkt")


@pytest.mark.asyncio
async def test_invalidate_bucket_meta_without_redis_is_noop() -> None:
    svc = ACLService(FakePool(ROW), redis_client=None)

    await svc.invalidate_bucket_meta("bkt")


@pytest.mark.asyncio
async def test_delete_then_recreate_by_other_account_resolves_new_owner() -> None:
    """Bucket names are globally unique only over live rows (buckets_bucket_name_active_key), so the
    instant a bucket is soft-deleted ANY account can claim the name. owner_id/bucket_id are therefore
    immutable only within one bucket's lifetime — not per name. Without a purge on DeleteBucket the
    stale entry keeps resolving the OLD owner for the whole TTL, which hands the previous owner the
    master-token ownership bypass (acl.py) and the "private" canned-ACL owner match on the NEW
    account's bucket, while locking the rightful owner out with 403s."""
    pool = FakePool(ROW)
    redis = FakeRedis()
    svc = ACLService(pool, redis_client=redis, cache_ttl=600)

    before = await svc.get_bucket_owner_and_id("bkt")
    assert before is not None
    assert before.owner_id == "owner-1"

    # DeleteBucket → the gateway purges the bucket-meta entry for that name.
    await svc.invalidate_bucket_meta("bkt")

    # A different account creates a bucket with the same name; it gets a fresh bucket_id.
    pool.row = {"main_account_id": "owner-2", "bucket_id": "b-2", "is_cache_warm": False}

    after = await svc.get_bucket_owner_and_id("bkt")
    assert after == BucketLookup(owner_id="owner-2", bucket_id="b-2", is_cache_warm=False)
