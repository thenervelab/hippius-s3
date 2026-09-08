"""Redis layer for the billing-plan caches.

The tests that matter here are the ones about what we REFUSE to publish. A plans-cacher that
publishes a partial or empty account map silently demotes every missing plan customer to
pay-as-you-go and 402s them on their next upload, with a successful-looking cycle in the logs.
"""

import json

import pytest

from hippius_s3.services import plans_cache
from hippius_s3.services.plans_cache import PLAN_ACCOUNTS_KEY
from hippius_s3.services.plans_cache import PLAN_CATALOG_KEY
from hippius_s3.services.plans_cache import PlanMapShrankTooMuch
from hippius_s3.services.plans_cache import PlanQuota


class FakeRedis:
    """Minimal hash/string Redis with a real RENAME, enough to prove the atomic-swap semantics."""

    def __init__(self) -> None:
        self.hashes: dict[str, dict[str, str]] = {}
        self.strings: dict[str, str] = {}

    async def hlen(self, key: str) -> int:
        return len(self.hashes.get(key, {}))

    async def hget(self, key: str, field: str) -> bytes | None:
        value = self.hashes.get(key, {}).get(field)
        return value.encode() if value is not None else None

    async def hset(self, key: str, field: str, value: str) -> None:
        self.hashes.setdefault(key, {})[field] = value

    async def delete(self, key: str) -> None:
        self.hashes.pop(key, None)
        self.strings.pop(key, None)

    async def rename(self, src: str, dst: str) -> None:
        if src not in self.hashes:
            raise KeyError(src)
        self.hashes[dst] = self.hashes.pop(src)

    async def get(self, key: str) -> bytes | None:
        value = self.strings.get(key)
        return value.encode() if value is not None else None

    async def set(self, key: str, value: str) -> None:
        self.strings[key] = value

    def pipeline(self) -> "FakeRedis":
        # Commands apply immediately; execute() is a no-op. Ordering within a publish is
        # irrelevant because nothing reads the :building key.
        return self

    async def execute(self) -> None:
        return None


@pytest.fixture
def redis() -> FakeRedis:
    return FakeRedis()


@pytest.mark.asyncio
async def test_publish_account_plans_swaps_atomically(redis: FakeRedis) -> None:
    await plans_cache.publish_account_plans(redis, {"acct-a": "plan-1", "acct-b": "plan-2"})

    assert redis.hashes[PLAN_ACCOUNTS_KEY] == {"acct-a": "plan-1", "acct-b": "plan-2"}
    # The scratch key must not survive the swap.
    assert PLAN_ACCOUNTS_KEY + ":building" not in redis.hashes


@pytest.mark.asyncio
async def test_an_account_that_left_a_plan_disappears_on_the_next_publish(redis: FakeRedis) -> None:
    """The reason this is a whole-hash swap and not per-key SETEX.

    With SETEX-per-account, a downgraded account keeps its old allowance until the TTL expires.
    """
    await plans_cache.publish_account_plans(redis, {"stays": "plan-1", "leaves": "plan-2"})
    await plans_cache.publish_account_plans(redis, {"stays": "plan-1", "filler": "plan-1"})

    assert await plans_cache.get_plan_id_for_account(redis, "stays") == "plan-1"
    assert await plans_cache.get_plan_id_for_account(redis, "leaves") is None


@pytest.mark.asyncio
async def test_an_empty_upstream_response_never_wipes_the_live_map(redis: FakeRedis) -> None:
    await plans_cache.publish_account_plans(redis, {"acct-a": "plan-1"})

    with pytest.raises(PlanMapShrankTooMuch):
        await plans_cache.publish_account_plans(redis, {})

    assert await plans_cache.get_plan_id_for_account(redis, "acct-a") == "plan-1"


@pytest.mark.asyncio
async def test_a_map_that_shrank_too_much_is_refused(redis: FakeRedis) -> None:
    """One bad upstream deploy returning a truncated-but-valid list must not demote the fleet."""
    await plans_cache.publish_account_plans(redis, {f"acct-{i}": "plan-1" for i in range(100)})

    with pytest.raises(PlanMapShrankTooMuch):
        await plans_cache.publish_account_plans(redis, {f"acct-{i}": "plan-1" for i in range(40)})

    assert len(redis.hashes[PLAN_ACCOUNTS_KEY]) == 100


@pytest.mark.asyncio
async def test_a_map_that_shrank_within_tolerance_is_published(redis: FakeRedis) -> None:
    await plans_cache.publish_account_plans(redis, {f"acct-{i}": "plan-1" for i in range(100)})
    await plans_cache.publish_account_plans(redis, {f"acct-{i}": "plan-1" for i in range(80)})

    assert len(redis.hashes[PLAN_ACCOUNTS_KEY]) == 80


@pytest.mark.asyncio
async def test_a_failed_scrape_leaves_the_live_map_untouched(redis: FakeRedis) -> None:
    """The highest-value invariant: publish is all-or-nothing.

    refresh_account_plans_once collects every page before calling publish, so a mid-pagination
    failure never reaches this layer at all -- but if it ever did, the live hash must survive.
    """
    await plans_cache.publish_account_plans(redis, {"acct-a": "plan-1", "acct-b": "plan-2"})
    before = dict(redis.hashes[PLAN_ACCOUNTS_KEY])

    original_rename = redis.rename

    async def exploding_rename(src: str, dst: str) -> None:
        raise RuntimeError("redis died mid-publish")

    redis.rename = exploding_rename  # type: ignore[method-assign]
    with pytest.raises(RuntimeError):
        await plans_cache.publish_account_plans(redis, {"acct-c": "plan-3", "acct-d": "plan-3"})
    redis.rename = original_rename  # type: ignore[method-assign]

    assert redis.hashes[PLAN_ACCOUNTS_KEY] == before


@pytest.mark.asyncio
async def test_publish_plan_catalog_round_trips(redis: FakeRedis) -> None:
    await plans_cache.publish_plan_catalog(redis, {"plan-1": {"name": "Starter", "storage_bytes": 100_000_000_000}})

    quota = await plans_cache.get_plan_quota(redis, "plan-1")
    assert quota == PlanQuota(plan_id="plan-1", storage_bytes=100_000_000_000, name="Starter")
    assert quota.enforceable


@pytest.mark.asyncio
async def test_an_empty_catalog_is_refused(redis: FakeRedis) -> None:
    with pytest.raises(PlanMapShrankTooMuch):
        await plans_cache.publish_plan_catalog(redis, {})


@pytest.mark.asyncio
async def test_unknown_plan_id_returns_no_quota(redis: FakeRedis) -> None:
    await plans_cache.publish_plan_catalog(redis, {"plan-1": {"storage_bytes": 1}})
    assert await plans_cache.get_plan_quota(redis, "plan-nope") is None


@pytest.mark.asyncio
async def test_a_cold_catalog_returns_no_quota(redis: FakeRedis) -> None:
    assert await plans_cache.get_plan_quota(redis, "plan-1") is None


@pytest.mark.parametrize("storage_bytes", [None, 0, -1])
@pytest.mark.asyncio
async def test_a_missing_or_nonpositive_allowance_is_not_enforceable(
    redis: FakeRedis, storage_bytes: int | None
) -> None:
    """0 means "we do not know this plan's limit", never "this plan permits nothing".

    Reading a malformed payload as a zero quota would deny every upload for a paying customer.
    """
    await plans_cache.publish_plan_catalog(redis, {"plan-1": {"storage_bytes": storage_bytes}})

    quota = await plans_cache.get_plan_quota(redis, "plan-1")
    assert quota is not None
    assert not quota.enforceable


@pytest.mark.asyncio
async def test_an_account_with_no_plan_is_pay_as_you_go(redis: FakeRedis) -> None:
    await plans_cache.publish_account_plans(redis, {"acct-a": "plan-1"})
    assert await plans_cache.get_plan_id_for_account(redis, "acct-unknown") is None


@pytest.mark.asyncio
async def test_meta_records_counts_for_both_loops(redis: FakeRedis) -> None:
    await plans_cache.touch_meta(redis, "plans_fetched_at", 3)
    await plans_cache.touch_meta(redis, "accounts_fetched_at", 40_000)

    meta = await plans_cache.get_meta(redis)
    assert meta["plans_fetched_at_count"] == 3
    assert meta["accounts_fetched_at_count"] == 40_000
    assert meta["plans_fetched_at"] > 0
    assert meta["accounts_fetched_at"] > 0


@pytest.mark.asyncio
async def test_the_caches_are_written_without_a_ttl(redis: FakeRedis) -> None:
    """No expire/setex call exists in this module by design.

    A TTL would delete the last-known-good map during an api.hippius.com outage -- the exact
    failure the cache exists to survive.
    """
    source = (plans_cache.__file__ or "").replace(".pyc", ".py")
    with open(source) as fp:
        body = fp.read()

    assert "setex" not in body
    assert "expire(" not in body


@pytest.mark.asyncio
async def test_plan_quota_json_shape_is_what_the_worker_writes(redis: FakeRedis) -> None:
    await plans_cache.publish_plan_catalog(redis, {"p": {"name": "N", "storage_bytes": 5}})
    raw = redis.hashes[PLAN_CATALOG_KEY]["p"]
    assert json.loads(raw) == {"name": "N", "storage_bytes": 5}
