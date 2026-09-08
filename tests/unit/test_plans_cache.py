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

    async def hset(self, key: str, mapping: dict[str, str]) -> None:
        self.hashes.setdefault(key, {}).update(mapping)

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


@pytest.fixture
def redis() -> FakeRedis:
    return FakeRedis()


def acct(plan: str, limit: int | None = 1_000, used: int = 0) -> dict:
    return {"plan": plan, "storage_limit_bytes": limit, "used_bytes": used}


async def publish_accounts(redis: "FakeRedis", accounts: dict) -> int:
    published, _ = await plans_cache.publish_plan_roll(redis, accounts, {})
    return published


@pytest.mark.asyncio
async def test_publish_plan_roll_swaps_atomically(redis: FakeRedis) -> None:
    await publish_accounts(redis, {"acct-a": acct("pro"), "acct-b": acct("business")})

    assert set(redis.hashes[PLAN_ACCOUNTS_KEY]) == {"acct-a", "acct-b"}
    # The scratch key must not survive the swap.
    assert PLAN_ACCOUNTS_KEY + ":building" not in redis.hashes


@pytest.mark.asyncio
async def test_an_account_that_left_a_plan_disappears_on_the_next_publish(redis: FakeRedis) -> None:
    """The reason this is a whole-hash swap and not per-key SETEX.

    With SETEX-per-account, a downgraded account keeps its old allowance until the TTL expires.
    """
    await publish_accounts(redis, {"stays": acct("pro"), "leaves": acct("business")})
    await publish_accounts(redis, {"stays": acct("pro"), "filler": acct("pro")})

    stays = await plans_cache.get_plan_for_account(redis, "stays")
    assert stays is not None and stays.plan_id == "pro"
    assert await plans_cache.get_plan_for_account(redis, "leaves") is None


@pytest.mark.asyncio
async def test_an_empty_upstream_response_never_wipes_the_live_map(redis: FakeRedis) -> None:
    await publish_accounts(redis, {"acct-a": acct("pro")})

    with pytest.raises(PlanMapShrankTooMuch):
        await publish_accounts(redis, {})

    a = await plans_cache.get_plan_for_account(redis, "acct-a")
    assert a is not None and a.plan_id == "pro"


@pytest.mark.asyncio
async def test_a_map_that_shrank_too_much_is_refused(redis: FakeRedis) -> None:
    """One bad upstream deploy returning a truncated-but-valid list must not demote the fleet."""
    await publish_accounts(redis, {f"acct-{i}": acct("pro") for i in range(100)})

    with pytest.raises(PlanMapShrankTooMuch):
        await publish_accounts(redis, {f"acct-{i}": acct("pro") for i in range(40)})

    assert len(redis.hashes[PLAN_ACCOUNTS_KEY]) == 100


@pytest.mark.asyncio
async def test_a_map_that_shrank_within_tolerance_is_published(redis: FakeRedis) -> None:
    await publish_accounts(redis, {f"acct-{i}": acct("pro") for i in range(100)})
    await publish_accounts(redis, {f"acct-{i}": acct("pro") for i in range(80)})

    assert len(redis.hashes[PLAN_ACCOUNTS_KEY]) == 80


@pytest.mark.asyncio
async def test_a_failed_scrape_leaves_the_live_map_untouched(redis: FakeRedis) -> None:
    """The highest-value invariant: publish is all-or-nothing.

    refresh_plan_roll_once collects every page before calling publish, so a mid-pagination
    failure never reaches this layer at all -- but if it ever did, the live hash must survive.
    """
    await publish_accounts(redis, {"acct-a": acct("pro"), "acct-b": acct("business")})
    before = dict(redis.hashes[PLAN_ACCOUNTS_KEY])

    original_rename = redis.rename

    async def exploding_rename(src: str, dst: str) -> None:
        raise RuntimeError("redis died mid-publish")

    redis.rename = exploding_rename  # type: ignore[method-assign]
    with pytest.raises(RuntimeError):
        await publish_accounts(redis, {"acct-c": acct("pro"), "acct-d": acct("pro")})
    redis.rename = original_rename  # type: ignore[method-assign]

    assert redis.hashes[PLAN_ACCOUNTS_KEY] == before


@pytest.mark.asyncio
async def test_the_catalog_prices_a_row_with_no_allowance_of_its_own(redis: FakeRedis) -> None:
    await plans_cache.publish_plan_roll(
        redis, {"acct-a": acct("pro", limit=100_000_000_000, used=7)}, {"pro": {"storage_bytes": 100_000_000_000}}
    )

    quota = await plans_cache.get_plan_for_account(redis, "acct-a")
    assert quota == PlanQuota(plan_id="pro", storage_bytes=100_000_000_000, used_bytes=7)
    assert quota.enforceable


@pytest.mark.asyncio
async def test_an_empty_roll_is_refused(redis: FakeRedis) -> None:
    await publish_accounts(redis, {"a": acct("pro")})
    with pytest.raises(PlanMapShrankTooMuch):
        await publish_accounts(redis, {})


@pytest.mark.asyncio
async def test_an_unknown_account_returns_no_quota(redis: FakeRedis) -> None:
    await publish_accounts(redis, {"acct-a": acct("pro")})
    assert await plans_cache.get_plan_for_account(redis, "acct-nope") is None


@pytest.mark.asyncio
async def test_a_cold_cache_returns_no_quota(redis: FakeRedis) -> None:
    assert await plans_cache.get_plan_for_account(redis, "acct-a") is None


@pytest.mark.parametrize("storage_bytes", [None, 0, -1])
@pytest.mark.asyncio
async def test_a_missing_or_nonpositive_allowance_is_not_enforceable(
    redis: FakeRedis, storage_bytes: int | None
) -> None:
    """0 means "we do not know this plan's limit", never "this plan permits nothing".

    Reading a malformed payload as a zero quota would deny every upload for a paying customer.
    """
    await publish_accounts(redis, {"acct-a": acct("pro", limit=storage_bytes)})

    quota = await plans_cache.get_plan_for_account(redis, "acct-a")
    assert quota is not None
    assert not quota.enforceable


@pytest.mark.asyncio
async def test_an_account_with_no_plan_is_pay_as_you_go(redis: FakeRedis) -> None:
    await publish_accounts(redis, {"acct-a": acct("pro")})
    assert await plans_cache.get_plan_for_account(redis, "acct-unknown") is None


@pytest.mark.asyncio
async def test_meta_records_counts(redis: FakeRedis) -> None:
    await plans_cache.touch_meta(redis, accounts=1284, plans=3)

    meta = await plans_cache.get_meta(redis)
    assert meta["accounts"] == 1284
    assert meta["plans"] == 3
    assert meta["fetched_at"] > 0


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
    await plans_cache.publish_plan_roll(
        redis, {"a": acct("pro", limit=5, used=2)}, {"pro": {"h256": "0x1", "storage_bytes": 5}}
    )
    assert json.loads(redis.hashes[PLAN_CATALOG_KEY]["pro"]) == {"h256": "0x1", "storage_bytes": 5}
    assert json.loads(redis.hashes[PLAN_ACCOUNTS_KEY]["a"]) == {
        "plan": "pro",
        "storage_limit_bytes": 5,
        "used_bytes": 2,
    }


@pytest.mark.asyncio
async def test_an_empty_roll_on_a_cold_cache_publishes_cleanly(redis: FakeRedis) -> None:
    """A fresh deploy with no subscribers yet must not wedge the cacher.

    RENAME on a never-written scratch key raises "no such key". Because accounts publish before the
    catalog, that aborted the whole cycle — silently, since run_cycle swallows it, and permanently,
    since touch_meta never ran so the staleness alarm had no timestamp to fire on. It is also the
    default e2e state.
    """
    accounts, plans = await plans_cache.publish_plan_roll(redis, {}, {"pro": {"storage_bytes": 1}})

    assert accounts == 0
    assert plans == 1
    assert await plans_cache.get_plan_for_account(redis, "anyone") is None


@pytest.mark.asyncio
async def test_an_empty_roll_still_records_meta_so_staleness_can_alarm(redis: FakeRedis) -> None:
    await plans_cache.publish_plan_roll(redis, {}, {})
    await plans_cache.touch_meta(redis, 0, 0)

    meta = await plans_cache.get_meta(redis)
    assert meta["fetched_at"] > 0, "without this the age metric is never recorded"
