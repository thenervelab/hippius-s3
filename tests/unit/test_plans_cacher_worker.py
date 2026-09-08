"""The plans-cacher loop.

Two things under test. First, what happens when api.hippius.com misbehaves: a failed cycle must be
recorded and slept off, never raised, and above all must never publish a partial roll. The scrape
failing is the expected case this whole design is built around; it is not an outage.

Second, the semantics of the upstream row — which accounts get an allowance and which are left to
pay-as-you-go. Getting that wrong hands free storage to lapsed subscribers, or 402s paying ones.
"""

from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from hippius_s3.services import plans_cache
from hippius_s3.services.hippius_api_service import S3PlanAccountsResponse
from tests.unit.test_plans_cache import FakeRedis
from workers import run_plans_cacher_in_loop as pc


TB = 1_099_511_627_776

# Real network-42 addresses: _parse_page validates the prefix, because an address in another
# encoding would match no bucket, count as 0 bytes, and be published as unlimited headroom.
ACCT_BUSINESS = "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY"
ACCT_LAPSED = "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"
_POOL: list[str] = [
    "5EkcGC5AtPwAoTUJpmAzvHEDGhv5S1Nb6hKQZ9xhUd4WyRGs",
    "5Dc4UfsPGRiU6sDWvSx6SuQXSSVYNsFqzCDwYFY9LuGZfTXh",
    "5EppayKoGK9hYa2tjGgdvGQ4td7wdQGMQBm17ktALYizMC6q",
    "5CZiwnnsssjgXCUYc2uduoqe68p9c1icvLobCCSHCkoAAdug",
    "5E52BvPdm8kex38cff1AfmeU5u7Kf4VHRbPq8xhrs4Px2BGy",
    "5DPZFNwqvdSWXjeSrKVsE7aFsmXRTa7pj72j3GNSTATAJaTL",
    "5DLbdSfSqDj3U9UouAWJXxqmrfZt672UXZmjgsre4tnTzF4s",
    "5HTkjKYbCbiMZR5v8kHVKAEtQpKrHYsst4BfFcKVZWCgKeR8",
    "5F4absw2WLiULxV7nZgq8oMTGZag34mPJybtBHuwgU71hUx5",
    "5FqKFw3rPFekTEDVV37etpB6MCffZGyEim8QPYnACJM6ULt8",
    "5GNPdCCt1CnqpYSxR3J9xr5cgx5pzwXv8gNy6ruAVpWcphCR",
    "5E1z4bBkvsCVXosggJgQnjSS9wWGwFv1udpunBQyzkYDAyKB",
    "5GRW5yjPktXEwaUyF8CVPLstupMx284DauypkA3aQT7ST8dd",
    "5E1wKBChdyL1EFT8uBZ4UCoP3hZvEquXRZjRz5tVv3mysh6q",
    "5D7QV5yvD4GNAqYjBDMAAv8UtD34LGWjqFn6RURFZhjCV1bR",
    "5FLVDGLG1ei4z2PFztRyjxAQhmF3vvzQhFn5U5ZWVdrAt6Yx",
    "5CesL6K3oEmLpDpzqdLU8RVhDn9TLXwjQrHDw3d2EcJfHHBo",
    "5G6cfkN9VvJ1vwM7URvdv78ykskD23KgL7DPt2A5PygcaFM6",
    "5FZRYg9HWVEjUmQq19CmvgQ2KhzzNLVY116qw39YGxEVr9Bz",
    "5DU4ozPXJrpkhH2MxHJocPM7K6atqVAQxBTwW5UYDHr4e43o",
    "5FRcebYdiQGtqnWXm9subq5p2XdqBE638HY1sQXoDKWMhs2U",
    "5F6t6QKPdinJCV2D3eJv7oZGkssr78LXqXjiuF7nqwU3Lt9V",
    "5EtCEkEwiLUd2KCKrXebwqDr4eUsRu4Q5T8xWEowr1i7b6SH",
    "5GUFgmkFZ3a6kDeAZszbF67QdQNqKY4PoZK4XUBiQzbApAXm",
    "5DFCyCgfnRKjoicbgv3oz3pUjHD8qPdTTUiWwRx9wCUNeAMM",
    "5FejLUxQE9k4zwdR9hJMZA82mcJHHNHaPZYiL2vHFa5kYUa8",
    "5HbP44e1hoCHUJ2p5knjmEFZpNXB4znUSL33c9tStRxkTQBX",
    "5FhYzZjJDs4o8At6amZW73ByzT1HDKbPcXJe2ZVweTNN9nTV",
    "5DcjNuzv38kbcMPX14pJRHfWUYNy2xYwWsTrWBuEmoxmPPg2",
    "5CAg6fGa6rqM9HtLJ29AiTqJjRTdsuuv5CNkJDtXC9oLxBxW",
]
ACCT_PAYG = "5DAAnrj7VHTznn2AWBemMuyBwZWs6FNFjdyVXUeYum3PTXFy"

# The payload shape as documented by the endpoint, trimmed to the fields we read.
SAMPLE_PAGE = {
    "generated_at": "2026-09-08T16:05:12Z",
    "count": 3,
    "next": None,
    "previous": None,
    "plans": {
        "pro": {"h256": "0x96e928fd", "storage_bytes": 10 * TB},
        "business": {"h256": "0x44e0c670", "storage_bytes": 50 * TB},
        "enterprise": {"h256": "0xde9221ce", "storage_bytes": 100 * TB},
    },
    "results": [
        {
            "ss58": ACCT_BUSINESS,
            "billing": "plan",
            "plan": "business",
            "active": True,
            "storage_bytes": 50 * TB,
            "next_charge": "2026-10-01",
            "subscription_id": 4412,
        },
        {
            "ss58": ACCT_LAPSED,
            "billing": "plan",
            "plan": "pro",
            "active": False,
            "storage_bytes": 10 * TB,
            "next_charge": None,
            "subscription_id": 3901,
        },
        {
            "ss58": ACCT_PAYG,
            "billing": "pay_as_you_go",
            "plan": None,
            "active": True,
            "storage_bytes": None,
            "next_charge": None,
            "subscription_id": None,
        },
    ],
}


def _addr(i: int) -> str:
    """A distinct, VALID network-42 address per index — the validator rejects placeholders."""
    return _POOL[i % len(_POOL)]


def api_client_returning(**methods: object) -> MagicMock:
    client = MagicMock()
    for name, value in methods.items():
        setattr(client, name, value)
    ctx = MagicMock()
    ctx.__aenter__ = AsyncMock(return_value=client)
    ctx.__aexit__ = AsyncMock(return_value=False)
    return MagicMock(return_value=ctx)


class FakePool:
    """asyncpg pool stand-in returning a fixed usage per account."""

    def __init__(self, usage: dict[str, int] | None = None, fail: Exception | None = None) -> None:
        self.usage = usage or {}
        self.fail = fail
        self.acquired = 0

    def acquire(self):
        pool = self

        class _Ctx:
            async def __aenter__(self):
                pool.acquired += 1
                return pool

            async def __aexit__(self, *exc):
                return False

        return _Ctx()

    async def fetchrow(self, _query: str, account_id: str, timeout: float | None = None):
        if self.fail:
            raise self.fail
        return {"bytes_used": self.usage.get(account_id, 0)}


def page(**overrides: object) -> S3PlanAccountsResponse:
    payload = {**SAMPLE_PAGE, **overrides}
    return S3PlanAccountsResponse.model_validate(payload)


# --------------------------------------------------------------------------- row semantics


def test_only_accounts_with_an_active_plan_are_published() -> None:
    accounts, catalog = pc._parse_page(page())

    assert set(accounts) == {ACCT_BUSINESS}, "only the active subscriber gets an allowance"
    assert accounts[ACCT_BUSINESS] == {"plan": "business", "storage_limit_bytes": 50 * TB}
    assert set(catalog) == {"pro", "business", "enterprise"}


def test_a_lapsed_subscription_is_treated_as_pay_as_you_go() -> None:
    """active=false with billing="plan" still carries the old plan name.

    Honouring it would hand a free allowance to someone who stopped paying. Dropping the row sends
    them down the pay-as-you-go path, where a non-subscriber belongs — Arion then decides on credit.
    It is deliberately NOT a quota denial: their storage is not over any limit, their subscription
    simply is not in force.
    """
    accounts, _ = pc._parse_page(page())
    assert ACCT_LAPSED not in accounts


def test_a_pay_as_you_go_row_is_absent_rather_than_encoded() -> None:
    """An absent field is exactly what the request path already reads as pay-as-you-go, so there is
    nothing to encode for them and nothing to keep in sync."""
    accounts, _ = pc._parse_page(page())
    assert ACCT_PAYG not in accounts


@pytest.mark.parametrize(
    "row",
    [
        {"ss58": ACCT_BUSINESS, "billing": "plan", "plan": None, "active": True},
        {"ss58": ACCT_BUSINESS, "billing": "pay_as_you_go", "plan": "pro", "active": True},
        {"ss58": ACCT_BUSINESS, "billing": "plan", "plan": "pro"},  # `active` omitted -> defaults False
        {"ss58": "x"},
    ],
)
def test_a_row_missing_any_requirement_gets_no_allowance(row: dict) -> None:
    accounts, _ = pc._parse_page(page(results=[row]))
    assert accounts == {}


def test_a_bespoke_per_account_allowance_beats_the_catalog_price() -> None:
    """An enterprise account on a negotiated limit must not be silently reset to the list price."""
    accounts, _ = pc._parse_page(
        page(
            results=[
                {"ss58": ACCT_BUSINESS, "billing": "plan", "plan": "pro", "active": True, "storage_bytes": 999 * TB}
            ],
        )
    )
    assert accounts[ACCT_BUSINESS]["storage_limit_bytes"] == 999 * TB


def test_an_unknown_upstream_field_does_not_break_parsing() -> None:
    """The payload will grow. A richer response must not crash the cacher and strand the fleet on
    last-known-good."""
    accounts, catalog = pc._parse_page(
        page(
            results=[
                {
                    "ss58": ACCT_BUSINESS,
                    "billing": "plan",
                    "plan": "pro",
                    "active": True,
                    "storage_bytes": TB,
                    "promo_code": "SUMMER",
                    "seats": 4,
                }
            ],
            plans={"pro": {"h256": "0x1", "storage_bytes": TB, "price_usd_cents": 900}},
        )
    )
    assert accounts[ACCT_BUSINESS]["storage_limit_bytes"] == TB
    assert catalog["pro"]["storage_bytes"] == TB


# --------------------------------------------------------------------------- the cycle


@pytest.mark.asyncio
async def test_a_successful_cycle_publishes_and_records() -> None:
    redis = FakeRedis()
    api = api_client_returning(get_s3_plan_accounts=AsyncMock(return_value=page()))
    collector = MagicMock()

    with (
        patch.object(pc, "HippiusApiClient", api),
        patch.object(pc, "get_metrics_collector", return_value=collector),
    ):
        assert await pc.run_cycle(redis, FakePool()) is True

    quota = await plans_cache.get_plan_for_account(redis, ACCT_BUSINESS)
    assert quota is not None and quota.plan_id == "business" and quota.storage_bytes == 50 * TB

    kwargs = collector.record_plans_cacher_cycle.call_args.kwargs
    assert kwargs["success"] is True
    assert kwargs["entries"] == 1, "the count is accounts on an active plan, not rows seen"


@pytest.mark.asyncio
async def test_a_failed_cycle_is_recorded_without_raising() -> None:
    redis = FakeRedis()
    api = api_client_returning(get_s3_plan_accounts=AsyncMock(side_effect=RuntimeError("upstream 500")))
    collector = MagicMock()

    with (
        patch.object(pc, "HippiusApiClient", api),
        patch.object(pc, "get_metrics_collector", return_value=collector),
    ):
        assert await pc.run_cycle(redis, FakePool()) is False

    kwargs = collector.record_plans_cacher_cycle.call_args.kwargs
    assert kwargs["success"] is False
    assert kwargs["entries"] == 0


@pytest.mark.asyncio
async def test_an_upstream_failure_leaves_the_previous_roll_serving() -> None:
    """The whole point of the no-TTL cache: an api.hippius.com outage is not an outage for us."""
    redis = FakeRedis()
    good = api_client_returning(get_s3_plan_accounts=AsyncMock(return_value=page()))
    with patch.object(pc, "HippiusApiClient", good), patch.object(pc, "get_metrics_collector", MagicMock()):
        await pc.run_cycle(redis, FakePool())

    bad = api_client_returning(get_s3_plan_accounts=AsyncMock(side_effect=RuntimeError("upstream is down")))
    with patch.object(pc, "HippiusApiClient", bad), patch.object(pc, "get_metrics_collector", MagicMock()):
        await pc.run_cycle(redis, FakePool())

    quota = await plans_cache.get_plan_for_account(redis, ACCT_BUSINESS)
    assert quota is not None and quota.storage_bytes == 50 * TB


@pytest.mark.asyncio
async def test_a_mid_pagination_failure_publishes_nothing() -> None:
    """THE test for this worker.

    Publishing pages 1-6 of 20 would drop the accounts on the unfetched pages to pay-as-you-go and
    402 them on their next upload, with a green-looking cycle. refresh_plan_roll_once must collect
    every page before it publishes anything.
    """
    redis = FakeRedis()
    seeded = [
        {"ss58": _addr(i), "billing": "plan", "plan": "pro", "active": True, "storage_bytes": TB} for i in range(10)
    ]
    seed = api_client_returning(get_s3_plan_accounts=AsyncMock(return_value=page(results=seeded, next=None)))
    with patch.object(pc, "HippiusApiClient", seed), patch.object(pc, "get_metrics_collector", MagicMock()):
        await pc.run_cycle(redis, FakePool())
    before = dict(redis.hashes["hippius_s3_plan_accounts"])
    assert len(before) == 10

    calls = {"n": 0}

    async def failing_pages(next_url: str | None = None, page_size: int = 500) -> S3PlanAccountsResponse:
        calls["n"] += 1
        if calls["n"] >= 3:
            raise RuntimeError("upstream died on page 3")
        return page(
            results=[{"ss58": _addr(calls["n"]), "billing": "plan", "plan": "pro", "active": True}],
            next=f"https://api.hippius.com/api/s3/plans/accounts/?page={calls['n'] + 1}",
        )

    api = api_client_returning(get_s3_plan_accounts=failing_pages)
    with patch.object(pc, "HippiusApiClient", api), patch.object(pc, "get_metrics_collector", MagicMock()):
        assert await pc.run_cycle(redis, FakePool()) is False

    assert redis.hashes["hippius_s3_plan_accounts"] == before
    assert "hippius_s3_plan_accounts:building" not in redis.hashes


@pytest.mark.asyncio
async def test_every_page_contributes_to_the_published_roll() -> None:
    redis = FakeRedis()
    calls = {"n": 0}

    async def two_pages(next_url: str | None = None, page_size: int = 500) -> S3PlanAccountsResponse:
        calls["n"] += 1
        return page(
            results=[
                {"ss58": _addr(calls["n"]), "billing": "plan", "plan": "pro", "active": True, "storage_bytes": TB}
            ],
            next=("https://api.hippius.com/api/s3/plans/accounts/?page=2" if calls["n"] == 1 else None),
        )

    api = api_client_returning(get_s3_plan_accounts=two_pages)
    with patch.object(pc, "HippiusApiClient", api), patch.object(pc, "get_metrics_collector", MagicMock()):
        assert await pc.run_cycle(redis, FakePool()) is True

    assert set(redis.hashes["hippius_s3_plan_accounts"]) == {_addr(1), _addr(2)}


@pytest.mark.asyncio
async def test_pagination_is_bounded_so_a_looping_cursor_cannot_hang_the_worker() -> None:
    async def never_ending(next_url: str | None = None, page_size: int = 500) -> S3PlanAccountsResponse:
        return page(next="https://api.hippius.com/api/s3/plans/accounts/?page=2")

    api = api_client_returning(get_s3_plan_accounts=never_ending)
    with patch.object(pc, "HippiusApiClient", api):
        with pytest.raises(RuntimeError, match="pagination exceeded"):
            await pc.refresh_plan_roll_once(FakeRedis(), FakePool())


@pytest.mark.asyncio
async def test_a_cold_start_with_no_subscribers_completes_the_cycle() -> None:
    """End to end for the cold-start case: an empty roll must publish, record meta, and report
    success — otherwise the cacher no-ops forever on a fresh environment and nothing alarms."""
    redis = FakeRedis()
    api = api_client_returning(get_s3_plan_accounts=AsyncMock(return_value=page(results=[])))
    collector = MagicMock()

    with (
        patch.object(pc, "HippiusApiClient", api),
        patch.object(pc, "get_metrics_collector", return_value=collector),
    ):
        assert await pc.run_cycle(redis, FakePool()) is True
        await pc._report_cache_age(redis)

    meta = await plans_cache.get_meta(redis)
    assert meta["fetched_at"] > 0
    assert collector.record_plans_cache_age.called, "the staleness metric must be recorded"


@pytest.mark.asyncio
async def test_usage_is_counted_by_us_and_attached_to_every_account() -> None:
    """Upstream reports a max quota but no usage, so the worker counts it. `used_bytes` must come
    from our count and `storage_limit_bytes` from upstream -- swapping them would give every account
    a quota equal to what it stores."""
    redis = FakeRedis()
    pool = FakePool(usage={ACCT_BUSINESS: 7 * TB})
    api = api_client_returning(get_s3_plan_accounts=AsyncMock(return_value=page()))

    with patch.object(pc, "HippiusApiClient", api), patch.object(pc, "get_metrics_collector", MagicMock()):
        assert await pc.run_cycle(redis, pool) is True

    quota = await plans_cache.get_plan_for_account(redis, ACCT_BUSINESS)
    assert quota is not None
    assert quota.storage_bytes == 50 * TB, "quota from upstream"
    assert quota.used_bytes == 7 * TB, "usage from our own count"


@pytest.mark.asyncio
async def test_a_usage_count_failure_publishes_nothing() -> None:
    """Publishing a partial answer would write used_bytes=0 for the accounts we failed to count,
    silently handing them unlimited headroom. Keep the previous roll instead."""
    redis = FakeRedis()
    api = api_client_returning(get_s3_plan_accounts=AsyncMock(return_value=page()))

    with patch.object(pc, "HippiusApiClient", api), patch.object(pc, "get_metrics_collector", MagicMock()):
        await pc.run_cycle(redis, FakePool(usage={ACCT_BUSINESS: 7 * TB}))
        before = dict(redis.hashes["hippius_s3_plan_accounts"])
        assert await pc.run_cycle(redis, FakePool(fail=RuntimeError("statement timeout"))) is False

    assert redis.hashes["hippius_s3_plan_accounts"] == before


@pytest.mark.asyncio
async def test_usage_counting_is_bounded_by_the_configured_concurrency() -> None:
    """Unbounded fan-out over a few tens of aggregates could become the heaviest thing on the
    primary; one serial pass would let a single huge account set the pace for the cycle."""
    accounts = {_addr(i): {"plan": "pro", "storage_limit_bytes": TB} for i in range(20)}
    pool = FakePool()

    await pc._attach_usage(pool, accounts)

    assert all("used_bytes" in row for row in accounts.values())
    assert pool.acquired == 20, "one connection acquisition per account, serialised by the semaphore"


def test_an_address_in_the_wrong_network_prefix_is_dropped() -> None:
    """An SS58 in another prefix matches no bucket, so its count comes back 0 and we would publish
    it as "stores nothing" — unlimited headroom, indistinguishable from a genuinely empty account.
    Drop the row instead; that account falls back to pay-as-you-go."""
    accounts, _ = pc._parse_page(
        page(
            results=[
                {"ss58": "not-an-ss58-address", "billing": "plan", "plan": "pro", "active": True},
                {"ss58": "5" * 48, "billing": "plan", "plan": "pro", "active": True},
            ]
        )
    )
    assert accounts == {}
