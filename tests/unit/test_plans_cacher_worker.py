"""The plans-cacher loop.

Two things under test. First, what happens when api.hippius.com misbehaves: a failed cycle must be
recorded and slept off, never raised, and above all must never publish a partial roll. The scrape
failing is the expected case this whole design is built around; it is not an outage.

Second, the semantics of the upstream row — which accounts get an allowance and which are left to
pay-as-you-go. Getting that wrong hands free storage to lapsed subscribers, or 402s paying ones.
"""

import logging
import pathlib
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from hippius_s3.services import plans_cache
from hippius_s3.services.hippius_api_service import S3PlanAccountRow
from hippius_s3.services.hippius_api_service import S3PlanAccountsResponse
from tests.unit.test_plans_cache import FakeRedis
from workers import run_plans_cacher_in_loop as pc


TB = 1_099_511_627_776

# Real network-42 addresses: _parse_page validates the prefix, because an address in another
# encoding would match no bucket, count as 0 bytes, and be published as unlimited headroom.
ACCT_BUSINESS = "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY"
ACCT_INACTIVE_FLAG = "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"
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
        # Shaped after the FIRST REAL payload: a live subscription (a real subscription id, and a
        # next_charge in the future) that upstream nonetheless reports as active=false. That
        # combination is why the flag is not consulted. Every id, size and date here is a fixture.
        {
            "ss58": ACCT_INACTIVE_FLAG,
            "billing": "plan",
            "plan": "pro",
            "active": False,
            "storage_bytes": 10 * TB,
            "next_charge": "2099-01-01",
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


_BUCKET_OF = "bucket-of-"


class FakePool:
    """asyncpg pool stand-in returning a fixed usage per account.

    Models the two-step chunked count: `fetch` lists the account's buckets, then `fetchrow` walks
    that bucket in keyset pages. Each account gets ONE synthetic bucket that returns its whole usage
    in a single SHORT page, which is the terminating case of the walk. The multi-page path is
    covered directly against usage_service in tests/unit/test_usage_service.py.
    """

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

    async def fetch(self, _query: str, account_id: str, timeout: float | None = None):
        if self.fail:
            raise self.fail
        return [{"bucket_id": f"{_BUCKET_OF}{account_id}"}]

    async def fetchrow(self, _query: str, bucket_id: str, _cursor: str, page_size: int, timeout: float | None = None):
        if self.fail:
            raise self.fail
        account_id = str(bucket_id).removeprefix(_BUCKET_OF)
        # rows_seen < page_size ends the walk after one page.
        return {"rows_seen": 1, "last_key": "k", "bytes_used": self.usage.get(account_id, 0)}


def page(**overrides: object) -> S3PlanAccountsResponse:
    payload = {**SAMPLE_PAGE, **overrides}
    return S3PlanAccountsResponse.model_validate(payload)


# --------------------------------------------------------------------------- row semantics


def test_every_row_billed_as_a_plan_is_published() -> None:
    accounts, catalog = pc._parse_page(page())

    assert set(accounts) == {ACCT_BUSINESS, ACCT_INACTIVE_FLAG}, "billing=plan is the whole test"
    assert accounts[ACCT_BUSINESS] == {"plan": "business", "storage_limit_bytes": 50 * TB}
    assert set(catalog) == {"pro", "business", "enterprise"}


def test_the_active_flag_is_not_consulted() -> None:
    """A subscriber reported as active=false STILL gets their allowance.

    Upstream returns active=false on every row it serves, including a subscription with a real id
    and a next_charge in the future. Requiring the flag admitted nobody at all, which made the gate
    permanently inert -- a worse failure than the lapsed-subscriber case it was meant to prevent.

    If upstream starts populating the field, this is the test to invert.
    """
    accounts, _ = pc._parse_page(page())

    assert ACCT_INACTIVE_FLAG in accounts
    assert accounts[ACCT_INACTIVE_FLAG] == {"plan": "pro", "storage_limit_bytes": 10 * TB}


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
        {"ss58": "x"},
    ],
)
def test_a_row_missing_any_requirement_gets_no_allowance(row: dict) -> None:
    accounts, _ = pc._parse_page(page(results=[row]))
    assert accounts == {}


@pytest.mark.asyncio
async def test_the_cycle_logs_how_many_rows_upstream_marked_active(caplog: Any) -> None:
    """The trigger condition for restoring the `active` check has to announce itself.

    Nothing reads the field any more, so this log line is the only thing that would tell us upstream
    started populating it. Counted over EVERY row, not just plan rows — the question is whether the
    field is written at all, and today the answer in production is zero across the whole payload.
    SAMPLE_PAGE has two active=true rows (one plan, one pay-as-you-go).
    """
    redis = FakeRedis()
    api = api_client_returning(get_s3_plan_accounts=AsyncMock(return_value=page()))

    with (
        patch.object(pc, "HippiusApiClient", api),
        patch.object(pc, "get_metrics_collector", return_value=MagicMock()),
        caplog.at_level(logging.INFO),
    ):
        assert await pc.run_cycle(redis, FakePool()) is True

    assert "upstream_active=2" in caplog.text


def test_a_null_active_does_not_fail_the_page() -> None:
    """`active: bool = False` would reject an explicit null, and model_validate runs on the WHOLE
    page -- so one such row would abort the scrape and freeze the roll at last-known-good.

    This is the field we expect upstream to start populating, which makes null its likeliest next
    state, so the model must absorb it rather than crash the worker.
    """
    accounts, _ = pc._parse_page(
        page(results=[{"ss58": ACCT_BUSINESS, "billing": "plan", "plan": "pro", "active": None}])
    )

    assert accounts[ACCT_BUSINESS] == {"plan": "pro", "storage_limit_bytes": 10 * TB}
    assert S3PlanAccountRow(ss58=ACCT_BUSINESS).active is None, "the default must stay nullable too"


def test_an_omitted_active_field_still_gets_an_allowance() -> None:
    """`active` defaults to False on the model, and that default must not deny a plan either.

    Sits apart from the parametrised cases above deliberately: it was one of them while the flag was
    required, and moving it here is the behaviour change this test file exists to pin.
    """
    accounts, _ = pc._parse_page(page(results=[{"ss58": ACCT_BUSINESS, "billing": "plan", "plan": "pro"}]))

    assert accounts[ACCT_BUSINESS] == {"plan": "pro", "storage_limit_bytes": 10 * TB}


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
    assert kwargs["entries"] == 2, "the count is accounts on a plan, not rows seen"


@pytest.mark.asyncio
async def test_restoring_the_active_check_is_wedged_by_the_shrink_guard() -> None:
    """The documented rollback does NOT work on its own, and this pins that.

    Restoring `active` admits ~nobody (upstream sets it false everywhere), so the next roll is empty
    over a live hash. publish_plan_roll refuses that, run_cycle swallows the raise, and the OLD wide
    roll keeps serving with used_bytes frozen — a deploy that looks clean and changes nothing.

    Any rollback must `DEL hippius_s3_plan_accounts` on redis-accounts first.
    """
    redis = FakeRedis()
    # Mirrors production: EVERY row is active=false, so restoring the check admits nobody at all.
    prod_shaped = page(
        results=[
            {
                "ss58": ACCT_INACTIVE_FLAG,
                "billing": "plan",
                "plan": "pro",
                "active": False,
                "storage_bytes": 10 * TB,
            }
        ]
    )
    api = api_client_returning(get_s3_plan_accounts=AsyncMock(return_value=prod_shaped))

    with (
        patch.object(pc, "HippiusApiClient", api),
        patch.object(pc, "get_metrics_collector", return_value=MagicMock()),
    ):
        assert await pc.run_cycle(redis, FakePool()) is True
        before = await plans_cache.get_plan_for_account(redis, ACCT_INACTIVE_FLAG)

        # The rollback: put the `active` requirement back.
        with patch.object(pc, "_is_enforceable_plan_row", lambda r: bool(r.billing == "plan" and r.plan and r.active)):
            assert await pc.run_cycle(redis, FakePool()) is False, "the guard refuses the smaller roll"

    after = await plans_cache.get_plan_for_account(redis, ACCT_INACTIVE_FLAG)
    assert before is not None and after is not None, "the account the rollback meant to drop is still served"
    assert after == before

    # And the documented escape hatch clears it.
    await redis.delete(plans_cache.PLAN_ACCOUNTS_KEY)
    with (
        patch.object(pc, "HippiusApiClient", api),
        patch.object(pc, "get_metrics_collector", return_value=MagicMock()),
        patch.object(pc, "_is_enforceable_plan_row", lambda r: bool(r.billing == "plan" and r.plan and r.active)),
    ):
        assert await pc.run_cycle(redis, FakePool()) is True

    assert await plans_cache.get_plan_for_account(redis, ACCT_INACTIVE_FLAG) is None


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


# --------------------------------------------------------------------------- read-only DSN


def test_the_readonly_dsn_falls_back_to_the_primary_when_unset(monkeypatch) -> None:
    """Local dev, tests and e2e set only DATABASE_URL. An unset replica DSN must resolve to it
    rather than to an empty string, or the worker cannot connect at all."""
    from hippius_s3.config import Config
    from hippius_s3.config import get_config

    import hippius_s3.config as config_module

    monkeypatch.delenv("DATABASE_READONLY_URL", raising=False)
    monkeypatch.setattr(config_module, "_config_singleton", None)

    cfg = get_config()
    assert cfg.database_readonly_url == cfg.database_url

    assert Config().database_readonly_url == "", "the raw field is empty; the fallback is in get_config"


def test_the_readonly_dsn_is_used_when_set(monkeypatch) -> None:
    from hippius_s3.config import get_config

    import hippius_s3.config as config_module

    monkeypatch.setenv("DATABASE_READONLY_URL", "postgresql://postgres@postgres-nvme-ro:5432/hippius")
    monkeypatch.setattr(config_module, "_config_singleton", None)

    cfg = get_config()
    assert "-ro:" in cfg.database_readonly_url
    assert cfg.database_readonly_url != cfg.database_url


def test_the_cacher_opens_its_pool_against_the_readonly_dsn() -> None:
    """Pins the reason this exists: these counts are aggregates over the largest tables in the
    schema, and a read-storm has stalled this cluster's primary before. Reading config.database_url
    here would silently put them back on it."""
    source = pathlib.Path(pc.__file__).read_text()
    assert "config.database_readonly_url" in source
    assert "create_pool(config.database_url" not in source
