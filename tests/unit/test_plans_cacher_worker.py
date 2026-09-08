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
            "ss58": "5E71kYuD",
            "billing": "plan",
            "plan": "business",
            "active": True,
            "storage_bytes": 50 * TB,
            "next_charge": "2026-10-01",
            "subscription_id": 4412,
        },
        {
            "ss58": "5FHneW46",
            "billing": "plan",
            "plan": "pro",
            "active": False,
            "storage_bytes": 10 * TB,
            "next_charge": None,
            "subscription_id": 3901,
        },
        {
            "ss58": "5DAAnrj7",
            "billing": "pay_as_you_go",
            "plan": None,
            "active": True,
            "storage_bytes": None,
            "next_charge": None,
            "subscription_id": None,
        },
    ],
}


def api_client_returning(**methods: object) -> MagicMock:
    client = MagicMock()
    for name, value in methods.items():
        setattr(client, name, value)
    ctx = MagicMock()
    ctx.__aenter__ = AsyncMock(return_value=client)
    ctx.__aexit__ = AsyncMock(return_value=False)
    return MagicMock(return_value=ctx)


def page(**overrides: object) -> S3PlanAccountsResponse:
    payload = {**SAMPLE_PAGE, **overrides}
    return S3PlanAccountsResponse.model_validate(payload)


# --------------------------------------------------------------------------- row semantics


def test_only_accounts_with_an_active_plan_are_published() -> None:
    accounts, catalog = pc._parse_page(page())

    assert set(accounts) == {"5E71kYuD"}, "only the active subscriber gets an allowance"
    assert accounts["5E71kYuD"] == {"plan": "business", "storage_limit_bytes": 50 * TB, "used_bytes": 50 * TB}
    assert set(catalog) == {"pro", "business", "enterprise"}


def test_a_lapsed_subscription_is_treated_as_pay_as_you_go() -> None:
    """active=false with billing="plan" still carries the old plan name.

    Honouring it would hand a free allowance to someone who stopped paying. Dropping the row sends
    them down the pay-as-you-go path, where a non-subscriber belongs — Arion then decides on credit.
    It is deliberately NOT a quota denial: their storage is not over any limit, their subscription
    simply is not in force.
    """
    accounts, _ = pc._parse_page(page())
    assert "5FHneW46" not in accounts


def test_a_pay_as_you_go_row_is_absent_rather_than_encoded() -> None:
    """An absent field is exactly what the request path already reads as pay-as-you-go, so there is
    nothing to encode for them and nothing to keep in sync."""
    accounts, _ = pc._parse_page(page())
    assert "5DAAnrj7" not in accounts


@pytest.mark.parametrize(
    "row",
    [
        {"ss58": "x", "billing": "plan", "plan": None, "active": True},
        {"ss58": "x", "billing": "pay_as_you_go", "plan": "pro", "active": True},
        {"ss58": "x", "billing": "plan", "plan": "pro"},  # `active` omitted -> defaults False
        {"ss58": "x"},
    ],
)
def test_a_row_missing_any_requirement_gets_no_allowance(row: dict) -> None:
    accounts, _ = pc._parse_page(page(results=[row]))
    assert accounts == {}


def test_the_two_storage_bytes_fields_are_never_confused() -> None:
    """`plans.<name>.storage_bytes` is the ALLOWANCE; `results[].storage_bytes` is that account's
    CURRENT USAGE, computed on chain. Reading usage as the allowance would give every plan customer
    a limit equal to what they already store and deny their very next upload."""
    accounts, _ = pc._parse_page(
        page(
            results=[{"ss58": "vip", "billing": "plan", "plan": "pro", "active": True, "storage_bytes": 3 * TB}],
        )
    )
    assert accounts["vip"]["used_bytes"] == 3 * TB, "the account row is usage"
    assert accounts["vip"]["storage_limit_bytes"] == 10 * TB, "the allowance comes from the catalog"


def test_an_unknown_upstream_field_does_not_break_parsing() -> None:
    """The payload will grow. A richer response must not crash the cacher and strand the fleet on
    last-known-good."""
    accounts, catalog = pc._parse_page(
        page(
            results=[
                {
                    "ss58": "a",
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
    assert accounts["a"]["used_bytes"] == TB
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
        assert await pc.run_cycle(redis) is True

    quota = await plans_cache.get_plan_for_account(redis, "5E71kYuD")
    assert quota is not None
    assert quota.plan_id == "business"
    assert quota.storage_bytes == 50 * TB, "allowance from the catalog"
    assert quota.used_bytes == 50 * TB, "usage from the account row"

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
        assert await pc.run_cycle(redis) is False

    kwargs = collector.record_plans_cacher_cycle.call_args.kwargs
    assert kwargs["success"] is False
    assert kwargs["entries"] == 0


@pytest.mark.asyncio
async def test_an_upstream_failure_leaves_the_previous_roll_serving() -> None:
    """The whole point of the no-TTL cache: an api.hippius.com outage is not an outage for us."""
    redis = FakeRedis()
    good = api_client_returning(get_s3_plan_accounts=AsyncMock(return_value=page()))
    with patch.object(pc, "HippiusApiClient", good), patch.object(pc, "get_metrics_collector", MagicMock()):
        await pc.run_cycle(redis)

    bad = api_client_returning(get_s3_plan_accounts=AsyncMock(side_effect=RuntimeError("upstream is down")))
    with patch.object(pc, "HippiusApiClient", bad), patch.object(pc, "get_metrics_collector", MagicMock()):
        await pc.run_cycle(redis)

    quota = await plans_cache.get_plan_for_account(redis, "5E71kYuD")
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
        {"ss58": f"acct-{i}", "billing": "plan", "plan": "pro", "active": True, "storage_bytes": TB} for i in range(10)
    ]
    seed = api_client_returning(get_s3_plan_accounts=AsyncMock(return_value=page(results=seeded, next=None)))
    with patch.object(pc, "HippiusApiClient", seed), patch.object(pc, "get_metrics_collector", MagicMock()):
        await pc.run_cycle(redis)
    before = dict(redis.hashes["hippius_s3_plan_accounts"])
    assert len(before) == 10

    calls = {"n": 0}

    async def failing_pages(next_url: str | None = None, page_size: int = 500) -> S3PlanAccountsResponse:
        calls["n"] += 1
        if calls["n"] >= 3:
            raise RuntimeError("upstream died on page 3")
        return page(
            results=[{"ss58": f"new-{calls['n']}", "billing": "plan", "plan": "pro", "active": True}],
            next=f"https://api.hippius.com/api/s3/plans/accounts/?page={calls['n'] + 1}",
        )

    api = api_client_returning(get_s3_plan_accounts=failing_pages)
    with patch.object(pc, "HippiusApiClient", api), patch.object(pc, "get_metrics_collector", MagicMock()):
        assert await pc.run_cycle(redis) is False

    assert redis.hashes["hippius_s3_plan_accounts"] == before
    assert "hippius_s3_plan_accounts:building" not in redis.hashes


@pytest.mark.asyncio
async def test_every_page_contributes_to_the_published_roll() -> None:
    redis = FakeRedis()
    calls = {"n": 0}

    async def two_pages(next_url: str | None = None, page_size: int = 500) -> S3PlanAccountsResponse:
        calls["n"] += 1
        return page(
            results=[{"ss58": f"p{calls['n']}", "billing": "plan", "plan": "pro", "active": True, "storage_bytes": TB}],
            next=("https://api.hippius.com/api/s3/plans/accounts/?page=2" if calls["n"] == 1 else None),
        )

    api = api_client_returning(get_s3_plan_accounts=two_pages)
    with patch.object(pc, "HippiusApiClient", api), patch.object(pc, "get_metrics_collector", MagicMock()):
        assert await pc.run_cycle(redis) is True

    assert set(redis.hashes["hippius_s3_plan_accounts"]) == {"p1", "p2"}


@pytest.mark.asyncio
async def test_pagination_is_bounded_so_a_looping_cursor_cannot_hang_the_worker() -> None:
    async def never_ending(next_url: str | None = None, page_size: int = 500) -> S3PlanAccountsResponse:
        return page(next="https://api.hippius.com/api/s3/plans/accounts/?page=2")

    api = api_client_returning(get_s3_plan_accounts=never_ending)
    with patch.object(pc, "HippiusApiClient", api):
        with pytest.raises(RuntimeError, match="pagination exceeded"):
            await pc.refresh_plan_roll_once(FakeRedis())


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
        assert await pc.run_cycle(redis) is True
        await pc._report_cache_age(redis)

    meta = await plans_cache.get_meta(redis)
    assert meta["fetched_at"] > 0
    assert collector.record_plans_cache_age.called, "the staleness metric must be recorded"
