"""The plans-cacher loops.

The behaviour under test is what happens when api.hippius.com misbehaves. A failed cycle must be
recorded and slept off, never raised -- and, above all, it must never publish a partial map. The
cron failing is the expected case this whole design is built around; it is not an outage.
"""

from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from hippius_s3.services.hippius_api_service import AccountPlanEntry
from hippius_s3.services.hippius_api_service import AccountPlansResponse
from hippius_s3.services.hippius_api_service import S3PlanQuota
from hippius_s3.services.hippius_api_service import S3PlansResponse
from tests.unit.test_plans_cache import FakeRedis
from workers import run_plans_cacher_in_loop as pc


def api_client_returning(**methods: object) -> MagicMock:
    client = MagicMock()
    for name, value in methods.items():
        setattr(client, name, value)
    ctx = MagicMock()
    ctx.__aenter__ = AsyncMock(return_value=client)
    ctx.__aexit__ = AsyncMock(return_value=False)
    return MagicMock(return_value=ctx)


@pytest.mark.asyncio
async def test_the_catalog_cycle_publishes_and_records_success() -> None:
    redis = FakeRedis()
    api = api_client_returning(
        get_s3_plans=AsyncMock(
            return_value=S3PlansResponse(plans=[S3PlanQuota(plan_id="plan-1", name="Starter", storage_bytes=1_000)])
        )
    )
    collector = MagicMock()

    with (
        patch.object(pc, "HippiusApiClient", api),
        patch.object(pc, "get_metrics_collector", return_value=collector),
    ):
        assert await pc.run_cycle("catalog", redis) is True

    assert redis.hashes["hippius_s3_plans"]
    kwargs = collector.record_plans_cacher_cycle.call_args.kwargs
    assert kwargs["success"] is True
    assert kwargs["entries"] == 1
    assert kwargs["loop"] == "catalog"


@pytest.mark.asyncio
async def test_a_failed_cycle_is_recorded_without_raising() -> None:
    redis = FakeRedis()
    api = api_client_returning(get_s3_plans=AsyncMock(side_effect=RuntimeError("upstream 500")))
    collector = MagicMock()

    with (
        patch.object(pc, "HippiusApiClient", api),
        patch.object(pc, "get_metrics_collector", return_value=collector),
    ):
        assert await pc.run_cycle("catalog", redis) is False

    kwargs = collector.record_plans_cacher_cycle.call_args.kwargs
    assert kwargs["success"] is False
    assert kwargs["entries"] == 0


@pytest.mark.asyncio
async def test_an_upstream_failure_leaves_the_previous_catalog_serving() -> None:
    """The whole point of the no-TTL cache: an api.hippius.com outage is not an outage for us."""
    redis = FakeRedis()
    good = api_client_returning(
        get_s3_plans=AsyncMock(return_value=S3PlansResponse(plans=[S3PlanQuota(plan_id="plan-1", storage_bytes=42)]))
    )
    with patch.object(pc, "HippiusApiClient", good), patch.object(pc, "get_metrics_collector", MagicMock()):
        await pc.run_cycle("catalog", redis)

    bad = api_client_returning(get_s3_plans=AsyncMock(side_effect=RuntimeError("upstream is down")))
    with patch.object(pc, "HippiusApiClient", bad), patch.object(pc, "get_metrics_collector", MagicMock()):
        await pc.run_cycle("catalog", redis)

    from hippius_s3.services import plans_cache

    quota = await plans_cache.get_plan_quota(redis, "plan-1")
    assert quota is not None and quota.storage_bytes == 42


@pytest.mark.asyncio
async def test_a_mid_pagination_failure_publishes_nothing() -> None:
    """THE test for this worker.

    Publishing pages 1-6 of 20 would drop ~65% of plan customers to pay-as-you-go and 402 them on
    their next upload, with a green-looking cycle. refresh_account_plans_once must collect every
    page before it publishes anything.
    """
    redis = FakeRedis()

    seed = api_client_returning(
        get_account_plans=AsyncMock(
            return_value=AccountPlansResponse(
                accounts=[AccountPlanEntry(account_id=f"acct-{i}", plan_id="plan-1") for i in range(10)],
                next=None,
            )
        )
    )
    with patch.object(pc, "HippiusApiClient", seed), patch.object(pc, "get_metrics_collector", MagicMock()):
        await pc.run_cycle("accounts", redis)
    before = dict(redis.hashes["hippius_s3_plan_accounts"])
    assert len(before) == 10

    calls = {"n": 0}

    async def failing_pages(page: str | None = None) -> AccountPlansResponse:
        calls["n"] += 1
        if calls["n"] >= 3:
            raise RuntimeError("upstream died on page 3")
        return AccountPlansResponse(
            accounts=[AccountPlanEntry(account_id=f"new-{calls['n']}", plan_id="plan-2")],
            next=f"page-{calls['n'] + 1}",
        )

    api = api_client_returning(get_account_plans=failing_pages)
    with patch.object(pc, "HippiusApiClient", api), patch.object(pc, "get_metrics_collector", MagicMock()):
        assert await pc.run_cycle("accounts", redis) is False

    assert redis.hashes["hippius_s3_plan_accounts"] == before
    assert "hippius_s3_plan_accounts:building" not in redis.hashes


@pytest.mark.asyncio
async def test_pagination_is_bounded_so_a_looping_cursor_cannot_hang_the_worker() -> None:
    async def never_ending(page: str | None = None) -> AccountPlansResponse:
        return AccountPlansResponse(accounts=[AccountPlanEntry(account_id="a", plan_id="plan-1")], next="always-more")

    api = api_client_returning(get_account_plans=never_ending)
    with patch.object(pc, "HippiusApiClient", api):
        with pytest.raises(RuntimeError, match="pagination exceeded"):
            await pc.refresh_account_plans_once(FakeRedis())


@pytest.mark.asyncio
async def test_accounts_with_no_plan_are_simply_absent_from_the_map() -> None:
    """A null plan_id means pay-as-you-go, which the request path reads as "no hash field". There
    is nothing to encode for them."""
    pages = [
        AccountPlansResponse(
            accounts=[
                AccountPlanEntry(account_id="on-a-plan", plan_id="plan-1"),
                AccountPlanEntry(account_id="payg", plan_id=None),
            ]
        )
    ]
    assert pc._parse_account_plans(pages) == {"on-a-plan": "plan-1"}


@pytest.mark.asyncio
async def test_an_upstream_payload_with_unknown_fields_still_parses() -> None:
    """The endpoints are not deployed yet, so the modelled shape is a guess. A richer payload must
    not crash the cacher and strand the fleet on last-known-good."""
    response = S3PlansResponse.model_validate(
        {
            "plans": [
                {
                    "plan_id": "plan-1",
                    "name": "Starter",
                    "storage_bytes": 5,
                    "price_usd_cents": 900,
                    "features": ["a", "b"],
                }
            ],
            "generated_at": "2026-09-08T00:00:00Z",
        }
    )

    assert pc._parse_plan_catalog(response) == {"plan-1": {"name": "Starter", "storage_bytes": 5}}


@pytest.mark.asyncio
async def test_a_plan_missing_its_allowance_still_parses() -> None:
    response = S3PlansResponse.model_validate({"plans": [{"plan_id": "plan-1"}]})
    assert pc._parse_plan_catalog(response) == {"plan-1": {"name": None, "storage_bytes": None}}
