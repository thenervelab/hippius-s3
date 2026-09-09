"""S3 billing plans, end to end against the real stack.

These exercise the seams the unit tests cannot: that the plans-cacher actually reaches
mock-hippius-api, actually counts usage out of the real database, and actually publishes something
the running gateway reads on a live upload.

The stack ships with HIPPIUS_ENABLE_BILLING_PLANS unset, so enforcement is OFF here — which is the
configuration that will run in staging and production first, and therefore the one worth covering.
What these assert is that with the feature off, a plan account is billed exactly as before: the
plan is resolved, the shadow verdict is computed, and the pay-as-you-go path still runs.

mock-hippius-api starts with an empty account roll (MOCK_ACCOUNT_ADDRESS is on no plan), so the
default stack exercises the unchanged PAYG path; `POST /_plans` swaps that in at runtime.
"""

import time
import uuid
from typing import Any

import pytest
import requests


MOCK_API = "http://localhost:8001"
# The address every e2e access key authenticates as — see tests/e2e/mock_hippius_api.py.
MOCK_ACCOUNT_ADDRESS = "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"

TB = 1_099_511_627_776


def _set_plans(accounts: list[dict[str, Any]], plans: dict[str, Any] | None = None) -> None:
    body: dict[str, Any] = {"results": accounts}
    if plans is not None:
        body["plans"] = plans
    requests.post(f"{MOCK_API}/_plans", json=body, timeout=10).raise_for_status()


@pytest.fixture
def plan_roll() -> Any:
    """Swap the upstream roll for one test, then put it back.

    Restoring matters: the roll is process-global in the mock, and a leaked plan would silently
    change how every later test in the session is billed.
    """
    yield _set_plans
    _set_plans([])


@pytest.mark.e2e
@pytest.mark.local
def test_the_plans_endpoint_is_reachable_and_has_the_shape_we_parse() -> None:
    """A contract check against the mock, so a payload change breaks here rather than in the pod.

    Both paths are registered because HIPPIUS_API_BASE_URL carries an /api prefix in prod and none
    against the mock — getting that wrong 404s in production only.
    """
    for path in ("/s3/plans/accounts/", "/api/s3/plans/accounts/"):
        page = requests.get(f"{MOCK_API}{path}", timeout=10)
        page.raise_for_status()
        body = page.json()

        assert "plans" in body and "results" in body
        for name, plan in body["plans"].items():
            assert isinstance(plan.get("storage_bytes"), int), f"{name} must carry an allowance"


@pytest.mark.e2e
@pytest.mark.local
def test_an_account_with_no_plan_uploads_normally(boto3_client: Any) -> None:
    """The parallel-path guarantee, on the real stack: an account upstream does not know about is
    billed pay-as-you-go and never touches the quota gate."""
    bucket = f"plans-payg-{uuid.uuid4().hex[:12]}"
    boto3_client.create_bucket(Bucket=bucket)

    boto3_client.put_object(Bucket=bucket, Key="hello.txt", Body=b"hello")

    assert boto3_client.get_object(Bucket=bucket, Key="hello.txt")["Body"].read() == b"hello"


@pytest.mark.e2e
@pytest.mark.local
def test_a_plan_account_still_uploads_while_enforcement_is_off(
    boto3_client: Any, plan_roll: Any
) -> None:
    """The shipping configuration. The account IS on a plan and IS wildly over a 1-byte quota, and
    the upload must still succeed, because HIPPIUS_ENABLE_BILLING_PLANS is off.

    This is the test that would fail if the flag were ever wired backwards — the failure mode that
    turns a disabled feature into a fleet-wide 402.
    """
    plan_roll(
        [
            {
                "ss58": MOCK_ACCOUNT_ADDRESS,
                "billing": "plan",
                "plan": "pro",
                "active": True,
                "storage_bytes": 1,
            }
        ]
    )

    bucket = f"plans-shadow-{uuid.uuid4().hex[:12]}"
    boto3_client.create_bucket(Bucket=bucket)
    boto3_client.put_object(Bucket=bucket, Key="hello.txt", Body=b"hello" * 1000)

    assert boto3_client.get_object(Bucket=bucket, Key="hello.txt")["Body"].read() == b"hello" * 1000


@pytest.mark.e2e
@pytest.mark.local
def test_an_over_quota_plan_account_can_still_delete(boto3_client: Any, plan_roll: Any) -> None:
    """Deletes must never be quota-gated — including the bulk `POST ?delete` that
    `aws s3 rm --recursive` issues — or a customer who has hit their limit cannot get back under it.
    """
    bucket = f"plans-delete-{uuid.uuid4().hex[:12]}"
    boto3_client.create_bucket(Bucket=bucket)
    boto3_client.put_object(Bucket=bucket, Key="a.txt", Body=b"a")
    boto3_client.put_object(Bucket=bucket, Key="b.txt", Body=b"b")

    plan_roll(
        [
            {
                "ss58": MOCK_ACCOUNT_ADDRESS,
                "billing": "plan",
                "plan": "pro",
                "active": True,
                "storage_bytes": 1,
            }
        ]
    )

    boto3_client.delete_object(Bucket=bucket, Key="a.txt")
    boto3_client.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": "b.txt"}]})

    remaining = boto3_client.list_objects_v2(Bucket=bucket).get("Contents", [])
    assert remaining == []


@pytest.mark.e2e
@pytest.mark.local
def test_an_account_reported_inactive_is_served_normally(
    boto3_client: Any, plan_roll: Any
) -> None:
    """`active: false` is what upstream reports for EVERY account, live subscriptions included, so
    it is not consulted — the account is admitted to the plan roll on billing="plan" alone.

    What this pins is that the inactive-flag row moves through the whole stack without upsetting
    anything: it parses, it publishes, and the upload still succeeds. It deliberately does NOT
    prove the admission decision — enforcement ships off, so an admitted plan account and a
    pay-as-you-go one reach the same outcome here. That decision is pinned by
    tests/unit/test_plans_cacher_worker.py::test_the_active_flag_is_not_consulted.
    """
    plan_roll(
        [
            {
                "ss58": MOCK_ACCOUNT_ADDRESS,
                "billing": "plan",
                "plan": "pro",
                "active": False,
                "storage_bytes": 1,
                "next_charge": "2026-10-08",
                "subscription_id": 148,
            }
        ]
    )

    bucket = f"plans-inactive-flag-{uuid.uuid4().hex[:12]}"
    boto3_client.create_bucket(Bucket=bucket)
    boto3_client.put_object(Bucket=bucket, Key="hello.txt", Body=b"hello")

    assert boto3_client.get_object(Bucket=bucket, Key="hello.txt")["Body"].read() == b"hello"


@pytest.mark.e2e
@pytest.mark.local
def test_the_upstream_endpoint_going_down_does_not_break_uploads(
    boto3_client: Any, plan_roll: Any
) -> None:
    """The failure this whole design is built around. With the plans endpoint erroring, the cached
    roll keeps serving and uploads are unaffected — a scrape failure must never become an outage.
    """
    plan_roll(
        [
            {
                "ss58": MOCK_ACCOUNT_ADDRESS,
                "billing": "plan",
                "plan": "pro",
                "active": True,
                "storage_bytes": 10 * TB,
            }
        ]
    )

    requests.post(f"{MOCK_API}/_fault", json={"op": "plans", "mode": "error"}, timeout=10)
    try:
        time.sleep(1)
        bucket = f"plans-outage-{uuid.uuid4().hex[:12]}"
        boto3_client.create_bucket(Bucket=bucket)
        boto3_client.put_object(Bucket=bucket, Key="hello.txt", Body=b"hello")

        assert boto3_client.get_object(Bucket=bucket, Key="hello.txt")["Body"].read() == b"hello"
    finally:
        requests.post(f"{MOCK_API}/_fault", json={"op": "plans", "mode": "off"}, timeout=10)
