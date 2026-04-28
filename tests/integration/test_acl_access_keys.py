"""Simplified integration tests for access key ACL enforcement"""

from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from fastapi import FastAPI
from httpx import ASGITransport
from httpx import AsyncClient

from gateway.middlewares.access_key_auth import TokenAuth
from gateway.middlewares.account import account_middleware
from gateway.middlewares.acl import acl_middleware
from gateway.middlewares.auth_router import auth_router_middleware


@pytest.fixture  # type: ignore[misc]
def integration_app() -> Any:
    """Create FastAPI app with full auth + ACL middleware chain"""
    app = FastAPI()
    app.state.acl_service = MagicMock()
    # get_bucket_owner_and_id delegates to get_bucket_owner so legacy tests that
    # only override get_bucket_owner keep working. Tests exercising the combined
    # path (e.g. the single-query assertion) override get_bucket_owner_and_id
    # directly and opt out of the delegation.
    app.state.acl_service.get_bucket_id = AsyncMock(side_effect=lambda b: b)

    async def _default_owner_and_id(bucket: str) -> tuple[str | None, str | None]:
        owner = await app.state.acl_service.get_bucket_owner(bucket)
        bid = await app.state.acl_service.get_bucket_id(bucket) if owner else None
        return owner, bid

    app.state.acl_service.get_bucket_owner_and_id = AsyncMock(side_effect=_default_owner_and_id)
    app.state.redis_accounts = AsyncMock()
    app.state.redis_client = AsyncMock()
    # Cache misses by default so the sub-token branch goes to the repo.
    app.state.redis_client.get = AsyncMock(return_value=None)
    app.state.redis_client.setex = AsyncMock(return_value=True)
    # sub_token_scope_repo is loaded via the middleware for sub-token requests.
    # Tests that exercise sub-tokens should override this with the scope shape
    # they want to test; the default returns None (→ default-deny).
    app.state.sub_token_scope_repo = MagicMock()
    app.state.sub_token_scope_repo.get = AsyncMock(return_value=None)

    @app.get("/")
    async def list_buckets() -> dict[str, Any]:
        return {"buckets": []}

    @app.get("/{bucket}")
    async def list_objects(bucket: str) -> dict[str, Any]:
        return {"bucket": bucket, "objects": []}

    @app.get("/{bucket}/{key:path}")
    async def get_object(bucket: str, key: str) -> dict[str, Any]:
        return {"bucket": bucket, "key": key, "status": "success"}

    @app.put("/{bucket}/{key:path}")
    async def put_object(bucket: str, key: str) -> dict[str, Any]:
        return {"bucket": bucket, "key": key, "status": "uploaded"}

    @app.delete("/{bucket}/{key:path}")
    async def delete_object(bucket: str, key: str) -> dict[str, Any]:
        return {"bucket": bucket, "key": key, "status": "deleted"}

    app.middleware("http")(acl_middleware)
    app.middleware("http")(account_middleware)
    app.middleware("http")(auth_router_middleware)

    return app


@pytest.mark.asyncio
async def test_bucket_metadata_fetched_in_single_query(integration_app: Any) -> None:
    """acl_middleware should resolve (owner, bucket_id) in a single DB call, not two."""
    alice_id = "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"
    bob_id = "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY"

    # Mock get_bucket_owner_and_id to return both in one go; assert the old
    # split methods are NOT called individually.
    integration_app.state.acl_service.get_bucket_owner_and_id = AsyncMock(
        return_value=(alice_id, "bucket-uuid-1")
    )
    integration_app.state.acl_service.get_bucket_owner = AsyncMock(
        side_effect=AssertionError("get_bucket_owner must not be called — use get_bucket_owner_and_id")
    )
    integration_app.state.acl_service.get_bucket_id = AsyncMock(
        side_effect=AssertionError("get_bucket_id must not be called — use get_bucket_owner_and_id")
    )
    integration_app.state.acl_service.check_permission = AsyncMock(return_value=True)

    auth_header = "AWS4-HMAC-SHA256 Credential=hip_bob_sub1/20250101/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=abc"
    mock_verify = AsyncMock(return_value=TokenAuth(access_key="hip_bob_sub1", account_address=bob_id, token_type="sub"))

    with patch("gateway.services.auth_orchestrator.verify_access_key_signature", mock_verify):
        with patch("gateway.middlewares.account.config.bypass_credit_check", True):
            async with AsyncClient(transport=ASGITransport(app=integration_app), base_url="http://test") as client:
                await client.get(
                    "/alice-bucket/test.txt",
                    headers={"Authorization": auth_header, "x-amz-date": "20250101T000000Z"},
                )

    integration_app.state.acl_service.get_bucket_owner_and_id.assert_awaited_once_with("alice-bucket")


@pytest.mark.asyncio
async def test_sub_token_denied_without_grants(integration_app: Any) -> None:
    """Sub token denied without ACL grants"""
    alice_id = "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"
    bob_id = "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY"

    integration_app.state.acl_service.get_bucket_owner = AsyncMock(return_value=alice_id)
    integration_app.state.acl_service.check_permission = AsyncMock(return_value=False)

    auth_header = "AWS4-HMAC-SHA256 Credential=hip_bob_sub1/20250101/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=abc"

    # Patch verify function as AsyncMock
    mock_verify = AsyncMock(return_value=TokenAuth(access_key="hip_bob_sub1", account_address=bob_id, token_type="sub"))

    with patch("gateway.services.auth_orchestrator.verify_access_key_signature", mock_verify):
        with patch("gateway.middlewares.account.config.bypass_credit_check", True):
            async with AsyncClient(transport=ASGITransport(app=integration_app), base_url="http://test") as client:
                response = await client.get(
                    "/alice-bucket/test.txt",
                    headers={"Authorization": auth_header, "x-amz-date": "20250101T000000Z"},
                )

    assert response.status_code == 403
    assert b"AccessDenied" in response.content


@pytest.mark.asyncio
async def test_sub_token_allowed_with_access_key_grant(integration_app: Any) -> None:
    """Sub token allowed with specific access key grant"""
    alice_id = "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"
    bob_id = "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY"

    integration_app.state.acl_service.get_bucket_owner = AsyncMock(return_value=alice_id)
    integration_app.state.acl_service.check_permission = AsyncMock(return_value=True)

    auth_header = "AWS4-HMAC-SHA256 Credential=hip_bob_sub1/20250101/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=abc"

    mock_verify = AsyncMock(return_value=TokenAuth(access_key="hip_bob_sub1", account_address=bob_id, token_type="sub"))

    with patch("gateway.services.auth_orchestrator.verify_access_key_signature", mock_verify):
        with patch("gateway.middlewares.account.config.bypass_credit_check", True):
            async with AsyncClient(transport=ASGITransport(app=integration_app), base_url="http://test") as client:
                response = await client.get(
                    "/alice-bucket/test.txt",
                    headers={"Authorization": auth_header, "x-amz-date": "20250101T000000Z"},
                )

    assert response.status_code == 200
    data = response.json()
    assert data["bucket"] == "alice-bucket"


@pytest.mark.asyncio
async def test_sub_token_denied_with_different_key_grant(integration_app: Any) -> None:
    """Sub token denied when grant is for different key"""
    alice_id = "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"
    bob_id = "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY"

    integration_app.state.acl_service.get_bucket_owner = AsyncMock(return_value=alice_id)
    integration_app.state.acl_service.check_permission = AsyncMock(return_value=False)

    auth_header = "AWS4-HMAC-SHA256 Credential=hip_bob_sub1/20250101/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=abc"

    mock_verify = AsyncMock(return_value=TokenAuth(access_key="hip_bob_sub1", account_address=bob_id, token_type="sub"))

    with patch("gateway.services.auth_orchestrator.verify_access_key_signature", mock_verify):
        with patch("gateway.middlewares.account.config.bypass_credit_check", True):
            async with AsyncClient(transport=ASGITransport(app=integration_app), base_url="http://test") as client:
                response = await client.get(
                    "/alice-bucket/test.txt",
                    headers={"Authorization": auth_header, "x-amz-date": "20250101T000000Z"},
                )

    assert response.status_code == 403


@pytest.mark.asyncio
async def test_master_token_bypasses_acl_for_owned_bucket(integration_app: Any) -> None:
    """Master token bypasses ACL for owned buckets"""
    alice_id = "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"

    integration_app.state.acl_service.get_bucket_owner = AsyncMock(return_value=alice_id)

    auth_header = "AWS4-HMAC-SHA256 Credential=hip_alice_master/20250101/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=abc"

    mock_verify = AsyncMock(return_value=TokenAuth(access_key="hip_alice_master", account_address=alice_id, token_type="master"))

    with patch("gateway.services.auth_orchestrator.verify_access_key_signature", mock_verify):
        with patch("gateway.middlewares.account.config.bypass_credit_check", True):
            async with AsyncClient(transport=ASGITransport(app=integration_app), base_url="http://test") as client:
                response = await client.put(
                    "/alice-bucket/test.txt",
                    headers={"Authorization": auth_header, "x-amz-date": "20250101T000000Z"},
                    content=b"data",
                )

    assert response.status_code == 200


@pytest.mark.asyncio
async def test_account_grant_allows_all_keys(integration_app: Any) -> None:
    """Account-level grant allows all keys from that account"""
    alice_id = "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"
    bob_id = "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY"

    integration_app.state.acl_service.get_bucket_owner = AsyncMock(return_value=alice_id)
    integration_app.state.acl_service.check_permission = AsyncMock(return_value=True)

    for bob_key in ["hip_bob_key1", "hip_bob_key2", "hip_bob_key99"]:
        auth_header = f"AWS4-HMAC-SHA256 Credential={bob_key}/20250101/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=abc"

        mock_verify = AsyncMock(return_value=TokenAuth(access_key="hip_bob_sub1", account_address=bob_id, token_type="sub"))

        with patch("gateway.services.auth_orchestrator.verify_access_key_signature", mock_verify):
            with patch("gateway.middlewares.account.config.bypass_credit_check", True):
                async with AsyncClient(transport=ASGITransport(app=integration_app), base_url="http://test") as client:
                    response = await client.get(
                        "/alice-bucket/test.txt",
                        headers={"Authorization": auth_header, "x-amz-date": "20250101T000000Z"},
                    )

        assert response.status_code == 200, f"Failed for {bob_key}"


@pytest.mark.asyncio
async def test_cross_account_access_key_grant(integration_app: Any) -> None:
    """Cross-account access key grant works"""
    alice_id = "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"
    bob_id = "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY"

    integration_app.state.acl_service.get_bucket_owner = AsyncMock(return_value=alice_id)
    integration_app.state.acl_service.check_permission = AsyncMock(return_value=True)

    auth_header = "AWS4-HMAC-SHA256 Credential=hip_bob_contractor/20250101/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=abc"

    mock_verify = AsyncMock(return_value=TokenAuth(access_key="hip_bob_sub1", account_address=bob_id, token_type="sub"))

    with patch("gateway.services.auth_orchestrator.verify_access_key_signature", mock_verify):
        with patch("gateway.middlewares.account.config.bypass_credit_check", True):
            async with AsyncClient(transport=ASGITransport(app=integration_app), base_url="http://test") as client:
                response = await client.get(
                    "/alice-bucket/test.txt",
                    headers={"Authorization": auth_header, "x-amz-date": "20250101T000000Z"},
                )

    assert response.status_code == 200


@pytest.mark.asyncio
async def test_presigned_get_uses_access_key_for_acl(integration_app: Any) -> None:
    """Presigned GET with hip_ key should populate access key account for ACL checks."""
    alice_id = "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"
    bob_id = "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY"

    integration_app.state.acl_service.get_bucket_owner = AsyncMock(return_value=alice_id)
    integration_app.state.acl_service.check_permission = AsyncMock(return_value=True)

    access_key = "hip_bob_presigned"

    query_params = {
        "X-Amz-Algorithm": "AWS4-HMAC-SHA256",
        "X-Amz-Credential": f"{access_key}/20250101/us-east-1/s3/aws4_request",
        "X-Amz-Date": "20250101T000000Z",
        "X-Amz-Expires": "3600",
        "X-Amz-SignedHeaders": "host",
        "X-Amz-Signature": "deadbeef",
    }

    # Patch presigned verifier to simulate successful verification and account mapping
    mock_verify_presigned = AsyncMock(return_value=TokenAuth(access_key=access_key, account_address=bob_id, token_type="sub"))

    with patch("gateway.services.auth_orchestrator.verify_access_key_presigned_url", mock_verify_presigned):
        with patch("gateway.middlewares.account.config.bypass_credit_check", True):
            async with AsyncClient(transport=ASGITransport(app=integration_app), base_url="http://test") as client:
                response = await client.get("/alice-bucket/test.txt", params=query_params)

    # Once implemented, presigned URLs should behave like normal access-key auth for ACL
    assert response.status_code == 200
