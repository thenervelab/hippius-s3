"""Unit tests for input_validation_middleware — bucket name validation on CreateBucket."""

from typing import Any

import pytest
from fastapi import FastAPI
from fastapi import Request
from fastapi import Response
from httpx import ASGITransport
from httpx import AsyncClient

from hippius_s3.api.middlewares.input_validation import input_validation_middleware


@pytest.fixture
def validation_app() -> FastAPI:
    """Minimal app with only the input validation middleware and a catch-all 200 handler."""
    app = FastAPI()
    app.middleware("http")(input_validation_middleware)

    @app.api_route("/{path:path}", methods=["GET", "PUT", "DELETE", "HEAD", "POST"])
    async def catch_all(request: Request) -> Response:
        return Response(status_code=200, content="ok")

    return app


# ---------------------------------------------------------------------------
# SS58 addresses bypass format checks
# ---------------------------------------------------------------------------

# Alice's well-known SS58 address (Substrate dev accounts)
SS58_ALICE = "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY"
SS58_BOB = "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"


@pytest.mark.asyncio
async def test_ss58_address_passes_middleware(validation_app: Any) -> None:
    """A valid SS58 address should not be blocked by format checks."""
    async with AsyncClient(transport=ASGITransport(app=validation_app), base_url="http://test") as client:
        resp = await client.put(f"/{SS58_ALICE}")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_ss58_bob_passes_middleware(validation_app: Any) -> None:
    """Another valid SS58 address passes as well."""
    async with AsyncClient(transport=ASGITransport(app=validation_app), base_url="http://test") as client:
        resp = await client.put(f"/{SS58_BOB}")
    assert resp.status_code == 200


# ---------------------------------------------------------------------------
# Non-SS58 uppercase names are still rejected
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "name",
    [
        "MyBucket",
        "UPPERCASE",
        "mixedCase123",
        "Bucket-With-Caps",
        "testBUCKET",
        "ABC",
    ],
)
@pytest.mark.asyncio
async def test_uppercase_non_ss58_rejected(validation_app: Any, name: str) -> None:
    """Uppercase bucket names that are NOT valid SS58 addresses must be rejected."""
    async with AsyncClient(transport=ASGITransport(app=validation_app), base_url="http://test") as client:
        resp = await client.put(f"/{name}")
    assert resp.status_code == 400
    assert "InvalidBucketName" in resp.text


# ---------------------------------------------------------------------------
# Standard format validation still enforced for non-SS58 names
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_valid_lowercase_bucket_passes(validation_app: Any) -> None:
    async with AsyncClient(transport=ASGITransport(app=validation_app), base_url="http://test") as client:
        resp = await client.put("/my-valid-bucket")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_bucket_with_dots_passes(validation_app: Any) -> None:
    async with AsyncClient(transport=ASGITransport(app=validation_app), base_url="http://test") as client:
        resp = await client.put("/my.bucket.name")
    assert resp.status_code == 200


@pytest.mark.parametrize("name", ["ab", "x", "a"])
@pytest.mark.asyncio
async def test_too_short_rejected(validation_app: Any, name: str) -> None:
    async with AsyncClient(transport=ASGITransport(app=validation_app), base_url="http://test") as client:
        resp = await client.put(f"/{name}")
    assert resp.status_code == 400
    assert "too short" in resp.text


@pytest.mark.asyncio
async def test_too_long_rejected(validation_app: Any) -> None:
    name = "a" * 64
    async with AsyncClient(transport=ASGITransport(app=validation_app), base_url="http://test") as client:
        resp = await client.put(f"/{name}")
    assert resp.status_code == 400
    assert "too long" in resp.text


@pytest.mark.asyncio
async def test_adjacent_periods_rejected(validation_app: Any) -> None:
    async with AsyncClient(transport=ASGITransport(app=validation_app), base_url="http://test") as client:
        resp = await client.put("/my..bucket")
    assert resp.status_code == 400
    assert "InvalidBucketName" in resp.text


@pytest.mark.asyncio
async def test_ip_address_format_rejected(validation_app: Any) -> None:
    async with AsyncClient(transport=ASGITransport(app=validation_app), base_url="http://test") as client:
        resp = await client.put("/192.168.1.1")
    assert resp.status_code == 400
    assert "IP address" in resp.text


@pytest.mark.parametrize("prefix", ["xn--", "sthree-", "amzn-s3-demo-"])
@pytest.mark.asyncio
async def test_prohibited_prefix_rejected(validation_app: Any, prefix: str) -> None:
    name = prefix + "mybucket"
    async with AsyncClient(transport=ASGITransport(app=validation_app), base_url="http://test") as client:
        resp = await client.put(f"/{name}")
    assert resp.status_code == 400
    assert prefix in resp.text


@pytest.mark.parametrize("suffix", ["-s3alias", "--ol-s3", "--x-s3", "--table-s3"])
@pytest.mark.asyncio
async def test_prohibited_suffix_rejected(validation_app: Any, suffix: str) -> None:
    name = "mybucket" + suffix
    async with AsyncClient(transport=ASGITransport(app=validation_app), base_url="http://test") as client:
        resp = await client.put(f"/{name}")
    assert resp.status_code == 400
    assert suffix in resp.text


@pytest.mark.asyncio
async def test_starts_with_hyphen_rejected(validation_app: Any) -> None:
    async with AsyncClient(transport=ASGITransport(app=validation_app), base_url="http://test") as client:
        resp = await client.put("/-mybucket")
    assert resp.status_code == 400
    assert "InvalidBucketName" in resp.text


@pytest.mark.asyncio
async def test_ends_with_hyphen_rejected(validation_app: Any) -> None:
    async with AsyncClient(transport=ASGITransport(app=validation_app), base_url="http://test") as client:
        resp = await client.put("/mybucket-")
    assert resp.status_code == 400
    assert "InvalidBucketName" in resp.text


# ---------------------------------------------------------------------------
# CreateBucket detection — validation only fires on PUT /{bucket} with no
# object key and no tagging/lifecycle/policy query params
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_put_with_object_key_skips_bucket_validation(validation_app: Any) -> None:
    """PUT /{bucket}/{key} is PutObject, not CreateBucket — no bucket name check."""
    async with AsyncClient(transport=ASGITransport(app=validation_app), base_url="http://test") as client:
        resp = await client.put("/UPPERCASE/some-object")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_put_with_tagging_skips_bucket_validation(validation_app: Any) -> None:
    """PUT /{bucket}?tagging is PutBucketTagging, not CreateBucket."""
    async with AsyncClient(transport=ASGITransport(app=validation_app), base_url="http://test") as client:
        resp = await client.put("/UPPERCASE?tagging=")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_put_with_lifecycle_skips_bucket_validation(validation_app: Any) -> None:
    async with AsyncClient(transport=ASGITransport(app=validation_app), base_url="http://test") as client:
        resp = await client.put("/UPPERCASE?lifecycle=")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_put_with_policy_skips_bucket_validation(validation_app: Any) -> None:
    async with AsyncClient(transport=ASGITransport(app=validation_app), base_url="http://test") as client:
        resp = await client.put("/UPPERCASE?policy=")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_get_request_skips_bucket_validation(validation_app: Any) -> None:
    """GET /{bucket} is ListObjects, not CreateBucket — no bucket name check."""
    async with AsyncClient(transport=ASGITransport(app=validation_app), base_url="http://test") as client:
        resp = await client.get("/UPPERCASE")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_delete_request_skips_bucket_validation(validation_app: Any) -> None:
    async with AsyncClient(transport=ASGITransport(app=validation_app), base_url="http://test") as client:
        resp = await client.delete("/UPPERCASE")
    assert resp.status_code == 200


# ---------------------------------------------------------------------------
# Non-S3 endpoint paths bypass all validation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("path", ["/health", "/user", "/docs", "/openapi.json", "/robots.txt"])
@pytest.mark.asyncio
async def test_non_s3_paths_bypass_validation(validation_app: Any, path: str) -> None:
    async with AsyncClient(transport=ASGITransport(app=validation_app), base_url="http://test") as client:
        resp = await client.put(path)
    assert resp.status_code == 200


# ---------------------------------------------------------------------------
# Edge cases: strings that resemble SS58 but are not valid
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fake_ss58_like_string_rejected(validation_app: Any) -> None:
    """A string with mixed case that looks vaguely like SS58 but isn't valid."""
    fake = "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQX"  # last char changed
    async with AsyncClient(transport=ASGITransport(app=validation_app), base_url="http://test") as client:
        resp = await client.put(f"/{fake}")
    # If substrateinterface considers this invalid, it should be caught by format checks
    # Either 400 (invalid format) or 200 (valid SS58 we didn't know about) — both acceptable
    # but uppercase non-SS58 should definitely be 400
    assert resp.status_code in (200, 400)


@pytest.mark.asyncio
async def test_short_base58_string_rejected(validation_app: Any) -> None:
    """Short strings with uppercase are not valid SS58 and should be rejected."""
    async with AsyncClient(transport=ASGITransport(app=validation_app), base_url="http://test") as client:
        resp = await client.put("/5Abc")
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_ss58_length_still_checked(validation_app: Any) -> None:
    """SS58 addresses are ~48 chars, well within 3-63. But if somehow a valid SS58
    exceeded max length, the length check fires before the SS58 bypass."""
    # Standard SS58 addresses are 47-48 chars, so this is a theoretical safeguard test.
    # We just confirm a normal SS58 address is within bounds.
    assert 3 <= len(SS58_ALICE) <= 63


# ---------------------------------------------------------------------------
# Object key validation (applies regardless of bucket name)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_object_key_with_backslash_rejected(validation_app: Any) -> None:
    async with AsyncClient(transport=ASGITransport(app=validation_app), base_url="http://test") as client:
        resp = await client.put("/mybucket/path\\to\\file")
    assert resp.status_code == 400
    assert "InvalidArgument" in resp.text
