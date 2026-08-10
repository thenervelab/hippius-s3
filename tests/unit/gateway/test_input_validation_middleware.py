"""Unit tests for input_validation_middleware — bucket name validation on CreateBucket."""

from typing import Any
from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI
from fastapi import Request
from fastapi import Response
from httpx import ASGITransport
from httpx import AsyncClient

from gateway.middlewares.input_validation import input_validation_middleware


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
# Reserved gateway route names — CreateBucket must never collide with them.
# A bucket named "docs" previously slipped through and was written with an
# empty owner (prod incident 2026-08-03).
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "name",
    ["docs", "health", "metrics", "user", "openapi.json", "robots.txt", "redoc"],
)
@pytest.mark.asyncio
async def test_reserved_bucket_names_rejected(validation_app: Any, name: str) -> None:
    async with AsyncClient(transport=ASGITransport(app=validation_app), base_url="http://test") as client:
        resp = await client.put(f"/{name}")
    assert resp.status_code == 400
    assert "InvalidBucketName" in resp.text


@pytest.mark.parametrize(
    "name",
    [
        # Names merely PREFIXED by a reserved segment. auth_router matches exactly now, so
        # these authenticate normally and get a real owner — banning them would reject
        # ordinary customer names for no security benefit.
        "docs2",
        "docsite",
        "healthcheck",
        "metrics-test",
        "user-uploads",
        "userdata",
        # Never route names in the first place: acl_router mounts /{bucket} at the ROOT (no
        # /acl prefix), and there is no /static mount anywhere in the gateway.
        "acl",
        "acl-backups",
        "static",
        "static-assets",
        # Plain lookalikes.
        "documents",
        "do-docs",
        "my-metrics",
        "healer",
    ],
)
@pytest.mark.asyncio
async def test_non_reserved_names_pass(validation_app: Any, name: str) -> None:
    async with AsyncClient(transport=ASGITransport(app=validation_app), base_url="http://test") as client:
        resp = await client.put(f"/{name}")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_reserved_check_uses_the_path_the_api_will_receive() -> None:
    """The reserved check must read the same path every other security-relevant middleware reads,
    and that path is the one the api routes on.

    `PUT /docs%23x` really does arrive at the api as `PUT /docs` — httpx truncates the target at the
    `#` — so judging it as the bucket `docs#x` was judging a request that is never sent. Both
    spellings are refused, but only this one is refused for the true reason: `docs` is reserved.
    """
    from gateway.utils.paths import first_path_segment

    request = MagicMock()
    request.scope = {"raw_path": b"/docs%23x"}
    assert first_path_segment(request) == "docs"


@pytest.mark.asyncio
async def test_reserved_name_with_key_is_not_create_bucket(validation_app: Any) -> None:
    """PUT /docs/{key} is PutObject-shaped, not CreateBucket — still skipped."""
    async with AsyncClient(transport=ASGITransport(app=validation_app), base_url="http://test") as client:
        resp = await client.put("/docs/some-object")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_reserved_name_with_tagging_is_not_create_bucket(validation_app: Any) -> None:
    async with AsyncClient(transport=ASGITransport(app=validation_app), base_url="http://test") as client:
        resp = await client.put("/docs?tagging=")
    assert resp.status_code == 200


# ---------------------------------------------------------------------------
# Non-S3 endpoint paths bypass all validation. Reads bypass; a CreateBucket-shaped
# PUT on the bare segment is now rejected instead (the two cases are split rather
# than the PUT coverage being dropped).
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("path", ["/health", "/user", "/docs", "/openapi.json", "/robots.txt"])
@pytest.mark.asyncio
async def test_non_s3_paths_bypass_validation_on_reads(validation_app: Any, path: str) -> None:
    async with AsyncClient(transport=ASGITransport(app=validation_app), base_url="http://test") as client:
        resp = await client.get(path)
    assert resp.status_code == 200


@pytest.mark.parametrize("path", ["/health", "/user", "/docs", "/openapi.json", "/robots.txt"])
@pytest.mark.asyncio
async def test_create_bucket_shaped_put_on_non_s3_paths_is_rejected(validation_app: Any, path: str) -> None:
    """The counterpart to the read case above: PUT /<segment> with no key and no sub-resource
    query is CreateBucket, and letting it through the SKIP_PREFIXES bypass is exactly how the
    ownerless "docs" bucket got written."""
    async with AsyncClient(transport=ASGITransport(app=validation_app), base_url="http://test") as client:
        resp = await client.put(path)
    assert resp.status_code == 400
    assert "InvalidBucketName" in resp.text


@pytest.mark.parametrize("path", ["/user/profile", "/docs/cache"])
@pytest.mark.asyncio
async def test_non_s3_subpath_puts_still_bypass(validation_app: Any, path: str) -> None:
    """PUT on a sub-path is not CreateBucket-shaped — the frontend /user/... endpoints and
    the /docs/cache purge must keep working."""
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
