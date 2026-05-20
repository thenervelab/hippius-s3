"""Tests for cache_control_middleware — Cache-Control header injection by method/status/ACL."""

from typing import Any

import pytest
from fastapi import FastAPI
from fastapi import Request
from fastapi import Response
from httpx import ASGITransport
from httpx import AsyncClient

from gateway.middlewares.cache_control import PRIVATE_CACHE_CONTROL
from gateway.middlewares.cache_control import PUBLIC_CACHE_CONTROL
from gateway.middlewares.cache_control import VISIBILITY_HEADER
from gateway.middlewares.cache_control import WARM_PUBLIC_CACHE_CONTROL
from gateway.middlewares.cache_control import cache_control_middleware


@pytest.fixture  # type: ignore[misc]
def app() -> Any:
    app = FastAPI()
    app.middleware("http")(cache_control_middleware)

    @app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "HEAD", "PATCH"])
    async def catch_all(request: Request) -> Response:
        status = int(request.headers.get("x-test-status", "200"))
        # Simulate acl_middleware wiring
        request.state.anonymous_read_allowed = request.headers.get("x-test-anon-read") == "true"
        request.state.bucket_is_cache_warm = request.headers.get("x-test-warm") == "true"
        # Simulate auth_router wiring
        request.state.auth_method = request.headers.get("x-test-auth-method", "anonymous")
        # Simulate auth_probe_middleware wiring (it sets this when the probe
        # secret matches; real flow would short-circuit before reaching here)
        if request.headers.get("x-test-is-auth-probe") == "true":
            request.state.is_auth_probe = True
        upstream_cc = request.headers.get("x-test-upstream-cc")
        headers = {"Cache-Control": upstream_cc} if upstream_cc else None
        return Response(status_code=status, content=b"ok", headers=headers)

    return app


@pytest.mark.asyncio
async def test_private_bucket_gets_no_store(app: Any) -> None:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.get("/private-bucket/foo.txt")
    assert r.headers["Cache-Control"] == PRIVATE_CACHE_CONTROL


@pytest.mark.asyncio
async def test_public_bucket_gets_cacheable_policy(app: Any) -> None:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.get("/public-bucket/foo.txt", headers={"x-test-anon-read": "true"})
    assert r.headers["Cache-Control"] == PUBLIC_CACHE_CONTROL


@pytest.mark.asyncio
async def test_head_public_bucket_gets_cacheable_policy(app: Any) -> None:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.head("/public-bucket/foo.txt", headers={"x-test-anon-read": "true"})
    assert r.headers["Cache-Control"] == PUBLIC_CACHE_CONTROL


@pytest.mark.asyncio
async def test_put_gets_no_cache_control_header(app: Any) -> None:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.put("/public-bucket/foo.txt", content=b"x", headers={"x-test-anon-read": "true"})
    assert "Cache-Control" not in r.headers


@pytest.mark.asyncio
async def test_delete_gets_no_cache_control_header(app: Any) -> None:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.delete("/public-bucket/foo.txt", headers={"x-test-anon-read": "true"})
    assert "Cache-Control" not in r.headers


@pytest.mark.asyncio
async def test_error_responses_get_no_store_to_prevent_negative_caching(app: Any) -> None:
    """ATS's heuristic negative caching must be overridden — stale 404s block post-upload reads."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.get(
            "/public-bucket/foo.txt",
            headers={"x-test-status": "500", "x-test-anon-read": "true"},
        )
    assert r.headers["Cache-Control"] == PRIVATE_CACHE_CONTROL


@pytest.mark.asyncio
async def test_404_gets_no_store(app: Any) -> None:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.get(
            "/public-bucket/foo.txt",
            headers={"x-test-status": "404"},
        )
    assert r.headers["Cache-Control"] == PRIVATE_CACHE_CONTROL


@pytest.mark.asyncio
async def test_403_gets_no_store(app: Any) -> None:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.get(
            "/public-bucket/foo.txt",
            headers={"x-test-status": "403"},
        )
    assert r.headers["Cache-Control"] == PRIVATE_CACHE_CONTROL


@pytest.mark.asyncio
async def test_304_response_still_gets_cache_control(app: Any) -> None:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.get(
            "/public-bucket/foo.txt",
            headers={"x-test-status": "304", "x-test-anon-read": "true"},
        )
    assert r.status_code == 304
    assert r.headers["Cache-Control"] == PUBLIC_CACHE_CONTROL


@pytest.mark.asyncio
async def test_partial_content_206_gets_cache_control(app: Any) -> None:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.get(
            "/public-bucket/foo.txt",
            headers={"x-test-status": "206", "x-test-anon-read": "true"},
        )
    assert r.status_code == 206
    assert r.headers["Cache-Control"] == PUBLIC_CACHE_CONTROL


@pytest.mark.asyncio
async def test_bucket_listing_gets_private(app: Any) -> None:
    """GET on a bucket (no key) should never be marked publicly cacheable."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.get("/public-bucket", headers={"x-test-anon-read": "true"})
    assert r.headers["Cache-Control"] == PRIVATE_CACHE_CONTROL


@pytest.mark.asyncio
async def test_missing_flag_defaults_to_private(app: Any) -> None:
    """If acl middleware didn't wire the flag (e.g., request was a healthcheck), default private."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.get("/public-bucket/foo.txt")  # no x-test-anon-read header
    assert r.headers["Cache-Control"] == PRIVATE_CACHE_CONTROL


@pytest.mark.asyncio
async def test_warm_bucket_gets_long_max_age(app: Any) -> None:
    """is_cache_warm flag promotes the response to the 30d max-age policy."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.get(
            "/warm-bucket/foo.txt",
            headers={"x-test-anon-read": "true", "x-test-warm": "true"},
        )
    assert r.headers["Cache-Control"] == WARM_PUBLIC_CACHE_CONTROL


@pytest.mark.asyncio
async def test_warm_private_emits_warm_policy_with_visibility_sentinel(app: Any) -> None:
    """warm=true + anon_read=false: ATS stores under WARM_PUBLIC, header_rewrite
    demotes Cache-Control on egress based on the X-Hippius-Visibility sentinel.
    Per-request auth gating is handled by ATS authproxy."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.get(
            "/warm-but-private/foo.txt",
            headers={"x-test-warm": "true"},
        )
    assert r.headers["Cache-Control"] == WARM_PUBLIC_CACHE_CONTROL
    assert r.headers[VISIBILITY_HEADER] == "private"


@pytest.mark.asyncio
async def test_signed_response_cache_control_override_preserved(app: Any) -> None:
    """A signed request with ?response-cache-control= must keep upstream Cache-Control."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.get(
            "/private-bucket/foo.txt?response-cache-control=no-cache%2C%20max-age%3D0",
            headers={
                "x-test-auth-method": "access_key",
                "x-test-upstream-cc": "no-cache, max-age=0",
            },
        )
    assert r.headers["Cache-Control"] == "no-cache, max-age=0"


@pytest.mark.asyncio
async def test_anonymous_cannot_smuggle_response_cache_control(app: Any) -> None:
    """An anonymous request carrying ?response-cache-control= must be ignored."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.get(
            "/private-bucket/foo.txt?response-cache-control=public%2C%20max-age%3D999999",
            headers={"x-test-upstream-cc": "public, max-age=999999"},
        )
    assert r.headers["Cache-Control"] == PRIVATE_CACHE_CONTROL


@pytest.mark.asyncio
async def test_signed_request_without_override_uses_default_policy(app: Any) -> None:
    """A signed request that does NOT carry response-cache-control still gets the gateway's policy."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.get(
            "/private-bucket/foo.txt",
            headers={"x-test-auth-method": "access_key"},
        )
    assert r.headers["Cache-Control"] == PRIVATE_CACHE_CONTROL


@pytest.mark.asyncio
async def test_warm_head_gets_long_max_age(app: Any) -> None:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.head(
            "/warm-bucket/foo.txt",
            headers={"x-test-anon-read": "true", "x-test-warm": "true"},
        )
    assert r.headers["Cache-Control"] == WARM_PUBLIC_CACHE_CONTROL


@pytest.mark.asyncio
async def test_warm_304_keeps_warm_policy(app: Any) -> None:
    """Revalidation responses for warm objects must keep the long max-age — otherwise ATS would re-store with a shorter TTL."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.get(
            "/warm-bucket/foo.txt",
            headers={"x-test-status": "304", "x-test-anon-read": "true", "x-test-warm": "true"},
        )
    assert r.status_code == 304
    assert r.headers["Cache-Control"] == WARM_PUBLIC_CACHE_CONTROL


@pytest.mark.asyncio
async def test_warm_head_304_keeps_warm_policy(app: Any) -> None:
    """HEAD revalidations on warm objects must also propagate the long max-age, mirroring GET."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.head(
            "/warm-bucket/foo.txt",
            headers={"x-test-status": "304", "x-test-anon-read": "true", "x-test-warm": "true"},
        )
    assert r.status_code == 304
    assert r.headers["Cache-Control"] == WARM_PUBLIC_CACHE_CONTROL


@pytest.mark.asyncio
async def test_warm_206_keeps_warm_policy(app: Any) -> None:
    """Range responses on warm objects must keep the long max-age."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.get(
            "/warm-bucket/foo.txt",
            headers={"x-test-status": "206", "x-test-anon-read": "true", "x-test-warm": "true"},
        )
    assert r.status_code == 206
    assert r.headers["Cache-Control"] == WARM_PUBLIC_CACHE_CONTROL


@pytest.mark.asyncio
async def test_warm_404_still_gets_no_store(app: Any) -> None:
    """Negative-caching protection wins over warm flag — a stale 404 in ATS would block writes."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.get(
            "/warm-bucket/missing.txt",
            headers={"x-test-status": "404", "x-test-anon-read": "true", "x-test-warm": "true"},
        )
    assert r.headers["Cache-Control"] == PRIVATE_CACHE_CONTROL


@pytest.mark.asyncio
async def test_warm_500_still_gets_no_store(app: Any) -> None:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.get(
            "/warm-bucket/foo.txt",
            headers={"x-test-status": "500", "x-test-anon-read": "true", "x-test-warm": "true"},
        )
    assert r.headers["Cache-Control"] == PRIVATE_CACHE_CONTROL


@pytest.mark.asyncio
async def test_warm_put_gets_no_cache_control_header(app: Any) -> None:
    """Writes never get a Cache-Control header, even on warm buckets."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.put(
            "/warm-bucket/foo.txt",
            content=b"x",
            headers={"x-test-anon-read": "true", "x-test-warm": "true"},
        )
    assert "Cache-Control" not in r.headers


@pytest.mark.asyncio
async def test_warm_bucket_listing_gets_private(app: Any) -> None:
    """GET on a bucket (no key) is never cacheable, even when warm-flagged."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.get(
            "/warm-bucket",
            headers={"x-test-anon-read": "true", "x-test-warm": "true"},
        )
    assert r.headers["Cache-Control"] == PRIVATE_CACHE_CONTROL


@pytest.mark.asyncio
async def test_warm_bucket_anon_disallowed_gets_warm_with_visibility(app: Any) -> None:
    """warm=true + private (per-object ACL or otherwise): cacheable for ATS,
    egress-demoted to private by ATS header_rewrite based on the sentinel."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.get(
            "/warm-bucket/foo.txt",
            headers={"x-test-warm": "true"},
        )
    assert r.headers["Cache-Control"] == WARM_PUBLIC_CACHE_CONTROL
    assert r.headers[VISIBILITY_HEADER] == "private"


@pytest.mark.asyncio
async def test_missing_warm_flag_defaults_to_public(app: Any) -> None:
    """If acl_middleware sets anon_read_allowed but never touches bucket_is_cache_warm,
    cache_control falls back to the regular PUBLIC policy (not WARM)."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        # x-test-warm header is omitted → request.state.bucket_is_cache_warm not set
        # but the fixture explicitly sets it to False from the header check, so we
        # exercise the same code path getattr() defaults to.
        r = await client.get(
            "/some-bucket/foo.txt",
            headers={"x-test-anon-read": "true"},
        )
    assert r.headers["Cache-Control"] == PUBLIC_CACHE_CONTROL


@pytest.mark.asyncio
async def test_warm_private_head_emits_warm_with_visibility(app: Any) -> None:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.head(
            "/warm-bucket/foo.txt",
            headers={"x-test-warm": "true"},
        )
    assert r.headers["Cache-Control"] == WARM_PUBLIC_CACHE_CONTROL
    assert r.headers[VISIBILITY_HEADER] == "private"


@pytest.mark.asyncio
async def test_warm_private_206_emits_warm_with_visibility(app: Any) -> None:
    """Range requests on warm-private must carry the same demotion sentinel."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.get(
            "/warm-bucket/foo.txt",
            headers={"x-test-status": "206", "x-test-warm": "true"},
        )
    assert r.status_code == 206
    assert r.headers["Cache-Control"] == WARM_PUBLIC_CACHE_CONTROL
    assert r.headers[VISIBILITY_HEADER] == "private"


@pytest.mark.asyncio
async def test_warm_private_304_emits_warm_with_visibility(app: Any) -> None:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.get(
            "/warm-bucket/foo.txt",
            headers={"x-test-status": "304", "x-test-warm": "true"},
        )
    assert r.status_code == 304
    assert r.headers["Cache-Control"] == WARM_PUBLIC_CACHE_CONTROL
    assert r.headers[VISIBILITY_HEADER] == "private"


@pytest.mark.asyncio
async def test_warm_private_404_no_visibility(app: Any) -> None:
    """Negative-caching protection wins: 404 on warm-private → no-store, no sentinel."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.get(
            "/warm-bucket/missing.txt",
            headers={"x-test-status": "404", "x-test-warm": "true"},
        )
    assert r.headers["Cache-Control"] == PRIVATE_CACHE_CONTROL
    assert VISIBILITY_HEADER not in r.headers


@pytest.mark.asyncio
async def test_warm_private_put_no_visibility_header(app: Any) -> None:
    """Writes never carry the visibility sentinel — ATS can't cache writes anyway."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.put(
            "/warm-bucket/foo.txt",
            content=b"x",
            headers={"x-test-warm": "true"},
        )
    assert "Cache-Control" not in r.headers
    assert VISIBILITY_HEADER not in r.headers


@pytest.mark.asyncio
async def test_warm_public_no_visibility_header(app: Any) -> None:
    """Public warm responses must NOT carry the sentinel — clients should be able to cache."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.get(
            "/warm-public/foo.txt",
            headers={"x-test-anon-read": "true", "x-test-warm": "true"},
        )
    assert r.headers["Cache-Control"] == WARM_PUBLIC_CACHE_CONTROL
    assert VISIBILITY_HEADER not in r.headers


@pytest.mark.asyncio
async def test_cold_private_no_visibility_header(app: Any) -> None:
    """Cold private (default branch) stays no-store with no sentinel."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.get("/cold-private/foo.txt")
    assert r.headers["Cache-Control"] == PRIVATE_CACHE_CONTROL
    assert VISIBILITY_HEADER not in r.headers


@pytest.mark.asyncio
async def test_cold_public_no_visibility_header(app: Any) -> None:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.get(
            "/cold-public/foo.txt",
            headers={"x-test-anon-read": "true"},
        )
    assert r.headers["Cache-Control"] == PUBLIC_CACHE_CONTROL
    assert VISIBILITY_HEADER not in r.headers


@pytest.mark.asyncio
async def test_auth_probe_response_left_untouched(app: Any) -> None:
    """When auth_probe_middleware has flagged the request as a probe (state set
    after secret-match), cache_control_middleware must NOT mutate the response.
    ATS doesn't cache subrequest responses; stray headers would just pollute logs."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.get(
            "/warm-bucket/foo.txt",
            headers={
                "x-test-is-auth-probe": "true",
                "x-test-anon-read": "true",
                "x-test-warm": "true",
            },
        )
    assert "Cache-Control" not in r.headers
    assert VISIBILITY_HEADER not in r.headers


@pytest.mark.asyncio
async def test_non_probe_request_with_smuggled_header_still_gets_warm_treatment(app: Any) -> None:
    """The cache_control skip is keyed off request.state.is_auth_probe (set
    only by auth_probe_middleware after secret-match), NOT off the raw header.
    A client smuggling X-Hippius-Auth-Probe with a guess value never flips
    is_auth_probe, so cache_control treats the request normally."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.get(
            "/warm-bucket/foo.txt",
            headers={
                "x-hippius-auth-probe": "1",  # smuggled — no secret matches
                "x-test-warm": "true",
            },
        )
    # Normal warm-private treatment applies — auth_probe state was never set.
    assert r.headers["Cache-Control"] == WARM_PUBLIC_CACHE_CONTROL
    assert r.headers[VISIBILITY_HEADER] == "private"
