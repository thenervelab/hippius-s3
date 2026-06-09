"""Tests for ats_purge_middleware — checks that PURGE calls are dispatched correctly per HTTP verb."""

from typing import Any

import pytest
from fastapi import FastAPI
from fastapi import Request
from fastapi import Response
from httpx import ASGITransport
from httpx import AsyncClient

from gateway import config as gateway_config
from gateway.middlewares.ats_purge import ats_purge_middleware


@pytest.fixture(autouse=True)  # type: ignore[misc]
def _ats_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ATS_CACHE_ENDPOINT", "http://ats.local:8080")
    gateway_config._config = None
    yield
    gateway_config._config = None


@pytest.fixture  # type: ignore[misc]
def captured_purges(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, str]]:
    calls: list[tuple[str, str]] = []

    def fake_schedule_purge(host: str, key: str) -> None:
        calls.append((host, key))

    monkeypatch.setattr("gateway.middlewares.ats_purge.schedule_purge", fake_schedule_purge)
    return calls


@pytest.fixture  # type: ignore[misc]
def app(captured_purges: list[tuple[str, str]]) -> Any:
    app = FastAPI()
    app.middleware("http")(ats_purge_middleware)

    @app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "HEAD", "PATCH"])
    async def catch_all(request: Request) -> Response:
        status = int(request.headers.get("x-test-status", "200"))
        return Response(status_code=status, content=b"ok")

    return app


@pytest.mark.asyncio
async def test_put_object_purges_key(app: Any, captured_purges: list[tuple[str, str]]) -> None:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://s3.hippius.com") as client:
        r = await client.put("/mybucket/my/key.bin", content=b"data")
    assert r.status_code == 200
    assert captured_purges == [("s3.hippius.com", "mybucket/my/key.bin")]


@pytest.mark.asyncio
async def test_delete_object_purges_key(app: Any, captured_purges: list[tuple[str, str]]) -> None:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://s3.hippius.com") as client:
        r = await client.delete("/mybucket/foo.txt")
    assert r.status_code == 200
    assert captured_purges == [("s3.hippius.com", "mybucket/foo.txt")]


@pytest.mark.asyncio
async def test_copy_object_purges_only_destination(app: Any, captured_purges: list[tuple[str, str]]) -> None:
    """COPY reads the source but only mutates the destination — only purge the dest key.

    Purging the source would needlessly cold its cache entry for a read-only operation.
    """
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://s3.hippius.com") as client:
        r = await client.put(
            "/destbucket/dest/key",
            content=b"",
            headers={"x-amz-copy-source": "/srcbucket/src/key"},
        )
    assert r.status_code == 200
    assert captured_purges == [("s3.hippius.com", "destbucket/dest/key")]


@pytest.mark.asyncio
async def test_complete_multipart_upload_purges_key(app: Any, captured_purges: list[tuple[str, str]]) -> None:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://s3.hippius.com") as client:
        r = await client.post("/mybucket/big/obj?uploadId=abc123", content=b"<CompleteMultipartUpload/>")
    assert r.status_code == 200
    assert captured_purges == [("s3.hippius.com", "mybucket/big/obj")]


@pytest.mark.asyncio
async def test_part_upload_does_not_purge(app: Any, captured_purges: list[tuple[str, str]]) -> None:
    """UploadPart (PUT with uploadId + partNumber) is invisible until CompleteMultipartUpload."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://s3.hippius.com") as client:
        r = await client.put("/mybucket/big/obj?uploadId=abc&partNumber=1", content=b"part-data")
    assert r.status_code == 200
    assert captured_purges == []


@pytest.mark.asyncio
async def test_post_with_partNumber_does_not_purge(app: Any, captured_purges: list[tuple[str, str]]) -> None:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://s3.hippius.com") as client:
        r = await client.post("/mybucket/big/obj?uploadId=abc&partNumber=2", content=b"")
    assert r.status_code == 200
    assert captured_purges == []


@pytest.mark.asyncio
async def test_batch_delete_does_not_purge_in_v1(app: Any, captured_purges: list[tuple[str, str]]) -> None:
    """`POST /{bucket}?delete` is deliberately skipped in v1 — see plan."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://s3.hippius.com") as client:
        r = await client.post("/mybucket?delete", content=b"<Delete/>")
    assert r.status_code == 200
    assert captured_purges == []


@pytest.mark.asyncio
async def test_bucket_acl_flip_does_not_fire_wildcard_purge(app: Any, captured_purges: list[tuple[str, str]]) -> None:
    """Stock ATS HTTP PURGE doesn't support globs — bucket-level invalidation is a no-op.

    Objects age out within the 5-min TTL; regex_revalidate plugin could close this gap later.
    """
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://s3.hippius.com") as client:
        r = await client.put("/mybucket?acl", content=b"<AccessControlPolicy/>")
    assert r.status_code == 200
    assert captured_purges == []


@pytest.mark.asyncio
async def test_bucket_delete_does_not_fire_wildcard_purge(app: Any, captured_purges: list[tuple[str, str]]) -> None:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://s3.hippius.com") as client:
        r = await client.delete("/mybucket")
    assert r.status_code == 200
    assert captured_purges == []


@pytest.mark.asyncio
async def test_failed_write_does_not_purge(app: Any, captured_purges: list[tuple[str, str]]) -> None:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://s3.hippius.com") as client:
        r = await client.put("/mybucket/k", content=b"x", headers={"x-test-status": "500"})
    assert r.status_code == 500
    assert captured_purges == []


@pytest.mark.asyncio
async def test_client_error_does_not_purge(app: Any, captured_purges: list[tuple[str, str]]) -> None:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://s3.hippius.com") as client:
        r = await client.delete("/mybucket/k", headers={"x-test-status": "403"})
    assert r.status_code == 403
    assert captured_purges == []


@pytest.mark.asyncio
async def test_get_does_not_purge(app: Any, captured_purges: list[tuple[str, str]]) -> None:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://s3.hippius.com") as client:
        r = await client.get("/mybucket/k")
    assert r.status_code == 200
    assert captured_purges == []


@pytest.mark.asyncio
async def test_head_does_not_purge(app: Any, captured_purges: list[tuple[str, str]]) -> None:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://s3.hippius.com") as client:
        r = await client.head("/mybucket/k")
    assert r.status_code == 200
    assert captured_purges == []


@pytest.mark.asyncio
async def test_root_path_does_not_purge(app: Any, captured_purges: list[tuple[str, str]]) -> None:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://s3.hippius.com") as client:
        r = await client.put("/", content=b"")
    assert r.status_code == 200
    assert captured_purges == []


@pytest.mark.asyncio
async def test_host_header_propagated(app: Any, captured_purges: list[tuple[str, str]]) -> None:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://s3-staging.hippius.com") as client:
        r = await client.put("/mybucket/k", content=b"x")
    assert r.status_code == 200
    assert captured_purges == [("s3-staging.hippius.com", "mybucket/k")]


@pytest.mark.asyncio
async def test_x_forwarded_host_wins_over_inbound_host(app: Any, captured_purges: list[tuple[str, str]]) -> None:
    """In prod the on-wire Host is the upstream rewritten by ATS — the public
    Host that built the cache key lives on x-forwarded-host. Must use that."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://192.168.1.199:30081") as client:
        r = await client.put(
            "/mybucket/k",
            content=b"x",
            headers={"x-forwarded-host": "s3.hippius.com"},
        )
    assert r.status_code == 200
    assert captured_purges == [("s3.hippius.com", "mybucket/k")]


@pytest.mark.asyncio
async def test_x_original_host_used_when_no_x_forwarded_host(app: Any, captured_purges: list[tuple[str, str]]) -> None:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://192.168.1.199:30081") as client:
        r = await client.put(
            "/mybucket/k",
            content=b"x",
            headers={"x-original-host": "eu-central-1.hippius.com"},
        )
    assert r.status_code == 200
    assert captured_purges == [("eu-central-1.hippius.com", "mybucket/k")]


@pytest.mark.asyncio
async def test_default_port_stripped_from_host(app: Any, captured_purges: list[tuple[str, str]]) -> None:
    """`s3.hippius.com:80` and `s3.hippius.com` must produce the same cache
    key — the cachekey.so plugin keys on the URL including port, but HTTP
    clients omit default ports in the URL the GET uses."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://x") as client:
        r = await client.put(
            "/mybucket/k",
            content=b"x",
            headers={"x-forwarded-host": "s3.hippius.com:80"},
        )
    assert r.status_code == 200
    assert captured_purges == [("s3.hippius.com", "mybucket/k")]


@pytest.mark.asyncio
async def test_falls_back_to_default_host_when_no_headers(
    app: Any, captured_purges: list[tuple[str, str]], monkeypatch: pytest.MonkeyPatch
) -> None:
    """For requests with no usable Host hint (in-cluster service DNS that
    doesn't map cleanly), fall back to s3.hippius.com so PURGEs at least hit
    a remap-mapped name instead of being rejected as ERR_INVALID_URL."""
    # ASGITransport always sends a Host header; simulate the no-host case by
    # invoking the middleware directly with a constructed scope.
    from fastapi import Request as _Request

    captured_purges.clear()
    scope = {
        "type": "http",
        "method": "PUT",
        "path": "/mybucket/k",
        "raw_path": b"/mybucket/k",
        "query_string": b"",
        "headers": [],  # no host at all
        "scheme": "http",
        "server": ("testserver", 80),
        "client": ("127.0.0.1", 1234),
        "state": {},
    }
    request = _Request(scope)

    async def _next(req: _Request) -> Response:
        return Response(status_code=200, content=b"ok")

    await ats_purge_middleware(request, _next)
    assert captured_purges == [("s3.hippius.com", "mybucket/k")]


@pytest.mark.asyncio
async def test_middleware_is_noop_when_endpoint_unset(
    app: Any, captured_purges: list[tuple[str, str]], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Early-return path: no PURGE scheduled when ATS_CACHE_ENDPOINT is empty."""
    monkeypatch.setenv("ATS_CACHE_ENDPOINT", "")
    gateway_config._config = None
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://s3.hippius.com") as client:
        r = await client.put("/mybucket/k", content=b"x")
    assert r.status_code == 200
    assert captured_purges == []
