"""GW-2: ray_id must be stamped before the auth/acl/account middlewares.

`gateway_overhead_ms` (and the forwarded `X-Hippius-Gateway-Time-Ms`) is measured from
`request.state.gateway_start_time`, which `ray_id_middleware` stamps. If ray_id runs
second-innermost (as it did before this fix) the metric excludes auth/acl/account and every
outer middleware logs `"no-ray-id"`. ray_id must therefore run just inside CORS — outer to the
whole chain it should measure, but still inner to CORS so CORS keeps wrapping error responses.
"""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI
from fastapi import Request


def _dispatch_order(app: Any) -> list[str | None]:
    # Starlette inserts each registered middleware at index 0, so user_middleware[0] is the
    # LAST-registered = OUTERMOST = first to run on the request path. app.middleware("http")(fn)
    # registers BaseHTTPMiddleware with the function passed as the `dispatch` kwarg.
    names: list[str | None] = []
    for mw in app.user_middleware:
        fn = (getattr(mw, "kwargs", {}) or {}).get("dispatch")
        if fn is None:
            for candidate in getattr(mw, "args", ()):
                if callable(candidate) and getattr(candidate, "__name__", "").endswith("_middleware"):
                    fn = candidate
                    break
        names.append(getattr(fn, "__name__", None))
    return names


def test_ray_id_runs_before_auth_acl_account_and_inside_cors() -> None:
    from hippius_s3.main import factory

    order = _dispatch_order(factory())

    def idx(name: str) -> int:
        assert name in order, f"{name} not registered; order={order}"
        return order.index(name)

    # Lower index == outer == runs earlier on the request path. ray_id must precede the
    # middlewares whose latency gateway_overhead_ms is meant to include.
    assert idx("ray_id_middleware") < idx("auth_router_middleware"), order
    assert idx("ray_id_middleware") < idx("acl_middleware"), order
    assert idx("ray_id_middleware") < idx("account_middleware"), order
    # CORS stays the absolute outermost so it still wraps error responses (incl. the ray_id header).
    assert idx("cors_middleware") < idx("ray_id_middleware"), order


def test_merged_chain_keeps_auth_outside_the_trusting_middlewares() -> None:
    """Post gateway/api merge, the app no longer has an internal trusted surface —
    every middleware/handler that used to trust gateway-stamped headers is one
    ordering mistake away from the internet. Pin the orderings that make the
    trust structural.
    """
    from hippius_s3.main import factory

    order = _dispatch_order(factory())

    def idx(name: str) -> int:
        assert name in order, f"{name} not registered; order={order}"
        return order.index(name)

    # Lower index == outer == runs earlier on the request path.
    # auth_probe answers 200 to the shared-secret PURGE bounce; acl_subresource
    # serves/creates ACLs. Neither may run before auth_router+acl have validated.
    assert idx("auth_router_middleware") < idx("auth_probe_middleware"), order
    assert idx("acl_middleware") < idx("auth_probe_middleware"), order
    assert idx("auth_router_middleware") < idx("acl_subresource_middleware"), order
    assert idx("acl_middleware") < idx("acl_subresource_middleware"), order
    # request_context derives main_account_id from what account+acl resolved,
    # so it must be inner to both and outer to everything that consumes the account
    # with bucket-owner semantics (metrics, audit, the routers).
    assert idx("acl_middleware") < idx("request_context_middleware"), order
    assert idx("account_middleware") < idx("request_context_middleware"), order
    assert idx("request_context_middleware") < idx("metrics_middleware"), order
    # Pressure shedding stays outside auth: a shed PUT must cost no SigV4/Arion work.
    assert idx("fs_cache_pressure_middleware") < idx("auth_router_middleware"), order


def test_path_normalization_is_outermost_data_plane_view() -> None:
    """The one-view rule: scope["path"] is normalized before any layer reads it, and
    the audit log sits outer to request_context (caller attribution) while metrics sits
    inner (bucket-owner attribution)."""
    from hippius_s3.main import factory

    order = _dispatch_order(factory())

    def idx(name: str) -> int:
        assert name in order, f"{name} not registered; order={order}"
        return order.index(name)

    assert idx("path_normalization_middleware") < idx("input_validation_middleware"), order
    assert idx("path_normalization_middleware") < idx("auth_router_middleware"), order
    assert idx("path_normalization_middleware") < idx("cache_control_middleware"), order
    assert idx("audit_log_middleware") < idx("request_context_middleware"), order
    assert idx("request_context_middleware") < idx("metrics_middleware"), order


def test_router_and_security_share_one_path_view() -> None:
    """`PUT /victim/../other/key` must route as `other/key`, matching what auth/acl
    judged — the split-view was an auth bypass (security judged the collapsed path,
    the router acted on the raw one). Driven with a hand-built ASGI scope because
    httpx collapses dot segments client-side and would mask the divergence."""
    import asyncio

    from gateway.middlewares.path_normalization import path_normalization_middleware

    app = FastAPI()
    seen: dict = {}

    @app.get("/{bucket}/{key:path}")
    async def probe(request: Request, bucket: str, key: str) -> dict:
        seen["bucket"] = bucket
        seen["key"] = key
        seen["scope_path"] = request.scope["path"]
        return {"ok": True}

    app.middleware("http")(path_normalization_middleware)

    raw = b"/victim-bucket/../other-bucket/key.txt"
    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": raw.decode(),
        "raw_path": raw,
        "query_string": b"",
        "root_path": "",
        "headers": [(b"host", b"test")],
        "client": ("127.0.0.1", 1234),
        "server": ("test", 80),
    }
    status: dict = {}
    body: list = []

    async def receive() -> dict:
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message: dict) -> None:
        if message["type"] == "http.response.start":
            status["code"] = message["status"]
        if message["type"] == "http.response.body":
            body.append(message.get("body", b""))

    asyncio.get_event_loop_policy().new_event_loop().run_until_complete(app(scope, receive, send))
    assert status["code"] == 200, (status, b"".join(body)[:300])
    assert seen["bucket"] == "other-bucket", seen
    assert seen["key"] == "key.txt", seen
    assert seen["scope_path"] == "/other-bucket/key.txt", seen
