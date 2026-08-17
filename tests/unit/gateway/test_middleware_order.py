"""GW-2: ray_id must be stamped before the auth/acl/account middlewares.

`gateway_overhead_ms` (and the forwarded `X-Hippius-Gateway-Time-Ms`) is measured from
`request.state.gateway_start_time`, which `ray_id_middleware` stamps. If ray_id runs
second-innermost (as it did before this fix) the metric excludes auth/acl/account and every
outer middleware logs `"no-ray-id"`. ray_id must therefore run just inside CORS — outer to the
whole chain it should measure, but still inner to CORS so CORS keeps wrapping error responses.
"""

from __future__ import annotations

from typing import Any


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
    from gateway.main import factory

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


def test_suspension_runs_after_auth_router_and_outer_to_account_acl() -> None:
    """Issue #421: suspension must see resolved identity (inner to auth_router) but run
    OUTER to account/acl — master tokens bypass ACL on their own buckets, and
    account_middleware clobbers account_id for bearer auth."""
    from gateway.main import factory

    order = _dispatch_order(factory())

    def idx(name: str) -> int:
        assert name in order, f"{name} not registered; order={order}"
        return order.index(name)

    assert idx("auth_router_middleware") < idx("suspension_middleware"), order
    assert idx("suspension_middleware") < idx("account_middleware"), order
    assert idx("suspension_middleware") < idx("acl_middleware"), order


def test_admin_hmac_is_registered() -> None:
    from gateway.main import factory

    order = _dispatch_order(factory())
    assert "verify_admin_hmac_middleware" in order, order
