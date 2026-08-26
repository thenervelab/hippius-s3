"""An ACL-bypassed route must not hand the S3 handlers an unauthorised request.

`/health` is a real route AND a valid `/{bucket_name}` shape. The health route is GET-only, but
the S3 routers bind DELETE and POST on that shape, so an ACL bypass written as "any method on
this exact path" let unauthenticated callers reach `handle_delete_bucket` (soft-delete),
`tags_delete_bucket_tags` and `handle_delete_objects` — none of which carry an ownership
predicate, because they are documented as relying on the check this bypass skipped.

`/user/` and `/admin/` are prefix-matched and sit behind their own HMAC middlewares, so their
bypass is backstopped. `/health` has no such backstop, which is why only it is method-scoped.
"""

from typing import Any
from unittest.mock import AsyncMock

import pytest
from fastapi import FastAPI
from httpx import ASGITransport
from httpx import AsyncClient

from hippius_s3.gateway.middlewares.acl import acl_middleware
from hippius_s3.gateway.services.acl_service import BucketLookup
from tests.unit.gateway._suspension_fakes import install_no_suspension_state


OWNER = "5EvT2ccmmY6t3q1U3PXwjzwFBjE2KzvWdC6mMsCvBbiBDs55"


def _app(caller: str | None = None) -> tuple[Any, Any, list[str]]:
    reached: list[str] = []
    service = AsyncMock()
    service.get_bucket_owner_and_id = AsyncMock(
        return_value=BucketLookup(owner_id=OWNER, bucket_id="health-id", is_cache_warm=False)
    )
    service.check_permission = AsyncMock(return_value=False)

    app = FastAPI()
    app.state.acl_service = service
    install_no_suspension_state(app)

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "healthy"}

    @app.api_route("/{bucket}", methods=["DELETE", "POST", "PUT", "HEAD"])
    async def bucket_ops(bucket: str) -> dict[str, str]:
        reached.append(bucket)
        return {"bucket": bucket}

    async def _stamp(request: Any, call_next: Any) -> Any:
        if caller is not None:
            request.state.account_id = caller
        return await call_next(request)

    app.middleware("http")(acl_middleware)
    app.middleware("http")(_stamp)
    return app, service, reached


class TestHealthBypassIsMethodScoped:
    @pytest.mark.parametrize("method", ["DELETE", "POST"])
    @pytest.mark.asyncio
    async def test_destructive_methods_on_health_are_authorised(self, method: str) -> None:
        """These reached the destructive bucket handlers with no auth and no ACL check."""
        app, service, reached = _app()
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
            r = await c.request(method, "/health")

        assert r.status_code == 403
        assert reached == [], "an unauthorised caller must not reach the S3 bucket handler"
        service.check_permission.assert_awaited()

    @pytest.mark.parametrize("query", ["", "?tagging", "?delete"])
    @pytest.mark.asyncio
    async def test_subresource_spellings_are_also_authorised(self, query: str) -> None:
        app, _, reached = _app()
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
            r = await c.delete(f"/health{query}")
        assert r.status_code == 403
        assert reached == []

    @pytest.mark.asyncio
    async def test_get_health_still_bypasses(self) -> None:
        """The liveness probe must not start paying for a bucket lookup or an ACL check."""
        app, service, _ = _app()
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
            r = await c.get("/health")
        assert r.status_code == 200
        service.check_permission.assert_not_awaited()
        service.get_bucket_owner_and_id.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_head_health_still_bypasses(self) -> None:
        app, service, _ = _app()
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
            r = await c.head("/health")
        assert r.status_code in (200, 405)
        service.check_permission.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_the_owner_can_still_operate_on_a_health_bucket(self) -> None:
        """Method-scoping must authorise, not blanket-deny — a real owner still gets through."""
        app, service, reached = _app(caller=OWNER)
        service.check_permission = AsyncMock(return_value=True)
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
            r = await c.delete("/health")
        assert r.status_code == 200
        assert reached == ["health"]

    @pytest.mark.asyncio
    async def test_user_and_admin_prefixes_still_bypass_on_every_method(self) -> None:
        """Their HMAC middlewares are the backstop; narrowing them here would break them."""
        app, service, _ = _app()

        @app.router.delete("/user/{rest:path}")
        async def _u(rest: str) -> dict[str, str]:
            return {"ok": rest}

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
            r = await c.delete("/user/profile")
        assert r.status_code == 200
        service.check_permission.assert_not_awaited()
