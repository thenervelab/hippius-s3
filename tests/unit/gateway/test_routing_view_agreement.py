"""Every layer that decides something from the path must judge the path the api will receive.

`ForwardService` hands httpx a URL string built from the decoded scope path, and httpx rewrites it
(collapses `.`/`..`, truncates at `#`/`?`) before it leaves the gateway. So there are two paths for
every request — the one the client sent and the one the api routes on — and any layer judging the
former is deciding about a different request than the one it lets through.

`/docs/../anybucket/key.txt` is the shape that matters. Judged as sent its first segment is `docs`,
which is on auth_router's exempt list, in account.py's credit-skip list, and (via
`request.url.path`) the bucket acl.py evaluates. Judged as forwarded it is `/anybucket/key.txt`. The
consequences, all live before this: authentication skipped entirely and the request processed as
anonymous; ACL evaluated against the ownerless prod bucket `docs` while the api served `anybucket`;
the credit gate skipped; and, on the `/user/` arm, the frontend HMAC check skipped for a request the
api answers from a frontend endpoint.

These tests build the ASGI scope by hand rather than going through httpx's AsyncClient, because that
client normalizes dot segments itself — a test that asked it for `/docs/../anybucket/key` would
never send the shape being tested.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from urllib.parse import unquote

import pytest
from fastapi import Request
from fastapi import Response

from hippius_s3.gateway.middlewares.account import account_middleware
from hippius_s3.gateway.middlewares.acl import acl_middleware
from hippius_s3.gateway.middlewares.auth_router import EXEMPT_SEGMENTS
from hippius_s3.gateway.middlewares.auth_router import auth_router_middleware
from hippius_s3.gateway.middlewares.frontend_hmac import verify_frontend_hmac_middleware
from hippius_s3.gateway.services.acl_service import BucketLookup


# Each is a client-sent raw path whose first segment reads as an exempt/skip route, paired with the
# path the api is actually handed. `%2E%2E` is the encoded spelling — uvicorn decodes it before the
# forwarder ever sees it, so it takes exactly the same route as literal `..`.
TRAVERSALS_OUT_OF_EXEMPT_ROUTES = [
    (b"/docs/%2E%2E/anybucket/key.txt", "/anybucket/key.txt"),
    (b"/docs/../anybucket/key.txt", "/anybucket/key.txt"),
    (b"/health/%2E%2E/anybucket/key.txt", "/anybucket/key.txt"),
    (b"/metrics/%2E%2E/anybucket/key.txt", "/anybucket/key.txt"),
    (b"/robots.txt/%2E%2E/anybucket/key.txt", "/anybucket/key.txt"),
    (b"/openapi.json/%2E%2E/anybucket/key.txt", "/anybucket/key.txt"),
    (b"/user/%2E%2E/anybucket/key.txt", "/anybucket/key.txt"),
]

# The routes the exemptions exist for. None contains a dot segment or a delimiter, so the routing
# view is the identity for all of them and every one must keep working unauthenticated.
EXEMPT_ROUTES = [
    b"/health",
    b"/docs",
    b"/docs/oauth2-redirect",
    b"/openapi.json",
    b"/robots.txt",
    b"/metrics",
]


def _request(raw_path: bytes, method: str = "GET", headers: list[tuple[bytes, bytes]] | None = None) -> Request:
    """The scope uvicorn builds: `raw_path` as sent, `path` decoded from it."""
    return Request(
        {
            "type": "http",
            "http_version": "1.1",
            "method": method,
            "scheme": "https",
            "server": ("s3.hippius.com", 443),
            "path": unquote(raw_path.decode()),
            "raw_path": raw_path,
            "root_path": "",
            "query_string": b"",
            "headers": headers or [],
            "app": MagicMock(),
        }
    )


async def _call_next(request: Request) -> Response:
    return Response(status_code=200)


class TestAuthRouterExemption:
    """`_is_exempt` decides whether `authenticate_request` runs at all."""

    @pytest.fixture
    def authenticate(self, monkeypatch: pytest.MonkeyPatch) -> AsyncMock:
        result = MagicMock(error_response=None, auth_method="anonymous")
        mock = AsyncMock(return_value=result)
        monkeypatch.setattr("hippius_s3.gateway.middlewares.auth_router.authenticate_request", mock)
        return mock

    @pytest.mark.asyncio
    @pytest.mark.parametrize(("raw_path", "api_path"), TRAVERSALS_OUT_OF_EXEMPT_ROUTES)
    async def test_a_traversal_out_of_an_exempt_route_is_authenticated(
        self, authenticate: AsyncMock, raw_path: bytes, api_path: str
    ) -> None:
        """The bypass: exempt as sent, an ordinary S3 request as forwarded.

        Skipping authentication here is not a soft failure — the request continues as `anonymous`,
        which is how anonymous-owned buckets got written in prod (2026-08-03).
        """
        await auth_router_middleware(_request(raw_path), _call_next)

        assert authenticate.await_count == 1, f"{raw_path!r} reaches the api as {api_path} and must be authenticated"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("raw_path", EXEMPT_ROUTES)
    async def test_every_exempt_route_still_skips_authentication(
        self, authenticate: AsyncMock, raw_path: bytes
    ) -> None:
        """The exemptions exist so these work with no credentials; one test per route."""
        await auth_router_middleware(_request(raw_path), _call_next)

        assert authenticate.await_count == 0, f"{raw_path!r} must stay exempt"

    @pytest.mark.asyncio
    async def test_the_user_subpath_exemption_still_requires_a_subpath(self, authenticate: AsyncMock) -> None:
        """`/user` alone is bucket-shaped, not a route, and must not be exempt."""
        await auth_router_middleware(_request(b"/user"), _call_next)
        assert authenticate.await_count == 1

        await auth_router_middleware(_request(b"/user/profile"), _call_next)
        assert authenticate.await_count == 1

    @pytest.mark.asyncio
    async def test_a_subpath_that_is_not_forwarded_does_not_grant_the_exemption(self, authenticate: AsyncMock) -> None:
        """`/user#/x` is forwarded as `/user` — bucket-shaped — so the `#` must not buy a subpath."""
        await auth_router_middleware(_request(b"/user%23/x"), _call_next)

        assert authenticate.await_count == 1

    @pytest.mark.asyncio
    async def test_a_bucket_merely_prefixed_with_an_exempt_name_is_authenticated(self, authenticate: AsyncMock) -> None:
        """Segment equality, not prefix — the `docs2` hole (prod, 2026-08-03)."""
        await auth_router_middleware(_request(b"/docs2/key.txt"), _call_next)

        assert authenticate.await_count == 1

    def test_the_exempt_list_is_unchanged_by_this_fix(self) -> None:
        """Pins the blast radius: this change is about WHICH path is judged, not which names are."""
        assert EXEMPT_SEGMENTS == frozenset({"docs", "openapi.json", "robots.txt", "metrics", "health"})


class TestAclJudgesTheForwardedBucket:
    @pytest.fixture
    def acl_service(self) -> Any:
        service = AsyncMock()
        service.get_bucket_owner_and_id = AsyncMock(
            return_value=BucketLookup(owner_id="owner-id", bucket_id="bucket-id", is_cache_warm=False)
        )
        service.check_permission = AsyncMock(return_value=True)
        return service

    def _request_with_acl(self, raw_path: bytes, acl_service: Any) -> Request:
        request = _request(raw_path)
        request.scope["app"].state.acl_service = acl_service
        request.scope["app"].state.redis_client = AsyncMock()
        return request

    @pytest.mark.asyncio
    async def test_the_evaluated_bucket_is_the_one_the_api_will_serve(self, acl_service: Any) -> None:
        """ACL judged `docs` — ownerless in prod — for a request the api answers from `anybucket`.

        Permissions granted on the wrong bucket are worse than none: the decision is made about an
        object nobody asked for.
        """
        await acl_middleware(self._request_with_acl(b"/docs/%2E%2E/anybucket/key.txt", acl_service), _call_next)

        acl_service.get_bucket_owner_and_id.assert_awaited_once_with("anybucket")
        assert acl_service.check_permission.await_args.kwargs["bucket"] == "anybucket"
        assert acl_service.check_permission.await_args.kwargs["key"] == "key.txt"

    @pytest.mark.asyncio
    async def test_the_bucket_and_key_stamped_on_request_state_match_the_api(self, acl_service: Any) -> None:
        """Downstream (audit log, ATS purge, the forwarded headers) reads these."""
        request = self._request_with_acl(b"/anybucket/a/%2E%2E/b.txt", acl_service)

        await acl_middleware(request, _call_next)

        assert request.state.s3_bucket == "anybucket"
        assert request.state.s3_key == "b.txt"

    @pytest.mark.asyncio
    async def test_health_and_user_still_skip_the_acl_check(self, acl_service: Any) -> None:
        for raw_path in (b"/health", b"/user/profile"):
            await acl_middleware(self._request_with_acl(raw_path, acl_service), _call_next)

        acl_service.check_permission.assert_not_awaited()


class TestAccountCreditGate:
    @pytest.mark.asyncio
    async def test_a_traversal_out_of_docs_does_not_skip_the_credit_gate(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """`/docs/../anybucket/key` matched the `/docs/` skip and took an S3 write past billing.

        The skip returns before the identity branches run, so whether `account_id` was stamped is
        the observable difference between "treated as a docs request" and "treated as S3".
        """
        monkeypatch.setattr("hippius_s3.gateway.middlewares.account.config", MagicMock(enable_bypass_credit_check=False))
        request = _request(b"/docs/%2E%2E/anybucket/key.txt", method="PUT")
        request.state.auth_method = None

        await account_middleware(request, _call_next)

        assert request.state.account_id == "anonymous", "an S3 write must reach the identity branch, not the skip"

    @pytest.mark.asyncio
    async def test_the_real_docs_route_still_skips_the_credit_gate(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The other direction, so the fix cannot be "stop skipping" — /docs must still bypass."""
        monkeypatch.setattr("hippius_s3.gateway.middlewares.account.config", MagicMock(enable_bypass_credit_check=False))
        request = _request(b"/docs/oauth2-redirect")

        response = await account_middleware(request, _call_next)

        assert response.status_code == 200
        assert getattr(request.state, "account_id", None) is None, "/docs must return via the skip"


class TestFrontendHmacGate:
    """acl.py and account.py both skip their checks for `/user/` on the routing view, so this is
    the only layer left in front of the frontend endpoints."""

    @pytest.mark.asyncio
    async def test_a_traversal_into_user_still_requires_a_signature(self) -> None:
        """`/x/../user/foo` did not start with `/user/` as sent, so HMAC was skipped — and the api
        was handed `/user/foo` regardless."""
        response = await verify_frontend_hmac_middleware(_request(b"/x/%2E%2E/user/profile"), _call_next)

        assert response.status_code == 401

    @pytest.mark.asyncio
    async def test_a_traversal_out_of_user_is_not_treated_as_a_frontend_call(self) -> None:
        """The converse: `/user/../bucket/key` is forwarded as an ordinary S3 request, so demanding
        a frontend signature for it would break a legitimate (if odd) S3 path."""
        response = await verify_frontend_hmac_middleware(_request(b"/user/%2E%2E/bucket/key.txt"), _call_next)

        assert response.status_code == 200

    @pytest.mark.asyncio
    async def test_an_ordinary_s3_path_still_needs_no_signature(self) -> None:
        response = await verify_frontend_hmac_middleware(_request(b"/bucket/key.txt"), _call_next)

        assert response.status_code == 200
