"""After trailing-slash normalization, the security view and the served path must still agree.

`path_normalization` establishes one view for both the security layers and the router.
`trailing_slash_normalizer` is the only middleware that rewrites the path afterwards, so it is
the one place that invariant can be broken — and it was: it stripped the decoded `scope["path"]`
with `rstrip("/")` while stripping `raw_path` with `rstrip(b"/")`, which is a no-op when the
slash was sent as `%2F`. The router then bound key `obj.txt` while the ACL layer judged
`obj.txt/`, and since no object ACL exists under that name the check fell back to the BUCKET
ACL — a public-read bucket served an object whose own ACL denied the caller.

These tests assert the invariant (`routing_path(request) == scope["path"]`) rather than an
expected string, so any future rewrite of either side has to keep the two in step.
"""

from typing import Any

import pytest
from fastapi import FastAPI
from fastapi import Request
from httpx import ASGITransport
from httpx import AsyncClient

from hippius_s3.gateway.middlewares.path_normalization import path_normalization_middleware
from hippius_s3.gateway.middlewares.trailing_slash import _strip_trailing_slashes
from hippius_s3.gateway.middlewares.trailing_slash import trailing_slash_normalizer
from hippius_s3.gateway.utils.paths import routing_path


def _app() -> tuple[Any, list[dict]]:
    seen: list[dict] = []
    app = FastAPI()

    @app.api_route("/{full:path}", methods=["GET", "PUT"])
    async def catch_all(full: str, request: Request) -> dict[str, str]:
        seen.append(
            {
                "served": request.scope["path"],
                "security_view": routing_path(request),
                "raw": request.scope.get("raw_path", b"").decode(),
            }
        )
        return {"ok": "1"}

    app.middleware("http")(trailing_slash_normalizer)
    app.middleware("http")(path_normalization_middleware)
    return app, seen


async def _send(path: str) -> dict:
    app, seen = _app()
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        await c.get(path)
    return seen[0]


class TestStripHelper:
    @pytest.mark.parametrize(
        "raw,expected",
        [
            (b"/b/key/", b"/b/key"),
            (b"/b/key%2F", b"/b/key"),
            (b"/b/key%2f", b"/b/key"),
            (b"/b/key%2F%2F", b"/b/key"),
            (b"/b/key/%2F/", b"/b/key"),
            (b"/b/key", b"/b/key"),
            (b"/b/key%2Fmore", b"/b/key%2Fmore"),
            (b"/", b""),
            (b"/b/a%252F", b"/b/a%252F"),
        ],
    )
    def test_strips_both_spellings_and_nothing_else(self, raw: bytes, expected: bytes) -> None:
        assert _strip_trailing_slashes(raw) == expected

    def test_does_not_touch_an_encoded_slash_in_the_middle(self) -> None:
        """Only a TRAILING slash is a normalization concern; an inner one is part of the key."""
        assert _strip_trailing_slashes(b"/b/dir%2Ffile.txt") == b"/b/dir%2Ffile.txt"


class TestViewsAgree:
    @pytest.mark.parametrize(
        "path",
        [
            "/bucket/obj.txt%2F",
            "/bucket/obj.txt%2f",
            "/bucket/obj.txt/",
            "/bucket/obj.txt%2F%2F",
            "/bucket/dir%2Fobj.txt%2F",
            "/bucket%2F",
            "/bucket/",
            "/bucket/obj.txt",
            "/bucket/dir/obj.txt",
            "/",
        ],
    )
    @pytest.mark.asyncio
    async def test_security_view_equals_served_path(self, path: str) -> None:
        r = await _send(path)
        assert r["security_view"] == r["served"], (
            f"split view for {path!r}: router serves {r['served']!r} "
            f"but the security layers judge {r['security_view']!r}"
        )

    @pytest.mark.asyncio
    async def test_encoded_trailing_slash_resolves_to_the_bare_key(self) -> None:
        """The concrete bypass: `obj.txt%2F` must not be a second, unprotected name for obj.txt."""
        r = await _send("/bucket/obj.txt%2F")
        assert r["served"] == "/bucket/obj.txt"
        assert r["security_view"] == "/bucket/obj.txt"

    @pytest.mark.asyncio
    async def test_double_encoding_is_not_decoded_twice(self) -> None:
        """`%252F` is a literal `%2F` in the key, not a trailing slash — it must survive."""
        r = await _send("/bucket/a%252F")
        assert r["security_view"] == r["served"]
        assert r["served"].endswith("%2F")

    @pytest.mark.asyncio
    async def test_root_is_untouched(self) -> None:
        r = await _send("/")
        assert r["served"] == "/"
