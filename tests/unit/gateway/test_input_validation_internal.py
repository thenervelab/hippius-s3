"""The gateway refuses to proxy `/internal/...` at all.

The api's peer secret is the boundary; this is defence in depth in front of it. It matters
because everything upstream of the forwarder treats `/internal/parts/...` as an ordinary S3
request: a GET with no Authorization header authenticates as `anonymous`, and the ACL
middleware passes through when the first segment names no bucket it knows. Nothing else on
the way to `ForwardService` would stop it.
"""

from __future__ import annotations

from typing import Any

import pytest
from fastapi import Request
from fastapi import Response

from gateway.middlewares.input_validation import input_validation_middleware


def _request(method: str, path: str, raw_path: bytes | None = None) -> Request:
    return Request(
        {
            "type": "http",
            "method": method,
            "path": path,
            "raw_path": raw_path if raw_path is not None else path.encode(),
            "root_path": "",
            "headers": [],
            "query_string": b"",
        }
    )


async def _run(method: str, path: str, raw_path: bytes | None = None) -> tuple[Response, list[Any]]:
    forwarded: list[Any] = []

    async def call_next(request: Request) -> Response:
        forwarded.append(request)
        return Response(status_code=200)

    response = await input_validation_middleware(_request(method, path, raw_path), call_next)
    return response, forwarded


@pytest.mark.asyncio
async def test_an_unauthenticated_internal_path_is_rejected_before_it_is_forwarded() -> None:
    response, forwarded = await _run("GET", "/internal/parts/466916c0-d61b-4518-b81b-9576b574270a/1/1/chunks/0")

    assert response.status_code == 400
    assert forwarded == [], "the request must die at the gateway, not reach the api"


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["GET", "HEAD", "PUT", "POST", "DELETE"])
async def test_every_method_is_rejected_not_just_create_bucket(method: str) -> None:
    """The existing reserved-name check only fires on a CreateBucket-shaped PUT.

    A GET is precisely the shape that reached the peer-serve endpoint, so a check that
    matched only PUT would leave the path this exists to close wide open.
    """
    response, forwarded = await _run(method, "/internal/parts/x/1/1/chunks/0")

    assert response.status_code == 400
    assert forwarded == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "path",
    [
        "/anybucket/../internal/parts/466916c0-d61b-4518-b81b-9576b574270a/1/1/chunks/0",
        "/a/b/../../internal/parts/x/1/1/chunks/0",
        "/../internal/parts/x/1/1/chunks/0",
        "/./internal/parts/x/1/1/chunks/0",
    ],
)
async def test_a_dot_segment_traversal_to_internal_is_rejected(path: str) -> None:
    """The denylist must judge the path the api will SEE, not the one the client sent.

    httpx collapses dot segments when the forwarder builds the outgoing URL, so every one of
    these reaches the api as `/internal/parts/...` — while their UNcollapsed first segment is
    an innocuous bucket name that sailed past a naive `path_parts[0]` check.
    """
    response, forwarded = await _run("GET", path)

    assert response.status_code == 400
    assert forwarded == [], "the collapsed path names `internal`, so it must die at the gateway"


@pytest.mark.asyncio
async def test_percent_encoded_dot_segments_are_collapsed_before_the_check() -> None:
    """`%2E%2E` decodes to `..` before the forwarder sees it — uvicorn hands httpx the DECODED
    scope path — so the encoded spelling takes exactly the same route to `/internal/...` and
    must be caught by the same collapse."""
    decoded = "/anybucket/../internal/parts/x/1/1/chunks/0"
    raw = b"/anybucket/%2E%2E/internal/parts/x/1/1/chunks/0"

    response, forwarded = await _run("GET", decoded, raw_path=raw)

    assert response.status_code == 400
    assert forwarded == []


@pytest.mark.asyncio
async def test_a_key_with_dot_segments_that_stays_inside_its_bucket_still_forwards() -> None:
    """Collapsing must not reject more than forwarding already rewrites. `/bucket/a/../b.txt`
    reaches the api as `/bucket/b.txt` today; the gateway's only change is to validate that
    same collapsed shape, so the request forwards exactly as before."""
    response, forwarded = await _run("GET", "/bucket/a/../b.txt")

    assert response.status_code == 200
    assert len(forwarded) == 1


@pytest.mark.asyncio
async def test_a_dot_segment_traversal_to_a_reserved_bucket_name_is_rejected() -> None:
    """The same hole re-opened the reserved-name check: `PUT /x/../docs` is not
    CreateBucket-shaped uncollapsed (three segments), but the api receives `PUT /docs` —
    exactly how the ownerless `docs` bucket got written the first time."""
    response, forwarded = await _run("PUT", "/x/../docs")

    assert response.status_code == 400
    assert forwarded == []


@pytest.mark.asyncio
async def test_a_bucket_merely_starting_with_internal_is_untouched() -> None:
    """Segment equality, not a prefix test — `internal-backups` is a legitimate bucket name.

    Matching on prefix is how the auth-exempt-paths incident happened (`docs2` skipped auth
    because the check used startswith), so the same mistake must not be repeated here.
    """
    response, forwarded = await _run("GET", "/internal-backups/some/key.txt")

    assert response.status_code == 200
    assert len(forwarded) == 1
