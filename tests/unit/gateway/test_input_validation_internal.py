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
@pytest.mark.parametrize("method", ["GET", "HEAD", "PUT", "POST", "DELETE"])
@pytest.mark.parametrize(
    ("decoded", "raw"),
    [
        ("/internal#x/parts/1", b"/internal%23x/parts/1"),
        ("/internal?x/parts/1", b"/internal%3Fx/parts/1"),
        ("/internal#", b"/internal%23"),
        ("/internal?", b"/internal%3F"),
        ("/x/../internal#y", b"/x/%2E%2E/internal%23y"),
    ],
)
async def test_a_fragment_or_query_delimiter_cannot_hide_internal(method: str, decoded: str, raw: bytes) -> None:
    """`%23`/`%3F` in the first segment truncate the path httpx forwards, not the one checked.

    uvicorn decodes them into a literal `#`/`?` in `scope["path"]`, `ForwardService` interpolates
    that into a URL string, and httpx then reads it as the fragment/query delimiter — so the
    request target it writes is `/internal`. Judged as sent, the first segment is `internal#x`,
    which is not `internal` and sailed straight through the denylist. Every method, because the
    shape that reached the api was a plain GET.
    """
    response, forwarded = await _run(method, decoded, raw_path=raw)

    assert response.status_code == 400
    assert forwarded == [], "httpx truncates at the delimiter, so the api would receive `/internal`"


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["GET", "HEAD", "PUT", "POST", "DELETE"])
@pytest.mark.parametrize(
    ("decoded", "raw"),
    [
        # The gateway decodes once, httpx forwards the escape verbatim, the api decodes again.
        ("/%69nternal/parts/1", b"/%2569nternal/parts/1"),
        # The escape can sit anywhere in the segment, so this is not a prefix problem.
        ("/int%65rnal/parts/1", b"/int%2565rnal/parts/1"),
        ("/interna%6C", b"/interna%256C"),
    ],
)
async def test_a_doubly_encoded_first_segment_cannot_hide_internal(method: str, decoded: str, raw: bytes) -> None:
    """The escape survives the gateway's view and vanishes at the api.

    Unlike the `#`/`?` truncation above, which can only shorten a path, this one reconstructs the
    full target: the api routes `/internal/parts/1` to the peer-serve endpoint. It is caught by
    refusing `%` in the first segment rather than by decoding twice here — decoding twice would
    also decode object keys twice and widen what the key check accepts.
    """
    response, forwarded = await _run(method, decoded, raw_path=raw)

    assert response.status_code == 400
    assert forwarded == [], "the api decodes the escape again, so it would receive `/internal/...`"


@pytest.mark.asyncio
async def test_percent_is_refused_in_the_first_segment_whatever_it_spells() -> None:
    """The rejection is the class of bug, not a list of spellings.

    `%69nternal`, `int%65rnal`, `%2569nternal`... enumerating them is the blocklist mistake that
    put `internal` in a denylist in the first place. No legitimate first segment contains `%`: a
    bucket name is `[a-z0-9.-]` or an SS58 address, and no gateway route has one.
    """
    response, forwarded = await _run("GET", "/my%bucket/key.txt", raw_path=b"/my%25bucket/key.txt")

    assert response.status_code == 400
    assert b"InvalidBucketName" in response.body
    assert forwarded == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("decoded", "raw"),
    [
        # `%` in the KEY keeps its existing treatment — the key check owns that, not the new
        # first-segment rule, and it must still fire.
        ("/bucket/100%.txt", b"/bucket/100%25.txt"),
        # A percent-escape that decodes to something harmless is still refused in segment 0,
        # while the same bytes in a key are unaffected by this rule.
        ("/%62ucket/key.txt", b"/%2562ucket/key.txt"),
    ],
)
async def test_percent_anywhere_in_a_path_is_still_refused(decoded: str, raw: bytes) -> None:
    response, forwarded = await _run("GET", decoded, raw_path=raw)

    assert response.status_code == 400
    assert forwarded == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("decoded", "raw"),
    [
        ("/bucket/with space.txt", b"/bucket/with%20space.txt"),
        ("/bucket/unicode-éè.txt", b"/bucket/unicode-%C3%A9%C3%A8.txt"),
        ("/internal-backups/some/key.txt", b"/internal-backups/some/key.txt"),
        ("/health", b"/health"),
    ],
)
async def test_ordinary_percent_encoded_requests_still_forward(decoded: str, raw: bytes) -> None:
    """The new rule must not touch normal traffic: escapes that decode to ordinary characters
    leave no `%` behind, so the first segment never sees one."""
    response, forwarded = await _run("GET", decoded, raw_path=raw)

    assert response.status_code == 200
    assert len(forwarded) == 1


@pytest.mark.asyncio
async def test_a_fragment_in_a_key_is_still_judged_as_a_key_not_as_a_reserved_bucket() -> None:
    """Truncating for the routing view must not cost the key view its `#`.

    `/internal-backups/report#v1.txt` forwards as `/internal-backups/report` — a legitimate bucket,
    so the denylist must not claim it — while the `#` still has to be rejected, because the
    truncation IS the silent overwrite that guard exists to prevent.

    The refusal now comes from the path-level delimiter check rather than the per-key character
    list, so the code is `InvalidURI` instead of `InvalidArgument`. That check runs earlier
    because it also has to cover paths with no key segment at all, and paths that take the
    SKIP_PREFIXES bypass before the key view is ever built. What this test exists to pin is
    unchanged and still asserted below: the request is refused, nothing is forwarded, and the
    bucket is NOT misjudged as a reserved name.
    """
    response, forwarded = await _run(
        "GET", "/internal-backups/report#v1.txt", raw_path=b"/internal-backups/report%23v1.txt"
    )

    assert response.status_code == 400
    assert forwarded == []
    assert b"InvalidURI" in response.body, "must fail on the character, not as a reserved bucket name"
    assert b"InvalidBucketName" not in response.body


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
