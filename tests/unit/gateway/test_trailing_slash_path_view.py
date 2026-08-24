"""The path a security layer judges must be the path the router serves.

`path_normalization` memoizes the routing view on the ASGI scope; `trailing_slash_normalizer`
runs after it and rewrites the path. Until this was fixed the memo went stale, so `acl` and
`account` judged `/bucket/` while the router served `/bucket` — and because the stale view
parsed as bucket + EMPTY key, `key is None` was False, `is_create_bucket` never fired, and the
sub-token branch returned `call_next` with no scope check.
"""

import pytest
from fastapi import Request

from hippius_s3.gateway.middlewares.acl import parse_s3_path
from hippius_s3.gateway.middlewares.trailing_slash import trailing_slash_normalizer
from hippius_s3.gateway.utils.paths import routing_path


def _request(raw_path: bytes) -> Request:
    return Request(
        {
            "type": "http",
            "method": "PUT",
            "path": raw_path.decode(),
            "raw_path": raw_path,
            "headers": [],
            "query_string": b"",
        }
    )


async def _passthrough(_request: Request) -> str:
    return "served"


@pytest.mark.parametrize(
    "raw_path,expected",
    [
        (b"/bucket/", "/bucket"),
        (b"/bucket//", "/bucket"),
        (b"/bucket/key/", "/bucket/key"),
        (b"/bucket", "/bucket"),
        (b"/", "/"),
    ],
)
@pytest.mark.asyncio
async def test_security_view_matches_the_served_path(raw_path: bytes, expected: str) -> None:
    request = _request(raw_path)
    # path_normalization runs first and memoizes the routing view.
    request.scope["path"] = routing_path(request)

    await trailing_slash_normalizer(request, _passthrough)

    assert request.scope["path"] == expected
    # The inner layers (account, acl) recompute here — they must agree with the router.
    assert routing_path(request) == expected


@pytest.mark.asyncio
async def test_trailing_slash_does_not_double_decode_the_key() -> None:
    """raw_path is edited as bytes, never re-encoded from the decoded path.

    Re-encoding `request.url.path` would hand routing_path a once-decoded value to decode
    again, so a key sent as `a%2541.txt` would be judged as `aA.txt`.
    """
    request = _request(b"/bucket/a%2541.txt/")
    request.scope["path"] = routing_path(request)

    await trailing_slash_normalizer(request, _passthrough)

    assert routing_path(request) == "/bucket/a%41.txt"


@pytest.mark.parametrize("path", ["/bucket/", "/bucket//"])
def test_a_trailing_slash_is_not_an_object_key(path: str) -> None:
    """`key is None` is how every caller asks "is this a bucket operation?"."""
    bucket, key = parse_s3_path(path)

    assert bucket == "bucket"
    assert key is None


def test_a_real_key_still_parses() -> None:
    assert parse_s3_path("/bucket/key") == ("bucket", "key")
    assert parse_s3_path("/bucket/dir/key") == ("bucket", "dir/key")


@pytest.mark.parametrize("path", ["/newbucket/", "/newbucket//"])
def test_create_bucket_is_recognized_through_a_trailing_slash(path: str) -> None:
    """The predicate acl uses to decide CreateBucket, evaluated exactly as the middleware does.

    When this returned False the sub-token branch fell through to `call_next` without ever
    calling evaluate_sub_token_scope — a token with no create-bucket grant could make a
    bucket by appending a slash.
    """
    _bucket, key = parse_s3_path(path)

    is_create_bucket = key is None and len({}) == 0

    assert is_create_bucket is True
