"""`collapse_dot_segments` must produce the path httpx will actually forward.

The function exists so the gateway's first-segment security checks judge the same path the api
receives, and `ForwardService` delegates that rewrite to httpx. A hand-rolled mirror can drift
from the library it mirrors, so the pin here is parity: for every shape of dot-segment path,
the collapse must equal the path of the URL the installed httpx builds. If an httpx upgrade
changes its normalization, THIS fails — instead of the denylist silently judging a different
request than the one being forwarded.
"""

from __future__ import annotations

import httpx
import pytest

from gateway.utils.paths import collapse_dot_segments


PATHS = [
    "/",
    "/bucket",
    "/bucket/key.txt",
    "/bucket/a/../b.txt",
    "/anybucket/../internal/parts/x/1/1/chunks/0",
    "/a/b/../../internal",
    "/..",
    "/../a",
    "/./a",
    "/a/.",
    "/a/..",
    "/a/./b",
    "/a/.../b",
    "/a..b/c",
    "/a/b/c/../../..",
    "/a//b",
    "/a/../../../b",
]


@pytest.mark.parametrize("path", PATHS)
def test_collapse_matches_what_httpx_forwards(path: str) -> None:
    # The same construction ForwardService uses: base URL + the (decoded) scope path.
    assert collapse_dot_segments(path) == httpx.URL(f"http://api{path}").path


def test_collapse_is_identity_without_dot_segments() -> None:
    """The fast path: no request without `.` in it pays anything, and no ordinary key changes."""
    for path in ("/bucket/deep/nested/key", "/bucket", "/"):
        assert collapse_dot_segments(path) is path
