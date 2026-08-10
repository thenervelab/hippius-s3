"""`forwarded_path` must produce the path httpx will actually forward.

The functions exist so the gateway's first-segment security checks judge the same path the api
receives, and `ForwardService` delegates that rewrite to httpx. A hand-rolled mirror can drift from
the library it mirrors, so the pin here is parity against the installed httpx: if an upgrade
changes its normalization, THIS fails — instead of the denylist silently judging a different
request than the one being forwarded.

Two claims, pinned separately because the middleware relies on them separately:

- `collapse_dot_segments` mirrors `httpx._urlparse.normalize_path` — the rewrite that decides the
  key an object is stored under.
- `forwarded_path` is everything httpx does to the request target, which is dot-collapsing PLUS
  truncation at the first `#`/`?`. Only this one may be used for a routing or security decision.

A fixed list cannot keep a universal claim honest, which is why the `#` divergence survived the
original 17 entries here. The generated-path property in `test_path_parity_property.py` is the real
oracle; these lists are the named-shape regression pins.
"""

from __future__ import annotations

import httpx
import pytest

from gateway.utils.paths import collapse_dot_segments
from gateway.utils.paths import forwarded_path


DOT_SEGMENT_PATHS = [
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

# `#`/`?` in the DECODED path — a client sent `%23`/`%3F`, and uvicorn hands the forwarder the
# decoded path. httpx reads the first of them as the fragment/query delimiter of the string
# ForwardService builds, so the request target it writes stops there. This is the divergence that
# let `/internal%23x/parts/1` past the first-segment denylist and arrive as `GET /internal`.
TRUNCATED_PATHS = [
    "/internal#x/parts/1",
    "/internal?x/parts/1",
    "/a#",
    "/a?",
    "/a/b#c/d",
    "/#x",
    # Truncation is not a dot-segment concern, so it must apply to paths with no `.` in them at
    # all — and dot segments AFTER the delimiter are never forwarded, so they must not collapse.
    "/a#b?c",
    "/a?b#c",
    "/a#b/../c",
    "/a/..#z",
    "/a/../b?c",
]


@pytest.mark.parametrize("path", DOT_SEGMENT_PATHS)
def test_collapse_matches_what_httpx_forwards(path: str) -> None:
    # The same construction ForwardService uses: base URL + the (decoded) scope path.
    assert collapse_dot_segments(path) == httpx.URL(f"http://api{path}").path


@pytest.mark.parametrize("path", DOT_SEGMENT_PATHS + TRUNCATED_PATHS)
def test_forwarded_path_matches_what_httpx_forwards(path: str) -> None:
    assert forwarded_path(path) == httpx.URL(f"http://api{path}").path


def test_collapse_is_identity_without_dot_segments() -> None:
    """The fast path: no request without `.` in it pays anything, and no ordinary key changes."""
    for path in ("/bucket/deep/nested/key", "/bucket", "/"):
        assert collapse_dot_segments(path) is path


def test_collapse_deliberately_does_not_truncate() -> None:
    """The two views must stay different, or the `#` key guard in `input_validation` dies.

    Rejecting `#` in an object key is only possible while the `#` is still there: truncate first
    and `report#v1.txt` looks like the perfectly valid key `report`, which is exactly how two keys
    silently collapsed onto one object in prod.
    """
    assert collapse_dot_segments("/bucket/report#v1.txt") == "/bucket/report#v1.txt"
    assert forwarded_path("/bucket/report#v1.txt") == "/bucket/report"


def test_percent_escapes_diverge_which_is_why_they_are_refused_instead() -> None:
    """The one rewrite `forwarded_path` deliberately does NOT model, stated as a test.

    httpx forwards percent-escapes verbatim and the api's uvicorn decodes them a second time, so
    `scope["path"]` of `/%69nternal/parts/1` (a client's `/%2569nternal/parts/1`) is put on the
    wire unchanged and READ BY THE API as `/internal/parts/1`. Unlike `#`/`?` truncation, which can
    only shorten a path, this reconstructs the full target and reaches the peer-serve route.

    Closing it here would mean decoding again, which would also decode object keys twice and turn a
    key sent as `a%252Fb` — rejected today for containing `%` — into the accepted key `a/b`. So the
    gap is left open and `input_validation` refuses `%` in the first segment instead; that is what
    `test_input_validation_internal.py::test_a_doubly_encoded_first_segment_cannot_hide_internal`
    and the `%`-escaped arm of the security property in `test_path_parity_property.py` cover.

    Asserting the divergence keeps this honest in both directions: if a future httpx (or a change
    here) makes the parity hold, this fails, and whoever sees it must revisit whether the `%`
    rejection is still the right fix.
    """
    path = "/%69nternal/parts/1"

    assert forwarded_path(path) == "/%69nternal/parts/1"
    assert httpx.URL(f"http://api{path}").path == "/internal/parts/1"
