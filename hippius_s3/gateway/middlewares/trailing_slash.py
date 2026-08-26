from __future__ import annotations

from typing import Awaitable
from typing import Callable

from fastapi import Request
from fastapi import Response

from hippius_s3.gateway.utils.paths import routing_path


def _strip_trailing_slashes(raw: bytes) -> bytes:
    """Strip trailing slashes from an UNDECODED path, in both spellings.

    `rstrip(b"/")` only sees the literal byte, so a slash sent as `%2F` survived it.
    """
    while raw:
        if raw.endswith(b"/"):
            raw = raw[:-1]
        elif raw[-3:].lower() == b"%2f":
            raw = raw[:-3]
        else:
            break
    return raw


async def trailing_slash_normalizer(
    request: Request,
    call_next: Callable[[Request], Awaitable[Response]],
) -> Response:
    """Normalize trailing slashes without redirecting.

    Removes a trailing slash from the URL path (except for "/") so both
    with/without trailing slash resolve to the same route.

    Two things here are load-bearing, both for the same reason: this middleware is the
    only layer that rewrites the path AFTER `path_normalization` has established the
    routing view, and `account`/`acl` run inner to it.

    - `raw_path` is edited as BYTES. Re-encoding `request.url.path` (already decoded once)
      would hand `routing_path` a value it decodes a second time, so a key sent as
      `a%2541.txt` would be judged as `aA.txt` — a decoded view no client asked for.
    - The memoized routing view is dropped, so every inner security layer recomputes the
      path the router will actually serve. Leaving it stale made `PUT /bucket/` parse as
      bucket + EMPTY key: `key is None` was False, so `is_create_bucket` never fired and
      the sub-token branch fell through to `call_next` with no scope check at all.

    The invariant this must leave behind is `routing_path(request) == scope["path"]` — the
    two views the security layers and the router respectively judge. `scope["path"]` is
    therefore derived FROM the recomputed routing view rather than stripped independently;
    stripping each side on its own is what let the two disagree for a `%2F`-spelled slash.
    """
    path = request.url.path
    if path != "/" and path.endswith("/"):
        raw = request.scope.get("raw_path")
        if raw is not None:
            request.scope["raw_path"] = _strip_trailing_slashes(raw) or b"/"

        request.scope.pop("_hippius_routing_path", None)
        request.scope["path"] = routing_path(request) if raw is not None else path.rstrip("/")

    return await call_next(request)
