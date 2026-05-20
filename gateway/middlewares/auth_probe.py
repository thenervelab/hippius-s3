from __future__ import annotations

import hmac
from typing import Awaitable
from typing import Callable

from fastapi import Request
from fastapi import Response

from gateway.config import get_config


AUTH_PROBE_HEADER = "x-hippius-auth-probe"


async def auth_probe_middleware(
    request: Request,
    call_next: Callable[[Request], Awaitable[Response]],
) -> Response:
    # When ATS's authproxy plugin asks "is this request authorized?", the
    # subrequest carries X-Hippius-Auth-Probe: <secret> (stamped by header_rewrite
    # on a dedicated auth-host remap, which is the ONLY ATS rule that emits the
    # header). By the time we get here, auth_router and acl_middleware have
    # already validated — they would have returned 401/403 otherwise. Return an
    # empty 200 so ATS proceeds with cache lookup / origin fetch; skip the
    # catch-all forward that would otherwise round-trip to the internal API for
    # no reason.
    #
    # Defense against client-supplied header injection: the value must match the
    # configured HIPPIUS_AUTH_PROBE_SECRET via constant-time compare. A client
    # sending the header (intentionally or accidentally) without the secret falls
    # through to normal request handling. When the secret is unset (dev / not
    # rolled out yet), the middleware is fully disabled (fail-closed).
    #
    # Registered as the innermost middleware so observability (ray_id, audit,
    # metrics, tracing) still runs for probe requests.
    secret = get_config().auth_probe_secret
    if secret:
        provided = request.headers.get(AUTH_PROBE_HEADER, "")
        if provided and hmac.compare_digest(provided, secret):
            # Flag for outer middlewares (cache_control, etc.) on the response
            # path so they can skip mutating a probe response. Set BEFORE
            # returning so the state is visible to every middleware as the
            # response bubbles out.
            request.state.is_auth_probe = True
            return Response(status_code=200, content=b"")

    return await call_next(request)
