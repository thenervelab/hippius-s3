import logging
import re
import time
from typing import Awaitable
from typing import Callable

from fastapi import Request
from fastapi import Response
from opentelemetry import trace
from starlette.types import Message

from hippius_s3.api.s3.errors import CLIENT_CLOSED_REQUEST
from hippius_s3.monitoring import enrich_span_with_account_info
from hippius_s3.monitoring import get_metrics_collector


logger = logging.getLogger(__name__)


async def metrics_middleware(
    request: Request,
    call_next: Callable[[Request], Awaitable[Response]],
) -> Response:
    start_time = time.time()

    # TTFB needs to see the first UPLOAD byte, and only the body path knows when that is: every
    # downstream body read (handler, streaming writer) awaits this request's `_receive`, so wrapping
    # it here stamps the moment the app first accepts a byte off the wire. Requests whose body is
    # never read (GET/HEAD/list, or a PUT rejected before the handler) simply never stamp.
    first_body_byte_at: float | None = None
    inner_receive = request._receive

    async def receive_with_first_byte_stamp() -> Message:
        nonlocal first_body_byte_at
        message = await inner_receive()
        # Stamp on the first `http.request`, whether or not it carries bytes. Testing the body for
        # truthiness reads as equivalent but silently excludes the zero-length body: a
        # `Content-Length: 0` PUT arrives as exactly one empty `http.request`, so it never stamped
        # and its TTFB quietly fell back to response start — measuring a different thing from every
        # other PUT on the same panel. Receiving the message at all is the event: the app asked for
        # the body and the server handed one over. Servers do not emit an empty leading chunk ahead
        # of real body bytes, so this does not move the stamp for ordinary uploads.
        if first_body_byte_at is None and message["type"] == "http.request":
            first_body_byte_at = time.time()
        return message

    request._receive = receive_with_first_byte_stamp

    response = await call_next(request)
    # BaseHTTPMiddleware resolves call_next at `http.response.start`, before the body streams —
    # and GetObject peeks its first decrypted chunk before returning (A2 in object_reader.py) —
    # so for body-less requests this timestamp IS first-byte-ready, not full-transfer.
    response_started_at = time.time()
    duration = response_started_at - start_time
    api_time_ms = duration * 1000

    # Baseline on the outermost clock (stamped by ray_id just inside CORS) so the TTFB includes
    # the auth/ACL chain above this middleware; `start_time` is the honest fallback when a path
    # skips ray_id. Clamped like pre_handler: a cross-depth clock artifact must not put a negative
    # sample in the histogram.
    ttfb_start = getattr(request.state, "gateway_start_time", None) or start_time
    ttfb = max(0.0, (first_body_byte_at if first_body_byte_at is not None else response_started_at) - ttfb_start)

    main_account = None
    subaccount_id = None
    bucket_name = None
    object_key = None

    if hasattr(request.state, "account"):
        main_account = getattr(request.state, "main_account_id", None)
        subaccount_id = getattr(request.state, "account_id", None)

    bucket_name = request.path_params.get("bucket_name")
    object_key = request.path_params.get("object_key")

    enrich_span_with_account_info(
        main_account=main_account,
        subaccount_id=subaccount_id,
        bucket_name=bucket_name,
        object_key=object_key,
    )

    span = trace.get_current_span()
    if span.is_recording():
        span.set_attribute("timing.api_ms", api_time_ms)
        response.headers["X-Hippius-API-Time-Ms"] = str(round(api_time_ms, 2))

    endpoint_name = "unknown"
    try:
        if "route" in request.scope:
            route = request.scope["route"]
            if hasattr(route, "endpoint") and hasattr(route.endpoint, "__name__"):
                endpoint_name = route.endpoint.__name__
    except Exception:
        pass

    get_metrics_collector().record_http_request(
        request=request,
        response=response,
        duration=duration,
        handler=endpoint_name,
        ttfb=ttfb,
    )

    # 499 is a client abort (see errors.CLIENT_CLOSED_REQUEST), not a failure we served. Returning
    # it above stops these being counted as internal_error, but without this they would simply
    # reappear as http_499 on the same error-rate panels — at the same ~13k/48h volume. The
    # gateway's metrics middleware excludes it for exactly this reason; both hops must agree or
    # the abort is still an "error" on one of them.
    if response.status_code >= 400 and response.status_code != CLIENT_CLOSED_REQUEST:
        error_type = f"http_{response.status_code}"

        if hasattr(response, "body"):
            try:
                body = response.body.decode("utf-8") if isinstance(response.body, bytes) else str(response.body)
                code_match = re.search(r"<Code>([^<]+)</Code>", body)
                if code_match:
                    error_type = code_match.group(1)
            except Exception:
                pass

        get_metrics_collector().record_error(
            error_type=error_type,
            operation=endpoint_name,
            bucket_name=bucket_name,
        )

    return response
