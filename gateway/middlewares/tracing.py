import logging
from typing import Any
from typing import Awaitable
from typing import Callable

from fastapi import Request
from fastapi import Response
from opentelemetry import trace
from opentelemetry.trace import Span
from opentelemetry.trace import Status
from opentelemetry.trace import StatusCode


logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


async def tracing_middleware(
    request: Request,
    call_next: Callable[[Request], Awaitable[Response]],
) -> Response:
    if not _should_trace(request.url.path):
        return await call_next(request)

    operation_name = _get_operation_name(request)
    span_name = f"gateway.{operation_name}"

    initial_attributes = {
        "gateway.operation": operation_name,
        "http.method": request.method,
        "http.route": request.url.path,
        "http.url": str(request.url),
    }

    with tracer.start_as_current_span(span_name, attributes=initial_attributes) as span:
        try:
            response = await call_next(request)

            if hasattr(request.state, "ray_id"):
                set_span_attributes(span, {"hippius.ray_id": request.state.ray_id})

            if hasattr(request.state, "account_id"):
                set_span_attributes(
                    span,
                    {
                        "hippius.main_account": request.state.account_id,
                    },
                )

            if hasattr(request.state, "bucket_owner_id"):
                set_span_attributes(
                    span,
                    {
                        "hippius.bucket_owner": request.state.bucket_owner_id,
                    },
                )

            if hasattr(request.state, "auth_method"):
                set_span_attributes(span, {"gateway.auth_method": request.state.auth_method})

            if hasattr(request.state, "acl_decision"):
                set_span_attributes(span, {"gateway.acl_decision": request.state.acl_decision})

            if hasattr(request.state, "rate_limited"):
                set_span_attributes(span, {"gateway.rate_limited": request.state.rate_limited})

            set_span_attributes(span, {"http.status_code": int(response.status_code)})

            if 400 <= response.status_code < 600:
                span.set_status(Status(StatusCode.ERROR))
                set_span_attributes(
                    span,
                    {
                        "error": True,
                        "error.type": "http_error",
                    },
                )
            else:
                span.set_status(Status(StatusCode.OK))

            return response

        except Exception as e:
            span.record_exception(e)
            span.set_status(Status(StatusCode.ERROR, str(e)))
            set_span_attributes(
                span,
                {
                    "error": True,
                    "error.type": type(e).__name__,
                    "error.message": str(e),
                },
            )
            raise


def _should_trace(path: str) -> bool:
    if path.startswith("/health"):
        return False
    if path.startswith("/metrics"):
        return False
    if path.startswith("/docs"):
        return False
    if path.startswith("/redoc"):
        return False
    if path.startswith("/openapi.json"):
        return False
    return path != "/robots.txt"


def _get_operation_name(request: Request) -> str:
    try:
        if "route" in request.scope:
            route = request.scope["route"]
            if hasattr(route, "endpoint") and hasattr(route.endpoint, "__name__"):
                return str(route.endpoint.__name__)
    except Exception as e:
        logger.debug(f"Failed to get operation name from route: {e}")

    return f"{request.method.lower()}_forward"


def set_span_attributes(span: Span, attributes: dict[str, Any]) -> None:
    if span is None:
        return
    if span.is_recording():
        for key, value in attributes.items():
            if value is not None:
                span.set_attribute(key, value)
