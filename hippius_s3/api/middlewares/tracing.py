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
    span_name = f"s3.{operation_name}"

    initial_attributes = {
        "s3.operation": operation_name,
        "http.method": request.method,
        "http.route": request.url.path,
        "http.url": str(request.url),
    }

    if "bucket_name" in request.path_params:
        initial_attributes["s3.bucket_name"] = request.path_params["bucket_name"]
    if "object_key" in request.path_params:
        initial_attributes["s3.object_key"] = request.path_params["object_key"]

    _add_query_param_attributes(request, initial_attributes)

    with tracer.start_as_current_span(span_name, attributes=initial_attributes) as span:
        try:
            response = await call_next(request)

            if hasattr(request.state, "account"):
                set_span_attributes(
                    span,
                    {
                        "s3.main_account": request.state.account.main_account,
                        "s3.subaccount": request.state.account.id,
                    },
                )

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
    if path == "/robots.txt":  # noqa: SIM103
        return False
    return True


def _get_operation_name(request: Request) -> str:
    try:
        if "route" in request.scope:
            route = request.scope["route"]
            if hasattr(route, "endpoint") and hasattr(route.endpoint, "__name__"):
                return str(route.endpoint.__name__)
    except Exception as e:
        logger.debug(f"Failed to get operation name from route: {e}")

    return f"{request.method.lower()}_unknown"


def _add_query_param_attributes(request: Request, attributes: dict) -> None:
    query_params = dict(request.query_params)
    if not query_params:
        return

    attributes["s3.query_params"] = str(query_params)

    if "uploads" in query_params:
        attributes["s3.multipart.operation"] = "initiate"
    elif "uploadId" in query_params:
        attributes["s3.multipart.upload_id"] = query_params["uploadId"]
        attributes["s3.multipart.has_upload_id"] = True
        if "partNumber" in query_params:
            attributes["s3.multipart.operation"] = "upload_part"
            attributes["s3.multipart.part_number"] = query_params["partNumber"]
        else:
            attributes["s3.multipart.operation"] = "complete"

    if "tagging" in query_params:
        attributes["s3.subresource"] = "tagging"
    if "lifecycle" in query_params:
        attributes["s3.subresource"] = "lifecycle"
    if "policy" in query_params:
        attributes["s3.subresource"] = "policy"


def set_span_attributes(span: Span, attributes: dict[str, Any]) -> None:
    """
    Helper to set multiple span attributes at once.

    Only sets attributes when the span is recording and skips None values.

    Args:
        span: OpenTelemetry span to set attributes on
        attributes: Dictionary of attribute key-value pairs

    Example:
        set_span_attributes(span, {
            "object_id": object_id,
            "has_object_id": True,
            "size_bytes": 1024,
        })
    """
    if span is None:
        return
    if span.is_recording():
        for key, value in attributes.items():
            if value is not None:
                span.set_attribute(key, value)
