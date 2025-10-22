from functools import wraps
from typing import Any
from typing import Callable
from typing import Optional

from fastapi import Request
from fastapi import Response
from opentelemetry import trace
from opentelemetry.trace import Status
from opentelemetry.trace import StatusCode


tracer = trace.get_tracer(__name__)


def trace_s3_operation(operation_name: str) -> Callable[[Callable], Callable]:
    """
    Decorator to trace S3 operations with rich context for Tempo debugging.

    Captures:
    - HTTP details (method, route, URL, status code)
    - Account context (main_account, subaccount)
    - S3 context (bucket_name, object_key, query params)
    - Error details (exception type, message, stack trace)

    Args:
        operation_name: Operation name for the span (e.g., "put_object", "get_object")

    Example usage:
        @trace_s3_operation("put_object")
        async def put_object(bucket_name: str, object_key: str, request: Request, ...):
            ...
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            request: Optional[Request] = kwargs.get("request")

            bucket_name = kwargs.get("bucket_name", "")
            object_key = kwargs.get("object_key", "")

            span_name = f"s3.{operation_name}"

            with tracer.start_as_current_span(span_name) as span:
                span.set_attribute("s3.operation", operation_name)

                if request:
                    span.set_attribute("http.method", request.method)
                    span.set_attribute("http.route", request.url.path)
                    span.set_attribute("http.url", str(request.url))

                    if hasattr(request.state, "account"):
                        main_account = getattr(request.state.account, "main_account", "")
                        subaccount_id = getattr(request.state.account, "id", "")
                        if main_account:
                            span.set_attribute("s3.main_account", main_account)
                        if subaccount_id:
                            span.set_attribute("s3.subaccount", subaccount_id)

                    if bucket_name:
                        span.set_attribute("s3.bucket_name", bucket_name)
                    if object_key:
                        span.set_attribute("s3.object_key", object_key)

                    query_params = dict(request.query_params)
                    if query_params:
                        span.set_attribute("s3.query_params", str(query_params))

                        if "uploads" in query_params:
                            span.set_attribute("s3.multipart.operation", "initiate")
                        elif "uploadId" in query_params:
                            span.set_attribute("s3.multipart.upload_id", query_params["uploadId"])
                            if "partNumber" in query_params:
                                span.set_attribute("s3.multipart.operation", "upload_part")
                                span.set_attribute("s3.multipart.part_number", query_params["partNumber"])
                            else:
                                span.set_attribute("s3.multipart.operation", "complete")

                        if "tagging" in query_params:
                            span.set_attribute("s3.subresource", "tagging")
                        if "lifecycle" in query_params:
                            span.set_attribute("s3.subresource", "lifecycle")
                        if "policy" in query_params:
                            span.set_attribute("s3.subresource", "policy")

                try:
                    result = await func(*args, **kwargs)

                    if isinstance(result, Response):
                        status_code = result.status_code
                        span.set_attribute("http.status_code", status_code)

                        if 400 <= status_code < 600:
                            span.set_status(Status(StatusCode.ERROR))
                            span.set_attribute("error", True)
                            span.set_attribute("error.type", "http_error")
                        else:
                            span.set_status(Status(StatusCode.OK))

                    return result

                except Exception as e:
                    span.record_exception(e)
                    span.set_status(Status(StatusCode.ERROR, str(e)))
                    span.set_attribute("error", True)
                    span.set_attribute("error.type", type(e).__name__)
                    span.set_attribute("error.message", str(e))
                    raise

        return wrapper

    return decorator
