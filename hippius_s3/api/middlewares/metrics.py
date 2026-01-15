import logging
import re
import time
from typing import Awaitable
from typing import Callable

from fastapi import Request
from fastapi import Response
from opentelemetry import trace

from hippius_s3.monitoring import enrich_span_with_account_info
from hippius_s3.monitoring import get_metrics_collector


logger = logging.getLogger(__name__)


def calculate_performance_metrics(
    operation: str,
    file_size_bytes: int,
    total_latency_ms: float,
) -> dict[str, float]:
    if file_size_bytes == 0 or total_latency_ms <= 0:
        return {
            "performance_index": 100.0,
            "throughput_mbps": 0.0,
            "overhead_ms": total_latency_ms,
        }

    file_size_mb = file_size_bytes / (1024 * 1024)
    total_latency_s = total_latency_ms / 1000
    actual_throughput_mbps = file_size_mb / total_latency_s

    target_throughput = 100 if operation == "upload" else 150
    throughput_score = min(100, (actual_throughput_mbps / target_throughput) * 100)

    ideal_transfer_time_ms = (file_size_mb / target_throughput) * 1000
    actual_overhead_ms = max(0, total_latency_ms - ideal_transfer_time_ms)

    max_overhead = 200 if operation == "upload" else 100
    overhead_score = max(0, 100 - (actual_overhead_ms / max_overhead) * 100)

    performance_index = (throughput_score * 0.6) + (overhead_score * 0.4)

    return {
        "performance_index": round(performance_index, 2),
        "throughput_mbps": round(actual_throughput_mbps, 2),
        "overhead_ms": round(actual_overhead_ms, 2),
    }


async def metrics_middleware(
    request: Request,
    call_next: Callable[[Request], Awaitable[Response]],
) -> Response:
    start_time = time.time()
    response = await call_next(request)
    duration = time.time() - start_time
    api_time_ms = duration * 1000

    main_account = None
    subaccount_id = None
    bucket_name = None
    object_key = None

    if hasattr(request.state, "account"):
        main_account = getattr(request.state.account, "main_account", None)
        subaccount_id = getattr(request.state.account, "id", None)

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
        if hasattr(request.state, "gateway_time_ms"):
            gateway_time = request.state.gateway_time_ms
            span.set_attributes(
                {
                    "timing.gateway_ms": gateway_time,
                    "timing.api_ms": api_time_ms,
                    "timing.total_ms": gateway_time + api_time_ms,
                }
            )
            response.headers["X-Hippius-Gateway-Time-Ms"] = str(round(gateway_time, 2))
            response.headers["X-Hippius-API-Time-Ms"] = str(round(api_time_ms, 2))
        else:
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
        main_account=main_account,
        subaccount_id=subaccount_id,
        handler=endpoint_name,
    )

    if response.status_code < 400 and bucket_name and object_key:
        operation = None
        file_size_bytes = 0

        if request.method == "PUT":
            operation = "upload"
            content_length = request.headers.get("content-length")
            if content_length:
                file_size_bytes = int(content_length)
        elif request.method == "GET":
            operation = "download"
            content_length = response.headers.get("content-length")
            if not content_length and hasattr(request.state, "object_size"):
                file_size_bytes = request.state.object_size
            elif content_length:
                file_size_bytes = int(content_length)

        if operation and file_size_bytes > 0:
            total_latency_ms = duration * 1000
            perf_metrics = calculate_performance_metrics(operation, file_size_bytes, total_latency_ms)

            get_metrics_collector().record_performance_metrics(
                operation=operation,
                bucket=bucket_name,
                performance_index=perf_metrics["performance_index"],
                throughput_mbps=perf_metrics["throughput_mbps"],
                overhead_ms=perf_metrics["overhead_ms"],
            )

    if response.status_code >= 400:
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
            main_account=main_account,
        )

    return response
