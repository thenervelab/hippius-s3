import re
import time
from typing import Awaitable
from typing import Callable

from fastapi import Request
from fastapi import Response

from hippius_s3.monitoring import enrich_span_with_account_info
from hippius_s3.monitoring import get_metrics_collector


async def metrics_middleware(
    request: Request,
    call_next: Callable[[Request], Awaitable[Response]],
) -> Response:
    start_time = time.time()
    response = await call_next(request)
    duration = time.time() - start_time

    main_account = None
    subaccount_id = None
    bucket_name = None
    object_key = None

    if hasattr(request.state, "account"):
        main_account = getattr(request.state.account, "main_account", None)
        subaccount_id = getattr(request.state.account, "id", None)

    if hasattr(request.state, "bucket_name"):
        bucket_name = request.state.bucket_name
    if hasattr(request.state, "object_key"):
        object_key = request.state.object_key

    enrich_span_with_account_info(
        main_account=main_account,
        subaccount_id=subaccount_id,
        bucket_name=bucket_name,
        object_key=object_key,
    )

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
        handler=endpoint_name,
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
