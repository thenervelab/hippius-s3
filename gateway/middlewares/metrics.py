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

    if hasattr(request.state, "account_id"):
        main_account = request.state.account_id
        subaccount_id = request.state.account_id

    enrich_span_with_account_info(
        main_account=main_account,
        subaccount_id=subaccount_id,
        bucket_name=None,
        object_key=None,
    )

    endpoint_name = "gateway_forward"
    try:
        if "route" in request.scope:
            route = request.scope["route"]
            if hasattr(route, "endpoint") and hasattr(route.endpoint, "__name__"):
                endpoint_name = f"gateway_{route.endpoint.__name__}"
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

    if response.status_code >= 400:
        error_type = f"http_{response.status_code}"

        get_metrics_collector().record_error(
            error_type=error_type,
            operation=endpoint_name,
            bucket_name=None,
            main_account=main_account,
        )

    return response
