"""A client that hangs up mid-PUT must not become a 500 on the API.

`iter_request_body` drives `request.stream()`, which raises ClientDisconnect when the peer goes
away. Uncaught it reaches `app.exception_handler(Exception)` — but starlette's
ServerErrorMiddleware re-raises unconditionally ("we always continue to raise the exception"),
so the global handler *cannot* suppress it and uvicorn logs a full traceback. That is why this
has to be caught at the endpoint, the way UploadPart already does.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from starlette.requests import ClientDisconnect

from hippius_s3.api.s3.errors import CLIENT_CLOSED_REQUEST
from hippius_s3.api.s3.objects import put_object_endpoint


def _request(bucket_id: str = "b-1") -> Any:
    request = MagicMock()
    request.headers = {"content-length": "17", "Content-Type": "application/octet-stream"}
    request.state.account.main_account = "5Grw"
    request.state.bucket_id = bucket_id
    request.app.state.fs_store = MagicMock()
    request.app.state.postgres_pool = MagicMock()
    return request


@pytest.mark.asyncio
async def test_client_abort_mid_put_is_not_a_server_error(monkeypatch: Any) -> None:
    async def _boom(*_a: Any, **_kw: Any) -> None:
        raise ClientDisconnect()

    writer = MagicMock()
    writer.put_simple_stream_full = _boom
    monkeypatch.setattr(put_object_endpoint, "ObjectWriter", lambda **_kw: writer)
    monkeypatch.setattr(put_object_endpoint, "_user_needs_upsert", AsyncMock(return_value=False))

    response = await put_object_endpoint.handle_put_object(
        bucket_name="bkt",
        object_key="key.bin",
        request=_request(),
        pool=MagicMock(),
        redis_client=MagicMock(),
    )

    assert response.status_code == CLIENT_CLOSED_REQUEST


@pytest.mark.asyncio
async def test_a_real_writer_failure_is_still_a_500(monkeypatch: Any) -> None:
    """Blast-radius guard: only ClientDisconnect is reclassified. A genuine write failure must
    stay a 500 InternalError and stay counted as an error."""

    async def _boom(*_a: Any, **_kw: Any) -> None:
        raise RuntimeError("fs_store write failed")

    writer = MagicMock()
    writer.put_simple_stream_full = _boom
    monkeypatch.setattr(put_object_endpoint, "ObjectWriter", lambda **_kw: writer)
    monkeypatch.setattr(put_object_endpoint, "_user_needs_upsert", AsyncMock(return_value=False))

    response = await put_object_endpoint.handle_put_object(
        bucket_name="bkt",
        object_key="key.bin",
        request=_request(),
        pool=MagicMock(),
        redis_client=MagicMock(),
    )

    assert response.status_code == 500
    assert b"InternalError" in response.body


@pytest.mark.asyncio
async def test_client_abort_is_not_counted_as_an_internal_error(monkeypatch: Any) -> None:
    """The reason this matters more on the API than on the gateway: the catch-all records
    s3_errors_total{error_type="internal_error"}, so aborts were inflating a real error metric."""
    recorded: list[str] = []
    collector = MagicMock()
    collector.record_error = lambda error_type, operation, bucket_name: recorded.append(error_type)
    monkeypatch.setattr(put_object_endpoint, "get_metrics_collector", lambda: collector)

    async def _boom(*_a: Any, **_kw: Any) -> None:
        raise ClientDisconnect()

    writer = MagicMock()
    writer.put_simple_stream_full = _boom
    monkeypatch.setattr(put_object_endpoint, "ObjectWriter", lambda **_kw: writer)
    monkeypatch.setattr(put_object_endpoint, "_user_needs_upsert", AsyncMock(return_value=False))

    await put_object_endpoint.handle_put_object(
        bucket_name="bkt",
        object_key="key.bin",
        request=_request(),
        pool=MagicMock(),
        redis_client=MagicMock(),
    )

    assert recorded == [], "a client abort must not land in the internal-error counter"


def test_all_hops_classify_a_client_abort_the_same_way() -> None:
    """One event, one code. UploadPart used to answer 408 while the gateway answered 499, which
    made a single abort look like different failures depending on which hop you were reading."""
    import inspect

    from hippius_s3.api.s3 import multipart

    source = inspect.getsource(multipart.upload_part)
    assert "CLIENT_CLOSED_REQUEST" in source
    assert "status_code=408" not in source
