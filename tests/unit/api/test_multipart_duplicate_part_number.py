"""UploadPart must carry exactly one partNumber.

The edge places an UploadPart by the FIRST `partNumber` value in the query string; Starlette's
`query_params.get` returns the LAST. Two values would have the request placed as one part and
stored as another, so the api refuses the request rather than picking either.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest
from starlette.requests import Request
from starlette.responses import Response

from hippius_s3.api.s3 import multipart


def _upload_part_request(query_string: str) -> Request:
    scope: dict[str, Any] = {
        "type": "http",
        "method": "PUT",
        "path": "/bucket/key",
        "raw_path": b"/bucket/key",
        "query_string": query_string.encode(),
        "headers": [],
    }
    return Request(scope)


@pytest.mark.asyncio
async def test_a_repeated_part_number_is_refused_before_anything_is_read() -> None:
    pool = MagicMock()

    response = await multipart.upload_part(_upload_part_request("uploadId=u-1&partNumber=1&partNumber=2"), pool)

    assert response.status_code == 400
    assert b"InvalidArgument" in response.body
    assert b"partNumber" in response.body
    pool.fetchrow.assert_not_called()


@pytest.mark.asyncio
async def test_the_same_part_number_twice_is_still_refused() -> None:
    """Equal values would resolve the same either way, but a repeated key is not a valid request."""
    response = await multipart.upload_part(_upload_part_request("uploadId=u-1&partNumber=3&partNumber=3"), MagicMock())

    assert response.status_code == 400
    assert b"InvalidArgument" in response.body


@pytest.mark.asyncio
async def test_a_single_part_number_passes_the_check() -> None:
    """One value reaches the existing validation, which is what rejects an out-of-range number."""
    response = await multipart.upload_part(_upload_part_request("uploadId=u-1&partNumber=0"), MagicMock())

    assert response.status_code == 400
    assert b"between 1 and" in response.body


@pytest.mark.asyncio
async def test_a_repeat_whose_last_value_is_empty_is_refused_too() -> None:
    """The dangerous shape: a value at the edge, nothing here.

    haproxy places this on `3`; `query_params.get` returns the empty last value. Before the
    router dispatched on presence this fell through to `handle_put_object` and the part's body
    REPLACED the whole object, so the refusal has to come from the repeat and not from the
    missing value — which is why the getlist check runs first.
    """
    response = await multipart.upload_part(_upload_part_request("uploadId=u-1&partNumber=3&partNumber="), MagicMock())

    assert response.status_code == 400
    assert b"InvalidArgument" in response.body
    assert b"exactly once" in response.body


def _put_object_request(query_string: str) -> Request:
    scope: dict[str, Any] = {
        "type": "http",
        "method": "PUT",
        "path": "/bucket/key",
        "raw_path": b"/bucket/key",
        "query_string": query_string.encode(),
        "headers": [],
    }
    return Request(scope)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "query_string",
    [
        "uploadId=u-1&partNumber=3",
        "uploadId=u-1&partNumber=3&partNumber=",
        "uploadId=u-1&partNumber=",
        "uploadId=&partNumber=3",
    ],
)
async def test_the_router_sends_every_shape_of_upload_part_to_upload_part(query_string: str) -> None:
    """Dispatch is on PRESENCE, so no empty value can route a part's body at the whole object.

    `upload_part` owns the verdict on every one of these — a repeat is `InvalidArgument`, an
    empty value is `InvalidRequest` — and none of them may reach `handle_put_object`, which
    would write the part as the object.
    """
    from unittest.mock import AsyncMock
    from unittest.mock import patch

    from hippius_s3.api.s3.objects import router as object_router

    with (
        patch.object(object_router, "upload_part", new_callable=AsyncMock) as mock_upload_part,
        patch.object(object_router, "handle_put_object", new_callable=AsyncMock) as mock_put,
    ):
        mock_upload_part.return_value = Response(status_code=200)

        await object_router.put_object("bucket", "key", _put_object_request(query_string), MagicMock(), MagicMock())

    mock_upload_part.assert_awaited_once()
    mock_put.assert_not_awaited()
