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
