import logging
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from hippius_s3.queue import UnpinChainRequest
from hippius_s3.workers.unpinner import process_unpin_request


def _make_request(attempts: int = 0) -> UnpinChainRequest:
    return UnpinChainRequest(
        address="5FakeAddress",
        object_id="obj-123",
        object_version=1,
        attempts=attempts,
        request_id="req-001",
    )


@pytest.mark.asyncio
async def test_unpin_retries_when_no_chunk_backend_rows():
    request = _make_request(attempts=0)

    mock_conn = AsyncMock()
    mock_conn.fetch = AsyncMock(return_value=[])
    mock_pool = MagicMock()
    mock_pool.acquire = MagicMock(
        return_value=MagicMock(__aenter__=AsyncMock(return_value=mock_conn))
    )

    worker_logger = logging.LoggerAdapter(logging.getLogger("test"), {})
    dlq_manager = MagicMock()

    with patch(
        "hippius_s3.workers.unpinner.enqueue_unpin_retry_request", new_callable=AsyncMock
    ) as mock_retry:
        await process_unpin_request(
            request,
            backend_name="ipfs",
            backend_client_factory=MagicMock(),
            worker_logger=worker_logger,
            dlq_manager=dlq_manager,
            db_pool=mock_pool,
        )

        mock_retry.assert_called_once()
        call_kwargs = mock_retry.call_args
        assert call_kwargs.kwargs["last_error"] == "no_chunk_backend_rows"
        assert call_kwargs.kwargs["backend_name"] == "ipfs"


@pytest.mark.asyncio
async def test_unpin_gives_up_after_max_empty_retries():
    request = _make_request(attempts=6)

    mock_conn = AsyncMock()
    mock_conn.fetch = AsyncMock(return_value=[])
    mock_pool = MagicMock()
    mock_pool.acquire = MagicMock(
        return_value=MagicMock(__aenter__=AsyncMock(return_value=mock_conn))
    )

    worker_logger = logging.LoggerAdapter(logging.getLogger("test"), {})
    dlq_manager = MagicMock()

    with patch(
        "hippius_s3.workers.unpinner.enqueue_unpin_retry_request", new_callable=AsyncMock
    ) as mock_retry:
        await process_unpin_request(
            request,
            backend_name="ipfs",
            backend_client_factory=MagicMock(),
            worker_logger=worker_logger,
            dlq_manager=dlq_manager,
            db_pool=mock_pool,
        )

        mock_retry.assert_not_called()
