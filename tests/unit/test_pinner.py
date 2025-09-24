"""Unit tests for pinner logic."""

from unittest.mock import AsyncMock
from unittest.mock import Mock
from unittest.mock import patch

import pytest

from hippius_s3.config import Config
from hippius_s3.queue import Chunk
from hippius_s3.queue import UploadChainRequest
from hippius_s3.workers.pinner import Pinner
from hippius_s3.workers.pinner import classify_error
from hippius_s3.workers.pinner import compute_backoff_ms


@pytest.fixture
def mock_config() -> Mock:
    """Mock config for testing."""
    config = Mock(spec=Config)
    config.pinner_max_attempts = 3
    config.pinner_backoff_base_ms = 100
    config.pinner_backoff_max_ms = 1000
    config.cache_ttl_seconds = 86400
    return config


@pytest.fixture
def pinner(mock_config: Mock) -> Pinner:
    """Pinner instance with mocked dependencies."""
    db = AsyncMock()
    ipfs_service = AsyncMock()
    redis_client = AsyncMock()
    return Pinner(db, ipfs_service, redis_client, mock_config)


def test_classify_error_transient() -> None:
    """Test error classification for transient errors."""
    assert classify_error(ConnectionError("timeout"))
    assert classify_error(ValueError("503 Service Unavailable"))
    assert classify_error(Exception("connection reset"))


def test_classify_error_permanent() -> None:
    """Test error classification for permanent errors."""
    assert not classify_error(ValueError("invalid input"))
    assert not classify_error(RuntimeError("auth failed"))
    assert not classify_error(Exception("normal error"))


def test_compute_backoff_ms() -> None:
    """Test backoff computation with jitter."""
    # First attempt (attempt=1 means first retry after initial failure)
    delay1 = compute_backoff_ms(attempt=1, base_ms=100, max_ms=10000)
    assert 80 <= delay1 <= 120  # 100ms ±20%

    # Second attempt
    delay2 = compute_backoff_ms(attempt=2, base_ms=100, max_ms=10000)
    assert 160 <= delay2 <= 240  # 200ms ±20%

    # Test max cap
    delay_max = compute_backoff_ms(attempt=10, base_ms=100, max_ms=500)
    assert delay_max <= 500


@pytest.mark.asyncio
async def test_process_item_success_simple(pinner: Pinner) -> None:
    """Test successful processing of simple upload."""
    payload = UploadChainRequest(
        substrate_url="http://test",
        ipfs_node="http://test",
        address="user1",
        subaccount="user1",
        subaccount_seed_phrase="test-seed",
        bucket_name="test-bucket",
        object_key="test-key",
        should_encrypt=False,
        object_id="obj-123",
        chunks=[Chunk(id=0)],
        upload_id=None,
        attempts=0,
        request_id="req-456",
    )

    # Mock successful processing
    mock_result = Mock()
    mock_result.file_hash = "bafkreid"
    mock_result.cid = "bafkreid"

    with patch.object(pinner, "_process_simple_upload", return_value=mock_result):
        files, success = await pinner.process_item(payload)

    assert success
    assert len(files) == 1
    assert files[0].file_hash == "bafkreid"

    # Should not call retry enqueue or DB failure update
    pinner.redis_client.zadd.assert_not_called()
    pinner.db.execute.assert_not_called()


@pytest.mark.asyncio
async def test_process_item_transient_error_retry(pinner: Pinner) -> None:
    """Test transient error triggers retry."""
    payload = UploadChainRequest(
        substrate_url="http://test",
        ipfs_node="http://test",
        address="user1",
        subaccount="user1",
        subaccount_seed_phrase="test-seed",
        bucket_name="test-bucket",
        object_key="test-key",
        should_encrypt=False,
        object_id="obj-123",
        chunks=[Chunk(id=0)],
        upload_id=None,
        attempts=1,  # Already tried once
        request_id="req-456",
    )

    with patch.object(pinner, "_process_chunks_only", side_effect=ConnectionError("timeout")):
        files, success = await pinner.process_item(payload)

    assert not success
    assert files == []

    # Should enqueue retry
    pinner.redis_client.zadd.assert_called_once()
    # Should not mark as failed
    pinner.db.execute.assert_not_called()


@pytest.mark.asyncio
async def test_process_item_permanent_error_fail(pinner: Pinner) -> None:
    """Test permanent error marks object as failed."""
    payload = UploadChainRequest(
        substrate_url="http://test",
        ipfs_node="http://test",
        address="user1",
        subaccount="user1",
        subaccount_seed_phrase="test-seed",
        bucket_name="test-bucket",
        object_key="test-key",
        should_encrypt=False,
        object_id="obj-123",
        chunks=[Chunk(id=0)],
        upload_id=None,
        attempts=5,  # Max attempts exceeded
        request_id="req-456",
    )

    with patch.object(pinner, "_process_chunks_only", side_effect=ValueError("invalid data")):
        files, success = await pinner.process_item(payload)

    assert not success
    assert files == []

    # Should mark as failed in DB
    pinner.db.execute.assert_called_once_with("UPDATE objects SET status = 'failed' WHERE object_id = $1", "obj-123")
    # Should not enqueue retry
    pinner.redis_client.zadd.assert_not_called()


@pytest.mark.asyncio
async def test_process_batch_mixed_results(pinner: Pinner) -> None:
    """Test batch processing isolates failures."""
    payload1 = UploadChainRequest(
        substrate_url="http://test",
        ipfs_node="http://test",
        address="user1",
        subaccount="user1",
        subaccount_seed_phrase="test-seed",
        bucket_name="test-bucket",
        object_key="test-key-1",
        should_encrypt=False,
        object_id="obj-1",
        chunks=[Chunk(id=0)],
        upload_id=None,
        attempts=0,
        request_id="req-1",
    )

    payload2 = UploadChainRequest(
        substrate_url="http://test",
        ipfs_node="http://test",
        address="user1",
        subaccount="user1",
        subaccount_seed_phrase="test-seed",
        bucket_name="test-bucket",
        object_key="test-key-2",
        should_encrypt=False,
        object_id="obj-2",
        chunks=[Chunk(id=0)],
        upload_id=None,
        attempts=0,
        request_id="req-2",
    )

    # Mock: first succeeds, second fails and retries
    mock_result1 = Mock()
    mock_result1.file_hash = "bafkreid1"
    mock_result1.cid = "bafkreid1"

    with patch.object(pinner, "_process_chunks_only", side_effect=[mock_result1, ConnectionError("timeout")]):
        files, succeeded_payloads = await pinner.process_batch([payload1, payload2])

    # Only successful file returned
    assert len(files) == 1
    assert files[0].file_hash == "bafkreid1"

    # Only successful payload returned
    assert len(succeeded_payloads) == 1
    assert succeeded_payloads[0].object_id == "obj-1"
