from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from fakeredis.aioredis import FakeRedis

from hippius_s3.cache import FileSystemPartsStore
from hippius_s3.queue import Chunk
from hippius_s3.queue import UploadChainRequest
from hippius_s3.services.hippius_api_service import UploadResponse
from hippius_s3.workers.uploader import Uploader


@pytest.fixture
def mock_config():
    config = MagicMock()
    config.uploader_multipart_max_concurrency = 5
    config.uploader_pin_parallelism = 5
    config.arion_upload_concurrency = 5
    config.arion_breaker_failure_threshold = 8
    config.arion_breaker_cooldown_seconds = 10.0
    config.cache_ttl_seconds = 1800
    config.object_cache_dir = "/tmp/test_cache"
    return config


@pytest.fixture
def mock_db_pool():
    pool = MagicMock()
    conn = AsyncMock()

    conn.fetchrow = AsyncMock()
    conn.fetch = AsyncMock()
    conn.execute = AsyncMock()

    async def mock_acquire():
        return conn

    pool.acquire = MagicMock(return_value=MagicMock(__aenter__=AsyncMock(return_value=conn)))

    return pool


@pytest.fixture
def mock_fs_store():
    store = MagicMock(spec=FileSystemPartsStore)
    store.get_chunk = AsyncMock(return_value=b"encrypted_chunk_data_123")
    store.get_meta = AsyncMock(return_value={"num_chunks": 1, "chunk_size": 1024})
    store.get_meta_with_wait = AsyncMock(return_value={"num_chunks": 1, "chunk_size": 1024})
    return store


class MockRow(dict):
    def __getitem__(self, key):
        if isinstance(key, int):
            keys = list(self.keys())
            return self[keys[key]]
        return super().__getitem__(key)


@pytest.mark.asyncio
async def test_upload_single_chunk_calls_new_api(mock_config, mock_db_pool, mock_fs_store):
    redis = FakeRedis()
    redis_queues = FakeRedis()

    mock_backend_client = MagicMock()
    uploader = Uploader(
        mock_db_pool, redis, redis_queues, mock_config, backend_name="arion", backend_client=mock_backend_client
    )
    uploader.fs_store = mock_fs_store

    mock_upload_response = UploadResponse(
        id="file-uuid-123",
        original_name="test.part1.chunk0",
        content_type="application/octet-stream",
        size_bytes=1024,
        sha256_hex="abc123",
        cid="QmChunk123",
        status="completed",
        file_url="https://example.com/file",
        created_at="2025-11-27T12:00:00Z",
        updated_at="2025-11-27T12:00:00Z",
    )

    chunk_uuid = "aaaabbbb-cccc-dddd-eeee-ffffffffffff"

    mock_conn = AsyncMock()
    mock_conn.fetchrow = AsyncMock(
        return_value=MockRow({"part_id": "part-uuid"}),
    )
    # First fetchval call returns chunk_id (UUID), second is insert_chunk_backend
    mock_conn.fetchval = AsyncMock(side_effect=[chunk_uuid, 1])

    mock_db_pool.acquire = MagicMock(return_value=MagicMock(__aenter__=AsyncMock(return_value=mock_conn)))

    # The backend client is injected — set up mock methods on it
    mock_api_instance = AsyncMock()
    mock_api_instance.upload_file_and_get_cid = AsyncMock(return_value=mock_upload_response)
    mock_api_instance.__aenter__ = AsyncMock(return_value=mock_api_instance)
    mock_api_instance.__aexit__ = AsyncMock()
    uploader.backend_client = mock_api_instance

    await uploader._upload_single_chunk(
        object_id="obj-123",
        object_key="test-key",
        chunk=Chunk(id=1),
        upload_id="upload-123",
        object_version=1,
        account_ss58="5FakeTestAccountAddress123456789012345678901234",
    )

    assert mock_api_instance.upload_file_and_get_cid.call_count == 1
    call_args = mock_api_instance.upload_file_and_get_cid.call_args
    assert call_args.kwargs["file_data"] == b"encrypted_chunk_data_123"
    assert call_args.kwargs["file_name"] == chunk_uuid
    assert call_args.kwargs["content_type"] == "application/octet-stream"
    assert call_args.kwargs["account_ss58"] == "5FakeTestAccountAddress123456789012345678901234"

    assert mock_conn.fetchval.call_count == 2


@pytest.mark.asyncio
async def test_upload_stores_backend_identifier_in_chunk_backend(mock_config, mock_db_pool, mock_fs_store):
    redis = FakeRedis()
    redis_queues = FakeRedis()

    mock_backend_client = MagicMock()
    uploader = Uploader(
        mock_db_pool, redis, redis_queues, mock_config, backend_name="arion", backend_client=mock_backend_client
    )
    uploader.fs_store = mock_fs_store

    mock_upload_response = UploadResponse(
        id="file-uuid-999",
        original_name="test.chunk",
        content_type="application/octet-stream",
        size_bytes=1024,
        sha256_hex="xyz789",
        cid="QmTestCID",
        status="completed",
        file_url="https://example.com/file",
        created_at="2025-11-27T12:00:00Z",
        updated_at="2025-11-27T12:00:00Z",
    )

    chunk_uuid = "aaaabbbb-cccc-dddd-eeee-ffffffffffff"

    mock_conn = AsyncMock()
    mock_conn.fetchrow = AsyncMock(
        return_value=MockRow({"part_id": "part-uuid"}),
    )
    # First fetchval call returns chunk_id (UUID), second is insert_chunk_backend
    mock_conn.fetchval = AsyncMock(side_effect=[chunk_uuid, 1])

    mock_db_pool.acquire = MagicMock(return_value=MagicMock(__aenter__=AsyncMock(return_value=mock_conn)))

    # The backend client is injected
    mock_api_instance = AsyncMock()
    mock_api_instance.upload_file_and_get_cid = AsyncMock(return_value=mock_upload_response)
    mock_api_instance.__aenter__ = AsyncMock(return_value=mock_api_instance)
    mock_api_instance.__aexit__ = AsyncMock()
    uploader.backend_client = mock_api_instance

    with patch("hippius_s3.workers.uploader.get_query") as mock_get_query:
        mock_get_query.return_value = "MOCKED_QUERY"

        await uploader._upload_single_chunk(
            object_id="obj-123",
            object_key="test-key",
            chunk=Chunk(id=1),
            upload_id="upload-123",
            object_version=1,
            account_ss58="5FakeTestAccountAddress123456789012345678901234",
        )

        fetchval_calls = mock_conn.fetchval.call_args_list
        insert_call = None
        for call in fetchval_calls:
            if call.args[0] == "MOCKED_QUERY":
                insert_call = call
                break

        assert insert_call is not None
        args = insert_call.args
        # insert_chunk_backend takes: query, part_id, chunk_index, backend, backend_identifier
        assert len(args) == 5
        assert args[3] == "arion"
        assert args[4] == "file-uuid-999"


@pytest.mark.asyncio
async def test_process_upload_skips_deleted_object(mock_config, mock_db_pool):
    redis = FakeRedis()
    redis_queues = FakeRedis()

    mock_backend_client = MagicMock()
    uploader = Uploader(
        mock_db_pool, redis, redis_queues, mock_config, backend_name="arion", backend_client=mock_backend_client
    )

    mock_conn = AsyncMock()
    mock_conn.fetchval = AsyncMock(return_value=True)
    mock_db_pool.acquire = MagicMock(return_value=MagicMock(__aenter__=AsyncMock(return_value=mock_conn)))

    payload = UploadChainRequest(
        substrate_url="http://test",
        address="user1",
        subaccount="user1",
        subaccount_seed_phrase="test-seed",
        bucket_name="test-bucket",
        object_key="test-key",
        should_encrypt=False,
        object_id="obj-123",
        object_version=1,
        chunks=[Chunk(id=1)],
        upload_id="upload-123",
        attempts=1,
        first_enqueued_at=1234567890.0,
        request_id="req-456",
    )

    with patch.object(uploader, "_upload_chunks", new_callable=AsyncMock) as mock_upload_chunks:
        result = await uploader.process_upload(payload)

        assert result == []
        mock_upload_chunks.assert_not_called()


@pytest.mark.asyncio
async def test_process_upload_skips_when_object_row_missing(mock_config, mock_db_pool):
    redis = FakeRedis()
    redis_queues = FakeRedis()

    mock_backend_client = MagicMock()
    uploader = Uploader(
        mock_db_pool, redis, redis_queues, mock_config, backend_name="arion", backend_client=mock_backend_client
    )

    mock_conn = AsyncMock()
    # Query returns True when the objects row doesn't exist (outer COALESCE defaults to TRUE)
    mock_conn.fetchval = AsyncMock(return_value=True)
    mock_db_pool.acquire = MagicMock(return_value=MagicMock(__aenter__=AsyncMock(return_value=mock_conn)))

    payload = UploadChainRequest(
        substrate_url="http://test",
        address="user1",
        subaccount="user1",
        subaccount_seed_phrase="test-seed",
        bucket_name="test-bucket",
        object_key="test-key",
        should_encrypt=False,
        object_id="obj-123",
        object_version=1,
        chunks=[Chunk(id=1)],
        upload_id="upload-123",
        attempts=1,
        first_enqueued_at=1234567890.0,
        request_id="req-456",
    )

    with patch.object(uploader, "_upload_chunks", new_callable=AsyncMock) as mock_upload_chunks:
        result = await uploader.process_upload(payload)

        assert result == []
        mock_upload_chunks.assert_not_called()


@pytest.mark.asyncio
async def test_upload_passes_extra_headers_when_bypass_billing(mock_config, mock_db_pool, mock_fs_store):
    mock_config.arion_billing_bypass_key = "secret-bypass-key"
    redis = FakeRedis()
    redis_queues = FakeRedis()

    mock_backend_client = MagicMock()
    uploader = Uploader(
        mock_db_pool, redis, redis_queues, mock_config, backend_name="arion", backend_client=mock_backend_client
    )
    uploader.fs_store = mock_fs_store

    mock_upload_response = UploadResponse(
        id="file-uuid-123",
        original_name="test.part1.chunk0",
        content_type="application/octet-stream",
        size_bytes=1024,
        sha256_hex="abc123",
        cid="QmChunk123",
        status="completed",
        file_url="https://example.com/file",
        created_at="2025-11-27T12:00:00Z",
        updated_at="2025-11-27T12:00:00Z",
    )

    chunk_uuid = "aaaabbbb-cccc-dddd-eeee-ffffffffffff"

    mock_conn = AsyncMock()
    mock_conn.fetchrow = AsyncMock(return_value=MockRow({"part_id": "part-uuid"}))
    mock_conn.fetchval = AsyncMock(side_effect=[chunk_uuid, 1])
    mock_db_pool.acquire = MagicMock(return_value=MagicMock(__aenter__=AsyncMock(return_value=mock_conn)))

    mock_api_instance = AsyncMock()
    mock_api_instance.upload_file_and_get_cid = AsyncMock(return_value=mock_upload_response)
    uploader.backend_client = mock_api_instance

    await uploader._upload_single_chunk(
        object_id="obj-123",
        object_key="test-key",
        chunk=Chunk(id=1),
        upload_id="upload-123",
        object_version=1,
        account_ss58="5FakeTestAccountAddress123456789012345678901234",
        extra_headers={"X-Billing-Bypass": "secret-bypass-key"},
    )

    call_args = mock_api_instance.upload_file_and_get_cid.call_args
    assert call_args.kwargs["extra_headers"] == {"X-Billing-Bypass": "secret-bypass-key"}


@pytest.mark.asyncio
async def test_process_upload_sets_bypass_billing_headers(mock_config, mock_db_pool):
    mock_config.arion_billing_bypass_key = "secret-bypass-key"
    redis = FakeRedis()
    redis_queues = FakeRedis()

    mock_backend_client = MagicMock()
    uploader = Uploader(
        mock_db_pool, redis, redis_queues, mock_config, backend_name="arion", backend_client=mock_backend_client
    )

    mock_conn = AsyncMock()
    mock_conn.fetchval = AsyncMock(return_value=False)
    mock_db_pool.acquire = MagicMock(return_value=MagicMock(__aenter__=AsyncMock(return_value=mock_conn)))

    payload = UploadChainRequest(
        address="user1",
        bucket_name="test-bucket",
        object_key="test-key",
        object_id="obj-123",
        object_version=1,
        chunks=[Chunk(id=1)],
        upload_id="upload-123",
        bypass_billing=True,
    )

    with patch.object(uploader, "_upload_chunks", new_callable=AsyncMock) as mock_upload_chunks:
        mock_upload_chunks.return_value = ["QmCID1"]
        await uploader.process_upload(payload)

        call_args = mock_upload_chunks.call_args
        assert call_args.kwargs["extra_headers"] == {"X-Billing-Bypass": "secret-bypass-key"}


@pytest.mark.asyncio
async def test_process_upload_no_bypass_headers_when_flag_false(mock_config, mock_db_pool):
    mock_config.arion_billing_bypass_key = "secret-bypass-key"
    redis = FakeRedis()
    redis_queues = FakeRedis()

    mock_backend_client = MagicMock()
    uploader = Uploader(
        mock_db_pool, redis, redis_queues, mock_config, backend_name="arion", backend_client=mock_backend_client
    )

    mock_conn = AsyncMock()
    mock_conn.fetchval = AsyncMock(return_value=False)
    mock_db_pool.acquire = MagicMock(return_value=MagicMock(__aenter__=AsyncMock(return_value=mock_conn)))

    payload = UploadChainRequest(
        address="user1",
        bucket_name="test-bucket",
        object_key="test-key",
        object_id="obj-123",
        object_version=1,
        chunks=[Chunk(id=1)],
        upload_id="upload-123",
        bypass_billing=False,
    )

    with patch.object(uploader, "_upload_chunks", new_callable=AsyncMock) as mock_upload_chunks:
        mock_upload_chunks.return_value = ["QmCID1"]
        await uploader.process_upload(payload)

        call_args = mock_upload_chunks.call_args
        assert call_args.kwargs["extra_headers"] is None


@pytest.mark.asyncio
async def test_process_upload_no_longer_calls_pin_on_api(mock_config, mock_db_pool):
    redis = FakeRedis()
    redis_queues = FakeRedis()
    mock_config.publish_to_chain = True

    mock_backend_client = MagicMock()
    uploader = Uploader(
        mock_db_pool, redis, redis_queues, mock_config, backend_name="arion", backend_client=mock_backend_client
    )

    mock_conn = AsyncMock()
    mock_conn.execute = AsyncMock()
    mock_conn.fetchval = AsyncMock(return_value=False)
    mock_db_pool.acquire = MagicMock(return_value=MagicMock(__aenter__=AsyncMock(return_value=mock_conn)))

    payload = UploadChainRequest(
        substrate_url="http://test",
        address="user1",
        subaccount="user1",
        subaccount_seed_phrase="test-seed",
        bucket_name="test-bucket",
        object_key="test-key",
        should_encrypt=False,
        object_id="obj-123",
        object_version=1,
        chunks=[Chunk(id=1)],
        upload_id="upload-123",
        attempts=1,
        first_enqueued_at=1234567890.0,
        request_id="req-456",
    )

    with patch.object(uploader, "_upload_chunks", new_callable=AsyncMock) as mock_upload_chunks:
        mock_upload_chunks.return_value = ["QmCID1"]

        result = await uploader.process_upload(payload)

        assert "QmCID1" in result
        assert not hasattr(uploader, "pin_on_api")


# ============================================================================ #
# Meta-write race: end-to-end chain that the original DLQ pileup hinged on.
#
# The chain we MUST keep working:
#   _upload_single_chunk
#     → fs_store.get_meta_with_wait(...) returns None  (poll deadline expired)
#     → raise RuntimeError("Missing meta in filesystem cache after Ns ...")
#     → classify_upload_error(error) == "permanent"
#     → uploader's main loop routes to DLQ as permanent (no infinite retry)
#
# Unit tests already cover each link in isolation (get_meta_with_wait polling
# in test_fs_store.py; the classifier in test_classify_errors.py). The
# regression risk is in the SEAM between them — that the upload code raises
# the EXACT message shape the classifier recognises.
# ============================================================================ #


@pytest.mark.asyncio
async def test_upload_raises_after_deadline_message_when_meta_missing(mock_config, mock_db_pool, mock_fs_store):
    """The uploader must raise the after-deadline message (not the pre-deadline
    one) when get_meta_with_wait returns None — otherwise the classifier would
    mark it transient and the worker would retry the failing slot forever."""
    redis = FakeRedis()
    redis_queues = FakeRedis()
    mock_config.fs_meta_wait_seconds = 0.1

    # The whole point of the race fix: get_meta_with_wait returns None when
    # the writer hasn't propagated meta within the deadline.
    mock_fs_store.get_meta_with_wait = AsyncMock(return_value=None)

    uploader = Uploader(
        mock_db_pool, redis, redis_queues, mock_config, backend_name="arion", backend_client=MagicMock()
    )
    uploader.fs_store = mock_fs_store

    with pytest.raises(RuntimeError) as excinfo:
        await uploader._upload_single_chunk(
            object_id="obj-race-1",
            object_key="test-key",
            chunk=Chunk(id=1),
            upload_id="upload-race-1",
            object_version=1,
            account_ss58="5FakeTestAccountAddress123456789012345678901234",
        )

    msg = str(excinfo.value)
    # The exact substring the classifier's _PERMANENT_KEYWORDS keys on. If this
    # ever drifts, the classifier will mis-route the failure (back to
    # transient) and we will retry until DLQ — re-creating the original bug.
    assert "missing meta in filesystem cache after" in msg.lower()
    assert "object_id=obj-race-1" in msg
    assert "version=1" in msg
    assert "part=1" in msg

    # And the deadline-poll must have been awaited with the configured value,
    # not skipped or hardcoded.
    mock_fs_store.get_meta_with_wait.assert_awaited_once()
    _, kwargs = mock_fs_store.get_meta_with_wait.call_args
    assert kwargs.get("deadline_seconds") == 0.1


@pytest.mark.asyncio
async def test_uploader_after_deadline_message_classifies_as_permanent(mock_config, mock_db_pool, mock_fs_store):
    """End-to-end seam check: take the EXACT exception the uploader produces
    on a stuck meta and run it through the actual classifier. The original
    bug was a mismatch between the message string and the keyword list — this
    test exists to make any future drift between them an immediate red CI."""
    from hippius_s3.workers.errors import classify_upload_error

    redis = FakeRedis()
    redis_queues = FakeRedis()
    mock_config.fs_meta_wait_seconds = 0.05
    mock_fs_store.get_meta_with_wait = AsyncMock(return_value=None)

    uploader = Uploader(
        mock_db_pool, redis, redis_queues, mock_config, backend_name="arion", backend_client=MagicMock()
    )
    uploader.fs_store = mock_fs_store

    with pytest.raises(RuntimeError) as excinfo:
        await uploader._upload_single_chunk(
            object_id="obj-race-2",
            object_key="test-key",
            chunk=Chunk(id=1),
            upload_id="upload-race-2",
            object_version=1,
            account_ss58="5FakeTestAccountAddress123456789012345678901234",
        )

    # The whole reason we changed the message string and the keyword list
    # together: this exception, raised by THIS code path, must be permanent.
    # If the classifier returns "transient" we'd retry forever on a fault.
    # If it returns "unknown" we'd land in DLQ but mark it for human review
    # (also wrong — this IS a genuine permanent failure).
    assert classify_upload_error(excinfo.value) == "permanent"


@pytest.mark.asyncio
async def test_uploader_does_not_raise_when_meta_lands_within_deadline(mock_config, mock_db_pool, mock_fs_store):
    """The race-fix sibling assertion: when get_meta_with_wait succeeds (the
    common case in production, where api + workers share a node), the upload
    path takes the normal happy path with no error and no retry overhead."""
    redis = FakeRedis()
    redis_queues = FakeRedis()
    mock_config.fs_meta_wait_seconds = 30.0

    # Meta returns normally — this is the steady-state production behavior.
    mock_fs_store.get_meta_with_wait = AsyncMock(return_value={"num_chunks": 1, "chunk_size": 1024})

    chunk_uuid = "aaaabbbb-cccc-dddd-eeee-ffffffffffff"
    mock_conn = AsyncMock()
    mock_conn.fetchrow = AsyncMock(return_value=MockRow({"part_id": "part-uuid"}))
    mock_conn.fetchval = AsyncMock(side_effect=[chunk_uuid, 1])
    mock_db_pool.acquire = MagicMock(return_value=MagicMock(__aenter__=AsyncMock(return_value=mock_conn)))

    mock_upload_response = UploadResponse(
        id="file-uuid-happy",
        original_name="test.part1.chunk0",
        content_type="application/octet-stream",
        size_bytes=1024,
        sha256_hex="abc",
        cid="QmHappy",
        status="completed",
        file_url="https://example.com/file",
        created_at="2025-11-27T12:00:00Z",
        updated_at="2025-11-27T12:00:00Z",
    )
    mock_api_instance = AsyncMock()
    mock_api_instance.upload_file_and_get_cid = AsyncMock(return_value=mock_upload_response)
    mock_api_instance.__aenter__ = AsyncMock(return_value=mock_api_instance)
    mock_api_instance.__aexit__ = AsyncMock()

    uploader = Uploader(
        mock_db_pool, redis, redis_queues, mock_config, backend_name="arion", backend_client=mock_api_instance
    )
    uploader.fs_store = mock_fs_store

    # Should NOT raise.
    await uploader._upload_single_chunk(
        object_id="obj-happy",
        object_key="test-key",
        chunk=Chunk(id=1),
        upload_id="upload-happy",
        object_version=1,
        account_ss58="5FakeTestAccountAddress123456789012345678901234",
    )

    # And the upload actually proceeded — no early bail-out hiding a regression.
    assert mock_api_instance.upload_file_and_get_cid.call_count == 1


@pytest.mark.asyncio
async def test_part_chunks_upload_concurrently_bounded_by_arion_concurrency(mock_config, mock_db_pool, mock_fs_store):
    """A multi-chunk part uploads its chunks CONCURRENTLY, capped by the shared
    per-pod arion_upload_concurrency semaphore — not one at a time. Regression
    guard for the serial inner loop that pinned a worker on a large object."""
    import asyncio

    mock_config.arion_upload_concurrency = 4
    num_chunks = 8
    mock_fs_store.get_meta_with_wait = AsyncMock(return_value={"num_chunks": num_chunks, "chunk_size": 1024})

    redis = FakeRedis()
    redis_queues = FakeRedis()

    # Record the peak number of simultaneously in-flight Arion uploads.
    inflight = {"cur": 0, "max": 0}

    async def fake_upload(**kwargs):
        inflight["cur"] += 1
        inflight["max"] = max(inflight["max"], inflight["cur"])
        await asyncio.sleep(0.05)
        inflight["cur"] -= 1
        return UploadResponse(
            id=f"hash-{kwargs['file_name']}",
            original_name="x",
            content_type="application/octet-stream",
            size_bytes=1024,
            sha256_hex="abc",
            cid=f"Qm-{kwargs['file_name']}",
            status="completed",
            file_url="https://example.com/file",
            created_at="2025-11-27T12:00:00Z",
            updated_at="2025-11-27T12:00:00Z",
        )

    mock_api = AsyncMock()
    mock_api.upload_file_and_get_cid = AsyncMock(side_effect=fake_upload)

    uploader = Uploader(mock_db_pool, redis, redis_queues, mock_config, backend_name="arion", backend_client=mock_api)
    uploader.fs_store = mock_fs_store

    mock_conn = AsyncMock()
    mock_conn.fetchrow = AsyncMock(return_value=MockRow({"part_id": "part-uuid"}))

    def fake_fetchval(sql, *args):
        if sql == "INSERT_SENTINEL":
            return 1
        # the chunk_id SELECT: args == (part_id, chunk_index)
        return f"chunk-{args[1]}"

    mock_conn.fetchval = AsyncMock(side_effect=fake_fetchval)
    mock_db_pool.acquire = MagicMock(return_value=MagicMock(__aenter__=AsyncMock(return_value=mock_conn)))

    with patch("hippius_s3.workers.uploader.get_query", return_value="INSERT_SENTINEL"):
        result = await uploader._upload_single_chunk(
            object_id="obj-multi",
            object_key="test-key",
            chunk=Chunk(id=1),
            upload_id="upload-multi",
            object_version=1,
            account_ss58="5FakeTestAccountAddress123456789012345678901234",
        )

    # Every chunk was uploaded.
    assert mock_api.upload_file_and_get_cid.call_count == num_chunks
    # They overlapped (parallel), and never exceeded the configured bound.
    assert inflight["max"] > 1, "chunks uploaded serially — within-part parallelization regressed"
    assert inflight["max"] <= 4
    # Returned hashes preserve chunk order 0..N-1 despite concurrent completion.
    assert result.cids == [f"hash-chunk-{ci}" for ci in range(num_chunks)]
    # Exactly one chunk_backend row written per chunk.
    insert_calls = [c for c in mock_conn.fetchval.call_args_list if c.args[0] == "INSERT_SENTINEL"]
    assert len(insert_calls) == num_chunks


@pytest.mark.asyncio
async def test_put_semaphore_bounds_arion_posts_across_concurrent_requests(mock_config, mock_db_pool, mock_fs_store):
    """The shared per-pod _put_semaphore caps concurrent Arion POSTs across
    MULTIPLE in-flight requests on one Uploader — the core throttle that lets
    outer request-concurrency scale without stampeding the backend."""
    import asyncio

    mock_config.arion_upload_concurrency = 3
    mock_fs_store.get_meta_with_wait = AsyncMock(return_value={"num_chunks": 4, "chunk_size": 1024})

    inflight = {"cur": 0, "max": 0}

    async def fake_upload(**kwargs):
        inflight["cur"] += 1
        inflight["max"] = max(inflight["max"], inflight["cur"])
        await asyncio.sleep(0.03)
        inflight["cur"] -= 1
        return UploadResponse(
            id=f"h-{kwargs['file_name']}",
            original_name="x",
            content_type="application/octet-stream",
            size_bytes=1024,
            sha256_hex="a",
            cid="Qm",
            status="completed",
            file_url="https://example.com/file",
            created_at="2025-11-27T12:00:00Z",
            updated_at="2025-11-27T12:00:00Z",
        )

    mock_api = AsyncMock()
    mock_api.upload_file_and_get_cid = AsyncMock(side_effect=fake_upload)
    uploader = Uploader(
        mock_db_pool, FakeRedis(), FakeRedis(), mock_config, backend_name="arion", backend_client=mock_api
    )
    uploader.fs_store = mock_fs_store

    mock_conn = AsyncMock()
    mock_conn.fetchrow = AsyncMock(return_value=MockRow({"part_id": "p"}))
    mock_conn.fetchval = AsyncMock(side_effect=lambda sql, *a: 1 if sql == "INSERT_SENTINEL" else f"c-{a[1]}")
    mock_db_pool.acquire = MagicMock(return_value=MagicMock(__aenter__=AsyncMock(return_value=mock_conn)))

    addr = "5FakeTestAccountAddress123456789012345678901234"
    with patch("hippius_s3.workers.uploader.get_query", return_value="INSERT_SENTINEL"):
        await asyncio.gather(
            uploader._upload_single_chunk(
                object_id="o1", object_key="k", chunk=Chunk(id=1), upload_id="u1", object_version=1, account_ss58=addr
            ),
            uploader._upload_single_chunk(
                object_id="o2", object_key="k", chunk=Chunk(id=1), upload_id="u2", object_version=1, account_ss58=addr
            ),
        )

    # 2 requests x 4 chunks = 8 POSTs, but the shared semaphore caps concurrency at 3.
    assert mock_api.upload_file_and_get_cid.call_count == 8
    assert inflight["max"] > 1, "no overlap — shared semaphore not parallelizing"
    assert inflight["max"] <= 3, "shared per-pod Arion concurrency bound breached"


@pytest.mark.asyncio
async def test_part_chunk_upload_failure_propagates(mock_config, mock_db_pool, mock_fs_store):
    """If any chunk in a part fails, the whole part upload raises (so the
    request is retried / DLQ'd) — concurrency must not swallow errors."""
    import asyncio

    mock_config.arion_upload_concurrency = 4
    num_chunks = 6
    mock_fs_store.get_meta_with_wait = AsyncMock(return_value={"num_chunks": num_chunks, "chunk_size": 1024})

    async def fake_upload(**kwargs):
        await asyncio.sleep(0.01)
        if kwargs["file_name"] == "chunk-3":
            raise RuntimeError("arion boom on chunk 3")
        return UploadResponse(
            id=f"hash-{kwargs['file_name']}",
            original_name="x",
            content_type="application/octet-stream",
            size_bytes=1024,
            sha256_hex="abc",
            cid="Qm",
            status="completed",
            file_url="https://example.com/file",
            created_at="2025-11-27T12:00:00Z",
            updated_at="2025-11-27T12:00:00Z",
        )

    mock_api = AsyncMock()
    mock_api.upload_file_and_get_cid = AsyncMock(side_effect=fake_upload)

    uploader = Uploader(
        mock_db_pool, FakeRedis(), FakeRedis(), mock_config, backend_name="arion", backend_client=mock_api
    )
    uploader.fs_store = mock_fs_store

    mock_conn = AsyncMock()
    mock_conn.fetchrow = AsyncMock(return_value=MockRow({"part_id": "part-uuid"}))
    mock_conn.fetchval = AsyncMock(side_effect=lambda sql, *a: 1 if sql == "INSERT_SENTINEL" else f"chunk-{a[1]}")
    mock_db_pool.acquire = MagicMock(return_value=MagicMock(__aenter__=AsyncMock(return_value=mock_conn)))

    with patch("hippius_s3.workers.uploader.get_query", return_value="INSERT_SENTINEL"):
        with pytest.raises(RuntimeError, match="arion boom on chunk 3"):
            await uploader._upload_single_chunk(
                object_id="obj-fail",
                object_key="test-key",
                chunk=Chunk(id=1),
                upload_id="upload-fail",
                object_version=1,
                account_ss58="5FakeTestAccountAddress123456789012345678901234",
            )
