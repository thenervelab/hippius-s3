import asyncio
import json
import time
from typing import Any
from typing import Callable
from typing import Dict
from typing import List

import asyncpg
import pytest
import pytest_asyncio  # type: ignore[import-not-found]
import redis

from hippius_s3.config import get_config


pytestmark = pytest.mark.integration


@pytest_asyncio.fixture
async def db_connection() -> Any:
    """Async database connection for direct DB queries."""
    conn = await asyncpg.connect("postgresql://postgres:postgres@localhost:5432/hippius")
    yield conn
    await conn.close()


@pytest.fixture
def redis_chain_client() -> Any:
    """Redis client for chain cache (where pinned_cids are stored)."""
    client = redis.Redis.from_url("redis://localhost:6381/0")
    yield client
    client.close()


@pytest.fixture
def redis_dlq_client() -> Any:
    """Redis client for DLQ (main cache)."""
    client = redis.Redis.from_url("redis://localhost:6379/0")
    yield client
    client.delete("pinner:dlq")  # type: ignore[misc]
    client.close()


@pytest_asyncio.fixture
async def pin_checker_runner(db_connection: asyncpg.Connection) -> Any:
    """Helper to manually run pin checker for a specific user."""
    from hippius_s3.monitoring import initialize_metrics_collector
    from hippius_s3.queue import initialize_queue_client
    from hippius_s3.redis_cache import initialize_cache_client
    from hippius_s3.redis_chain import initialize_chain_client

    config = get_config()

    import redis.asyncio as async_redis

    redis_cache = async_redis.from_url(config.redis_url)
    redis_chain = async_redis.from_url(config.redis_chain_url)
    redis_queues = async_redis.from_url(config.redis_queues_url)

    initialize_cache_client(redis_cache)
    initialize_chain_client(redis_chain)
    initialize_queue_client(redis_queues)
    initialize_metrics_collector(redis_chain)

    from workers.chain_pin_checker import check_user_cids

    async def _run(user: str) -> None:
        await check_user_cids(db_connection, user)

    yield _run

    await redis_cache.close()  # type: ignore[misc]
    await redis_chain.close()  # type: ignore[misc]
    await redis_queues.close()  # type: ignore[misc]


async def get_user_from_bucket(db: asyncpg.Connection, bucket_name: str) -> str:
    """Get main_account_id from bucket."""
    row = await db.fetchrow(
        """
        SELECT main_account_id FROM buckets
        WHERE bucket_name = $1
        """,
        bucket_name,
    )
    if not row or not row["main_account_id"]:
        raise RuntimeError(f"No main_account_id found for bucket {bucket_name}")
    return str(row["main_account_id"])


async def backdate_object_creation(db: asyncpg.Connection, object_id: str, hours_ago: int = 2) -> None:
    """Backdate object created_at timestamp to make it eligible for pin checking.

    Pin checker only processes objects older than 1 hour to avoid race conditions with uploader.
    """
    await db.execute(
        """
        UPDATE objects
        SET created_at = NOW() - INTERVAL '1 hour' * $2
        WHERE object_id = $1
        """,
        object_id,
        hours_ago,
    )


def mock_chain_profile(redis_chain: redis.Redis, user: str, cids: List[str], timestamp: float | None = None) -> None:
    """Mock chain profile data for a user."""
    data = {
        "timestamp": timestamp or time.time(),
        "cids": cids,
    }
    redis_chain.set(f"pinned_cids:{user}", json.dumps(data))  # type: ignore[misc]


async def get_part_chunk_cids(db: asyncpg.Connection, object_id: str, object_version: int) -> List[Dict[str, Any]]:
    """Get all part_chunk CIDs for an object."""
    rows = await db.fetch(
        """
        SELECT pc.cid, pc.pin_attempts, pc.last_pinned_at
        FROM part_chunks pc
        JOIN parts p ON p.part_id = pc.part_id
        WHERE p.object_id = $1 AND p.object_version = $2
        ORDER BY p.part_number, pc.chunk_index
        """,
        object_id,
        object_version,
    )
    return [{"cid": str(r[0]), "pin_attempts": r[1], "last_pinned_at": r[2]} for r in rows]


def get_dlq_entries(redis_client: redis.Redis) -> List[Dict[str, Any]]:
    """Get all DLQ entries."""
    raw_data = redis_client.lrange("pinner:dlq", 0, -1)  # type: ignore[misc]
    raw: List[Any] = list(raw_data) if raw_data else []  # type: ignore[arg-type]
    return [json.loads(entry) for entry in raw]


async def get_object_status(db: asyncpg.Connection, object_id: str, object_version: int) -> str | None:
    """Get object_versions status."""
    row = await db.fetchrow(
        """
        SELECT status FROM object_versions
        WHERE object_id = $1 AND object_version = $2
        """,
        object_id,
        object_version,
    )
    return row["status"] if row else None


async def wait_for_cids(
    db: asyncpg.Connection,
    bucket_name: str,
    object_key: str,
    min_cids: int = 1,
    timeout_seconds: float = 30.0,
) -> tuple[str, int, List[str]]:
    """Wait for uploader worker to assign CIDs to part_chunks.

    Returns: (object_id, object_version, list_of_cids)
    """
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        row = await db.fetchrow(
            """
            SELECT o.object_id, o.current_object_version
            FROM objects o
            JOIN buckets b ON b.bucket_id = o.bucket_id
            WHERE b.bucket_name = $1 AND o.object_key = $2
            """,
            bucket_name,
            object_key,
        )
        if not row:
            await asyncio.sleep(0.5)
            continue

        object_id, object_version = str(row[0]), int(row[1] or 1)

        rows = await db.fetch(
            """
            SELECT pc.cid
            FROM part_chunks pc
            JOIN parts p ON p.part_id = pc.part_id
            WHERE p.object_id = $1 AND p.object_version = $2
            AND pc.cid IS NOT NULL
            AND pc.cid != ''
            AND pc.cid != 'pending'
            ORDER BY p.part_number, pc.chunk_index
            """,
            object_id,
            object_version,
        )

        cids = [str(r[0]) for r in rows]
        if len(cids) >= min_cids:
            return object_id, object_version, cids

        await asyncio.sleep(0.5)

    raise TimeoutError(f"Timeout waiting for {min_cids} CIDs for {bucket_name}/{object_key}")


@pytest.mark.asyncio
async def test_pin_checker_dlq_workflow_complete(
    docker_services: Any,
    boto3_client: Any,
    test_seed_phrase: str,
    cleanup_buckets: Callable[[str], None],
    unique_bucket_name: Callable[[str], str],
    db_connection: asyncpg.Connection,
    redis_chain_client: redis.Redis,
    redis_dlq_client: redis.Redis,
    pin_checker_runner: Callable[[str], Any],
) -> None:
    """Test complete pin checker DLQ workflow from upload to chain confirmation.

    Workflow:
    1. Upload multipart object → creates part_chunks with CIDs
    2. Wait for uploader worker to assign CIDs
    3. Mock chain to NOT include these CIDs
    4. Run pin checker cycle #1 → pin_attempts = 1, enqueued
    5. Run pin checker cycle #2 → pin_attempts = 2, enqueued
    6. Run pin checker cycle #3 → pin_attempts = 3, enqueued
    7. Run pin checker cycle #4 → attempts >= max, moved to DLQ
    8. Verify CID is in DLQ with correct metadata
    9. Mock chain to include CIDs (simulate validator finally pinned them)
    10. Run pin checker cycle #5 → resets pin_attempts to 0
    11. Verify attempts reset and last_pinned_at cleared
    """
    from tests.e2e.support.compose import enable_ipfs_proxy

    enable_ipfs_proxy()
    print("IPFS proxies enabled")

    bucket = unique_bucket_name("pin-dlq-test")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)
    key = "test-file.bin"

    user = await get_user_from_bucket(db_connection, bucket)
    print(f"Test user address: {user}")

    print(f"Uploading multipart object to {bucket}/{key}...")
    create_resp = boto3_client.create_multipart_upload(Bucket=bucket, Key=key)
    upload_id = create_resp["UploadId"]

    part_size = 5 * 1024 * 1024
    part1_data = b"A" * part_size
    part2_data = b"B" * part_size

    etag1 = boto3_client.upload_part(Bucket=bucket, Key=key, UploadId=upload_id, PartNumber=1, Body=part1_data)["ETag"]
    etag2 = boto3_client.upload_part(Bucket=bucket, Key=key, UploadId=upload_id, PartNumber=2, Body=part2_data)["ETag"]

    boto3_client.complete_multipart_upload(
        Bucket=bucket,
        Key=key,
        UploadId=upload_id,
        MultipartUpload={
            "Parts": [
                {"ETag": etag1, "PartNumber": 1},
                {"ETag": etag2, "PartNumber": 2},
            ]
        },
    )
    print("Multipart upload completed")

    print("Waiting for uploader worker to assign CIDs...")
    object_id, object_version, cids = await wait_for_cids(db_connection, bucket, key, min_cids=2, timeout_seconds=60.0)
    print(f"Got object_id={object_id}, version={object_version}, cids={len(cids)}")
    assert len(cids) >= 2, f"Should have at least 2 chunk CIDs, got {len(cids)}"

    print("Backdating object creation to 2 hours ago (pin checker requires objects > 1 hour old)...")
    await backdate_object_creation(db_connection, object_id, hours_ago=2)

    print(f"Mocking chain profile for user {user} with dummy CID (NOT including our object CIDs)...")
    mock_chain_profile(redis_chain_client, user, ["bafyDummyCIDNotInOurObject123456789"])

    print("\n=== PIN CHECKER CYCLE #1 ===")
    await pin_checker_runner(user)

    chunks = await get_part_chunk_cids(db_connection, object_id, object_version)
    print(f"After cycle 1: {len(chunks)} chunks")
    for chunk in chunks:
        print(f"  CID {chunk['cid'][:20]}... attempts={chunk['pin_attempts']}")
        assert chunk["pin_attempts"] == 1, f"After cycle 1, attempts should be 1, got {chunk['pin_attempts']}"

    dlq_entries = get_dlq_entries(redis_dlq_client)
    dlq_cids = [e["cid"] for e in dlq_entries]
    for chunk in chunks:
        assert chunk["cid"] not in dlq_cids, f"CID {chunk['cid']} should NOT be in DLQ after cycle 1"
    print(f"DLQ size: {len(dlq_entries)} (expected 0)")

    print("\n=== PIN CHECKER CYCLE #2 ===")
    await pin_checker_runner(user)

    chunks = await get_part_chunk_cids(db_connection, object_id, object_version)
    print(f"After cycle 2: {len(chunks)} chunks")
    for chunk in chunks:
        print(f"  CID {chunk['cid'][:20]}... attempts={chunk['pin_attempts']}")
        assert chunk["pin_attempts"] == 2, f"After cycle 2, attempts should be 2, got {chunk['pin_attempts']}"

    dlq_entries = get_dlq_entries(redis_dlq_client)
    dlq_cids = [e["cid"] for e in dlq_entries]
    for chunk in chunks:
        assert chunk["cid"] not in dlq_cids, f"CID {chunk['cid']} should NOT be in DLQ after cycle 2"
    print(f"DLQ size: {len(dlq_entries)} (expected 0)")

    print("\n=== PIN CHECKER CYCLE #3 ===")
    await pin_checker_runner(user)

    chunks = await get_part_chunk_cids(db_connection, object_id, object_version)
    print(f"After cycle 3: {len(chunks)} chunks")
    for chunk in chunks:
        print(f"  CID {chunk['cid'][:20]}... attempts={chunk['pin_attempts']}")
        assert chunk["pin_attempts"] == 3, f"After cycle 3, attempts should be 3, got {chunk['pin_attempts']}"

    dlq_entries = get_dlq_entries(redis_dlq_client)
    dlq_cids = [e["cid"] for e in dlq_entries]
    for chunk in chunks:
        assert chunk["cid"] not in dlq_cids, f"CID {chunk['cid']} should NOT be in DLQ after cycle 3"
    print(f"DLQ size: {len(dlq_entries)} (expected 0)")

    print("\n=== PIN CHECKER CYCLE #4 (should DLQ after max attempts) ===")
    await pin_checker_runner(user)

    chunks_dlq_check = await get_part_chunk_cids(db_connection, object_id, object_version)
    print(f"After cycle 4: {len(chunks_dlq_check)} chunks")
    for chunk in chunks_dlq_check:
        print(f"  CID {chunk['cid'][:20]}... attempts={chunk['pin_attempts']}")
        assert chunk["pin_attempts"] == 3, f"After cycle 4, attempts should still be 3, got {chunk['pin_attempts']}"

    dlq_entries = get_dlq_entries(redis_dlq_client)
    dlq_cids = [e["cid"] for e in dlq_entries]
    print(f"DLQ size: {len(dlq_entries)} (expected {len(chunks_dlq_check)})")
    for chunk in chunks_dlq_check:
        assert chunk["cid"] in dlq_cids, f"CID {chunk['cid']} SHOULD be in DLQ after cycle 4"

    print("\n=== VERIFYING DLQ ENTRY METADATA ===")
    for entry in dlq_entries:
        print(f"DLQ entry: cid={entry['cid'][:20]}..., user={entry['user']}, attempts={entry['pin_attempts']}")
        assert entry["user"] == user, f"DLQ entry user mismatch: {entry['user']} != {user}"
        assert entry["object_id"] == object_id, "DLQ entry object_id mismatch"
        assert entry["object_version"] == object_version, "DLQ entry version mismatch"
        assert entry["reason"] == "max_pin_attempts_exceeded", "DLQ entry reason mismatch"
        assert entry["pin_attempts"] == 3, "DLQ entry attempts mismatch"

    print("\n=== MOCKING CHAIN WITH ALL CIDS ===")
    all_cids = [c["cid"] for c in chunks_dlq_check]
    print(f"Adding {len(all_cids)} CIDs to chain profile")
    mock_chain_profile(redis_chain_client, user, all_cids)

    print("\n=== PIN CHECKER CYCLE #5 (with CIDs on chain) ===")
    await pin_checker_runner(user)

    chunks_after = await get_part_chunk_cids(db_connection, object_id, object_version)
    print(f"After cycle 5: {len(chunks_after)} chunks")
    for chunk in chunks_after:
        print(f"  CID {chunk['cid'][:20]}... attempts={chunk['pin_attempts']}, last_pinned={chunk['last_pinned_at']}")
        assert chunk["pin_attempts"] == 0, (
            f"After chain confirmation, attempts should be 0, got {chunk['pin_attempts']}"
        )
        assert chunk["last_pinned_at"] is None, "After chain confirmation, last_pinned_at should be None"

    print("\n=== TEST PASSED ===")
    print("✅ Pin attempts incremented correctly (0 → 1 → 2 → 3)")
    print("✅ CIDs enqueued for 3 attempts before DLQ")
    print("✅ CIDs moved to DLQ on cycle 4 (attempts >= max_attempts)")
    print("✅ DLQ entries have correct metadata")
    print("✅ Pin attempts reset to 0 after chain confirmation (cycle 5)")
