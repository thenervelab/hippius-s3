from __future__ import annotations

import asyncio
import time
from typing import Any
from typing import Callable

import pytest

from .support.cache import get_object_cids
from .support.cache import get_object_id_and_version
from .support.chunks import get_part_chunks
from .support.recovery import count_blobs_by_kind
from .support.recovery import decrypt_cipher_chunk
from .support.recovery import download_cid
from .support.recovery import get_key_bytes_async
from .support.recovery import get_part_ec
from .support.recovery import get_part_id
from .support.recovery import get_replica_cids_for_chunk


@pytest.mark.local
def test_replication_creates_replicas_and_ciphertexts_are_distinct(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """End-to-end: upload a small object (< threshold) and assert replicas exist and differ from data.

    Recovery note: With random nonces, replica ciphertext differs from data ciphertext; plaintext equality is
    out-of-scope here (requires decrypt helpers). This test ensures redundancy materialization and distinctness.
    """
    bucket = unique_bucket_name("replicas")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)

    # Small body below typical threshold (k * min_chunk_size). 256 KiB is safe for defaults.
    body = b"Z" * (256 * 1024)
    key = "small.bin"
    put = boto3_client.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/octet-stream")
    assert put["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)

    object_id, ov = get_object_id_and_version(bucket, key)
    part_id = get_part_id(bucket, key, part_number=1)

    # Wait for redundancy worker to upsert part_ec with rep-v1
    deadline = time.time() + 60.0
    pv, scheme, k, m, stripes = 0, "", 0, 0, 0
    while time.time() < deadline:
        row = get_part_ec(part_id)
        if row:
            pv, scheme, k, m, stripes = row
            if scheme == "rep-v1" and m >= 1:
                break
        time.sleep(0.2)
    assert scheme == "rep-v1", "expected rep-v1 scheme for small object replication"
    assert m >= 1, "expected at least 1 replica per chunk"

    # Determine num data chunks
    chunk_rows = get_part_chunks(bucket, key, part_number=1)
    num_chunks = len(chunk_rows)
    assert num_chunks >= 1

    # Wait until expected replica blobs are materialized (uploaded/pinning/pinned)
    expected = num_chunks * m
    deadline = time.time() + 90.0
    while time.time() < deadline:
        cnt = count_blobs_by_kind(part_id, kind="replica")
        if cnt >= expected:
            break
        time.sleep(0.3)
    assert count_blobs_by_kind(part_id, kind="replica") >= expected

    # Distinctness: for the first chunk, fetch data CID and all replica CIDs and ensure they are all different
    first = chunk_rows[0]
    data_cid = str(first.cid)
    replica_cids = get_replica_cids_for_chunk(part_id, first.chunk_index)
    # We expect at least m replica cids
    assert len(replica_cids) >= m
    assert data_cid not in set(replica_cids), "replica CID should differ from data CID"
    assert len(set(replica_cids)) == len(replica_cids), "replica CIDs for a chunk must be unique"

    # Optional: fetch bytes to ensure all CIDs resolve and are non-empty
    data_bytes = download_cid(data_cid)
    assert isinstance(data_bytes, (bytes, bytearray)) and len(data_bytes) > 0
    # Also decrypt and validate plaintext equals uploaded body for single-chunk case
    # (Below threshold + default chunk size -> typically 1 chunk.)
    _, _, main_account_id, _, _ = get_object_cids(bucket, key)
    key_bytes = asyncio.run(get_key_bytes_async(main_account_id, bucket))
    pt_data = decrypt_cipher_chunk(
        data_bytes,
        object_id=object_id,
        part_number=1,
        chunk_index=first.chunk_index,
        key_bytes=key_bytes,
    )
    # For small body (single chunk), plaintext should equal original body
    assert bytes(pt_data) == body

    for rcid in replica_cids:
        rbytes = download_cid(rcid)
        assert isinstance(rbytes, (bytes, bytearray)) and len(rbytes) > 0
        pt_rep = decrypt_cipher_chunk(
            rbytes,
            object_id=object_id,
            part_number=1,
            chunk_index=first.chunk_index,
            key_bytes=key_bytes,
        )
        assert bytes(pt_rep) == bytes(pt_data)
