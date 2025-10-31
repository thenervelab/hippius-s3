from __future__ import annotations

import asyncio
import time
from typing import Any
from typing import Callable

import pytest

from .support.cache import get_object_cids
from .support.cache import get_object_id_and_version
from .support.chunks import get_part_chunks
from .support.recovery import decrypt_cipher_chunk
from .support.recovery import download_cid
from .support.recovery import get_key_bytes_async
from .support.recovery import get_parity_cid
from .support.recovery import get_part_ec
from .support.recovery import get_part_id
from .support.recovery import reconstruct_single_miss


@pytest.mark.local
def test_parity_created_and_single_miss_recovery_works(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """End-to-end: upload a large object (≥ threshold) and assert parity creation and RS single-miss recovery."""
    bucket = unique_bucket_name("parity")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)

    # Body above typical threshold (k * min_chunk_size). 2 MiB is safe for defaults.
    body = (b"A" * (1024 * 1024)) + (b"B" * (1024 * 1024))
    key = "large.bin"
    put = boto3_client.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/octet-stream")
    assert put["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)

    object_id, ov = get_object_id_and_version(bucket, key)
    part_id = get_part_id(bucket, key, part_number=1)

    # Wait for redundancy worker to upsert part_ec with rs-v1
    deadline = time.time() + 90.0
    pv, scheme, k, m, stripes = 0, "", 0, 0, 0
    while time.time() < deadline:
        row = get_part_ec(part_id)
        if row:
            pv, scheme, k, m, stripes = row
            if scheme == "rs-v1" and stripes >= 1:
                break
        time.sleep(0.3)
    assert scheme == "rs-v1", "expected rs-v1 scheme for EC path"
    assert stripes >= 1, "expected at least one stripe"
    assert k >= 1
    # m may be >=1; parity staging currently at least index 0

    # Determine data chunk CIDs; wait until at least first stripe's k CIDs are persisted
    chunk_rows = []
    deadline = time.time() + 60.0
    while time.time() < deadline:
        chunk_rows = get_part_chunks(bucket, key, part_number=1)
        if len(chunk_rows) >= int(k):
            break
        time.sleep(0.3)
    num_chunks = len(chunk_rows)
    assert num_chunks >= int(k), f"expected at least k={k} data chunks persisted by uploader, got {num_chunks}"

    # Choose stripe 0 (first stripe) and attempt single-miss reconstruction for ci=0 within that stripe
    stripe_index = 0
    missing_ci = 0  # missing the first data chunk in stripe 0
    # Wait (bounded) for parity CID to be persisted
    parity_cid = None
    deadline = time.time() + 20.0
    while time.time() < deadline:
        parity_cid = get_parity_cid(part_id, pv, stripe_index, parity_index=0)
        if parity_cid:
            break
        time.sleep(0.3)
    assert parity_cid is not None, "expected parity shard available for stripe 0"

    # Collect data ciphertext bytes for the k data chunks in this stripe
    data_cids_ordered = [cr.cid for cr in chunk_rows]
    stripe_start = stripe_index * k
    stripe_end = min(stripe_start + k, len(data_cids_ordered))
    stripe_data_cids = data_cids_ordered[stripe_start:stripe_end]
    assert len(stripe_data_cids) >= 1

    # Download bytes
    parity_bytes = download_cid(str(parity_cid))
    others: list[bytes] = []
    original_missing_bytes: bytes | None = None
    for idx, cid in enumerate(stripe_data_cids):
        b = download_cid(str(cid))
        if idx == missing_ci:
            original_missing_bytes = b
        else:
            others.append(b)
    assert isinstance(original_missing_bytes, (bytes, bytearray))

    # Reconstruct and compare (RS if available, else XOR)
    reconstructed = reconstruct_single_miss(
        bytes(parity_bytes),
        others,
        k=int(k),
        missing_index=missing_ci,
        expected_payload_size=len(bytes(original_missing_bytes)),
    )
    assert reconstructed == bytes(original_missing_bytes), "Single-miss reconstruction should match"

    # Additionally, decrypt both ciphertexts and assert plaintext equality
    _, _, main_account_id, _, _ = get_object_cids(bucket, key)
    key_bytes = asyncio.run(get_key_bytes_async(main_account_id, bucket))
    pt_recon = decrypt_cipher_chunk(
        reconstructed,
        object_id=object_id,
        part_number=1,
        chunk_index=stripe_start + missing_ci,
        key_bytes=key_bytes,
    )
    pt_original = decrypt_cipher_chunk(
        bytes(original_missing_bytes),
        object_id=object_id,
        part_number=1,
        chunk_index=stripe_start + missing_ci,
        key_bytes=key_bytes,
    )
    assert bytes(pt_recon) == bytes(pt_original)
