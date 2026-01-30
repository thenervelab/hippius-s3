from __future__ import annotations

import os
from typing import Any
from typing import Callable

import pytest

from .support.cache import wait_for_all_backends_ready
from .support.chunks import get_first_chunk_cid
from .support.ipfs import fetch_raw_cid


@pytest.mark.local
def test_private_single_part_encrypted_at_rest(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """Uploads a <4MB object to a private bucket and asserts raw IPFS bytes != plaintext, while API GET matches."""
    bucket = unique_bucket_name("enc-rest-private")
    cleanup_buckets(bucket)

    boto3_client.create_bucket(Bucket=bucket)

    key = "small.bin"
    content = os.urandom(1 * 1024 * 1024)  # 1MB single-part

    boto3_client.put_object(Bucket=bucket, Key=key, Body=content, ContentType="application/octet-stream")

    # Wait for at least one part to have a CID and chunk rows to exist
    assert wait_for_all_backends_ready(bucket, key, min_count=1, timeout_seconds=30.0)

    # Fetch first chunk CID for part 1 and compare raw bytes to plaintext
    cid = get_first_chunk_cid(bucket, key)
    assert cid is not None, "expected first chunk CID to be present"

    raw_bytes = fetch_raw_cid(cid)
    assert raw_bytes != content, "raw IPFS chunk should be ciphertext, not plaintext"

    # API GET should still return plaintext
    resp = boto3_client.get_object(Bucket=bucket, Key=key)
    fetched = resp["Body"].read()
    assert fetched == content


@pytest.mark.local
def test_public_single_part_encrypted_at_rest(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """Public bucket version of the same test; currently expected to fail until public encryption is enabled."""
    bucket = unique_bucket_name("enc-rest-public")
    cleanup_buckets(bucket)

    boto3_client.create_bucket(Bucket=bucket)

    # Make bucket public (policy grants GetObject)
    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": "*",
                "Action": ["s3:GetObject"],
                "Resource": [f"arn:aws:s3:::{bucket}/*"],
            }
        ],
    }
    import json as _json

    boto3_client.put_bucket_policy(Bucket=bucket, Policy=_json.dumps(policy))

    key = "small-public.bin"
    content = os.urandom(1 * 1024 * 1024)

    boto3_client.put_object(Bucket=bucket, Key=key, Body=content, ContentType="application/octet-stream")

    assert wait_for_all_backends_ready(bucket, key, min_count=1, timeout_seconds=30.0)

    cid = get_first_chunk_cid(bucket, key)
    assert cid is not None

    raw_bytes = fetch_raw_cid(cid)
    # This will currently compare equal on legacy public plaintext; xfail documents the desired change
    assert raw_bytes != content

    # API GET should return plaintext
    resp = boto3_client.get_object(Bucket=bucket, Key=key)
    fetched = resp["Body"].read()
    assert fetched == content
