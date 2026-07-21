"""E2E test for the MPU broken-v5 envelope window (M7).

InitiateMultipartUpload bumps objects.current_object_version and inserts a storage_version=5 row.
The DEK envelope used to be written lazily on the first UploadPart, so between initiate and that
first part the LIVE version carried a NULL kek_id/wrapped_dek — a GET in that gap 500s on
v5_missing_envelope_metadata, and an MPU that is never continued leaves the row broken forever.

Unlike test_EnvelopeRace.py (which NULLs the envelope by hand to exercise the read-path fallback),
this asserts the write path never produces the NULL state in the first place: the envelope is
observable in the DB immediately after initiate, with zero parts uploaded.
"""

from __future__ import annotations

import hashlib
import secrets
from typing import Any
from typing import Callable

import psycopg

from .support.dsn import DEFAULT_DSN


DB_DSN = DEFAULT_DSN

PART_SIZE = 5 * 1024 * 1024  # S3 minimum for a non-final part


def _envelope_for_upload(bucket: str, key: str) -> tuple[Any, Any, int, int]:
    """Return (kek_id, wrapped_dek, storage_version, part_count) for the object's CURRENT version.

    Reads through objects.current_object_version deliberately: that pointer is what a concurrent GET
    resolves, so it is the row that must never carry a NULL envelope.
    """
    with psycopg.connect(DB_DSN) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT ov.kek_id, ov.wrapped_dek, ov.storage_version, o.object_id, o.current_object_version
              FROM objects o
              JOIN buckets b ON b.bucket_id = o.bucket_id
              JOIN object_versions ov
                ON ov.object_id = o.object_id
               AND ov.object_version = o.current_object_version
             WHERE b.bucket_name = %s AND o.object_key = %s
            """,
            (bucket, key),
        )
        row = cur.fetchone()
        assert row is not None, f"no current object_version row for {bucket}/{key}"
        kek_id, wrapped_dek, storage_version, object_id, object_version = row

        cur.execute(
            "SELECT COUNT(*) FROM parts WHERE object_id = %s AND object_version = %s",
            (object_id, object_version),
        )
        part_row = cur.fetchone()
        assert part_row is not None
        return kek_id, wrapped_dek, int(storage_version), int(part_row[0])


def test_initiate_multipart_writes_envelope_before_any_part(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket = unique_bucket_name("mpu-envelope")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)
    key = "envelope-on-initiate.bin"

    upload_id = boto3_client.create_multipart_upload(Bucket=bucket, Key=key)["UploadId"]

    kek_id, wrapped_dek, storage_version, part_count = _envelope_for_upload(bucket, key)

    assert part_count == 0, "precondition: the window under test is initiate BEFORE any UploadPart"
    assert storage_version >= 5, f"expected a v5 reserve, got storage_version={storage_version}"
    assert kek_id is not None, "kek_id must be non-NULL immediately after initiate"
    assert wrapped_dek, "wrapped_dek must be non-NULL immediately after initiate"

    body = secrets.token_bytes(PART_SIZE)
    part = boto3_client.upload_part(
        Bucket=bucket,
        Key=key,
        UploadId=upload_id,
        PartNumber=1,
        Body=body,
    )
    boto3_client.complete_multipart_upload(
        Bucket=bucket,
        Key=key,
        UploadId=upload_id,
        MultipartUpload={"Parts": [{"ETag": part["ETag"], "PartNumber": 1}]},
    )

    # The initiate-written DEK must be the one parts encrypt under: a rotation at first UploadPart
    # would leave the stored ciphertext unreadable by the stored envelope.
    got = boto3_client.get_object(Bucket=bucket, Key=key)["Body"].read()
    assert hashlib.md5(got).hexdigest() == hashlib.md5(body).hexdigest()

    final_kek_id, final_wrapped_dek, _, _ = _envelope_for_upload(bucket, key)
    assert final_kek_id == kek_id
    assert bytes(final_wrapped_dek) == bytes(wrapped_dek)


def test_abandoned_multipart_leaves_no_broken_v5_row(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """The permanent-damage case: 203k prod rows came from MPUs initiated and never continued."""
    bucket = unique_bucket_name("mpu-envelope-abandon")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)
    key = "abandoned.bin"

    boto3_client.create_multipart_upload(Bucket=bucket, Key=key)

    kek_id, wrapped_dek, storage_version, part_count = _envelope_for_upload(bucket, key)
    assert part_count == 0
    assert storage_version >= 5
    assert kek_id is not None and wrapped_dek, "abandoned MPU must not leave a broken-v5 row"
