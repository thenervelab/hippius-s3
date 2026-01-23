from typing import Any
from typing import Callable

import psycopg
import pytest


@pytest.mark.local
def test_simple_object_overwrite_creates_new_versions(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket_name = unique_bucket_name("versioning-simple")
    cleanup_buckets(bucket_name)

    boto3_client.create_bucket(Bucket=bucket_name)
    key = "test-versioning.txt"

    resp1 = boto3_client.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=b"version 1 content",
        ContentType="text/plain",
    )
    assert resp1["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)
    etag1 = resp1["ETag"].strip('"')

    resp2 = boto3_client.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=b"version 2 content different",
        ContentType="text/plain",
    )
    assert resp2["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)
    etag2 = resp2["ETag"].strip('"')

    assert etag1 != etag2

    with psycopg.connect("postgresql://postgres:postgres@localhost:5432/hippius") as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT object_id, current_object_version FROM objects WHERE bucket_id = (SELECT bucket_id FROM buckets WHERE bucket_name = %s) AND object_key = %s",
            (bucket_name, key)
        )
        row = cur.fetchone()
        assert row is not None, "Object not found in database"
        object_id, current_version = row
        assert current_version == 2, f"Expected current_object_version=2, got {current_version}"

        cur.execute(
            "SELECT object_version, size_bytes, md5_hash FROM object_versions WHERE object_id = %s ORDER BY object_version",
            (object_id,)
        )
        versions = cur.fetchall()
        assert len(versions) == 2, f"Expected 2 versions, got {len(versions)}"

        assert versions[0][0] == 1
        assert versions[0][1] == len(b"version 1 content")

        assert versions[1][0] == 2
        assert versions[1][1] == len(b"version 2 content different")

        cur.execute(
            "SELECT object_version, COUNT(*) FROM parts WHERE object_id = %s GROUP BY object_version ORDER BY object_version",
            (object_id,)
        )
        part_counts = cur.fetchall()
        assert len(part_counts) == 2, "Should have parts for both versions"
        assert part_counts[0][0] == 1
        assert part_counts[1][0] == 2

    resp_get = boto3_client.get_object(Bucket=bucket_name, Key=key)
    body = resp_get["Body"].read()
    assert body == b"version 2 content different"


@pytest.mark.local
def test_multipart_upload_overwrite_creates_new_versions(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket_name = unique_bucket_name("versioning-mpu")
    cleanup_buckets(bucket_name)

    boto3_client.create_bucket(Bucket=bucket_name)
    key = "large-versioned.bin"

    create1 = boto3_client.create_multipart_upload(Bucket=bucket_name, Key=key)
    upload_id1 = create1["UploadId"]

    part_size = 5 * 1024 * 1024
    etag1_p1 = boto3_client.upload_part(
        Bucket=bucket_name, Key=key, UploadId=upload_id1, PartNumber=1, Body=b"a" * part_size
    )["ETag"]

    completed1 = boto3_client.complete_multipart_upload(
        Bucket=bucket_name,
        Key=key,
        UploadId=upload_id1,
        MultipartUpload={"Parts": [{"ETag": etag1_p1, "PartNumber": 1}]}
    )
    etag1 = completed1["ETag"].strip('"')

    create2 = boto3_client.create_multipart_upload(Bucket=bucket_name, Key=key)
    upload_id2 = create2["UploadId"]

    etag2_p1 = boto3_client.upload_part(
        Bucket=bucket_name, Key=key, UploadId=upload_id2, PartNumber=1, Body=b"b" * part_size
    )["ETag"]
    etag2_p2 = boto3_client.upload_part(
        Bucket=bucket_name, Key=key, UploadId=upload_id2, PartNumber=2, Body=b"c" * part_size
    )["ETag"]

    completed2 = boto3_client.complete_multipart_upload(
        Bucket=bucket_name,
        Key=key,
        UploadId=upload_id2,
        MultipartUpload={"Parts": [
            {"ETag": etag2_p1, "PartNumber": 1},
            {"ETag": etag2_p2, "PartNumber": 2},
        ]}
    )
    etag2 = completed2["ETag"].strip('"')

    assert etag1 != etag2

    with psycopg.connect("postgresql://postgres:postgres@localhost:5432/hippius") as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT object_id, current_object_version FROM objects WHERE bucket_id = (SELECT bucket_id FROM buckets WHERE bucket_name = %s) AND object_key = %s",
            (bucket_name, key)
        )
        row = cur.fetchone()
        assert row is not None
        object_id, current_version = row
        assert current_version == 2, f"Expected current_object_version=2, got {current_version}"

        cur.execute(
            "SELECT object_version, size_bytes FROM object_versions WHERE object_id = %s ORDER BY object_version",
            (object_id,)
        )
        versions = cur.fetchall()
        assert len(versions) == 2
        assert versions[0][0] == 1
        assert versions[1][0] == 2

        cur.execute(
            "SELECT object_version, part_number FROM parts WHERE object_id = %s ORDER BY object_version, part_number",
            (object_id,)
        )
        parts = cur.fetchall()
        v1_parts = [p for p in parts if p[0] == 1]
        v2_parts = [p for p in parts if p[0] == 2]

        assert len(v1_parts) == 1, f"Version 1 should have 1 part, got {len(v1_parts)}"
        assert len(v2_parts) == 2, f"Version 2 should have 2 parts, got {len(v2_parts)}"


@pytest.mark.local
def test_three_overwrites_create_three_versions(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket_name = unique_bucket_name("versioning-triple")
    cleanup_buckets(bucket_name)

    boto3_client.create_bucket(Bucket=bucket_name)
    key = "triple-version.txt"

    for i in range(1, 4):
        boto3_client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=f"version {i}".encode(),
        )

    with psycopg.connect("postgresql://postgres:postgres@localhost:5432/hippius") as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT object_id, current_object_version FROM objects WHERE bucket_id = (SELECT bucket_id FROM buckets WHERE bucket_name = %s) AND object_key = %s",
            (bucket_name, key)
        )
        row = cur.fetchone()
        assert row is not None
        object_id, current_version = row
        assert current_version == 3, f"Expected current_object_version=3, got {current_version}"

        cur.execute(
            "SELECT object_version FROM object_versions WHERE object_id = %s ORDER BY object_version",
            (object_id,)
        )
        versions = [v[0] for v in cur.fetchall()]
        assert versions == [1, 2, 3], f"Expected versions [1, 2, 3], got {versions}"

    resp = boto3_client.get_object(Bucket=bucket_name, Key=key)
    assert resp["Body"].read() == b"version 3"
