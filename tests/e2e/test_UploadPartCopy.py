import contextlib
import json as _json
import time
from typing import Any
from typing import Callable

import pytest
from botocore.exceptions import ClientError


def test_upload_part_copy(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    src_bucket = unique_bucket_name("upc-src")
    dst_bucket = unique_bucket_name("upc-dst")
    cleanup_buckets(src_bucket)
    cleanup_buckets(dst_bucket)

    boto3_client.create_bucket(Bucket=src_bucket)
    # Make source bucket public so source objects are unencrypted
    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": "*",
                "Action": ["s3:GetObject"],
                "Resource": [f"arn:aws:s3:::{src_bucket}/*"],
            }
        ],
    }
    boto3_client.put_bucket_policy(Bucket=src_bucket, Policy=_json.dumps(policy))
    boto3_client.create_bucket(Bucket=dst_bucket)

    key_src = "source.bin"
    data = b"a" * (5 * 1024 * 1024) + b"b" * (5 * 1024 * 1024)
    boto3_client.put_object(Bucket=src_bucket, Key=key_src, Body=data, ContentType="application/octet-stream")

    key_dst = "dest.bin"
    create = boto3_client.create_multipart_upload(
        Bucket=dst_bucket, Key=key_dst, ContentType="application/octet-stream"
    )
    upload_id = create["UploadId"]

    # Copy first 5MiB as part 1
    src_range = f"bytes=0-{5 * 1024 * 1024 - 1}"
    part1 = boto3_client.upload_part_copy(
        Bucket=dst_bucket,
        Key=key_dst,
        UploadId=upload_id,
        PartNumber=1,
        CopySource={"Bucket": src_bucket, "Key": key_src},
        CopySourceRange=src_range,
    )

    # Copy second 5MiB as part 2
    src_range2 = f"bytes={5 * 1024 * 1024}-{2 * 5 * 1024 * 1024 - 1}"
    part2 = boto3_client.upload_part_copy(
        Bucket=dst_bucket,
        Key=key_dst,
        UploadId=upload_id,
        PartNumber=2,
        CopySource=f"{src_bucket}/{key_src}",
        CopySourceRange=src_range2,
    )

    # Complete MPU
    completed = boto3_client.complete_multipart_upload(
        Bucket=dst_bucket,
        Key=key_dst,
        UploadId=upload_id,
        MultipartUpload={
            "Parts": [
                {"ETag": part1["CopyPartResult"]["ETag"], "PartNumber": 1},
                {"ETag": part2["CopyPartResult"]["ETag"], "PartNumber": 2},
            ]
        },
    )
    assert completed["ETag"].strip('"').endswith("-2")

    # Poll until ready, then assert exact content
    deadline = time.time() + 30
    while time.time() < deadline:
        try:
            obj = boto3_client.get_object(Bucket=dst_bucket, Key=key_dst)
            body = obj["Body"].read()
            if body == data:
                break
        except Exception:
            pass
        time.sleep(0.5)
    # Once ready, content must match
    obj = boto3_client.get_object(Bucket=dst_bucket, Key=key_dst)
    assert obj["Body"].read() == data


def _make_private_bucket(boto3_client: Any, bucket: str) -> None:
    # Best-effort: remove any public-read policy (our impl marks bucket public via policy helper)
    with contextlib.suppress(Exception):
        boto3_client.delete_bucket_policy(Bucket=bucket)


@pytest.mark.skip(reason="UploadPartCopy for encrypted/private buckets is not supported yet")
def test_upload_part_copy_encrypted_source(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    src_bucket = unique_bucket_name("upc-src-enc")
    dst_bucket = unique_bucket_name("upc-dst-enc")
    cleanup_buckets(src_bucket)
    cleanup_buckets(dst_bucket)

    boto3_client.create_bucket(Bucket=src_bucket)
    _make_private_bucket(boto3_client, src_bucket)
    boto3_client.create_bucket(Bucket=dst_bucket)

    key_src = "source.bin"
    data = b"a" * (5 * 1024 * 1024)
    boto3_client.put_object(Bucket=src_bucket, Key=key_src, Body=data, ContentType="application/octet-stream")

    key_dst = "dest.bin"
    create = boto3_client.create_multipart_upload(
        Bucket=dst_bucket, Key=key_dst, ContentType="application/octet-stream"
    )
    upload_id = create["UploadId"]

    # Expect NotImplemented/501 once supported test should be updated
    with pytest.raises(ClientError) as exc:
        boto3_client.upload_part_copy(
            Bucket=dst_bucket,
            Key=key_dst,
            UploadId=upload_id,
            PartNumber=1,
            CopySource=f"{src_bucket}/{key_src}",
        )
    # When enabled, service should return 501 NotImplemented for encrypted sources
    err = exc.value
    assert err.response.get("ResponseMetadata", {}).get("HTTPStatusCode") in (501, 400, 403)
