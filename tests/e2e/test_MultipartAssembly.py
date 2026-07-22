"""E2E: multi-part MPU assembly is byte-exact on GET (and Range), and HEAD headers match.

Gate for MPU-3 (CompleteMPU reads the parts table 3× — must keep the combined ETag/size identical)
and HD-4 (a lighter HEAD query — headers must stay byte-identical across multipart objects). The
existing MPU e2e only checks the combined ETag; these download the assembled bytes and verify them.
"""

import hashlib
from typing import Any
from typing import Callable


def _combined_etag(part_etags: list[str]) -> str:
    raw = b"".join(bytes.fromhex(e.strip('"')) for e in part_etags)
    return f"{hashlib.md5(raw).hexdigest()}-{len(part_etags)}"


def test_multipart_get_is_byte_exact_and_head_matches(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket = unique_bucket_name("mpu-assembly")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)
    key = "assembled.bin"

    # 3 parts of distinct bytes. All but the last must be >= 5 MiB (S3 rule).
    part_size = 5 * 1024 * 1024
    bodies = [b"\x11" * part_size, b"\x22" * part_size, b"\x33" * (1024 * 1024)]
    expected = b"".join(bodies)

    create = boto3_client.create_multipart_upload(Bucket=bucket, Key=key, ContentType="application/octet-stream")
    upload_id = create["UploadId"]

    etags = []
    for i, body in enumerate(bodies, start=1):
        r = boto3_client.upload_part(Bucket=bucket, Key=key, UploadId=upload_id, PartNumber=i, Body=body)
        etags.append(r["ETag"])

    completed = boto3_client.complete_multipart_upload(
        Bucket=bucket,
        Key=key,
        UploadId=upload_id,
        MultipartUpload={"Parts": [{"ETag": e, "PartNumber": i} for i, e in enumerate(etags, start=1)]},
    )
    final_etag = completed["ETag"].strip('"')
    assert final_etag == _combined_etag(etags), "combined ETag must be md5-of-part-md5s + -N"

    # GET the whole object and assert byte-exact assembly.
    got = boto3_client.get_object(Bucket=bucket, Key=key)["Body"].read()
    assert got == expected, "assembled multipart object must be byte-identical to the concatenated parts"

    # HEAD must expose the same size + ETag (the headers HD-4's lighter query must preserve).
    head = boto3_client.head_object(Bucket=bucket, Key=key)
    assert head["ContentLength"] == len(expected)
    assert head["ETag"].strip('"') == final_etag


def test_multipart_range_get_spans_part_boundary(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket = unique_bucket_name("mpu-range")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)
    key = "ranged.bin"

    part_size = 5 * 1024 * 1024
    bodies = [b"\xa1" * part_size, b"\xb2" * part_size]
    expected = b"".join(bodies)

    create = boto3_client.create_multipart_upload(Bucket=bucket, Key=key)
    upload_id = create["UploadId"]
    etags = []
    for i, body in enumerate(bodies, start=1):
        etags.append(
            boto3_client.upload_part(Bucket=bucket, Key=key, UploadId=upload_id, PartNumber=i, Body=body)["ETag"]
        )
    boto3_client.complete_multipart_upload(
        Bucket=bucket,
        Key=key,
        UploadId=upload_id,
        MultipartUpload={"Parts": [{"ETag": e, "PartNumber": i} for i, e in enumerate(etags, start=1)]},
    )

    # A range straddling the part-1/part-2 boundary must return the exact plaintext slice.
    start = part_size - 100
    end = part_size + 99  # inclusive
    r = boto3_client.get_object(Bucket=bucket, Key=key, Range=f"bytes={start}-{end}")
    got = r["Body"].read()
    assert got == expected[start : end + 1], "cross-boundary range must be byte-exact"
    assert len(got) == (end - start + 1)
