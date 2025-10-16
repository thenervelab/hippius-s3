import time
from typing import Any
from typing import Callable
from urllib.parse import parse_qsl
from urllib.parse import urlencode
from urllib.parse import urlparse

import requests  # type: ignore[import-untyped]


def _tamper_signature(url: str) -> str:
    parsed = urlparse(url)
    q = dict(parse_qsl(parsed.query))
    sig = q.get("X-Amz-Signature")
    if not sig or len(sig) < 2:
        return url
    # Flip last hex digit
    last = sig[-1]
    flipped = "f" if last != "f" else "e"
    q["X-Amz-Signature"] = sig[:-1] + flipped
    new_query = urlencode(sorted(q.items()))
    return parsed._replace(query=new_query).geturl()


def test_presigned_put_and_get(
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
    test_seed_phrase: str,
) -> None:
    bucket = unique_bucket_name("presign")
    boto3_client.create_bucket(Bucket=bucket)
    cleanup_buckets(bucket)

    key = "folder/file.txt"
    body = b"hello presign"

    # Use boto3 to generate AWS-compatible presigned URLs
    put_url = boto3_client.generate_presigned_url(
        ClientMethod="put_object",
        Params={"Bucket": bucket, "Key": key},
        ExpiresIn=300,
        HttpMethod="PUT",
    )

    # Use the presigned PUT to upload bytes
    put_resp = requests.put(put_url, data=body, timeout=30)
    assert put_resp.status_code in (200, 201)

    # Generate presigned GET URL using boto3
    get_url = boto3_client.generate_presigned_url(
        ClientMethod="get_object",
        Params={"Bucket": bucket, "Key": key},
        ExpiresIn=300,
        HttpMethod="GET",
    )

    # Fetch via presigned GET
    get_resp = requests.get(get_url, timeout=30)
    assert get_resp.status_code == 200
    assert get_resp.content == body

    # Expiry behavior: create a short-lived URL and wait past expiry
    short_get_url = boto3_client.generate_presigned_url(
        ClientMethod="get_object",
        Params={"Bucket": bucket, "Key": key},
        ExpiresIn=1,
        HttpMethod="GET",
    )
    time.sleep(2)
    expired = requests.get(short_get_url, timeout=10)
    assert expired.status_code in {400, 401, 403}


def test_presigned_get_expiry(
    boto3_client: Any, unique_bucket_name: Callable[[str], str], cleanup_buckets: Callable[[str], None]
) -> None:
    bucket = unique_bucket_name("presign-expire")
    boto3_client.create_bucket(Bucket=bucket)
    cleanup_buckets(bucket)

    key = "file.txt"
    body = b"hello expiry"
    boto3_client.put_object(Bucket=bucket, Key=key, Body=body)

    url = boto3_client.generate_presigned_url(
        ClientMethod="get_object",
        Params={"Bucket": bucket, "Key": key},
        ExpiresIn=1,
        HttpMethod="GET",
    )
    time.sleep(2)
    r = requests.get(url, timeout=10)
    assert r.status_code in (400, 401, 403)


def test_presigned_get_tampered_signature(
    boto3_client: Any, unique_bucket_name: Callable[[str], str], cleanup_buckets: Callable[[str], None]
) -> None:
    bucket = unique_bucket_name("presign-tamper")
    boto3_client.create_bucket(Bucket=bucket)
    cleanup_buckets(bucket)

    key = "file.txt"
    body = b"hello tamper"
    boto3_client.put_object(Bucket=bucket, Key=key, Body=body)

    url = boto3_client.generate_presigned_url(
        ClientMethod="get_object",
        Params={"Bucket": bucket, "Key": key},
        ExpiresIn=300,
        HttpMethod="GET",
    )
    bad = _tamper_signature(url)
    r = requests.get(bad, timeout=10)
    assert r.status_code in (400, 401, 403)


def test_presigned_head(
    boto3_client: Any, unique_bucket_name: Callable[[str], str], cleanup_buckets: Callable[[str], None]
) -> None:
    bucket = unique_bucket_name("presign-head")
    boto3_client.create_bucket(Bucket=bucket)
    cleanup_buckets(bucket)

    key = "folder/file.txt"
    body = b"hello head"
    boto3_client.put_object(Bucket=bucket, Key=key, Body=body)

    url = boto3_client.generate_presigned_url(
        ClientMethod="head_object",
        Params={"Bucket": bucket, "Key": key},
        ExpiresIn=300,
        HttpMethod="HEAD",
    )
    r = requests.head(url, timeout=10)
    assert r.status_code == 200
    assert int(r.headers.get("content-length", "0")) == len(body)


def test_presigned_key_encoding(
    boto3_client: Any, unique_bucket_name: Callable[[str], str], cleanup_buckets: Callable[[str], None]
) -> None:
    bucket = unique_bucket_name("presign-enc")
    boto3_client.create_bucket(Bucket=bucket)
    cleanup_buckets(bucket)

    key = "weird folder/a b+c~☃/file %2B name.txt"
    body = b"hello encoding"
    boto3_client.put_object(Bucket=bucket, Key=key, Body=body)

    url = boto3_client.generate_presigned_url(
        ClientMethod="get_object",
        Params={"Bucket": bucket, "Key": key},
        ExpiresIn=300,
        HttpMethod="GET",
    )
    r = requests.get(url, timeout=10)
    assert r.status_code == 200
    assert r.content == body


def test_presigned_get_range(
    boto3_client: Any, unique_bucket_name: Callable[[str], str], cleanup_buckets: Callable[[str], None]
) -> None:
    bucket = unique_bucket_name("presign-range")
    boto3_client.create_bucket(Bucket=bucket)
    cleanup_buckets(bucket)

    key = "range/file.bin"
    body = b"hello range body"
    boto3_client.put_object(Bucket=bucket, Key=key, Body=body)

    url = boto3_client.generate_presigned_url(
        ClientMethod="get_object",
        Params={"Bucket": bucket, "Key": key},
        ExpiresIn=300,
        HttpMethod="GET",
    )
    r = requests.get(url, headers={"Range": "bytes=0-4"}, timeout=10)
    assert r.status_code in (200, 206)
    assert r.content == body[0:5]
