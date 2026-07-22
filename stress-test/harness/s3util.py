"""boto3 client + S3 operations for the harness (path-style, SigV4, hippius endpoint)."""

from __future__ import annotations

import dataclasses
import hashlib
import io

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.config import Config as BotoConfig

from .config import Config


# 8 MiB threshold/chunk forces multipart on anything >= ~8 MiB (well under hippius' own 64 MiB CLI default,
# so the harness deterministically exercises the MPU drain path on the "large" corpus objects).
MPU_THRESHOLD = 8 * 1024 * 1024
_TRANSFER = TransferConfig(multipart_threshold=MPU_THRESHOLD, multipart_chunksize=MPU_THRESHOLD, max_concurrency=4)


def make_client(cfg: Config):
    return boto3.client(
        "s3",
        endpoint_url=cfg.endpoint_url,
        aws_access_key_id=cfg.access_key,
        aws_secret_access_key=cfg.secret_key,
        region_name=cfg.region,
        config=BotoConfig(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
            retries={"max_attempts": 2, "mode": "standard"},
            connect_timeout=15,
            read_timeout=120,
        ),
    )


def random_body(size: int, seed: bytes) -> bytes:
    """Deterministic-per-seed pseudo-random bytes (so md5 is reproducible across a run)."""
    out = bytearray()
    counter = 0
    while len(out) < size:
        out += hashlib.sha256(seed + counter.to_bytes(8, "big")).digest()
        counter += 1
    return bytes(out[:size])


def md5_hex(data: bytes) -> str:
    return hashlib.md5(data).hexdigest()


@dataclasses.dataclass
class PutResult:
    etag: str
    elapsed_s: float
    multipart: bool


def put(client, bucket: str, key: str, body: bytes) -> str:
    """Simple PUT; returns ETag."""
    resp = client.put_object(Bucket=bucket, Key=key, Body=body)
    return resp["ETag"].strip('"')


def upload_forced_multipart(client, bucket: str, key: str, body: bytes) -> str:
    """Upload via the transfer manager with an 8 MiB threshold so >=8 MiB bodies go multipart."""
    client.upload_fileobj(io.BytesIO(body), bucket, key, Config=_TRANSFER)
    return client.head_object(Bucket=bucket, Key=key)["ETag"].strip('"')


def get_bytes(client, bucket: str, key: str, range_header: str | None = None, retries: int = 4) -> bytes:
    """GET the body, retrying transient read failures (streaming breaks, 503/SlowDown, cross-node
    cold-read gaps) the way a real client would. Raises the last error after `retries` attempts so a
    *persistent* failure is still surfaced (not silently swallowed)."""
    import time

    import botocore.exceptions

    kw = {"Bucket": bucket, "Key": key}
    if range_header:
        kw["Range"] = range_header
    last: Exception | None = None
    for attempt in range(retries):
        try:
            return client.get_object(**kw)["Body"].read()
        except (botocore.exceptions.ResponseStreamingError, botocore.exceptions.ReadTimeoutError,
                botocore.exceptions.ConnectionError) as e:
            last = e
        except botocore.exceptions.ClientError as e:
            status = e.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0)
            if status not in (500, 503):
                raise
            last = e
        time.sleep(1.0 * (attempt + 1))
    raise last  # type: ignore[misc]


def ensure_bucket(client, bucket: str, public: bool = False, retries: int = 4) -> None:
    """CreateBucket, retrying the transient billing-balance-fetch failure hippius returns
    (`UploadNotPermitted: Failed to fetch billing balance`) and 503s."""
    import time

    import botocore.exceptions

    for attempt in range(retries):
        try:
            client.create_bucket(Bucket=bucket)
            break
        except botocore.exceptions.ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            msg = e.response.get("Error", {}).get("Message", "")
            status = e.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0)
            transient = status == 503 or "billing balance" in msg or code in ("UploadNotPermitted", "SlowDown")
            if not transient or attempt == retries - 1:
                raise
            time.sleep(2.0 * (attempt + 1))
    if public:
        client.put_bucket_acl(Bucket=bucket, ACL="public-read")


def delete_bucket_recursive(client, bucket: str) -> None:
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket):
        objs = [{"Key": o["Key"]} for o in page.get("Contents", [])]
        if objs:
            client.delete_objects(Bucket=bucket, Delete={"Objects": objs})
    client.delete_bucket(Bucket=bucket)


def anon_get_status(cfg: Config, bucket: str, key: str) -> int:
    """Unauthenticated GET via plain HTTP — returns the status code (no creds)."""
    import urllib.request

    url = f"{cfg.endpoint_url.rstrip('/')}/{bucket}/{key}"
    req = urllib.request.Request(url, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:  # noqa: S310 (trusted staging endpoint)
            return resp.status
    except urllib.error.HTTPError as e:
        return e.code
