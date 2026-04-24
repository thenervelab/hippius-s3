"""Regional cache smoke tests.

Parametrized across the regional cache endpoints; runs in parallel under
pytest-xdist. Exercises the three hot paths for regional reads/writes:

- authenticated upload + download round-trip
- anonymous download of an object with public-read ACL
- presigned URL round-trip (PUT + GET)

Each region gets its own boto3 client so presigned URLs are signed for
that region's hostname (SigV4 canonicalizes Host).
"""

import hashlib
import os
import uuid
from datetime import datetime

import boto3
import pytest
import httpx
from botocore.config import Config


REGIONAL_ENDPOINTS = [
    "https://eu-central-1.hippius.com",
    "https://us-central-1.hippius.com",
]


@pytest.fixture
def regional_s3_client(request):
    endpoint = request.param
    access_key = os.environ["AWS_ACCESS_KEY"]
    secret_key = os.environ["AWS_SECRET_KEY"]

    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
        config=Config(
            s3={"addressing_style": "path"},
            signature_version="s3v4",
        ),
    )


def _endpoint_id(endpoint: str) -> str:
    # "https://eu-central-1.hippius.com" -> "eu-central-1"
    return endpoint.replace("https://", "").split(".", 1)[0]


@pytest.mark.parametrize("regional_s3_client", REGIONAL_ENDPOINTS, indirect=True, ids=_endpoint_id)
def test_regional_upload_download_roundtrip(regional_s3_client, session_tracker, file_generator):
    endpoint = regional_s3_client.meta.endpoint_url
    region = _endpoint_id(endpoint)
    size = 512 * 1024
    data, expected_hash = file_generator(size)
    key = f"smoke-test/{session_tracker.session_id}/regional/{region}/{uuid.uuid4()}.bin"

    put_resp = regional_s3_client.put_object(Bucket=session_tracker.bucket, Key=key, Body=data)
    assert put_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    get_resp = regional_s3_client.get_object(Bucket=session_tracker.bucket, Key=key)
    assert get_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    downloaded = get_resp["Body"].read()
    assert hashlib.md5(downloaded).hexdigest() == expected_hash
    assert len(downloaded) == size

    print(f"[{region}] upload/download roundtrip OK: {key}")


@pytest.mark.parametrize("regional_s3_client", REGIONAL_ENDPOINTS, indirect=True, ids=_endpoint_id)
def test_regional_public_object_anonymous_download(regional_s3_client, session_tracker, file_generator):
    endpoint = regional_s3_client.meta.endpoint_url
    region = _endpoint_id(endpoint)
    size = 128 * 1024
    data, expected_hash = file_generator(size)
    key = f"smoke-test/{session_tracker.session_id}/regional/{region}/public-{uuid.uuid4()}.bin"

    regional_s3_client.put_object(Bucket=session_tracker.bucket, Key=key, Body=data)
    regional_s3_client.put_object_acl(Bucket=session_tracker.bucket, Key=key, ACL="public-read")

    anon_url = f"{endpoint.rstrip('/')}/{session_tracker.bucket}/{key}"
    resp = httpx.get(anon_url, timeout=60)
    assert resp.status_code == 200, f"anonymous GET failed: status={resp.status_code} body={resp.text[:200]}"
    assert hashlib.md5(resp.content).hexdigest() == expected_hash
    assert len(resp.content) == size

    cache_control = resp.headers.get("Cache-Control", "")
    assert "public" in cache_control.lower(), f"expected public Cache-Control, got: {cache_control!r}"

    print(f"[{region}] public anonymous download OK: {key} (Cache-Control={cache_control!r})")


@pytest.mark.parametrize("regional_s3_client", REGIONAL_ENDPOINTS, indirect=True, ids=_endpoint_id)
def test_regional_presigned_url_roundtrip(regional_s3_client, session_tracker, file_generator):
    endpoint = regional_s3_client.meta.endpoint_url
    region = _endpoint_id(endpoint)
    size = 256 * 1024
    data, expected_hash = file_generator(size)
    key = f"smoke-test/{session_tracker.session_id}/regional/{region}/presigned-{uuid.uuid4()}.bin"

    put_url = regional_s3_client.generate_presigned_url(
        "put_object",
        Params={"Bucket": session_tracker.bucket, "Key": key},
        ExpiresIn=300,
    )
    put_resp = httpx.put(put_url, content=data, timeout=60)
    assert put_resp.status_code == 200, f"[{region}] presigned PUT failed: {put_resp.status_code} {put_resp.text[:200]}"

    get_url = regional_s3_client.generate_presigned_url(
        "get_object",
        Params={"Bucket": session_tracker.bucket, "Key": key},
        ExpiresIn=300,
    )
    get_resp = httpx.get(get_url, timeout=60)
    assert get_resp.status_code == 200, f"[{region}] presigned GET failed: {get_resp.status_code} {get_resp.text[:200]}"
    assert hashlib.md5(get_resp.content).hexdigest() == expected_hash

    print(f"[{region}] presigned URL roundtrip OK: {key}")


@pytest.fixture(scope="session", autouse=True)
def _regional_session_banner():
    print(f"Regional smoke tests starting at {datetime.utcnow().isoformat()} against {REGIONAL_ENDPOINTS}")
    yield
