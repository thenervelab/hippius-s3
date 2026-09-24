"""Presigned SigV4 URLs that carry Object Lock intent in SIGNED headers.

A backup writer presigns PutObject / CreateMultipartUpload with `x-amz-object-lock-mode` and
`x-amz-object-lock-retain-until-date` in X-Amz-SignedHeaders, so whoever holds the URL must send
exactly that lock — they can neither drop it nor weaken it. Two halves have to hold for that:

1. the presigned verifier folds those headers into the canonical request, so a changed or missing
   value is a signature mismatch rather than a silently different lock;
2. the write path reads the lock from those same headers, in the exact wire format botocore signs.

The signing here is botocore's real S3SigV4QueryAuth, and verification runs the real canonical
request and signature code; only the account lookup and secret decryption are stubbed.
"""

from __future__ import annotations

from datetime import datetime
from datetime import timedelta
from datetime import timezone
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch
from urllib.parse import urlsplit

import pytest
from botocore.auth import S3SigV4QueryAuth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials
from fastapi import Request

from hippius_s3.api.s3.objects.object_lock_endpoints import lock_for_new_version
from hippius_s3.gateway.middlewares.access_key_auth import AccessKeyAuthError
from hippius_s3.gateway.middlewares.access_key_auth import verify_access_key_presigned_url


ACCESS_KEY = "hip_backup_writer_0001"
SECRET = "backup-writer-secret"
ACCOUNT = "5FH2aQUbix3nNatzST4mPM8iuebGvSMFerZLdwvDmAwRDFep"
HOST = "s3.example.test"
RETAIN_UNTIL = (datetime.now(timezone.utc) + timedelta(days=3)).replace(microsecond=0)
LOCK_HEADERS = {
    "x-amz-object-lock-mode": "COMPLIANCE",
    "x-amz-object-lock-retain-until-date": RETAIN_UNTIL.strftime("%Y-%m-%dT%H:%M:%SZ"),
}


def _presign(method: str, path_and_query: str, headers: dict[str, str]) -> str:
    request = AWSRequest(method=method, url=f"https://{HOST}{path_and_query}", headers=dict(headers))
    S3SigV4QueryAuth(Credentials(ACCESS_KEY, SECRET), "s3", "us-east-1", expires=600).add_auth(request)
    return str(request.url)


def _server_request(method: str, url: str, headers: dict[str, str]) -> Request:
    parts = urlsplit(url)
    sent = {"host": HOST, **headers}
    return Request(
        {
            "type": "http",
            "method": method,
            "path": parts.path,
            "raw_path": parts.path.encode("latin-1"),
            "query_string": parts.query.encode("latin-1"),
            "scheme": "https",
            "server": (HOST, 443),
            "headers": [(k.lower().encode("latin-1"), v.encode("latin-1")) for k, v in sent.items()],
        }
    )


async def _verify(request: Request) -> Any:
    token = MagicMock(
        valid=True,
        status="active",
        account_address=ACCOUNT,
        token_type="sub",
        encrypted_secret="enc",
        nonce="nonce",
    )
    with (
        patch("hippius_s3.gateway.middlewares.access_key_auth.cached_auth", AsyncMock(return_value=token)),
        patch("hippius_s3.gateway.middlewares.access_key_auth.decrypt_secret", return_value=SECRET),
    ):
        return await verify_access_key_presigned_url(request, ACCESS_KEY, AsyncMock())


# PutObject and CreateMultipartUpload are the two calls AWS applies lock headers on. UploadPart
# is included because a writer may sign the same headers on every call of an upload: they are
# verified, and ignored there as on AWS. CompleteMultipartUpload is NOT: it refuses lock headers
# with 501 (see multipart.handle_post_object), because the lock was fixed at initiate.
SIGNED_SHAPES = [
    pytest.param("PUT", "/backups/vm-1/chain/0001.full.raw.zst", id="PutObject"),
    pytest.param("POST", "/backups/vm-1/chain/0001.full.raw.zst?uploads=", id="CreateMultipartUpload"),
    pytest.param("PUT", "/backups/vm-1/chain/0001.full.raw.zst?partNumber=1&uploadId=u-1", id="UploadPart"),
]


@pytest.mark.asyncio
@pytest.mark.parametrize("method,path", SIGNED_SHAPES)
async def test_signed_lock_headers_verify(method: str, path: str) -> None:
    url = _presign(method, path, LOCK_HEADERS)
    assert "x-amz-object-lock-mode" in url and "x-amz-object-lock-retain-until-date" in url, (
        "precondition: the lock headers must be in X-Amz-SignedHeaders"
    )
    auth = await _verify(_server_request(method, url, LOCK_HEADERS))
    assert auth.access_key == ACCESS_KEY


@pytest.mark.asyncio
@pytest.mark.parametrize("method,path", SIGNED_SHAPES)
@pytest.mark.parametrize(
    "sent",
    [
        pytest.param({**LOCK_HEADERS, "x-amz-object-lock-mode": "GOVERNANCE"}, id="mode-weakened"),
        pytest.param(
            {**LOCK_HEADERS, "x-amz-object-lock-retain-until-date": "2020-01-01T00:00:00Z"}, id="date-shortened"
        ),
        pytest.param({"x-amz-object-lock-mode": "COMPLIANCE"}, id="date-dropped"),
        pytest.param({}, id="lock-dropped"),
    ],
)
async def test_url_holder_cannot_change_or_drop_the_signed_lock(method: str, path: str, sent: dict[str, str]) -> None:
    url = _presign(method, path, LOCK_HEADERS)
    with pytest.raises(AccessKeyAuthError, match="Signature mismatch"):
        await _verify(_server_request(method, url, sent))


@pytest.mark.parametrize("method,path", SIGNED_SHAPES[:2])
def test_write_path_applies_the_signed_lock(method: str, path: str) -> None:
    """The verified headers are the ones the write path turns into the version's lock."""
    request = _server_request(method, _presign(method, path, LOCK_HEADERS), LOCK_HEADERS)
    request.state.bucket_object_lock = {"enabled": True, "mode": "GOVERNANCE", "days": 1}

    outcome = lock_for_new_version(request)

    assert outcome == ("COMPLIANCE", RETAIN_UNTIL, False), (
        "explicit signed headers must override the bucket default, in botocore's date format"
    )
