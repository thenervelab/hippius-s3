"""Live-stack smoke tests for R2-style sub-token scope enforcement.

Mints a real `hip_sub_smoketest_*` sub-token via api.hippius.com, installs
scope on the gateway with the HMAC-protected `/user/sub-tokens/{key}/scope`
endpoint, and drives a real boto3 client at the gateway to assert each tier's
behavior. Always revokes the sub-token and empties the test buckets on
teardown so re-running is idempotent.

# Local-run setup
#
# Add to .aws.cli.env (gitignored) once:
#   export AWS_ACCESS_KEY=hip_master_...
#   export AWS_SECRET_KEY=...
#   export HIPPIUS_USER_TOKEN=<your-drf-token-from-api.hippius.com>
#   export HIPPIUS_ENDPOINT=https://s3-staging.hippius.com
#
# The account SS58 is auto-derived from AWS_ACCESS_KEY/AWS_SECRET_KEY via
# list_buckets — the DRF token in HIPPIUS_USER_TOKEN must belong to the
# *same* Hippius account those AWS keys do, otherwise PUT scope rejects
# with "Buckets not owned by account_id".
#
# Then before each run:
#   source .aws.cli.env
#   export FRONTEND_HMAC_SECRET=$(kubectl get secret -n hippius-s3-staging \\
#     hippius-s3-secrets -o jsonpath='{.data.FRONTEND_HMAC_SECRET}' | base64 -d)
#   pytest tests/smoke/test_smoke_subtoken_scope.py -v -m smoke_subtoken
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import uuid
from dataclasses import dataclass
from datetime import datetime
from datetime import timezone
from typing import Any

import boto3
import httpx
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError

from .hippius_api_client import HippiusUserApiClient
from .hippius_api_client import SubTokenCreateResponse


pytestmark = pytest.mark.smoke_subtoken


# ---- Helpers --------------------------------------------------------------


@dataclass
class ScopeHttpClient:
    """HMAC-signed client for `/user/sub-tokens/{key}/scope` on the gateway.

    Mirrors the e2e ScopeClient (tests/e2e/conftest.py:435-465) — same canonical
    message shape (METHOD + path[+?query]) and `X-HMAC-Signature` header.
    """

    base_url: str
    secret: str

    def _sign(self, method: str, path: str, query: str = "") -> str:
        message = f"{method}{path}?{query}" if query else f"{method}{path}"
        return hmac.new(self.secret.encode(), message.encode(), hashlib.sha256).hexdigest()

    def get(self, access_key_id: str, account_id: str) -> httpx.Response:
        path = f"/user/sub-tokens/{access_key_id}/scope"
        query = f"account_id={account_id}"
        return httpx.get(
            f"{self.base_url}{path}?{query}",
            headers={"X-HMAC-Signature": self._sign("GET", path, query)},
            timeout=30.0,
        )

    def put(self, access_key_id: str, body: dict[str, Any]) -> httpx.Response:
        path = f"/user/sub-tokens/{access_key_id}/scope"
        return httpx.put(
            f"{self.base_url}{path}",
            json=body,
            headers={"X-HMAC-Signature": self._sign("PUT", path)},
            timeout=30.0,
        )

    def delete(self, access_key_id: str) -> httpx.Response:
        path = f"/user/sub-tokens/{access_key_id}/scope"
        return httpx.delete(
            f"{self.base_url}{path}",
            headers={"X-HMAC-Signature": self._sign("DELETE", path)},
            timeout=30.0,
        )


def _http_status(exc: ClientError) -> int:
    return int(exc.response["ResponseMetadata"]["HTTPStatusCode"])


# ---- Per-file fixtures ----------------------------------------------------


@pytest.fixture(scope="session")
def scope_http_client(hippius_endpoint, frontend_hmac_secret) -> ScopeHttpClient:
    return ScopeHttpClient(base_url=hippius_endpoint, secret=frontend_hmac_secret)


@pytest.fixture
def provisioned_sub_token(hippius_user_token, scope_http_client) -> Any:
    """Mint a fresh hip_sub_smoketest_* token, yield (access_key, secret, token_id), revoke on teardown."""
    name = f"smoketest-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:8]}"

    async def _create() -> SubTokenCreateResponse:
        async with HippiusUserApiClient(user_token=hippius_user_token) as client:
            return await client.create_sub_token(name=name)

    created = asyncio.run(_create())
    yield (created.access_key_id, created.secret, created.token_id)

    # Teardown: scope DELETE first (cheap, no api dep), then revoke. Both idempotent.
    try:
        scope_http_client.delete(created.access_key_id)
    except Exception as e:  # noqa: BLE001 — best-effort cleanup
        print(f"teardown: scope DELETE failed for {created.access_key_id[:12]}***: {e}")

    async def _revoke() -> None:
        async with HippiusUserApiClient(user_token=hippius_user_token) as client:
            await client.revoke_sub_token(created.token_id)

    try:
        asyncio.run(_revoke())
    except Exception as e:  # noqa: BLE001 — best-effort cleanup
        print(f"teardown: revoke failed for token_id={created.token_id}: {e}")


@pytest.fixture
def sub_token_s3_client(provisioned_sub_token, hippius_endpoint) -> Any:
    access_key_id, secret_key, _ = provisioned_sub_token
    return boto3.client(
        "s3",
        endpoint_url=hippius_endpoint,
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}, signature_version="s3v4"),
    )


@pytest.fixture
def scope_test_buckets(production_s3_client, session_tracker) -> Any:
    """Create two scratch buckets, yield names, empty + delete on teardown."""
    suffix_a = uuid.uuid4().hex[:6]
    suffix_b = uuid.uuid4().hex[:6]
    bucket_a = f"hippius-smoke-subtoken-{session_tracker.session_id}-{suffix_a}"
    bucket_b = f"hippius-smoke-subtoken-{session_tracker.session_id}-{suffix_b}"

    for name in (bucket_a, bucket_b):
        production_s3_client.create_bucket(Bucket=name)

    yield bucket_a, bucket_b

    for name in (bucket_a, bucket_b):
        try:
            paginator = production_s3_client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=name):
                for obj in page.get("Contents", []) or []:
                    production_s3_client.delete_object(Bucket=name, Key=obj["Key"])
            production_s3_client.delete_bucket(Bucket=name)
        except ClientError as e:
            print(f"teardown: failed to drop {name}: {e}")


def _install_scope(
    scope_http_client: ScopeHttpClient,
    access_key_id: str,
    account_id: str,
    permission: str,
    bucket_scope: str,
    buckets: list[str],
) -> None:
    resp = scope_http_client.put(
        access_key_id,
        {
            "account_id": account_id,
            "permission": permission,
            "bucket_scope": bucket_scope,
            "buckets": buckets,
        },
    )
    assert resp.status_code == 200, f"scope PUT failed: {resp.status_code} {resp.text}"


# ---- Tests ----------------------------------------------------------------


def test_default_deny_no_scope_installed(
    sub_token_s3_client,
    scope_test_buckets,
    file_generator,
):
    """No scope row → every op on every bucket is 403."""
    bucket_a, _ = scope_test_buckets
    data, _ = file_generator(1024)

    with pytest.raises(ClientError) as exc:
        sub_token_s3_client.get_object(Bucket=bucket_a, Key="missing.bin")
    assert _http_status(exc.value) == 403

    with pytest.raises(ClientError) as exc:
        sub_token_s3_client.put_object(Bucket=bucket_a, Key="x.bin", Body=data)
    assert _http_status(exc.value) == 403


def test_object_read_allows_get_denies_writes(
    production_s3_client,
    sub_token_s3_client,
    scope_http_client,
    provisioned_sub_token,
    scope_test_buckets,
    hippius_master_account_ss58,
    file_generator,
):
    """object_read tier on bucket A: GET works, DELETE/PUT 403."""
    bucket_a, _ = scope_test_buckets
    access_key_id, _, _ = provisioned_sub_token
    data, expected_md5 = file_generator(64 * 1024)
    key = "scope-test/readable.bin"

    production_s3_client.put_object(Bucket=bucket_a, Key=key, Body=data)
    _install_scope(scope_http_client, access_key_id, hippius_master_account_ss58, "object_read", "specific", [bucket_a])

    got = sub_token_s3_client.get_object(Bucket=bucket_a, Key=key)
    assert hashlib.md5(got["Body"].read()).hexdigest() == expected_md5

    # DELETE first (no body) — body-bearing 403 corrupts boto3 keepalive on the next req.
    with pytest.raises(ClientError) as exc:
        sub_token_s3_client.delete_object(Bucket=bucket_a, Key=key)
    assert _http_status(exc.value) == 403

    with pytest.raises(ClientError) as exc:
        sub_token_s3_client.put_object(Bucket=bucket_a, Key="write.bin", Body=b"x")
    assert _http_status(exc.value) == 403


def test_object_read_write_full_object_crud(
    sub_token_s3_client,
    scope_http_client,
    provisioned_sub_token,
    scope_test_buckets,
    hippius_master_account_ss58,
    file_generator,
):
    """object_read_write on bucket A: PUT, GET, DELETE all succeed."""
    bucket_a, _ = scope_test_buckets
    access_key_id, _, _ = provisioned_sub_token
    data, expected_md5 = file_generator(32 * 1024)
    key = "scope-test/rw.bin"

    _install_scope(
        scope_http_client, access_key_id, hippius_master_account_ss58, "object_read_write", "specific", [bucket_a]
    )

    sub_token_s3_client.put_object(Bucket=bucket_a, Key=key, Body=data)
    got = sub_token_s3_client.get_object(Bucket=bucket_a, Key=key)
    assert hashlib.md5(got["Body"].read()).hexdigest() == expected_md5
    sub_token_s3_client.delete_object(Bucket=bucket_a, Key=key)


def test_specific_bucket_scope_excludes_other_bucket(
    production_s3_client,
    sub_token_s3_client,
    scope_http_client,
    provisioned_sub_token,
    scope_test_buckets,
    hippius_master_account_ss58,
):
    """Scope on A only → ops on B all 403 even with object_read_write tier."""
    bucket_a, bucket_b = scope_test_buckets
    access_key_id, _, _ = provisioned_sub_token
    production_s3_client.put_object(Bucket=bucket_b, Key="other.bin", Body=b"no")

    _install_scope(
        scope_http_client, access_key_id, hippius_master_account_ss58, "object_read_write", "specific", [bucket_a]
    )

    with pytest.raises(ClientError) as exc:
        sub_token_s3_client.get_object(Bucket=bucket_b, Key="other.bin")
    assert _http_status(exc.value) == 403

    with pytest.raises(ClientError) as exc:
        sub_token_s3_client.put_object(Bucket=bucket_b, Key="x.bin", Body=b"y")
    assert _http_status(exc.value) == 403


def test_scope_update_is_immediate(
    production_s3_client,
    sub_token_s3_client,
    scope_http_client,
    provisioned_sub_token,
    scope_test_buckets,
    hippius_master_account_ss58,
):
    """Flip object_read → object_read_write; the next PUT must succeed (cache invalidation regression guard)."""
    bucket_a, _ = scope_test_buckets
    access_key_id, _, _ = provisioned_sub_token
    production_s3_client.put_object(Bucket=bucket_a, Key="probe.bin", Body=b"v0")

    # Read-only first: DELETE should be denied (probe with DELETE — no body, keeps keepalive clean).
    _install_scope(scope_http_client, access_key_id, hippius_master_account_ss58, "object_read", "specific", [bucket_a])
    with pytest.raises(ClientError) as exc:
        sub_token_s3_client.delete_object(Bucket=bucket_a, Key="probe.bin")
    assert _http_status(exc.value) == 403

    # Upgrade: subsequent PUT must succeed on the very next request.
    _install_scope(
        scope_http_client, access_key_id, hippius_master_account_ss58, "object_read_write", "specific", [bucket_a]
    )
    sub_token_s3_client.put_object(Bucket=bucket_a, Key="probe.bin", Body=b"v1")


def test_revoke_scope_resumes_default_deny(
    sub_token_s3_client,
    scope_http_client,
    provisioned_sub_token,
    scope_test_buckets,
    hippius_master_account_ss58,
):
    """Install scope → write succeeds; DELETE the scope row → next write 403."""
    bucket_a, _ = scope_test_buckets
    access_key_id, _, _ = provisioned_sub_token

    _install_scope(
        scope_http_client, access_key_id, hippius_master_account_ss58, "object_read_write", "specific", [bucket_a]
    )
    sub_token_s3_client.put_object(Bucket=bucket_a, Key="before-revoke.bin", Body=b"ok")

    delete_resp = scope_http_client.delete(access_key_id)
    assert delete_resp.status_code == 204, delete_resp.text

    with pytest.raises(ClientError) as exc:
        sub_token_s3_client.put_object(Bucket=bucket_a, Key="after-revoke.bin", Body=b"no")
    assert _http_status(exc.value) == 403


def test_admin_read_allows_list_buckets(
    sub_token_s3_client,
    scope_http_client,
    provisioned_sub_token,
    hippius_master_account_ss58,
):
    """admin_read with bucket_scope='all' → ListBuckets succeeds."""
    access_key_id, _, _ = provisioned_sub_token
    _install_scope(scope_http_client, access_key_id, hippius_master_account_ss58, "admin_read", "all", [])

    listing = sub_token_s3_client.list_buckets()
    assert listing["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_object_read_denies_list_buckets(
    sub_token_s3_client,
    scope_http_client,
    provisioned_sub_token,
    scope_test_buckets,
    hippius_master_account_ss58,
):
    """object_read tier (any bucket_scope) → ListBuckets 403."""
    bucket_a, _ = scope_test_buckets
    access_key_id, _, _ = provisioned_sub_token
    _install_scope(scope_http_client, access_key_id, hippius_master_account_ss58, "object_read", "specific", [bucket_a])

    with pytest.raises(ClientError) as exc:
        sub_token_s3_client.list_buckets()
    assert _http_status(exc.value) == 403


def test_object_read_write_can_bulk_delete(
    production_s3_client,
    sub_token_s3_client,
    scope_http_client,
    provisioned_sub_token,
    scope_test_buckets,
    hippius_master_account_ss58,
):
    """Regression guard for the resolver fix: POST /bucket?delete (bulk
    DeleteObjects) must require `delete_object`, not `write_bucket_meta`.
    Without the fix, an `object_read_write` token cannot bulk-delete its
    own files even though it can DELETE them one-by-one.
    """
    bucket_a, _ = scope_test_buckets
    access_key_id, _, _ = provisioned_sub_token

    production_s3_client.put_object(Bucket=bucket_a, Key="bulk-1.bin", Body=b"a")
    production_s3_client.put_object(Bucket=bucket_a, Key="bulk-2.bin", Body=b"b")

    _install_scope(
        scope_http_client, access_key_id, hippius_master_account_ss58, "object_read_write", "specific", [bucket_a]
    )

    resp = sub_token_s3_client.delete_objects(
        Bucket=bucket_a,
        Delete={"Objects": [{"Key": "bulk-1.bin"}, {"Key": "bulk-2.bin"}]},
    )
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert len(resp.get("Deleted", [])) == 2


def test_copy_object_source_bucket_must_be_in_scope(
    production_s3_client,
    sub_token_s3_client,
    scope_http_client,
    provisioned_sub_token,
    scope_test_buckets,
    hippius_master_account_ss58,
):
    """Regression guard for the middleware fix: CopyObject's source bucket is
    enforced against the sub-token's scope. A token scoped only to bucket B
    cannot use copy as a back-door to read bucket A.
    """
    bucket_a, bucket_b = scope_test_buckets
    access_key_id, _, _ = provisioned_sub_token

    # Put a "secret" file in bucket A (only reachable by master).
    production_s3_client.put_object(Bucket=bucket_a, Key="secret.txt", Body=b"private data")

    # Sub-token is scoped to bucket B only with object_read_write.
    _install_scope(
        scope_http_client, access_key_id, hippius_master_account_ss58, "object_read_write", "specific", [bucket_b]
    )

    # Copy-from-A-to-B must be denied (source out of scope).
    with pytest.raises(ClientError) as exc:
        sub_token_s3_client.copy_object(
            Bucket=bucket_b, Key="leaked.txt", CopySource={"Bucket": bucket_a, "Key": "secret.txt"}
        )
    assert _http_status(exc.value) == 403

    # When scope covers BOTH buckets, the copy succeeds.
    _install_scope(
        scope_http_client,
        access_key_id,
        hippius_master_account_ss58,
        "object_read_write",
        "specific",
        [bucket_a, bucket_b],
    )
    resp = sub_token_s3_client.copy_object(
        Bucket=bucket_b, Key="legit.txt", CopySource={"Bucket": bucket_a, "Key": "secret.txt"}
    )
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
