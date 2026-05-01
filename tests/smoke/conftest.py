import asyncio
import hashlib
import os
from datetime import datetime
from datetime import timedelta
from datetime import timezone

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError

from .session_tracker import SessionTracker


@pytest.fixture(scope="session")
def production_s3_client():
    access_key = os.environ["AWS_ACCESS_KEY"]
    secret_key = os.environ["AWS_SECRET_KEY"]

    endpoint = os.environ.get("HIPPIUS_ENDPOINT", "https://s3.hippius.com")

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


@pytest.fixture(scope="session")
def session_tracker(production_s3_client):
    session_id = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    bucket = "hippius-smoke-tests"

    tracker = SessionTracker(s3_client=production_s3_client, session_id=session_id, bucket_name=bucket)

    yield tracker


@pytest.fixture
def file_generator():
    def generate(size_bytes: int) -> tuple[bytes, str]:
        data = os.urandom(size_bytes)
        hash_md5 = hashlib.md5(data).hexdigest()
        return data, hash_md5

    return generate


@pytest.fixture(scope="session", autouse=True)
def cleanup_old_files(production_s3_client):
    bucket = "hippius-smoke-tests"
    prefix = "smoke-test/"
    retention_days = 30
    cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)

    try:
        production_s3_client.create_bucket(Bucket=bucket)
        print(f"Bucket '{bucket}' created")
    except ClientError as e:
        if e.response["Error"]["Code"] in ["BucketAlreadyOwnedByYou", "BucketAlreadyExists"]:
            print(f"Bucket '{bucket}' already exists")
        else:
            raise

    deleted_count = 0
    paginator = production_s3_client.get_paginator("list_objects_v2")

    try:
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            if "Contents" not in page:
                continue

            for obj in page["Contents"]:
                key = obj["Key"]
                parts = key.split("/")
                if len(parts) < 2:
                    continue

                if parts[1] == ".index":
                    # Sweep stale manifests too — keeping them around after their
                    # data files were deleted is what causes test_05 to download
                    # NoSuchKey ghosts.
                    if len(parts) < 3:
                        continue
                    manifest_session_id = parts[2].removesuffix(".json")
                    try:
                        manifest_ts = datetime.strptime(manifest_session_id, "%Y%m%d-%H%M%S").replace(
                            tzinfo=timezone.utc
                        )
                    except ValueError:
                        continue
                    if manifest_ts < cutoff:
                        production_s3_client.delete_object(Bucket=bucket, Key=key)
                        deleted_count += 1
                    continue

                try:
                    session_ts = datetime.strptime(parts[1], "%Y%m%d-%H%M%S").replace(tzinfo=timezone.utc)
                    if session_ts < cutoff:
                        production_s3_client.delete_object(Bucket=bucket, Key=key)
                        deleted_count += 1
                except ValueError:
                    continue
    except ClientError as e:
        # Best-effort housekeeping. If the shared bucket exists but isn't ours
        # (e.g. running against staging with a different account), skip the
        # sweep instead of blocking the actual tests.
        if e.response["Error"]["Code"] == "AccessDenied":
            print(f"Cleanup skipped: AccessDenied on '{bucket}' — not owned by this account")
        else:
            raise

    print(f"Cleanup: deleted {deleted_count} files older than {retention_days} days")

    yield


# ---- Sub-token scope smoke-test fixtures ----------------------------------
# These are session-scoped knobs the test_smoke_subtoken_scope.py tests need.
# Each one skips the test (via pytest.skip) when the corresponding env var is
# missing, so the rest of the smoke suite is unaffected if these aren't set.


@pytest.fixture(scope="session")
def hippius_endpoint():
    return os.environ.get("HIPPIUS_ENDPOINT", "https://s3.hippius.com")


@pytest.fixture(scope="session")
def target_environment(hippius_endpoint):
    return "staging" if "s3-staging." in hippius_endpoint else "production"


@pytest.fixture(scope="session")
def frontend_hmac_secret():
    secret = os.environ.get("FRONTEND_HMAC_SECRET")
    if not secret:
        pytest.skip("FRONTEND_HMAC_SECRET not set — see scripts/print_smoke_test_secrets.sh")
    return secret


@pytest.fixture(scope="session")
def hippius_user_token():
    token = os.environ.get("HIPPIUS_USER_TOKEN")
    if not token:
        pytest.skip("HIPPIUS_USER_TOKEN (DRF token) not set")
    return token


@pytest.fixture(scope="session")
def hippius_master_account_ss58(production_s3_client):
    """Discover the SS58 of whatever account the AWS_ACCESS_KEY/AWS_SECRET_KEY
    creds belong to. Avoids drift between the boto3 master client and the
    body.account_id we send to PUT /user/sub-tokens/{key}/scope: when both
    derive from the same source, swapping CI credentials between staging /
    prod / a smoke-test account just works."""
    return production_s3_client.list_buckets()["Owner"]["ID"]


@pytest.fixture(scope="session", autouse=True)
def cleanup_orphan_subtoken_buckets(production_s3_client):
    # Only sweep when the env vars below are present — otherwise the test file
    # using them is skipped and there's nothing to clean.
    if not os.environ.get("HIPPIUS_USER_TOKEN"):
        yield
        return

    prefix = "hippius-smoke-subtoken-"
    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
    deleted = 0

    try:
        listing = production_s3_client.list_buckets()
    except ClientError as e:
        print(f"orphan-bucket sweep: list_buckets failed: {e}")
        yield
        return

    for entry in listing.get("Buckets", []):
        name = entry["Name"]
        if not name.startswith(prefix):
            continue
        # name shape: hippius-smoke-subtoken-<YYYYMMDD-HHMMSS>-<short>
        parts = name.removeprefix(prefix).split("-")
        if len(parts) < 2:
            continue
        try:
            ts = datetime.strptime("-".join(parts[:2]), "%Y%m%d-%H%M%S").replace(tzinfo=timezone.utc)
        except ValueError:
            continue
        if ts >= cutoff:
            continue

        try:
            paginator = production_s3_client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=name):
                for obj in page.get("Contents", []) or []:
                    production_s3_client.delete_object(Bucket=name, Key=obj["Key"])
            production_s3_client.delete_bucket(Bucket=name)
            deleted += 1
        except ClientError as e:
            print(f"orphan-bucket sweep: failed to drop {name}: {e}")

    if deleted:
        print(f"orphan-bucket sweep: deleted {deleted} stale sub-token test buckets")
    yield


@pytest.fixture(scope="session", autouse=True)
def cleanup_orphan_sub_tokens():
    if not os.environ.get("HIPPIUS_USER_TOKEN"):
        yield
        return

    from .hippius_api_client import HippiusUserApiClient

    user_token = os.environ["HIPPIUS_USER_TOKEN"]
    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)

    async def _sweep():
        revoked = 0
        async with HippiusUserApiClient(user_token=user_token) as client:
            tokens = await client.list_sub_tokens()
            for tok in tokens:
                if not tok.name.startswith("smoketest-"):
                    continue
                if tok.status and tok.status != "active":
                    continue
                try:
                    created = datetime.fromisoformat(tok.created_at.replace("Z", "+00:00"))
                except ValueError:
                    continue
                if created >= cutoff:
                    continue
                try:
                    await client.revoke_sub_token(tok.token_id)
                    revoked += 1
                except Exception as e:
                    print(f"orphan-sub-token sweep: failed to revoke {tok.access_key_id[:12]}***: {e}")
        return revoked

    try:
        revoked = asyncio.run(_sweep())
        if revoked:
            print(f"orphan-sub-token sweep: revoked {revoked} stale smoketest tokens")
    except Exception as e:
        print(f"orphan-sub-token sweep: skipped due to error: {e}")

    yield
