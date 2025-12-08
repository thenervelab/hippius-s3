import hashlib
import os
from datetime import datetime
from datetime import timedelta

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError

from .session_tracker import SessionTracker


@pytest.fixture(scope="session")
def production_s3_client():
    access_key = os.environ["AWS_ACCESS_KEY"]
    secret_key = os.environ["AWS_SECRET_KEY"]

    return boto3.client(
        "s3",
        endpoint_url="https://s3.hippius.com",
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
    session_id = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
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
    cutoff = datetime.utcnow() - timedelta(days=retention_days)

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

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        if "Contents" not in page:
            continue

        for obj in page["Contents"]:
            key = obj["Key"]
            parts = key.split("/")
            if len(parts) < 2:
                continue

            if parts[1] == ".index":
                continue

            try:
                session_ts = datetime.strptime(parts[1], "%Y%m%d-%H%M%S")
                if session_ts < cutoff:
                    production_s3_client.delete_object(Bucket=bucket, Key=key)
                    deleted_count += 1
            except ValueError:
                continue

    print(f"Cleanup: deleted {deleted_count} files older than {retention_days} days")

    yield
