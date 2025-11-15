import base64
import os
import uuid
from io import BytesIO
from pathlib import Path
from typing import Any
from typing import Dict
from typing import Tuple

import boto3
import pytest
from botocore.client import Config
from dotenv import load_dotenv


@pytest.fixture(scope="session")
def config() -> Dict[str, str]:
    """Load configuration from .env file in project root."""
    project_root = Path(__file__).parent.parent.parent
    env_file = project_root / ".env"

    if not env_file.exists():
        pytest.fail(f".env file not found at {env_file}")

    load_dotenv(env_file)

    required_vars = [
        "HIPPIUS_ACC_1_SUBACCOUNT_UPLOAD",
        "HIPPIUS_ACC_1_SUBACCOUNT_UPLOADDELETE",
        "HIPPIUS_ACC_2_SUBACCOUNT_UPLOAD",
        "HIPPIUS_ACC_2_SUBACCOUNT_UPLOADDELETE",
    ]

    for var in required_vars:
        if not os.getenv(var):
            pytest.fail(f"Required environment variable {var} not set in .env")

    return {
        "endpoint_url": os.getenv("HIPPIUS_ENDPOINT_URL", "http://localhost:8080"),
        "acc1_upload": os.getenv("HIPPIUS_ACC_1_SUBACCOUNT_UPLOAD") or "",
        "acc1_uploaddelete": os.getenv("HIPPIUS_ACC_1_SUBACCOUNT_UPLOADDELETE") or "",
        "acc2_upload": os.getenv("HIPPIUS_ACC_2_SUBACCOUNT_UPLOAD") or "",
        "acc2_uploaddelete": os.getenv("HIPPIUS_ACC_2_SUBACCOUNT_UPLOADDELETE") or "",
    }


def create_s3_client(seed_phrase: str, endpoint_url: str) -> Any:
    """Create boto3 S3 client with HMAC-style authentication."""
    access_key = base64.b64encode(seed_phrase.encode()).decode()
    secret_key = seed_phrase

    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
        config=Config(signature_version="s3v4"),
    )


@pytest.fixture(scope="session")
def s3_acc1_upload(config: Dict[str, str]) -> Any:
    """Boto3 S3 client for Account 1 upload-only subaccount."""
    return create_s3_client(config["acc1_upload"], config["endpoint_url"])


@pytest.fixture(scope="session")
def s3_acc1_uploaddelete(config: Dict[str, str]) -> Any:
    """Boto3 S3 client for Account 1 upload/delete subaccount."""
    return create_s3_client(config["acc1_uploaddelete"], config["endpoint_url"])


@pytest.fixture(scope="session")
def s3_acc2_upload(config: Dict[str, str]) -> Any:
    """Boto3 S3 client for Account 2 upload-only subaccount."""
    return create_s3_client(config["acc2_upload"], config["endpoint_url"])


@pytest.fixture(scope="session")
def s3_acc2_uploaddelete(config: Dict[str, str]) -> Any:
    """Boto3 S3 client for Account 2 upload/delete subaccount."""
    return create_s3_client(config["acc2_uploaddelete"], config["endpoint_url"])


@pytest.fixture(scope="session")
def canonical_ids(s3_acc1_uploaddelete: Any, s3_acc2_uploaddelete: Any) -> Dict[str, str]:
    """Get canonical user IDs for both accounts."""
    temp_bucket_acc1 = f"temp-canonical-id-acc1-{uuid.uuid4().hex[:8]}"
    temp_bucket_acc2 = f"temp-canonical-id-acc2-{uuid.uuid4().hex[:8]}"

    ids = {}

    s3_acc1_uploaddelete.create_bucket(Bucket=temp_bucket_acc1)
    acl = s3_acc1_uploaddelete.get_bucket_acl(Bucket=temp_bucket_acc1)
    ids["acc1"] = acl["Owner"]["ID"]
    s3_acc1_uploaddelete.delete_bucket(Bucket=temp_bucket_acc1)

    s3_acc2_uploaddelete.create_bucket(Bucket=temp_bucket_acc2)
    acl = s3_acc2_uploaddelete.get_bucket_acl(Bucket=temp_bucket_acc2)
    ids["acc2"] = acl["Owner"]["ID"]
    s3_acc2_uploaddelete.delete_bucket(Bucket=temp_bucket_acc2)

    return ids


@pytest.fixture
def clean_bucket(s3_acc1_uploaddelete: Any, request: Any) -> Any:
    """
    Create a clean bucket for each test.
    Automatically deletes bucket and all objects after test completes.
    """
    bucket_name = f"acl-test-{request.node.name[:30]}-{uuid.uuid4().hex[:8]}"

    s3_acc1_uploaddelete.create_bucket(Bucket=bucket_name)

    yield bucket_name

    try:
        paginator = s3_acc1_uploaddelete.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket_name):
            if "Contents" in page:
                objects = [{"Key": obj["Key"]} for obj in page["Contents"]]
                s3_acc1_uploaddelete.delete_objects(Bucket=bucket_name, Delete={"Objects": objects})
    except:
        pass

    try:
        s3_acc1_uploaddelete.delete_bucket(Bucket=bucket_name)
    except:
        pass


@pytest.fixture
def test_object(s3_acc1_uploaddelete: Any, clean_bucket: str) -> Tuple[str, str]:
    """
    Upload a test object to the clean bucket.
    Returns (bucket_name, object_key) tuple.
    """
    key = "test-object.txt"
    content = b"This is test content for ACL testing"

    s3_acc1_uploaddelete.put_object(
        Bucket=clean_bucket,
        Key=key,
        Body=BytesIO(content),
    )

    return clean_bucket, key


pytestmark = pytest.mark.acl
