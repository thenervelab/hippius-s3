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


def pytest_addoption(parser: Any) -> None:
    """Add custom command line options."""
    parser.addoption(
        "--r2",
        action="store_true",
        default=False,
        help="Run tests against Cloudflare R2 instead of Hippius S3",
    )
    parser.addoption(
        "--aws",
        action="store_true",
        default=False,
        help="Run tests against AWS S3 for comparison",
    )


@pytest.fixture(scope="session")
def use_r2(request: Any) -> bool:
    """Check if --r2 flag was provided."""
    return request.config.getoption("--r2")


@pytest.fixture(scope="session")
def use_aws(request: Any) -> bool:
    """Check if --aws flag was provided."""
    return request.config.getoption("--aws")


@pytest.fixture(scope="session")
def config(use_r2: bool, use_aws: bool) -> Dict[str, str]:
    """Load configuration from .env file in project root."""
    project_root = Path(__file__).parent.parent.parent
    env_file = project_root / ".env"

    if not env_file.exists():
        pytest.fail(f".env file not found at {env_file}")

    load_dotenv(env_file)

    if use_r2 and use_aws:
        pytest.fail("Cannot use both --r2 and --aws flags simultaneously")

    if use_r2:
        required_vars = [
            "R2_ACC_1_ENDPOINT_URL",
            "R2_ACC_1_SUBACCOUNT_UPLOAD",
            "R2_ACC_1_SUBACCOUNT_UPLOADDELETE",
            "R2_ACC_2_ENDPOINT_URL",
            "R2_ACC_2_SUBACCOUNT_UPLOAD",
            "R2_ACC_2_SUBACCOUNT_UPLOADDELETE",
        ]

        for var in required_vars:
            if not os.getenv(var):
                pytest.fail(f"Required R2 environment variable {var} not set in .env")

        return {
            "acc1_endpoint_url": os.getenv("R2_ACC_1_ENDPOINT_URL") or "",
            "acc1_upload": os.getenv("R2_ACC_1_SUBACCOUNT_UPLOAD") or "",
            "acc1_uploaddelete": os.getenv("R2_ACC_1_SUBACCOUNT_UPLOADDELETE") or "",
            "acc2_endpoint_url": os.getenv("R2_ACC_2_ENDPOINT_URL") or "",
            "acc2_upload": os.getenv("R2_ACC_2_SUBACCOUNT_UPLOAD") or "",
            "acc2_uploaddelete": os.getenv("R2_ACC_2_SUBACCOUNT_UPLOADDELETE") or "",
            "use_r2": "true",
            "use_aws": "false",
        }
    elif use_aws:
        required_vars = [
            "AWS_ACC_1_ACCESS_KEY",
            "AWS_ACC_1_SECRET_KEY",
            "AWS_ACC_2_ACCESS_KEY",
            "AWS_ACC_2_SECRET_KEY",
        ]

        for var in required_vars:
            if not os.getenv(var):
                pytest.fail(f"Required AWS environment variable {var} not set in .env")

        return {
            "acc1_endpoint_url": os.getenv("AWS_ENDPOINT_URL", ""),
            "acc1_upload": f"{os.getenv('AWS_ACC_1_ACCESS_KEY')}:{os.getenv('AWS_ACC_1_SECRET_KEY')}",
            "acc1_uploaddelete": f"{os.getenv('AWS_ACC_1_ACCESS_KEY')}:{os.getenv('AWS_ACC_1_SECRET_KEY')}",
            "acc2_endpoint_url": os.getenv("AWS_ENDPOINT_URL", ""),
            "acc2_upload": f"{os.getenv('AWS_ACC_2_ACCESS_KEY')}:{os.getenv('AWS_ACC_2_SECRET_KEY')}",
            "acc2_uploaddelete": f"{os.getenv('AWS_ACC_2_ACCESS_KEY')}:{os.getenv('AWS_ACC_2_SECRET_KEY')}",
            "use_r2": "false",
            "use_aws": "true",
        }
    else:
        required_vars = [
            "HIPPIUS_ACC_1_SUBACCOUNT_UPLOAD",
            "HIPPIUS_ACC_1_SUBACCOUNT_UPLOADDELETE",
            "HIPPIUS_ACC_2_SUBACCOUNT_UPLOAD",
            "HIPPIUS_ACC_2_SUBACCOUNT_UPLOADDELETE",
        ]

        for var in required_vars:
            if not os.getenv(var):
                pytest.fail(f"Required environment variable {var} not set in .env")

        endpoint_url = os.getenv("HIPPIUS_ENDPOINT_URL", "http://localhost:8080")
        return {
            "acc1_endpoint_url": endpoint_url,
            "acc1_upload": os.getenv("HIPPIUS_ACC_1_SUBACCOUNT_UPLOAD") or "",
            "acc1_uploaddelete": os.getenv("HIPPIUS_ACC_1_SUBACCOUNT_UPLOADDELETE") or "",
            "acc2_endpoint_url": endpoint_url,
            "acc2_upload": os.getenv("HIPPIUS_ACC_2_SUBACCOUNT_UPLOAD") or "",
            "acc2_uploaddelete": os.getenv("HIPPIUS_ACC_2_SUBACCOUNT_UPLOADDELETE") or "",
            "use_r2": "false",
            "use_aws": "false",
        }


def create_s3_client(credentials: str, endpoint_url: str, is_r2: bool = False, is_aws: bool = False) -> Any:
    """
    Create boto3 S3 client with authentication.

    For Hippius: credentials is a seed phrase
    For R2/AWS: credentials is "access_key:secret_key"
    """
    if is_r2 or is_aws:
        if ":" not in credentials:
            provider = "R2" if is_r2 else "AWS"
            raise ValueError(f"{provider} credentials must be in format 'access_key:secret_key', got: {credentials[:20]}...")
        access_key, secret_key = credentials.split(":", 1)
    else:
        access_key = base64.b64encode(credentials.encode()).decode()
        secret_key = credentials

    if is_r2:
        region = "auto"
    elif is_aws:
        region = os.getenv("AWS_REGION", "us-east-1")
    else:
        region = "us-east-1"

    client_kwargs = {
        "aws_access_key_id": access_key,
        "aws_secret_access_key": secret_key,
        "region_name": region,
        "config": Config(signature_version="s3v4"),
    }

    if endpoint_url:
        client_kwargs["endpoint_url"] = endpoint_url

    return boto3.client("s3", **client_kwargs)


@pytest.fixture(scope="session")
def s3_acc1_upload(config: Dict[str, str]) -> Any:
    """Boto3 S3 client for Account 1 upload-only subaccount."""
    is_r2 = config.get("use_r2") == "true"
    is_aws = config.get("use_aws") == "true"
    return create_s3_client(config["acc1_upload"], config["acc1_endpoint_url"], is_r2=is_r2, is_aws=is_aws)


@pytest.fixture(scope="session")
def s3_acc1_uploaddelete(config: Dict[str, str]) -> Any:
    """Boto3 S3 client for Account 1 upload/delete subaccount."""
    is_r2 = config.get("use_r2") == "true"
    is_aws = config.get("use_aws") == "true"
    return create_s3_client(config["acc1_uploaddelete"], config["acc1_endpoint_url"], is_r2=is_r2, is_aws=is_aws)


@pytest.fixture(scope="session")
def s3_acc2_upload(config: Dict[str, str]) -> Any:
    """Boto3 S3 client for Account 2 upload-only subaccount."""
    is_r2 = config.get("use_r2") == "true"
    is_aws = config.get("use_aws") == "true"
    return create_s3_client(config["acc2_upload"], config["acc2_endpoint_url"], is_r2=is_r2, is_aws=is_aws)


@pytest.fixture(scope="session")
def s3_acc2_uploaddelete(config: Dict[str, str]) -> Any:
    """Boto3 S3 client for Account 2 upload/delete subaccount."""
    is_r2 = config.get("use_r2") == "true"
    is_aws = config.get("use_aws") == "true"
    return create_s3_client(config["acc2_uploaddelete"], config["acc2_endpoint_url"], is_r2=is_r2, is_aws=is_aws)


@pytest.fixture(scope="session")
def canonical_ids(s3_acc1_uploaddelete: Any, s3_acc2_uploaddelete: Any, config: Dict[str, str]) -> Dict[str, str]:
    """Get canonical user IDs for both accounts."""
    temp_bucket_acc1 = f"temp-canonical-id-acc1-{uuid.uuid4().hex[:8]}".replace("_", "-").lower()
    temp_bucket_acc2 = f"temp-canonical-id-acc2-{uuid.uuid4().hex[:8]}".replace("_", "-").lower()

    ids = {}

    is_aws = config.get("use_aws") == "true"
    aws_region = os.getenv("AWS_REGION", "us-east-1") if is_aws else None

    create_kwargs_acc1 = {"Bucket": temp_bucket_acc1}
    if is_aws and aws_region and aws_region != "us-east-1":
        create_kwargs_acc1["CreateBucketConfiguration"] = {"LocationConstraint": aws_region}

    create_kwargs_acc2 = {"Bucket": temp_bucket_acc2}
    if is_aws and aws_region and aws_region != "us-east-1":
        create_kwargs_acc2["CreateBucketConfiguration"] = {"LocationConstraint": aws_region}

    s3_acc1_uploaddelete.create_bucket(**create_kwargs_acc1)
    if is_aws:
        s3_acc1_uploaddelete.put_bucket_ownership_controls(
            Bucket=temp_bucket_acc1,
            OwnershipControls={"Rules": [{"ObjectOwnership": "BucketOwnerPreferred"}]},
        )
        s3_acc1_uploaddelete.delete_public_access_block(Bucket=temp_bucket_acc1)
    acl = s3_acc1_uploaddelete.get_bucket_acl(Bucket=temp_bucket_acc1)
    ids["acc1"] = acl["Owner"]["ID"]
    s3_acc1_uploaddelete.delete_bucket(Bucket=temp_bucket_acc1)

    s3_acc2_uploaddelete.create_bucket(**create_kwargs_acc2)
    if is_aws:
        s3_acc2_uploaddelete.put_bucket_ownership_controls(
            Bucket=temp_bucket_acc2,
            OwnershipControls={"Rules": [{"ObjectOwnership": "BucketOwnerPreferred"}]},
        )
        s3_acc2_uploaddelete.delete_public_access_block(Bucket=temp_bucket_acc2)
    acl = s3_acc2_uploaddelete.get_bucket_acl(Bucket=temp_bucket_acc2)
    ids["acc2"] = acl["Owner"]["ID"]
    s3_acc2_uploaddelete.delete_bucket(Bucket=temp_bucket_acc2)

    return ids


@pytest.fixture
def clean_bucket(s3_acc1_uploaddelete: Any, request: Any, config: Dict[str, str]) -> Any:
    """
    Create a clean bucket for each test.
    Automatically deletes bucket and all objects after test completes.
    """
    bucket_name = f"acl-test-{request.node.name[:30]}-{uuid.uuid4().hex[:8]}".replace("_", "-").lower()

    is_aws = config.get("use_aws") == "true"
    aws_region = os.getenv("AWS_REGION", "us-east-1") if is_aws else None

    create_kwargs = {"Bucket": bucket_name}
    if is_aws and aws_region and aws_region != "us-east-1":
        create_kwargs["CreateBucketConfiguration"] = {"LocationConstraint": aws_region}

    s3_acc1_uploaddelete.create_bucket(**create_kwargs)
    if is_aws:
        s3_acc1_uploaddelete.put_bucket_ownership_controls(
            Bucket=bucket_name,
            OwnershipControls={"Rules": [{"ObjectOwnership": "BucketOwnerPreferred"}]},
        )
        s3_acc1_uploaddelete.delete_public_access_block(Bucket=bucket_name)

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
