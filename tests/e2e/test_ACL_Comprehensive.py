"""Comprehensive E2E tests for S3 Access Control Lists (ACLs).

This test suite covers all ACL functionality including:
- Canned ACLs (private, public-read, public-read-write, authenticated-read)
- ACL management operations (GET/PUT bucket/object ACL)
- Permission enforcement (READ, WRITE, READ_ACP, WRITE_ACP, FULL_CONTROL)
- ACL inheritance from bucket to object
- Cross-account access
- Anonymous and authenticated user access
- Error scenarios

Test entities:
- User A (Owner): Primary account that creates resources
- User B (Authenticated): Secondary account for cross-account testing
- Anonymous: Unauthenticated requests for public access testing
"""

import base64
import os
import xml.etree.ElementTree as ET
from typing import Any
from typing import Callable

import pytest
import requests
from botocore.config import Config  # type: ignore[import-untyped]
from botocore.exceptions import ClientError  # type: ignore[import-untyped]


@pytest.fixture(scope="session")
def test_seed_phrase_b() -> str:
    """Second user seed phrase for User B (authenticated cross-account user)."""
    return "second user test seed phrase with twelve words total for authentication"


@pytest.fixture
def boto3_client_user_b(test_seed_phrase_b: str) -> Any:
    """Create boto3 S3 client for User B (authenticated user)."""
    import boto3  # type: ignore[import-untyped]

    access_key = base64.b64encode(test_seed_phrase_b.encode()).decode()
    secret_key = test_seed_phrase_b

    return boto3.client(
        "s3",
        endpoint_url="http://localhost:8080",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
        config=Config(
            s3={"addressing_style": "path"},
            signature_version="s3v4",
        ),
    )


@pytest.fixture
def s3_base_url(boto3_client: Any) -> str:
    """Get the S3 service base URL."""
    env_url = os.getenv("S3_ENDPOINT_URL")
    if env_url and env_url.strip():
        return env_url.strip()
    return "http://localhost:8080"


@pytest.fixture
def anonymous_http_client(s3_base_url: str) -> Callable:
    """HTTP client for anonymous (unsigned) requests."""

    def _request(method: str, bucket: str, key: str = "", params: dict[str, str] | None = None) -> requests.Response:
        url = f"{s3_base_url}/{bucket}"
        if key:
            url += f"/{key}"
        return requests.request(method, url, params=params, timeout=10)

    return _request


@pytest.fixture
def get_account_id() -> Callable[[Any], str]:
    """Extract account ID from boto3 client seed phrase (AWS canonical ID format)."""

    def _get_id(client: Any) -> str:
        import base64
        import hashlib

        access_key = client._request_signer._credentials.access_key
        seed_phrase = base64.b64decode(access_key).decode("utf-8")

        # Derive same account ID as middleware uses (AWS canonical ID format)
        seed_hash = hashlib.sha256(seed_phrase.encode()).digest()
        return seed_hash.hex()

    return _get_id


def assert_xml_has_grant(xml_content: str, grantee_type: str, permission: str) -> None:
    """Assert that XML ACL contains a specific grant."""
    root = ET.fromstring(xml_content)
    ns = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}

    grants = root.findall(".//s3:Grant", ns)
    for grant in grants:
        grantee = grant.find("s3:Grantee", ns)
        perm = grant.find("s3:Permission", ns)
        if grantee is not None and perm is not None:
            grantee_type_attr = grantee.get("{http://www.w3.org/2001/XMLSchema-instance}type")
            if grantee_type_attr == grantee_type and perm.text == permission:
                return
    raise AssertionError(f"Grant not found: {grantee_type} with {permission}")


def assert_access_denied(func: Callable, *args: Any, **kwargs: Any) -> None:
    """Assert that a function call raises AccessDenied error."""
    with pytest.raises(ClientError) as exc_info:
        func(*args, **kwargs)
    assert exc_info.value.response["Error"]["Code"] == "AccessDenied"


def assert_malformed_acl_error(func: Callable, *args: Any, **kwargs: Any) -> None:
    """Assert that a function call raises MalformedACLError."""
    with pytest.raises(ClientError) as exc_info:
        func(*args, **kwargs)
    assert exc_info.value.response["Error"]["Code"] == "MalformedACLError"


class TestCannedACLs:
    """Test canned ACL functionality for buckets and objects."""

    def test_bucket_canned_acl_private(
        self,
        docker_services: Any,
        boto3_client: Any,
        boto3_client_user_b: Any,
        anonymous_http_client: Callable,
        unique_bucket_name: Callable[[str], str],
        cleanup_buckets: Callable[[str], None],
    ) -> None:
        """Test private canned ACL denies all access except owner."""
        bucket_name = unique_bucket_name("acl-private")
        cleanup_buckets(bucket_name)

        boto3_client.create_bucket(Bucket=bucket_name, ACL="private")
        boto3_client.put_object(Bucket=bucket_name, Key="test.txt", Body=b"private content")

        response = boto3_client.get_object(Bucket=bucket_name, Key="test.txt")
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

        assert_access_denied(boto3_client_user_b.get_object, Bucket=bucket_name, Key="test.txt")

        anon_response = anonymous_http_client("GET", bucket_name, "test.txt")
        assert anon_response.status_code == 403

    def test_bucket_canned_acl_public_read(
        self,
        docker_services: Any,
        boto3_client: Any,
        boto3_client_user_b: Any,
        anonymous_http_client: Callable,
        unique_bucket_name: Callable[[str], str],
        cleanup_buckets: Callable[[str], None],
    ) -> None:
        """Test public-read canned ACL allows read access to everyone."""
        bucket_name = unique_bucket_name("acl-public-read")
        cleanup_buckets(bucket_name)

        boto3_client.create_bucket(Bucket=bucket_name, ACL="public-read")
        boto3_client.put_object(Bucket=bucket_name, Key="public.txt", Body=b"public content")

        response = boto3_client.get_object(Bucket=bucket_name, Key="public.txt")
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

        response_b = boto3_client_user_b.get_object(Bucket=bucket_name, Key="public.txt")
        assert response_b["ResponseMetadata"]["HTTPStatusCode"] == 200

        anon_response = anonymous_http_client("GET", bucket_name, "public.txt")
        assert anon_response.status_code == 200
        assert anon_response.content == b"public content"

        assert_access_denied(boto3_client_user_b.put_object, Bucket=bucket_name, Key="write-test.txt", Body=b"test")

    def test_bucket_canned_acl_public_read_write(
        self,
        docker_services: Any,
        boto3_client: Any,
        boto3_client_user_b: Any,
        anonymous_http_client: Callable,
        unique_bucket_name: Callable[[str], str],
        cleanup_buckets: Callable[[str], None],
    ) -> None:
        """Test public-read-write canned ACL allows read/write to authenticated users."""
        bucket_name = unique_bucket_name("acl-public-rw")
        cleanup_buckets(bucket_name)

        boto3_client.create_bucket(Bucket=bucket_name, ACL="public-read-write")
        boto3_client.put_object(Bucket=bucket_name, Key="obj1.txt", Body=b"content1")

        response = boto3_client.get_object(Bucket=bucket_name, Key="obj1.txt")
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

        response_b = boto3_client_user_b.get_object(Bucket=bucket_name, Key="obj1.txt")
        assert response_b["ResponseMetadata"]["HTTPStatusCode"] == 200

        anon_response = anonymous_http_client("GET", bucket_name, "obj1.txt")
        assert anon_response.status_code == 200

        response_b_put = boto3_client_user_b.put_object(Bucket=bucket_name, Key="obj2.txt", Body=b"user b content")
        assert response_b_put["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_bucket_canned_acl_authenticated_read(
        self,
        docker_services: Any,
        boto3_client: Any,
        boto3_client_user_b: Any,
        anonymous_http_client: Callable,
        unique_bucket_name: Callable[[str], str],
        cleanup_buckets: Callable[[str], None],
    ) -> None:
        """Test authenticated-read canned ACL allows read to authenticated users only."""
        bucket_name = unique_bucket_name("acl-auth-read")
        cleanup_buckets(bucket_name)

        boto3_client.create_bucket(Bucket=bucket_name, ACL="authenticated-read")
        boto3_client.put_object(Bucket=bucket_name, Key="auth.txt", Body=b"authenticated content")

        response = boto3_client.get_object(Bucket=bucket_name, Key="auth.txt")
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

        response_b = boto3_client_user_b.get_object(Bucket=bucket_name, Key="auth.txt")
        assert response_b["ResponseMetadata"]["HTTPStatusCode"] == 200

        anon_response = anonymous_http_client("GET", bucket_name, "auth.txt")
        assert anon_response.status_code == 403

    def test_object_canned_acl_private(
        self,
        docker_services: Any,
        boto3_client: Any,
        boto3_client_user_b: Any,
        anonymous_http_client: Callable,
        unique_bucket_name: Callable[[str], str],
        cleanup_buckets: Callable[[str], None],
    ) -> None:
        """Test object-level private canned ACL."""
        bucket_name = unique_bucket_name("obj-acl-private")
        cleanup_buckets(bucket_name)

        boto3_client.create_bucket(Bucket=bucket_name)
        boto3_client.put_object(Bucket=bucket_name, Key="private-obj.txt", Body=b"private", ACL="private")

        response = boto3_client.get_object(Bucket=bucket_name, Key="private-obj.txt")
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

        assert_access_denied(boto3_client_user_b.get_object, Bucket=bucket_name, Key="private-obj.txt")

        anon_response = anonymous_http_client("GET", bucket_name, "private-obj.txt")
        assert anon_response.status_code == 403

    def test_object_canned_acl_public_read(
        self,
        docker_services: Any,
        boto3_client: Any,
        boto3_client_user_b: Any,
        anonymous_http_client: Callable,
        unique_bucket_name: Callable[[str], str],
        cleanup_buckets: Callable[[str], None],
    ) -> None:
        """Test object-level public-read canned ACL."""
        bucket_name = unique_bucket_name("obj-acl-public")
        cleanup_buckets(bucket_name)

        boto3_client.create_bucket(Bucket=bucket_name)
        boto3_client.put_object(Bucket=bucket_name, Key="public-obj.txt", Body=b"public", ACL="public-read")

        response = boto3_client.get_object(Bucket=bucket_name, Key="public-obj.txt")
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

        response_b = boto3_client_user_b.get_object(Bucket=bucket_name, Key="public-obj.txt")
        assert response_b["ResponseMetadata"]["HTTPStatusCode"] == 200

        anon_response = anonymous_http_client("GET", bucket_name, "public-obj.txt")
        assert anon_response.status_code == 200
        assert anon_response.content == b"public"

    def test_object_canned_acl_overrides_bucket_acl(
        self,
        docker_services: Any,
        boto3_client: Any,
        anonymous_http_client: Callable,
        unique_bucket_name: Callable[[str], str],
        cleanup_buckets: Callable[[str], None],
    ) -> None:
        """Test that object ACL overrides bucket ACL."""
        bucket_name = unique_bucket_name("obj-override")
        cleanup_buckets(bucket_name)

        boto3_client.create_bucket(Bucket=bucket_name, ACL="public-read")

        boto3_client.put_object(Bucket=bucket_name, Key="private-in-public.txt", Body=b"private", ACL="private")

        anon_response = anonymous_http_client("GET", bucket_name, "private-in-public.txt")
        assert anon_response.status_code == 403


class TestACLManagementOperations:
    """Test ACL CRUD operations via S3 API."""

    def test_get_bucket_acl_returns_xml(
        self,
        docker_services: Any,
        boto3_client: Any,
        unique_bucket_name: Callable[[str], str],
        cleanup_buckets: Callable[[str], None],
        get_account_id: Callable[[Any], str],
    ) -> None:
        """Test GET bucket ACL returns valid XML."""
        bucket_name = unique_bucket_name("get-bucket-acl")
        cleanup_buckets(bucket_name)

        boto3_client.create_bucket(Bucket=bucket_name)

        response = boto3_client.get_bucket_acl(Bucket=bucket_name)
        assert "Owner" in response
        assert "Grants" in response
        assert response["Owner"]["ID"] == get_account_id(boto3_client)
        assert len(response["Grants"]) >= 1
        assert any(g["Permission"] == "FULL_CONTROL" for g in response["Grants"])

    def test_get_object_acl_returns_xml(
        self,
        docker_services: Any,
        boto3_client: Any,
        unique_bucket_name: Callable[[str], str],
        cleanup_buckets: Callable[[str], None],
        get_account_id: Callable[[Any], str],
    ) -> None:
        """Test GET object ACL returns valid XML."""
        bucket_name = unique_bucket_name("get-obj-acl")
        cleanup_buckets(bucket_name)

        boto3_client.create_bucket(Bucket=bucket_name)
        boto3_client.put_object(Bucket=bucket_name, Key="test.txt", Body=b"test")

        response = boto3_client.get_object_acl(Bucket=bucket_name, Key="test.txt")
        assert "Owner" in response
        assert "Grants" in response
        assert response["Owner"]["ID"] == get_account_id(boto3_client)

    def test_put_bucket_acl_with_canned_header(
        self,
        docker_services: Any,
        boto3_client: Any,
        anonymous_http_client: Callable,
        unique_bucket_name: Callable[[str], str],
        cleanup_buckets: Callable[[str], None],
    ) -> None:
        """Test PUT bucket ACL with canned ACL header."""
        bucket_name = unique_bucket_name("put-bucket-canned")
        cleanup_buckets(bucket_name)

        boto3_client.create_bucket(Bucket=bucket_name)
        boto3_client.put_object(Bucket=bucket_name, Key="test.txt", Body=b"test")

        anon_response = anonymous_http_client("GET", bucket_name, "test.txt")
        assert anon_response.status_code == 403

        boto3_client.put_bucket_acl(Bucket=bucket_name, ACL="public-read")

        anon_response_after = anonymous_http_client("GET", bucket_name, "test.txt")
        assert anon_response_after.status_code == 200

    def test_put_object_acl_with_canned_header(
        self,
        docker_services: Any,
        boto3_client: Any,
        anonymous_http_client: Callable,
        unique_bucket_name: Callable[[str], str],
        cleanup_buckets: Callable[[str], None],
    ) -> None:
        """Test PUT object ACL with canned ACL header."""
        bucket_name = unique_bucket_name("put-obj-canned")
        cleanup_buckets(bucket_name)

        boto3_client.create_bucket(Bucket=bucket_name)
        boto3_client.put_object(Bucket=bucket_name, Key="test.txt", Body=b"test", ACL="private")

        anon_response = anonymous_http_client("GET", bucket_name, "test.txt")
        assert anon_response.status_code == 403

        boto3_client.put_object_acl(Bucket=bucket_name, Key="test.txt", ACL="public-read")

        anon_response_after = anonymous_http_client("GET", bucket_name, "test.txt")
        assert anon_response_after.status_code == 200

    def test_put_bucket_acl_with_xml_body(
        self,
        docker_services: Any,
        boto3_client: Any,
        boto3_client_user_b: Any,
        unique_bucket_name: Callable[[str], str],
        cleanup_buckets: Callable[[str], None],
        get_account_id: Callable[[Any], str],
    ) -> None:
        """Test PUT bucket ACL with XML body granting specific permissions."""
        bucket_name = unique_bucket_name("put-bucket-xml")
        cleanup_buckets(bucket_name)

        boto3_client.create_bucket(Bucket=bucket_name)

        owner_id = get_account_id(boto3_client)
        user_b_id = get_account_id(boto3_client_user_b)

        acl = {
            "Grants": [
                {"Grantee": {"Type": "CanonicalUser", "ID": owner_id}, "Permission": "FULL_CONTROL"},
                {"Grantee": {"Type": "CanonicalUser", "ID": user_b_id}, "Permission": "READ"},
            ],
            "Owner": {"ID": owner_id},
        }

        boto3_client.put_bucket_acl(Bucket=bucket_name, AccessControlPolicy=acl)

        boto3_client.put_object(Bucket=bucket_name, Key="test.txt", Body=b"test")

        response_b = boto3_client_user_b.get_object(Bucket=bucket_name, Key="test.txt")
        assert response_b["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_put_object_acl_with_xml_body(
        self,
        docker_services: Any,
        boto3_client: Any,
        boto3_client_user_b: Any,
        unique_bucket_name: Callable[[str], str],
        cleanup_buckets: Callable[[str], None],
        get_account_id: Callable[[Any], str],
    ) -> None:
        """Test PUT object ACL with XML body granting specific permissions."""
        bucket_name = unique_bucket_name("put-obj-xml")
        cleanup_buckets(bucket_name)

        boto3_client.create_bucket(Bucket=bucket_name)
        boto3_client.put_object(Bucket=bucket_name, Key="test.txt", Body=b"test")

        owner_id = get_account_id(boto3_client)
        user_b_id = get_account_id(boto3_client_user_b)

        acl = {
            "Grants": [
                {"Grantee": {"Type": "CanonicalUser", "ID": owner_id}, "Permission": "FULL_CONTROL"},
                {"Grantee": {"Type": "CanonicalUser", "ID": user_b_id}, "Permission": "READ"},
            ],
            "Owner": {"ID": owner_id},
        }

        boto3_client.put_object_acl(Bucket=bucket_name, Key="test.txt", AccessControlPolicy=acl)

        response_b = boto3_client_user_b.get_object(Bucket=bucket_name, Key="test.txt")
        assert response_b["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_non_owner_cannot_modify_acl(
        self,
        docker_services: Any,
        boto3_client: Any,
        boto3_client_user_b: Any,
        unique_bucket_name: Callable[[str], str],
        cleanup_buckets: Callable[[str], None],
    ) -> None:
        """Test that non-owners cannot modify ACLs without WRITE_ACP permission."""
        bucket_name = unique_bucket_name("non-owner-acl")
        cleanup_buckets(bucket_name)

        boto3_client.create_bucket(Bucket=bucket_name)
        boto3_client.put_object(Bucket=bucket_name, Key="test.txt", Body=b"test")

        assert_access_denied(boto3_client_user_b.put_bucket_acl, Bucket=bucket_name, ACL="public-read")
        assert_access_denied(boto3_client_user_b.put_object_acl, Bucket=bucket_name, Key="test.txt", ACL="public-read")


class TestACLInheritance:
    """Test ACL inheritance from bucket to object."""

    def test_object_inherits_bucket_acl_when_not_set(
        self,
        docker_services: Any,
        boto3_client: Any,
        anonymous_http_client: Callable,
        unique_bucket_name: Callable[[str], str],
        cleanup_buckets: Callable[[str], None],
    ) -> None:
        """Test that objects without explicit ACL inherit from bucket."""
        bucket_name = unique_bucket_name("inherit-test")
        cleanup_buckets(bucket_name)

        boto3_client.create_bucket(Bucket=bucket_name, ACL="public-read")
        boto3_client.put_object(Bucket=bucket_name, Key="inherited.txt", Body=b"inherited")

        anon_response = anonymous_http_client("GET", bucket_name, "inherited.txt")
        assert anon_response.status_code == 200

    def test_object_acl_overrides_bucket_acl(
        self,
        docker_services: Any,
        boto3_client: Any,
        anonymous_http_client: Callable,
        unique_bucket_name: Callable[[str], str],
        cleanup_buckets: Callable[[str], None],
    ) -> None:
        """Test that explicit object ACL overrides bucket ACL."""
        bucket_name = unique_bucket_name("override-test")
        cleanup_buckets(bucket_name)

        boto3_client.create_bucket(Bucket=bucket_name, ACL="public-read")
        boto3_client.put_object(Bucket=bucket_name, Key="explicit-private.txt", Body=b"private", ACL="private")

        anon_response = anonymous_http_client("GET", bucket_name, "explicit-private.txt")
        assert anon_response.status_code == 403

    def test_default_acl_is_private(
        self,
        docker_services: Any,
        boto3_client: Any,
        boto3_client_user_b: Any,
        anonymous_http_client: Callable,
        unique_bucket_name: Callable[[str], str],
        cleanup_buckets: Callable[[str], None],
    ) -> None:
        """Test that default ACL (no ACL specified) is private."""
        bucket_name = unique_bucket_name("default-private")
        cleanup_buckets(bucket_name)

        boto3_client.create_bucket(Bucket=bucket_name)
        boto3_client.put_object(Bucket=bucket_name, Key="default.txt", Body=b"default")

        response = boto3_client.get_object(Bucket=bucket_name, Key="default.txt")
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

        assert_access_denied(boto3_client_user_b.get_object, Bucket=bucket_name, Key="default.txt")

        anon_response = anonymous_http_client("GET", bucket_name, "default.txt")
        assert anon_response.status_code == 403


class TestErrorScenarios:
    """Test error handling in ACL operations."""

    def test_anonymous_user_denied_on_private_bucket(
        self,
        docker_services: Any,
        boto3_client: Any,
        anonymous_http_client: Callable,
        unique_bucket_name: Callable[[str], str],
        cleanup_buckets: Callable[[str], None],
    ) -> None:
        """Test anonymous access denied on private bucket."""
        bucket_name = unique_bucket_name("anon-denied")
        cleanup_buckets(bucket_name)

        boto3_client.create_bucket(Bucket=bucket_name)
        boto3_client.put_object(Bucket=bucket_name, Key="private.txt", Body=b"private")

        anon_response = anonymous_http_client("GET", bucket_name, "private.txt")
        assert anon_response.status_code == 403
        assert "SignatureDoesNotMatch" in anon_response.text or "AccessDenied" in anon_response.text

    def test_authenticated_user_denied_on_private_bucket(
        self,
        docker_services: Any,
        boto3_client: Any,
        boto3_client_user_b: Any,
        unique_bucket_name: Callable[[str], str],
        cleanup_buckets: Callable[[str], None],
    ) -> None:
        """Test authenticated user (non-owner) denied on private bucket."""
        bucket_name = unique_bucket_name("auth-denied")
        cleanup_buckets(bucket_name)

        boto3_client.create_bucket(Bucket=bucket_name)
        boto3_client.put_object(Bucket=bucket_name, Key="private.txt", Body=b"private")

        assert_access_denied(boto3_client_user_b.get_object, Bucket=bucket_name, Key="private.txt")

    def test_anonymous_user_denied_on_authenticated_read_bucket(
        self,
        docker_services: Any,
        boto3_client: Any,
        anonymous_http_client: Callable,
        unique_bucket_name: Callable[[str], str],
        cleanup_buckets: Callable[[str], None],
    ) -> None:
        """Test anonymous user denied on authenticated-read bucket."""
        bucket_name = unique_bucket_name("anon-auth-denied")
        cleanup_buckets(bucket_name)

        boto3_client.create_bucket(Bucket=bucket_name, ACL="authenticated-read")
        boto3_client.put_object(Bucket=bucket_name, Key="auth-only.txt", Body=b"authenticated")

        anon_response = anonymous_http_client("GET", bucket_name, "auth-only.txt")
        assert anon_response.status_code == 403
