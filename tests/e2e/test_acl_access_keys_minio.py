"""E2E tests for access key ACL grants using MinIO client

These tests use the MinIO Python client to make real S3 API calls
to verify that access key ACL grants work end-to-end.

Prerequisites:
- Gateway must be running (docker compose up)
- HIPPIUS_BYPASS_CREDIT_CHECK=true for testing
- Test access keys must be configured in Hippius API (or mocked)
"""

import io
from typing import Any
from xml.etree import ElementTree as ET

import pytest
from minio import Minio
from minio.error import S3Error


@pytest.fixture  # type: ignore[misc]
def minio_client_alice() -> Minio:
    """MinIO client authenticated as Alice (owner)"""
    return Minio(
        "localhost:8080",
        access_key="hip_alice_master",
        secret_key="alice_secret",
        secure=False,
    )


@pytest.fixture  # type: ignore[misc]
def minio_client_bob() -> Minio:
    """MinIO client authenticated as Bob (different account)"""
    return Minio(
        "localhost:8080",
        access_key="hip_bob_sub1",
        secret_key="bob_secret",
        secure=False,
    )


@pytest.fixture  # type: ignore[misc]
def test_bucket(minio_client_alice: Minio) -> str:
    """Create a test bucket owned by Alice"""
    bucket_name = "test-acl-access-keys"

    # Clean up if exists
    try:
        minio_client_alice.remove_bucket(bucket_name)
    except S3Error:
        pass

    # Create fresh bucket
    minio_client_alice.make_bucket(bucket_name)

    yield bucket_name

    # Cleanup
    try:
        # Remove all objects first
        objects = minio_client_alice.list_objects(bucket_name, recursive=True)
        for obj in objects:
            minio_client_alice.remove_object(bucket_name, obj.object_name)
        minio_client_alice.remove_bucket(bucket_name)
    except S3Error:
        pass


@pytest.mark.e2e
@pytest.mark.skip(reason="Requires running gateway and configured access keys")
def test_set_bucket_acl_with_access_key_grant_via_xml(
    minio_client_alice: Minio, test_bucket: str
) -> None:
    """Test setting bucket ACL with AccessKey grant using custom XML"""

    # Alice uploads an object
    minio_client_alice.put_object(
        test_bucket,
        "test.txt",
        io.BytesIO(b"test data"),
        length=9,
    )

    # Alice sets ACL granting READ to Bob's specific key
    acl_xml = f'''<?xml version="1.0" encoding="UTF-8"?>
    <AccessControlPolicy xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
      <Owner>
        <ID>5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty</ID>
      </Owner>
      <AccessControlList>
        <Grant>
          <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                   xsi:type="CanonicalUser">
            <ID>5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty</ID>
          </Grantee>
          <Permission>FULL_CONTROL</Permission>
        </Grant>
        <Grant>
          <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                   xsi:type="AccessKey">
            <ID>hip_bob_sub1</ID>
          </Grantee>
          <Permission>READ</Permission>
        </Grant>
      </AccessControlList>
    </AccessControlPolicy>'''

    # Use MinIO's low-level API to set custom ACL
    response = minio_client_alice._url_open(
        "PUT",
        bucket_name=test_bucket,
        query_params={"acl": ""},
        body=acl_xml.encode('utf-8'),
        headers={"Content-Type": "application/xml"}
    )

    assert response.status == 200, f"Failed to set ACL: {response.status}"


@pytest.mark.e2e
@pytest.mark.skip(reason="Requires running gateway and configured access keys")
def test_get_bucket_acl_returns_access_key_grant(
    minio_client_alice: Minio, test_bucket: str
) -> None:
    """Test that GetBucketAcl returns AccessKey grants in XML"""

    # Alice sets ACL with AccessKey grant
    acl_xml = f'''<?xml version="1.0" encoding="UTF-8"?>
    <AccessControlPolicy xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
      <Owner><ID>5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty</ID></Owner>
      <AccessControlList>
        <Grant>
          <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="AccessKey">
            <ID>hip_bob_readonly</ID>
          </Grantee>
          <Permission>READ</Permission>
        </Grant>
      </AccessControlList>
    </AccessControlPolicy>'''

    minio_client_alice._url_open(
        "PUT",
        bucket_name=test_bucket,
        query_params={"acl": ""},
        body=acl_xml.encode('utf-8'),
    )

    # Get ACL back
    response = minio_client_alice._url_open(
        "GET",
        bucket_name=test_bucket,
        query_params={"acl": ""}
    )

    assert response.status == 200
    acl_response = response.data.decode('utf-8')

    # Parse XML and verify AccessKey grant is present
    root = ET.fromstring(acl_response)

    # Find AccessKey grant
    ns = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
    grants = root.findall(".//Grant", ns) or root.findall(".//Grant")

    access_key_grant = None
    for grant in grants:
        grantee = grant.find(".//Grantee", ns) or grant.find(".//Grantee")
        if grantee is not None:
            grantee_type = grantee.get("{http://www.w3.org/2001/XMLSchema-instance}type")
            if grantee_type == "AccessKey":
                access_key_grant = grant
                break

    assert access_key_grant is not None, "AccessKey grant not found in response"

    # Verify the grant details
    grantee_id = (
        access_key_grant.find(".//s3:ID", ns) or
        access_key_grant.find(".//ID")
    )
    permission = (
        access_key_grant.find(".//s3:Permission", ns) or
        access_key_grant.find(".//Permission")
    )

    assert grantee_id is not None and grantee_id.text == "hip_bob_readonly"
    assert permission is not None and permission.text == "READ"


@pytest.mark.e2e
@pytest.mark.skip(reason="Requires running gateway and configured access keys")
def test_access_key_grant_enforces_permissions(
    minio_client_alice: Minio,
    minio_client_bob: Minio,
    test_bucket: str,
) -> None:
    """Test that access key grants actually enforce permissions"""

    # Alice uploads an object
    minio_client_alice.put_object(
        test_bucket,
        "secret.txt",
        io.BytesIO(b"secret data"),
        length=11,
    )

    # Initially, Bob cannot read (no grants)
    with pytest.raises(S3Error) as exc_info:
        minio_client_bob.get_object(test_bucket, "secret.txt")
    assert exc_info.value.code == "AccessDenied"

    # Alice grants READ to Bob's specific key
    acl_xml = f'''<?xml version="1.0" encoding="UTF-8"?>
    <AccessControlPolicy xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
      <Owner><ID>5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty</ID></Owner>
      <AccessControlList>
        <Grant>
          <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser">
            <ID>5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty</ID>
          </Grantee>
          <Permission>FULL_CONTROL</Permission>
        </Grant>
        <Grant>
          <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="AccessKey">
            <ID>hip_bob_sub1</ID>
          </Grantee>
          <Permission>READ</Permission>
        </Grant>
      </AccessControlList>
    </AccessControlPolicy>'''

    minio_client_alice._url_open(
        "PUT",
        bucket_name=test_bucket,
        query_params={"acl": ""},
        body=acl_xml.encode('utf-8'),
    )

    # Now Bob can read with hip_bob_sub1
    response = minio_client_bob.get_object(test_bucket, "secret.txt")
    data = response.read()
    assert data == b"secret data"


@pytest.mark.e2e
@pytest.mark.skip(reason="Requires running gateway and configured access keys")
def test_different_access_key_denied(
    minio_client_alice: Minio, test_bucket: str
) -> None:
    """Test that different access key from same account is denied"""

    # Alice uploads object
    minio_client_alice.put_object(
        test_bucket,
        "test.txt",
        io.BytesIO(b"data"),
        length=4,
    )

    # Alice grants READ to hip_bob_sub1 only
    acl_xml = f'''<?xml version="1.0" encoding="UTF-8"?>
    <AccessControlPolicy xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
      <Owner><ID>5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty</ID></Owner>
      <AccessControlList>
        <Grant>
          <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser">
            <ID>5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty</ID>
          </Grantee>
          <Permission>FULL_CONTROL</Permission>
        </Grant>
        <Grant>
          <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="AccessKey">
            <ID>hip_bob_sub1</ID>
          </Grantee>
          <Permission>READ</Permission>
        </Grant>
      </AccessControlList>
    </AccessControlPolicy>'''

    minio_client_alice._url_open(
        "PUT",
        bucket_name=test_bucket,
        query_params={"acl": ""},
        body=acl_xml.encode('utf-8'),
    )

    # Bob's hip_bob_sub2 should be denied
    minio_client_bob_sub2 = Minio(
        "localhost:8080",
        access_key="hip_bob_sub2",
        secret_key="bob_secret",
        secure=False,
    )

    with pytest.raises(S3Error) as exc_info:
        minio_client_bob_sub2.get_object(test_bucket, "test.txt")
    assert exc_info.value.code == "AccessDenied"


@pytest.mark.e2e
@pytest.mark.skip(reason="Requires running gateway and configured access keys")
def test_set_acl_via_grant_headers(minio_client_alice: Minio, test_bucket: str) -> None:
    """Test setting ACL using x-amz-grant-* headers with accessKey format"""

    # Use low-level API with grant headers
    response = minio_client_alice._url_open(
        "PUT",
        bucket_name=test_bucket,
        query_params={"acl": ""},
        headers={
            "x-amz-grant-read": 'accessKey="hip_bob_readonly"',
            "x-amz-grant-write": 'accessKey="hip_bob_writer"',
        }
    )

    assert response.status == 200

    # Verify ACL was set correctly
    response = minio_client_alice._url_open(
        "GET",
        bucket_name=test_bucket,
        query_params={"acl": ""}
    )

    acl_xml = response.data.decode('utf-8')

    # Should contain both access keys
    assert "hip_bob_readonly" in acl_xml
    assert "hip_bob_writer" in acl_xml
    assert "READ" in acl_xml
    assert "WRITE" in acl_xml


@pytest.mark.e2e
@pytest.mark.skip(reason="Requires running gateway and configured access keys")
def test_mixed_canonical_and_access_key_grants(
    minio_client_alice: Minio, test_bucket: str
) -> None:
    """Test ACL with both CanonicalUser and AccessKey grants"""

    # Alice uploads object
    minio_client_alice.put_object(
        test_bucket,
        "mixed.txt",
        io.BytesIO(b"mixed grants"),
        length=12,
    )

    # Set ACL with both grant types
    acl_xml = f'''<?xml version="1.0" encoding="UTF-8"?>
    <AccessControlPolicy xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
      <Owner><ID>5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty</ID></Owner>
      <AccessControlList>
        <!-- Owner -->
        <Grant>
          <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser">
            <ID>5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty</ID>
          </Grantee>
          <Permission>FULL_CONTROL</Permission>
        </Grant>

        <!-- Account-level grant -->
        <Grant>
          <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser">
            <ID>5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY</ID>
          </Grantee>
          <Permission>READ</Permission>
        </Grant>

        <!-- Access key-level grant -->
        <Grant>
          <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="AccessKey">
            <ID>hip_charlie_special</ID>
          </Grantee>
          <Permission>WRITE</Permission>
        </Grant>
      </AccessControlList>
    </AccessControlPolicy>'''

    response = minio_client_alice._url_open(
        "PUT",
        bucket_name=test_bucket,
        query_params={"acl": ""},
        body=acl_xml.encode('utf-8'),
    )

    assert response.status == 200

    # Verify ACL contains both types
    response = minio_client_alice._url_open(
        "GET",
        bucket_name=test_bucket,
        query_params={"acl": ""}
    )

    acl_response = response.data.decode('utf-8')
    root = ET.fromstring(acl_response)

    # Count grant types
    canonical_count = 0
    access_key_count = 0

    grants = root.findall(".//Grant") or root.findall(".//{http://s3.amazonaws.com/doc/2006-03-01/}Grant")

    for grant in grants:
        grantee = grant.find(".//Grantee") or grant.find(".//{http://s3.amazonaws.com/doc/2006-03-01/}Grantee")
        if grantee is not None:
            grantee_type = grantee.get("{http://www.w3.org/2001/XMLSchema-instance}type")
            if grantee_type == "CanonicalUser":
                canonical_count += 1
            elif grantee_type == "AccessKey":
                access_key_count += 1

    assert canonical_count >= 2, "Should have at least 2 CanonicalUser grants"
    assert access_key_count >= 1, "Should have at least 1 AccessKey grant"


@pytest.mark.e2e
@pytest.mark.skip(reason="Requires running gateway and configured access keys")
def test_object_acl_with_access_key_grant(
    minio_client_alice: Minio, test_bucket: str
) -> None:
    """Test setting object-level ACL with AccessKey grant"""

    # Alice uploads object
    object_name = "object-with-acl.txt"
    minio_client_alice.put_object(
        test_bucket,
        object_name,
        io.BytesIO(b"object data"),
        length=11,
    )

    # Set object ACL with AccessKey grant
    acl_xml = f'''<?xml version="1.0" encoding="UTF-8"?>
    <AccessControlPolicy xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
      <Owner><ID>5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty</ID></Owner>
      <AccessControlList>
        <Grant>
          <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser">
            <ID>5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty</ID>
          </Grantee>
          <Permission>FULL_CONTROL</Permission>
        </Grant>
        <Grant>
          <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="AccessKey">
            <ID>hip_bob_sub1</ID>
          </Grantee>
          <Permission>READ</Permission>
        </Grant>
      </AccessControlList>
    </AccessControlPolicy>'''

    # Set object ACL (not bucket ACL)
    response = minio_client_alice._url_open(
        "PUT",
        bucket_name=test_bucket,
        object_name=object_name,
        query_params={"acl": ""},
        body=acl_xml.encode('utf-8'),
    )

    assert response.status == 200

    # Get object ACL back
    response = minio_client_alice._url_open(
        "GET",
        bucket_name=test_bucket,
        object_name=object_name,
        query_params={"acl": ""}
    )

    assert response.status == 200
    acl_response = response.data.decode('utf-8')
    assert "AccessKey" in acl_response or "hip_bob_sub1" in acl_response


@pytest.mark.e2e
@pytest.mark.skip(reason="Requires running gateway and configured access keys")
def test_invalid_access_key_format_rejected(
    minio_client_alice: Minio, test_bucket: str
) -> None:
    """Test that invalid access key format is rejected"""

    # Try to set ACL with invalid access key format (missing hip_ prefix)
    acl_xml = f'''<?xml version="1.0" encoding="UTF-8"?>
    <AccessControlPolicy xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
      <Owner><ID>5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty</ID></Owner>
      <AccessControlList>
        <Grant>
          <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="AccessKey">
            <ID>invalid_key_format</ID>
          </Grantee>
          <Permission>READ</Permission>
        </Grant>
      </AccessControlList>
    </AccessControlPolicy>'''

    # This should fail validation
    response = minio_client_alice._url_open(
        "PUT",
        bucket_name=test_bucket,
        query_params={"acl": ""},
        body=acl_xml.encode('utf-8'),
    )

    # Should return 400 Bad Request
    assert response.status == 400, "Invalid access key format should be rejected"
