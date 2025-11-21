"""
ACL Response Format Tests

Tests that verify ACL responses contain correct structure and data.
Tests owner fields, grants arrays, grantee types, and canonical ID format.
"""

import pytest


pytestmark = pytest.mark.acl


class TestACLResponseStructure:
    """Test that ACL responses have correct structure."""

    def test_bucket_acl_has_owner_field(self, s3_acc1_uploaddelete, clean_bucket) -> None:
        """Test that get_bucket_acl response contains Owner field."""
        bucket = clean_bucket

        acl = s3_acc1_uploaddelete.get_bucket_acl(Bucket=bucket)

        assert "Owner" in acl, "ACL response must contain Owner field"
        assert "ID" in acl["Owner"], "Owner must have ID field"
        assert isinstance(acl["Owner"]["ID"], str), "Owner ID must be string"
        assert len(acl["Owner"]["ID"]) > 0, "Owner ID must not be empty"

    @pytest.mark.skipif(
        "config.getoption('--r2')",
        reason="PutObjectAcl/GetObjectAcl not implemented in R2. See: https://developers.cloudflare.com/r2/api/s3/api/"
    )
    def test_object_acl_has_owner_field(self, s3_acc1_uploaddelete, test_object) -> None:
        """Test that get_object_acl response contains Owner field."""
        bucket, key = test_object

        acl = s3_acc1_uploaddelete.get_object_acl(Bucket=bucket, Key=key)

        assert "Owner" in acl, "ACL response must contain Owner field"
        assert "ID" in acl["Owner"], "Owner must have ID field"
        assert isinstance(acl["Owner"]["ID"], str), "Owner ID must be string"
        assert len(acl["Owner"]["ID"]) > 0, "Owner ID must not be empty"

    def test_bucket_acl_has_grants_array(self, s3_acc1_uploaddelete, clean_bucket) -> None:
        """Test that get_bucket_acl response contains Grants array."""
        bucket = clean_bucket

        acl = s3_acc1_uploaddelete.get_bucket_acl(Bucket=bucket)

        assert "Grants" in acl, "ACL response must contain Grants field"
        assert isinstance(acl["Grants"], list), "Grants must be an array"
        assert len(acl["Grants"]) >= 1, "Default ACL must have at least owner grant"

    @pytest.mark.skipif(
        "config.getoption('--r2')",
        reason="PutObjectAcl/GetObjectAcl not implemented in R2. See: https://developers.cloudflare.com/r2/api/s3/api/"
    )
    def test_object_acl_has_grants_array(self, s3_acc1_uploaddelete, test_object) -> None:
        """Test that get_object_acl response contains Grants array."""
        bucket, key = test_object

        acl = s3_acc1_uploaddelete.get_object_acl(Bucket=bucket, Key=key)

        assert "Grants" in acl, "ACL response must contain Grants field"
        assert isinstance(acl["Grants"], list), "Grants must be an array"
        assert len(acl["Grants"]) >= 1, "Default ACL must have at least owner grant"


class TestGrantStructure:
    """Test that individual grants have correct structure."""

    def test_grant_has_grantee_and_permission(self, s3_acc1_uploaddelete, clean_bucket) -> None:
        """Test that each grant contains Grantee and Permission."""
        bucket = clean_bucket

        acl = s3_acc1_uploaddelete.get_bucket_acl(Bucket=bucket)
        grants = acl["Grants"]

        for grant in grants:
            assert "Grantee" in grant, "Grant must have Grantee field"
            assert "Permission" in grant, "Grant must have Permission field"
            assert isinstance(grant["Permission"], str), "Permission must be string"

    def test_canonical_user_grantee_structure(self, s3_acc1_uploaddelete, clean_bucket) -> None:
        """Test that CanonicalUser grantee has correct structure."""
        bucket = clean_bucket

        acl = s3_acc1_uploaddelete.get_bucket_acl(Bucket=bucket)
        grants = acl["Grants"]

        canonical_grants = [g for g in grants if g["Grantee"].get("Type") == "CanonicalUser"]
        assert len(canonical_grants) >= 1, "Must have at least one CanonicalUser grant"

        for grant in canonical_grants:
            grantee = grant["Grantee"]
            assert grantee["Type"] == "CanonicalUser"
            assert "ID" in grantee, "CanonicalUser grantee must have ID"
            assert isinstance(grantee["ID"], str), "Grantee ID must be string"
            assert len(grantee["ID"]) > 0, "Grantee ID must not be empty"

    @pytest.mark.skipif(
        "config.getoption('--r2')",
        reason="PutBucketAcl not implemented in R2. See: https://developers.cloudflare.com/r2/api/s3/api/"
    )
    def test_group_grantee_structure(self, s3_acc1_uploaddelete, clean_bucket) -> None:
        """Test that Group grantee has correct structure."""
        bucket = clean_bucket

        s3_acc1_uploaddelete.put_bucket_acl(Bucket=bucket, ACL="public-read")

        acl = s3_acc1_uploaddelete.get_bucket_acl(Bucket=bucket)
        grants = acl["Grants"]

        group_grants = [g for g in grants if g["Grantee"].get("Type") == "Group"]
        assert len(group_grants) >= 1, "public-read should have at least one Group grant"

        for grant in group_grants:
            grantee = grant["Grantee"]
            assert grantee["Type"] == "Group"
            assert "URI" in grantee, "Group grantee must have URI"
            assert isinstance(grantee["URI"], str), "Grantee URI must be string"
            assert "http" in grantee["URI"].lower(), "Group URI should be HTTP URI"


class TestPermissionValues:
    """Test that permission values are correct."""

    @pytest.mark.skipif(
        "config.getoption('--r2')",
        reason="PutBucketAcl not implemented in R2. See: https://developers.cloudflare.com/r2/api/s3/api/"
    )
    def test_valid_permission_values(self, s3_acc1_uploaddelete, clean_bucket, canonical_ids) -> None:
        """Test that all permissions use valid values."""
        bucket = clean_bucket

        s3_acc1_uploaddelete.put_bucket_acl(
            Bucket=bucket,
            GrantRead=f'id="{canonical_ids["acc2"]}"',
            GrantWrite=f'id="{canonical_ids["acc2"]}"',
            GrantReadACP=f'id="{canonical_ids["acc2"]}"',
            GrantWriteACP=f'id="{canonical_ids["acc2"]}"',
        )

        acl = s3_acc1_uploaddelete.get_bucket_acl(Bucket=bucket)
        grants = acl["Grants"]

        valid_permissions = {"READ", "WRITE", "READ_ACP", "WRITE_ACP", "FULL_CONTROL"}

        for grant in grants:
            assert grant["Permission"] in valid_permissions, f"Invalid permission value: {grant['Permission']}"

    @pytest.mark.skipif(
        "config.getoption('--r2')",
        reason="PutBucketAcl not implemented in R2. See: https://developers.cloudflare.com/r2/api/s3/api/"
    )
    def test_owner_always_has_full_control(self, s3_acc1_uploaddelete, clean_bucket, canonical_ids) -> None:
        """Test that owner always has FULL_CONTROL grant."""
        bucket = clean_bucket

        s3_acc1_uploaddelete.put_bucket_acl(Bucket=bucket, ACL="public-read")

        acl = s3_acc1_uploaddelete.get_bucket_acl(Bucket=bucket)
        grants = acl["Grants"]

        owner_full_control = next(
            (
                g
                for g in grants
                if g["Grantee"].get("Type") == "CanonicalUser"
                and g["Grantee"].get("ID") == canonical_ids["acc1"]
                and g["Permission"] == "FULL_CONTROL"
            ),
            None,
        )

        assert owner_full_control is not None, "Owner must always have FULL_CONTROL grant"


class TestCanonicalIDFormat:
    """Test canonical ID format."""

    def test_get_canonical_user_id_from_bucket_acl(self, s3_acc1_uploaddelete, clean_bucket) -> None:
        """Test that canonical user ID can be extracted from bucket ACL."""
        bucket = clean_bucket

        acl = s3_acc1_uploaddelete.get_bucket_acl(Bucket=bucket)
        canonical_id = acl["Owner"]["ID"]

        assert isinstance(canonical_id, str)
        assert len(canonical_id) > 0
        assert canonical_id[0].isalnum(), "Canonical ID should start with alphanumeric character"

    @pytest.mark.skipif(
        "config.getoption('--r2')",
        reason="PutObjectAcl/GetObjectAcl not implemented in R2. See: https://developers.cloudflare.com/r2/api/s3/api/"
    )
    def test_get_canonical_user_id_from_object_acl(self, s3_acc1_uploaddelete, test_object) -> None:
        """Test that canonical user ID can be extracted from object ACL."""
        bucket, key = test_object

        acl = s3_acc1_uploaddelete.get_object_acl(Bucket=bucket, Key=key)
        canonical_id = acl["Owner"]["ID"]

        assert isinstance(canonical_id, str)
        assert len(canonical_id) > 0
        assert canonical_id[0].isalnum(), "Canonical ID should start with alphanumeric character"

    @pytest.mark.skipif(
        "config.getoption('--r2')",
        reason="PutObjectAcl/GetObjectAcl not implemented in R2. See: https://developers.cloudflare.com/r2/api/s3/api/"
    )
    def test_canonical_id_consistent_across_requests(self, s3_acc1_uploaddelete, clean_bucket, test_object) -> None:
        """Test that canonical ID is consistent across different ACL requests."""
        bucket = clean_bucket
        bucket2, key = test_object

        bucket_acl = s3_acc1_uploaddelete.get_bucket_acl(Bucket=bucket)
        object_acl = s3_acc1_uploaddelete.get_object_acl(Bucket=bucket2, Key=key)

        bucket_canonical_id = bucket_acl["Owner"]["ID"]
        object_canonical_id = object_acl["Owner"]["ID"]

        assert bucket_canonical_id == object_canonical_id, (
            "Canonical ID should be consistent across bucket and object ACLs"
        )
