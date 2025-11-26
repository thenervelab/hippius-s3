"""Edge case tests for access key ACL grants"""

from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from gateway.services.acl_service import ACLService
from hippius_s3.models.acl import ACL
from hippius_s3.models.acl import Grant
from hippius_s3.models.acl import Grantee
from hippius_s3.models.acl import GranteeType
from hippius_s3.models.acl import Owner
from hippius_s3.models.acl import Permission
from hippius_s3.models.acl import validate_grant_grantees


@pytest.fixture  # type: ignore[misc]
def mock_db_pool() -> Any:
    pool = MagicMock()
    pool.fetchrow = AsyncMock()
    return pool


@pytest.fixture  # type: ignore[misc]
def acl_service(mock_db_pool: Any) -> Any:
    return ACLService(mock_db_pool)


class TestAccessKeyValidation:
    def test_valid_access_key_formats(self) -> None:
        """Test that valid access key formats pass validation"""
        valid_keys = [
            "hip_a",
            "hip_abc123",
            "hip_ABC_123-xyz",
            "hip_" + "a" * 240,  # Max length
        ]

        for key_id in valid_keys:
            acl = ACL(
                owner=Owner(id="5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"),
                grants=[
                    Grant(
                        grantee=Grantee(type=GranteeType.ACCESS_KEY, id=key_id),
                        permission=Permission.READ,
                    )
                ],
            )
            validate_grant_grantees(acl)  # Should not raise

    def test_invalid_access_key_formats(self) -> None:
        """Test that invalid access key formats fail validation"""
        invalid_keys = [
            "hip_",  # Empty after prefix
            "hip",  # Missing underscore
            "HIP_abc",  # Wrong case prefix
            "hip_abc!@#",  # Invalid characters
            "hip_" + "a" * 241,  # Too long
            "hip ",  # Space
            "hip_abc def",  # Space in middle
        ]

        for key_id in invalid_keys:
            acl = ACL(
                owner=Owner(id="5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"),
                grants=[
                    Grant(
                        grantee=Grantee(type=GranteeType.ACCESS_KEY, id=key_id),
                        permission=Permission.READ,
                    )
                ],
            )
            with pytest.raises(ValueError, match="Invalid access key format"):
                validate_grant_grantees(acl)

    def test_access_key_requires_id(self) -> None:
        """Test that ACCESS_KEY grantee requires an id"""
        with pytest.raises(ValueError, match="AccessKey grantee must have id"):
            Grantee(type=GranteeType.ACCESS_KEY, id=None)


class TestMultipleGrantsToSameKey:
    @pytest.mark.asyncio
    async def test_multiple_permissions_to_same_key_all_apply(
        self, acl_service: Any, mock_db_pool: Any
    ) -> None:
        """Test that multiple grants to same key allow all permissions"""
        owner_id = "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"

        acl = ACL(
            owner=Owner(id=owner_id),
            grants=[
                Grant(
                    grantee=Grantee(type=GranteeType.CANONICAL_USER, id=owner_id),
                    permission=Permission.FULL_CONTROL,
                ),
                Grant(
                    grantee=Grantee(type=GranteeType.ACCESS_KEY, id="hip_bob_key"),
                    permission=Permission.READ,
                ),
                Grant(
                    grantee=Grantee(type=GranteeType.ACCESS_KEY, id="hip_bob_key"),
                    permission=Permission.WRITE,
                ),
                Grant(
                    grantee=Grantee(type=GranteeType.ACCESS_KEY, id="hip_bob_key"),
                    permission=Permission.READ_ACP,
                ),
            ],
        )

        acl_service.acl_repo.get_bucket_acl = AsyncMock(return_value=acl)

        # All permissions should be allowed
        assert await acl_service.check_permission(
            None, "bucket1", None, Permission.READ, access_key="hip_bob_key"
        ) is True

        assert await acl_service.check_permission(
            None, "bucket1", None, Permission.WRITE, access_key="hip_bob_key"
        ) is True

        assert await acl_service.check_permission(
            None, "bucket1", None, Permission.READ_ACP, access_key="hip_bob_key"
        ) is True


class TestMixedAccountAndKeyGrants:
    @pytest.mark.asyncio
    async def test_account_grant_allows_all_keys(
        self, acl_service: Any, mock_db_pool: Any
    ) -> None:
        """Test that account-level grant allows all keys from that account"""
        owner_id = "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"
        bob_account = "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY"

        acl = ACL(
            owner=Owner(id=owner_id),
            grants=[
                Grant(
                    grantee=Grantee(type=GranteeType.CANONICAL_USER, id=owner_id),
                    permission=Permission.FULL_CONTROL,
                ),
                Grant(
                    grantee=Grantee(type=GranteeType.CANONICAL_USER, id=bob_account),
                    permission=Permission.READ,
                ),
            ],
        )

        acl_service.acl_repo.get_bucket_acl = AsyncMock(return_value=acl)

        # All of Bob's keys should have READ access
        for key in ["hip_bob_key1", "hip_bob_key2", "hip_bob_key99"]:
            assert await acl_service.check_permission(
                bob_account, "bucket1", None, Permission.READ, access_key=key
            ) is True

    @pytest.mark.asyncio
    async def test_key_grant_overrides_no_account_grant(
        self, acl_service: Any, mock_db_pool: Any
    ) -> None:
        """Test that specific key grant works even without account grant"""
        owner_id = "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"
        bob_account = "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY"

        acl = ACL(
            owner=Owner(id=owner_id),
            grants=[
                Grant(
                    grantee=Grantee(type=GranteeType.CANONICAL_USER, id=owner_id),
                    permission=Permission.FULL_CONTROL,
                ),
                # Only specific key grant, no account grant
                Grant(
                    grantee=Grantee(type=GranteeType.ACCESS_KEY, id="hip_bob_special"),
                    permission=Permission.WRITE,
                ),
            ],
        )

        acl_service.acl_repo.get_bucket_acl = AsyncMock(return_value=acl)

        # Special key has WRITE via key grant
        assert await acl_service.check_permission(
            bob_account, "bucket1", None, Permission.WRITE, access_key="hip_bob_special"
        ) is True

        # Other keys don't have WRITE (no account grant)
        assert await acl_service.check_permission(
            bob_account, "bucket1", None, Permission.WRITE, access_key="hip_bob_other"
        ) is False

    @pytest.mark.asyncio
    async def test_account_and_key_grant_union(
        self, acl_service: Any, mock_db_pool: Any
    ) -> None:
        """Test that permissions are union of account and key grants"""
        owner_id = "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"
        bob_account = "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY"

        acl = ACL(
            owner=Owner(id=owner_id),
            grants=[
                Grant(
                    grantee=Grantee(type=GranteeType.CANONICAL_USER, id=owner_id),
                    permission=Permission.FULL_CONTROL,
                ),
                # Account has READ
                Grant(
                    grantee=Grantee(type=GranteeType.CANONICAL_USER, id=bob_account),
                    permission=Permission.READ,
                ),
                # Specific key has WRITE
                Grant(
                    grantee=Grantee(type=GranteeType.ACCESS_KEY, id="hip_bob_writer"),
                    permission=Permission.WRITE,
                ),
            ],
        )

        acl_service.acl_repo.get_bucket_acl = AsyncMock(return_value=acl)

        # hip_bob_writer has both READ (via account) and WRITE (via key)
        assert await acl_service.check_permission(
            bob_account, "bucket1", None, Permission.READ, access_key="hip_bob_writer"
        ) is True

        assert await acl_service.check_permission(
            bob_account, "bucket1", None, Permission.WRITE, access_key="hip_bob_writer"
        ) is True

        # Other Bob keys have only READ (via account)
        assert await acl_service.check_permission(
            bob_account, "bucket1", None, Permission.READ, access_key="hip_bob_reader"
        ) is True

        assert await acl_service.check_permission(
            bob_account, "bucket1", None, Permission.WRITE, access_key="hip_bob_reader"
        ) is False


class TestOwnershipAndAccessKeys:
    @pytest.mark.asyncio
    async def test_owner_has_full_control_regardless_of_key(
        self, acl_service: Any, mock_db_pool: Any
    ) -> None:
        """Test that bucket owner has full control with any access key"""
        owner_id = "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"

        acl = ACL(
            owner=Owner(id=owner_id),
            grants=[],  # No explicit grants
        )

        acl_service.acl_repo.get_bucket_acl = AsyncMock(return_value=acl)

        # Owner has all permissions regardless of which key they use
        for permission in [Permission.READ, Permission.WRITE, Permission.READ_ACP, Permission.WRITE_ACP]:
            assert await acl_service.check_permission(
                owner_id, "bucket1", None, permission, access_key="hip_owner_key1"
            ) is True

            assert await acl_service.check_permission(
                owner_id, "bucket1", None, permission, access_key="hip_owner_key2"
            ) is True

    @pytest.mark.asyncio
    async def test_key_grant_does_not_transfer_ownership(
        self, acl_service: Any, mock_db_pool: Any
    ) -> None:
        """Test that granting FULL_CONTROL to key doesn't make it owner"""
        owner_id = "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"
        bob_account = "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY"

        acl = ACL(
            owner=Owner(id=owner_id),
            grants=[
                Grant(
                    grantee=Grantee(type=GranteeType.CANONICAL_USER, id=owner_id),
                    permission=Permission.FULL_CONTROL,
                ),
                # Bob's key has FULL_CONTROL but is not owner
                Grant(
                    grantee=Grantee(type=GranteeType.ACCESS_KEY, id="hip_bob_admin"),
                    permission=Permission.FULL_CONTROL,
                ),
            ],
        )

        acl_service.acl_repo.get_bucket_acl = AsyncMock(return_value=acl)

        # Bob's key has permissions but ownership check should still be based on account
        # Owner is alice, so bob_account is not the owner
        assert acl.owner.id == owner_id
        assert acl.owner.id != bob_account


class TestNullAndMissingValues:
    @pytest.mark.asyncio
    async def test_none_access_key_doesnt_match_key_grant(
        self, acl_service: Any, mock_db_pool: Any
    ) -> None:
        """Test that None access_key doesn't match ACCESS_KEY grants"""
        owner_id = "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"

        acl = ACL(
            owner=Owner(id=owner_id),
            grants=[
                Grant(
                    grantee=Grantee(type=GranteeType.CANONICAL_USER, id=owner_id),
                    permission=Permission.FULL_CONTROL,
                ),
                Grant(
                    grantee=Grantee(type=GranteeType.ACCESS_KEY, id="hip_bob_key"),
                    permission=Permission.READ,
                ),
            ],
        )

        acl_service.acl_repo.get_bucket_acl = AsyncMock(return_value=acl)

        # Seed phrase auth (no access_key) doesn't match ACCESS_KEY grant
        assert await acl_service.check_permission(
            None, "bucket1", None, Permission.READ, access_key=None
        ) is False

    @pytest.mark.asyncio
    async def test_empty_string_access_key_doesnt_match(
        self, acl_service: Any, mock_db_pool: Any
    ) -> None:
        """Test that empty string access_key doesn't match grants"""
        owner_id = "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"

        acl = ACL(
            owner=Owner(id=owner_id),
            grants=[
                Grant(
                    grantee=Grantee(type=GranteeType.CANONICAL_USER, id=owner_id),
                    permission=Permission.FULL_CONTROL,
                ),
                Grant(
                    grantee=Grantee(type=GranteeType.ACCESS_KEY, id="hip_bob_key"),
                    permission=Permission.READ,
                ),
            ],
        )

        acl_service.acl_repo.get_bucket_acl = AsyncMock(return_value=acl)

        # Empty string doesn't match
        assert await acl_service.check_permission(
            None, "bucket1", None, Permission.READ, access_key=""
        ) is False


class TestCrosAccountAccessKeyGrants:
    @pytest.mark.asyncio
    async def test_cross_account_key_grant_works(
        self, acl_service: Any, mock_db_pool: Any
    ) -> None:
        """Test granting permission to another account's access key"""
        alice_id = "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"
        bob_id = "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY"

        acl = ACL(
            owner=Owner(id=alice_id),
            grants=[
                Grant(
                    grantee=Grantee(type=GranteeType.CANONICAL_USER, id=alice_id),
                    permission=Permission.FULL_CONTROL,
                ),
                # Alice grants to Bob's specific key
                Grant(
                    grantee=Grantee(type=GranteeType.ACCESS_KEY, id="hip_bob_contractor"),
                    permission=Permission.READ,
                ),
            ],
        )

        acl_service.acl_repo.get_bucket_acl = AsyncMock(return_value=acl)

        # Bob's contractor key can read Alice's bucket
        assert await acl_service.check_permission(
            bob_id, "alice-bucket", None, Permission.READ, access_key="hip_bob_contractor"
        ) is True

        # Bob's other keys cannot
        assert await acl_service.check_permission(
            bob_id, "alice-bucket", None, Permission.READ, access_key="hip_bob_personal"
        ) is False


class TestFullControlPermission:
    @pytest.mark.asyncio
    async def test_full_control_implies_all_permissions(
        self, acl_service: Any, mock_db_pool: Any
    ) -> None:
        """Test that FULL_CONTROL grant allows all operations"""
        owner_id = "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"

        acl = ACL(
            owner=Owner(id=owner_id),
            grants=[
                Grant(
                    grantee=Grantee(type=GranteeType.CANONICAL_USER, id=owner_id),
                    permission=Permission.FULL_CONTROL,
                ),
                Grant(
                    grantee=Grantee(type=GranteeType.ACCESS_KEY, id="hip_bob_admin"),
                    permission=Permission.FULL_CONTROL,
                ),
            ],
        )

        acl_service.acl_repo.get_bucket_acl = AsyncMock(return_value=acl)

        # FULL_CONTROL allows all permissions
        for permission in [Permission.READ, Permission.WRITE, Permission.READ_ACP, Permission.WRITE_ACP]:
            assert await acl_service.check_permission(
                None, "bucket1", None, permission, access_key="hip_bob_admin"
            ) is True


class TestCaseSensitivity:
    @pytest.mark.asyncio
    async def test_access_key_matching_is_case_sensitive(
        self, acl_service: Any, mock_db_pool: Any
    ) -> None:
        """Test that access key matching is case sensitive"""
        owner_id = "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"

        acl = ACL(
            owner=Owner(id=owner_id),
            grants=[
                Grant(
                    grantee=Grantee(type=GranteeType.CANONICAL_USER, id=owner_id),
                    permission=Permission.FULL_CONTROL,
                ),
                Grant(
                    grantee=Grantee(type=GranteeType.ACCESS_KEY, id="hip_Bob_Key"),
                    permission=Permission.READ,
                ),
            ],
        )

        acl_service.acl_repo.get_bucket_acl = AsyncMock(return_value=acl)

        # Exact case match works
        assert await acl_service.check_permission(
            None, "bucket1", None, Permission.READ, access_key="hip_Bob_Key"
        ) is True

        # Different case doesn't match
        assert await acl_service.check_permission(
            None, "bucket1", None, Permission.READ, access_key="hip_bob_key"
        ) is False

        assert await acl_service.check_permission(
            None, "bucket1", None, Permission.READ, access_key="HIP_BOB_KEY"
        ) is False
