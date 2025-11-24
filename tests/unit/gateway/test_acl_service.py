from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from gateway.services.acl_service import ACLService
from hippius_s3.models.acl import Grant
from hippius_s3.models.acl import Grantee
from hippius_s3.models.acl import GranteeType
from hippius_s3.models.acl import Permission
from hippius_s3.models.acl import WellKnownGroups


@pytest.fixture  # type: ignore[misc]
def mock_db_pool() -> Any:
    pool = MagicMock()
    pool.fetchrow = AsyncMock()
    return pool


@pytest.fixture  # type: ignore[misc]
def acl_service(mock_db_pool: Any) -> Any:
    return ACLService(mock_db_pool)


class TestCannedACLConversion:
    @pytest.mark.asyncio
    async def test_private_acl_grants_owner_only(self, acl_service: Any) -> None:
        owner_id = "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"
        acl = await acl_service.canned_acl_to_acl("private", owner_id)

        assert acl.owner.id == owner_id
        assert len(acl.grants) == 1
        assert acl.grants[0].grantee.type == GranteeType.CANONICAL_USER
        assert acl.grants[0].grantee.id == owner_id
        assert acl.grants[0].permission == Permission.FULL_CONTROL

    @pytest.mark.asyncio
    async def test_public_read_acl_grants_owner_and_all_users(self, acl_service: Any) -> None:
        owner_id = "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"
        acl = await acl_service.canned_acl_to_acl("public-read", owner_id)

        assert acl.owner.id == owner_id
        assert len(acl.grants) == 2

        owner_grant = acl.grants[0]
        assert owner_grant.grantee.type == GranteeType.CANONICAL_USER
        assert owner_grant.grantee.id == owner_id
        assert owner_grant.permission == Permission.FULL_CONTROL

        public_grant = acl.grants[1]
        assert public_grant.grantee.type == GranteeType.GROUP
        assert public_grant.grantee.uri == WellKnownGroups.ALL_USERS
        assert public_grant.permission == Permission.READ

    @pytest.mark.asyncio
    async def test_public_read_write_acl_grants_read_and_write(self, acl_service: Any) -> None:
        owner_id = "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"
        acl = await acl_service.canned_acl_to_acl("public-read-write", owner_id)

        assert acl.owner.id == owner_id
        assert len(acl.grants) == 3

        owner_grant = acl.grants[0]
        assert owner_grant.permission == Permission.FULL_CONTROL

        read_grant = acl.grants[1]
        assert read_grant.grantee.uri == WellKnownGroups.ALL_USERS
        assert read_grant.permission == Permission.READ

        write_grant = acl.grants[2]
        assert write_grant.grantee.uri == WellKnownGroups.ALL_USERS
        assert write_grant.permission == Permission.WRITE

    @pytest.mark.asyncio
    async def test_authenticated_read_acl_grants_authenticated_users(self, acl_service: Any) -> None:
        owner_id = "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"
        acl = await acl_service.canned_acl_to_acl("authenticated-read", owner_id)

        assert acl.owner.id == owner_id
        assert len(acl.grants) == 2

        owner_grant = acl.grants[0]
        assert owner_grant.permission == Permission.FULL_CONTROL

        auth_grant = acl.grants[1]
        assert auth_grant.grantee.type == GranteeType.GROUP
        assert auth_grant.grantee.uri == WellKnownGroups.AUTHENTICATED_USERS
        assert auth_grant.permission == Permission.READ

    @pytest.mark.asyncio
    async def test_invalid_canned_acl_raises_error(self, acl_service: Any) -> None:
        with pytest.raises(ValueError, match="Unknown canned ACL"):
            await acl_service.canned_acl_to_acl("invalid-acl", "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty")


class TestHelperFunctions:
    def test_grant_matches_canonical_user(self, acl_service: Any) -> None:
        grant = Grant(
            grantee=Grantee(type=GranteeType.CANONICAL_USER, id="account123"),
            permission=Permission.READ,
        )
        assert acl_service._grant_matches(grant, "account123") is True
        assert acl_service._grant_matches(grant, "account456") is False
        assert acl_service._grant_matches(grant, None) is False

    def test_grant_matches_all_users_group(self, acl_service: Any) -> None:
        grant = Grant(
            grantee=Grantee(type=GranteeType.GROUP, uri=WellKnownGroups.ALL_USERS),
            permission=Permission.READ,
        )
        assert acl_service._grant_matches(grant, "account123") is True
        assert acl_service._grant_matches(grant, None) is True

    def test_grant_matches_authenticated_users_group(self, acl_service: Any) -> None:
        grant = Grant(
            grantee=Grantee(type=GranteeType.GROUP, uri=WellKnownGroups.AUTHENTICATED_USERS),
            permission=Permission.READ,
        )
        assert acl_service._grant_matches(grant, "account123") is True
        assert acl_service._grant_matches(grant, None) is False

    def test_permission_implies_full_control(self, acl_service: Any) -> None:
        assert acl_service._permission_implies(Permission.FULL_CONTROL, Permission.READ) is True
        assert acl_service._permission_implies(Permission.FULL_CONTROL, Permission.WRITE) is True
        assert acl_service._permission_implies(Permission.FULL_CONTROL, Permission.READ_ACP) is True
        assert acl_service._permission_implies(Permission.FULL_CONTROL, Permission.WRITE_ACP) is True

    def test_permission_implies_exact_match(self, acl_service: Any) -> None:
        assert acl_service._permission_implies(Permission.READ, Permission.READ) is True
        assert acl_service._permission_implies(Permission.WRITE, Permission.WRITE) is True

    def test_permission_does_not_imply_different(self, acl_service: Any) -> None:
        assert acl_service._permission_implies(Permission.READ, Permission.WRITE) is False
        assert acl_service._permission_implies(Permission.WRITE, Permission.READ) is False


class TestPermissionEvaluation:
    @pytest.mark.asyncio
    async def test_owner_has_all_permissions(self, acl_service: Any, mock_db_pool: Any) -> None:
        owner_id = "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"
        acl = await acl_service.canned_acl_to_acl("private", owner_id)

        acl_service.acl_repo.get_bucket_acl = AsyncMock(return_value=acl)

        assert await acl_service.check_permission(owner_id, "bucket1", None, Permission.READ) is True
        assert await acl_service.check_permission(owner_id, "bucket1", None, Permission.WRITE) is True
        assert await acl_service.check_permission(owner_id, "bucket1", None, Permission.READ_ACP) is True

    @pytest.mark.asyncio
    async def test_non_owner_denied_for_private_acl(self, acl_service: Any, mock_db_pool: Any) -> None:
        owner_id = "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"
        other_id = "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY"
        acl = await acl_service.canned_acl_to_acl("private", owner_id)

        acl_service.acl_repo.get_bucket_acl = AsyncMock(return_value=acl)

        assert await acl_service.check_permission(other_id, "bucket1", None, Permission.READ) is False

    @pytest.mark.asyncio
    async def test_anonymous_can_read_public_read_acl(self, acl_service: Any, mock_db_pool: Any) -> None:
        owner_id = "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"
        acl = await acl_service.canned_acl_to_acl("public-read", owner_id)

        acl_service.acl_repo.get_bucket_acl = AsyncMock(return_value=acl)

        assert await acl_service.check_permission(None, "bucket1", None, Permission.READ) is True

    @pytest.mark.asyncio
    async def test_authenticated_user_can_read_public_read_acl(self, acl_service: Any, mock_db_pool: Any) -> None:
        owner_id = "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"
        user_id = "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY"
        acl = await acl_service.canned_acl_to_acl("public-read", owner_id)

        acl_service.acl_repo.get_bucket_acl = AsyncMock(return_value=acl)

        assert await acl_service.check_permission(user_id, "bucket1", None, Permission.READ) is True

    @pytest.mark.asyncio
    async def test_authenticated_user_can_read_authenticated_read_acl(
        self, acl_service: Any, mock_db_pool: Any
    ) -> None:
        owner_id = "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"
        user_id = "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY"
        acl = await acl_service.canned_acl_to_acl("authenticated-read", owner_id)

        acl_service.acl_repo.get_bucket_acl = AsyncMock(return_value=acl)

        assert await acl_service.check_permission(user_id, "bucket1", None, Permission.READ) is True

    @pytest.mark.asyncio
    async def test_anonymous_denied_for_authenticated_read_acl(self, acl_service: Any, mock_db_pool: Any) -> None:
        owner_id = "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"
        acl = await acl_service.canned_acl_to_acl("authenticated-read", owner_id)

        acl_service.acl_repo.get_bucket_acl = AsyncMock(return_value=acl)

        assert await acl_service.check_permission(None, "bucket1", None, Permission.READ) is False

    @pytest.mark.asyncio
    async def test_anonymous_cannot_write_public_read_acl(self, acl_service: Any, mock_db_pool: Any) -> None:
        owner_id = "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"
        acl = await acl_service.canned_acl_to_acl("public-read", owner_id)

        acl_service.acl_repo.get_bucket_acl = AsyncMock(return_value=acl)

        assert await acl_service.check_permission(None, "bucket1", None, Permission.WRITE) is False

    @pytest.mark.asyncio
    async def test_public_read_write_allows_write(self, acl_service: Any, mock_db_pool: Any) -> None:
        owner_id = "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"
        acl = await acl_service.canned_acl_to_acl("public-read-write", owner_id)

        acl_service.acl_repo.get_bucket_acl = AsyncMock(return_value=acl)

        assert await acl_service.check_permission(None, "bucket1", None, Permission.WRITE) is True


class TestACLRetrieval:
    @pytest.mark.asyncio
    async def test_get_object_acl_returns_object_acl(self, acl_service: Any, mock_db_pool: Any) -> None:
        owner_id = "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"
        object_acl = await acl_service.canned_acl_to_acl("private", owner_id)

        acl_service.acl_repo.get_object_acl = AsyncMock(return_value=object_acl)

        result = await acl_service.get_effective_acl("bucket1", "key1")

        assert result == object_acl
        acl_service.acl_repo.get_object_acl.assert_called_once_with("bucket1", "key1")

    @pytest.mark.skip(reason="Objects now inherit bucket ownership, no separate object owner")
    @pytest.mark.asyncio
    async def test_object_without_explicit_acl_gets_private_acl_with_object_owner(
        self, acl_service: Any, mock_db_pool: Any
    ) -> None:
        bucket_owner_id = "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"
        object_owner_id = "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY"
        bucket_acl = await acl_service.canned_acl_to_acl("public-read", bucket_owner_id)

        acl_service.acl_repo.get_object_acl = AsyncMock(return_value=None)
        acl_service.acl_repo.get_bucket_acl = AsyncMock(return_value=bucket_acl)

        mock_db_pool.fetchrow = AsyncMock(return_value={"main_account_id": object_owner_id})

        result = await acl_service.get_effective_acl("bucket1", "key1")

        assert result.owner.id == object_owner_id
        assert len(result.grants) == 1
        assert result.grants[0].grantee.id == object_owner_id
        assert result.grants[0].permission == Permission.FULL_CONTROL

    @pytest.mark.asyncio
    async def test_get_bucket_acl_returns_bucket_acl(self, acl_service: Any, mock_db_pool: Any) -> None:
        owner_id = "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"
        bucket_acl = await acl_service.canned_acl_to_acl("public-read", owner_id)

        acl_service.acl_repo.get_bucket_acl = AsyncMock(return_value=bucket_acl)

        result = await acl_service.get_effective_acl("bucket1", None)

        assert result == bucket_acl

    @pytest.mark.asyncio
    async def test_no_acl_returns_default_private(self, acl_service: Any, mock_db_pool: Any) -> None:
        owner_id = "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"

        acl_service.acl_repo.get_bucket_acl = AsyncMock(return_value=None)
        mock_db_pool.fetchrow = AsyncMock(return_value={"main_account_id": owner_id})

        result = await acl_service.get_effective_acl("bucket1", None)

        assert result.owner.id == owner_id
        assert len(result.grants) == 1
        assert result.grants[0].permission == Permission.FULL_CONTROL

    @pytest.mark.asyncio
    async def test_bucket_not_found_raises_error(self, acl_service: Any, mock_db_pool: Any) -> None:
        acl_service.acl_repo.get_bucket_acl = AsyncMock(return_value=None)
        mock_db_pool.fetchrow = AsyncMock(return_value=None)

        with pytest.raises(ValueError, match="Bucket not found"):
            await acl_service.get_effective_acl("nonexistent", None)


class TestWritePermissionOwnership:
    @pytest.mark.skip(reason="Objects now inherit bucket ownership, no separate object owner")
    @pytest.mark.asyncio
    async def test_write_permission_denied_for_non_owner_object(self, mock_db_pool: Any) -> None:
        alice_id = "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"
        bob_id = "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY"

        acl_service = ACLService(mock_db_pool)

        from hippius_s3.models.acl import ACL
        from hippius_s3.models.acl import Owner

        bucket_acl = ACL(
            owner=Owner(id=alice_id),
            grants=[
                Grant(
                    grantee=Grantee(type=GranteeType.CANONICAL_USER, id=alice_id),
                    permission=Permission.FULL_CONTROL,
                ),
                Grant(
                    grantee=Grantee(type=GranteeType.CANONICAL_USER, id=bob_id),
                    permission=Permission.WRITE,
                ),
            ],
        )

        acl_service.acl_repo = MagicMock()
        acl_service.acl_repo.db = mock_db_pool
        acl_service.acl_repo.get_object_acl = AsyncMock(return_value=None)
        acl_service.acl_repo.get_bucket_acl = AsyncMock(return_value=bucket_acl)

        mock_db_pool.fetchrow = AsyncMock(
            side_effect=lambda query, *args: (
                {"main_account_id": alice_id} if "buckets" in query else {"main_account_id": alice_id}
            )
        )

        has_permission = await acl_service.check_permission(
            account_id=bob_id, bucket="alice-bucket", key="alice-file.txt", permission=Permission.WRITE
        )

        assert not has_permission

    @pytest.mark.asyncio
    async def test_write_permission_granted_for_bucket_owner(self, mock_db_pool: Any) -> None:
        alice_id = "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"
        bob_id = "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY"

        acl_service = ACLService(mock_db_pool)

        from hippius_s3.models.acl import ACL
        from hippius_s3.models.acl import Owner

        bucket_acl = ACL(
            owner=Owner(id=bob_id),
            grants=[
                Grant(
                    grantee=Grantee(type=GranteeType.CANONICAL_USER, id=bob_id),
                    permission=Permission.FULL_CONTROL,
                ),
                Grant(
                    grantee=Grantee(type=GranteeType.CANONICAL_USER, id=alice_id),
                    permission=Permission.WRITE,
                ),
            ],
        )

        acl_service.acl_repo = MagicMock()
        acl_service.acl_repo.db = mock_db_pool
        acl_service.acl_repo.get_object_acl = AsyncMock(return_value=None)
        acl_service.acl_repo.get_bucket_acl = AsyncMock(return_value=bucket_acl)

        mock_db_pool.fetchrow = AsyncMock(
            side_effect=lambda query, *args: (
                {"main_account_id": bob_id} if "buckets" in query else {"main_account_id": alice_id}
            )
        )

        has_permission = await acl_service.check_permission(
            account_id=alice_id, bucket="bob-bucket", key="alice-file.txt", permission=Permission.WRITE
        )

        assert has_permission

    @pytest.mark.asyncio
    async def test_write_permission_granted_for_object_owner(self, mock_db_pool: Any) -> None:
        alice_id = "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"

        acl_service = ACLService(mock_db_pool)

        from hippius_s3.models.acl import ACL
        from hippius_s3.models.acl import Owner

        bucket_acl = ACL(
            owner=Owner(id=alice_id),
            grants=[
                Grant(
                    grantee=Grantee(type=GranteeType.CANONICAL_USER, id=alice_id),
                    permission=Permission.FULL_CONTROL,
                ),
            ],
        )

        acl_service.acl_repo = MagicMock()
        acl_service.acl_repo.db = mock_db_pool
        acl_service.acl_repo.get_object_acl = AsyncMock(return_value=None)
        acl_service.acl_repo.get_bucket_acl = AsyncMock(return_value=bucket_acl)

        mock_db_pool.fetchrow = AsyncMock(side_effect=lambda query, *args: {"main_account_id": alice_id})

        has_permission = await acl_service.check_permission(
            account_id=alice_id, bucket="alice-bucket", key="alice-file.txt", permission=Permission.WRITE
        )

        assert has_permission


class TestObjectACLInheritance:
    @pytest.mark.skip(reason="Objects now inherit bucket ownership, no separate object owner")
    @pytest.mark.asyncio
    async def test_object_without_acl_uses_object_owner_not_bucket_owner(self, mock_db_pool: Any) -> None:
        alice_id = "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"
        bob_id = "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY"

        acl_service = ACLService(mock_db_pool)

        from hippius_s3.models.acl import ACL
        from hippius_s3.models.acl import Owner

        bucket_acl = ACL(
            owner=Owner(id=alice_id),
            grants=[
                Grant(
                    grantee=Grantee(type=GranteeType.CANONICAL_USER, id=alice_id),
                    permission=Permission.FULL_CONTROL,
                ),
            ],
        )

        acl_service.acl_repo = MagicMock()
        acl_service.acl_repo.db = mock_db_pool
        acl_service.acl_repo.get_object_acl = AsyncMock(return_value=None)
        acl_service.acl_repo.get_bucket_acl = AsyncMock(return_value=bucket_acl)

        mock_db_pool.fetchrow = AsyncMock(
            side_effect=lambda query, *args: (
                {"main_account_id": bob_id} if "objects" in query else {"main_account_id": alice_id}
            )
        )

        effective_acl = await acl_service.get_effective_acl("alice-bucket", "bob-file.txt")

        assert effective_acl.owner.id == bob_id
        assert len(effective_acl.grants) == 1
        assert effective_acl.grants[0].grantee.id == bob_id
        assert effective_acl.grants[0].permission == Permission.FULL_CONTROL

    @pytest.mark.asyncio
    async def test_object_with_explicit_acl_uses_that_acl(self, mock_db_pool: Any) -> None:
        alice_id = "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"
        bob_id = "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY"

        acl_service = ACLService(mock_db_pool)

        from hippius_s3.models.acl import ACL
        from hippius_s3.models.acl import Owner

        object_acl = ACL(
            owner=Owner(id=bob_id),
            grants=[
                Grant(
                    grantee=Grantee(type=GranteeType.CANONICAL_USER, id=bob_id),
                    permission=Permission.FULL_CONTROL,
                ),
                Grant(
                    grantee=Grantee(type=GranteeType.CANONICAL_USER, id=alice_id),
                    permission=Permission.READ,
                ),
            ],
        )

        acl_service.acl_repo = MagicMock()
        acl_service.acl_repo.db = mock_db_pool
        acl_service.acl_repo.get_object_acl = AsyncMock(return_value=object_acl)

        effective_acl = await acl_service.get_effective_acl("alice-bucket", "bob-file.txt")

        assert effective_acl.owner.id == bob_id
        assert len(effective_acl.grants) == 2
        assert effective_acl == object_acl
