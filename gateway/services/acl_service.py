import logging

import asyncpg

from hippius_s3.models.acl import ACL
from hippius_s3.models.acl import Grant
from hippius_s3.models.acl import GranteeType
from hippius_s3.models.acl import Permission
from hippius_s3.models.acl import WellKnownGroups
from hippius_s3.repositories.acl_repository import ACLRepository


logger = logging.getLogger(__name__)


class ACLService:
    def __init__(self, db_pool: asyncpg.Pool):
        self.acl_repo = ACLRepository(db_pool)
        logger.info("ACLService initialized (direct DB queries, no caching)")

    async def canned_acl_to_acl(self, canned_acl: str, owner_id: str, bucket: str | None = None) -> ACL:
        """Convert canned ACL name to ACL object with grants."""
        from hippius_s3.services.acl_helper import canned_acl_to_acl as shared_canned_acl_to_acl

        return await shared_canned_acl_to_acl(canned_acl, owner_id, self.acl_repo.db, bucket)

    def _grant_matches(self, grant: Grant, account_id: str | None, access_key: str | None = None) -> bool:
        """Check if grant applies to this account or access key."""
        if grant.grantee.type == GranteeType.ACCESS_KEY:
            return grant.grantee.id == access_key
        if grant.grantee.type == GranteeType.CANONICAL_USER:
            return grant.grantee.id == account_id
        if grant.grantee.type == GranteeType.GROUP:
            if grant.grantee.uri == WellKnownGroups.ALL_USERS:
                return True
            if grant.grantee.uri == WellKnownGroups.AUTHENTICATED_USERS:
                return account_id is not None and account_id != "anonymous"
        return False

    def _permission_implies(self, granted: Permission, required: Permission) -> bool:
        """Check if granted permission satisfies required permission."""
        if granted == Permission.FULL_CONTROL:
            return True
        return granted == required

    async def get_bucket_owner(self, bucket: str) -> str | None:
        """Get bucket owner from buckets table."""
        query = "SELECT main_account_id FROM buckets WHERE bucket_name = $1"
        row = await self.acl_repo.db.fetchrow(query, bucket)
        return str(row["main_account_id"]) if row else None

    async def get_object_owner(self, bucket: str, key: str) -> str | None:
        """Get object owner (inherits from bucket owner)."""
        query = """
            SELECT b.main_account_id
            FROM objects o
            JOIN buckets b ON o.bucket_id = b.bucket_id
            WHERE b.bucket_name = $1 AND o.object_key = $2
        """
        row = await self.acl_repo.db.fetchrow(query, bucket, key)
        return str(row["main_account_id"]) if row else None

    async def check_permission(
        self,
        account_id: str | None,
        bucket: str,
        key: str | None,
        permission: Permission,
        access_key: str | None = None,
    ) -> bool:
        """Check if account or access key has permission for bucket/object."""

        acl = await self.get_effective_acl(bucket, key)

        grants_summary = [
            f"{{type={g.grantee.type.value}, id={g.grantee.id or 'None'}, uri={g.grantee.uri or 'None'}, perm={g.permission.value}}}"
            for g in acl.grants
        ]

        if account_id and acl.owner.id == account_id:
            logger.info(
                f"ACL check: account={account_id}, access_key={access_key or 'None'}, bucket={bucket}, "
                f"key={key or 'None'}, required_perm={permission.value}, owner={acl.owner.id}, "
                f"grants={len(acl.grants)}{grants_summary}, result=GRANTED (owner match)"
            )
            return True

        for grant in acl.grants:
            if self._grant_matches(grant, account_id, access_key) and self._permission_implies(
                grant.permission, permission
            ):
                match_reason = f"grant matched: grantee_type={grant.grantee.type.value}, grantee_id={grant.grantee.id or 'None'}, grant_perm={grant.permission.value}"
                logger.info(
                    f"ACL check: account={account_id}, access_key={access_key or 'None'}, bucket={bucket}, "
                    f"key={key or 'None'}, required_perm={permission.value}, owner={acl.owner.id}, "
                    f"grants={len(acl.grants)}{grants_summary}, result=GRANTED ({match_reason})"
                )
                return True

        logger.info(
            f"ACL check: account={account_id}, access_key={access_key or 'None'}, bucket={bucket}, "
            f"key={key or 'None'}, required_perm={permission.value}, owner={acl.owner.id}, "
            f"grants={len(acl.grants)}{grants_summary}, result=DENIED"
        )
        return False

    async def get_effective_acl(self, bucket: str, key: str | None) -> ACL:
        """Get effective ACL with inheritance (direct DB queries)."""
        if key:
            acl = await self.acl_repo.get_object_acl(bucket, key)
            if acl:
                return acl

        acl = await self.acl_repo.get_bucket_acl(bucket)
        if acl:
            return acl

        owner_id = await self.get_bucket_owner(bucket)
        if not owner_id:
            raise ValueError(f"Bucket not found: {bucket}")

        return await self.canned_acl_to_acl("private", owner_id, bucket)

    async def invalidate_cache(self, bucket: str, key: str | None = None) -> None:
        """No-op: caching removed for simplicity and consistency."""
        pass
