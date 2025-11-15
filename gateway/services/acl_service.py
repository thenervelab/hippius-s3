import logging

import asyncpg

from hippius_s3.models.acl import ACL
from hippius_s3.models.acl import Grant
from hippius_s3.models.acl import Grantee
from hippius_s3.models.acl import GranteeType
from hippius_s3.models.acl import Owner
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
        if canned_acl == "private":
            return ACL(
                owner=Owner(id=owner_id),
                grants=[
                    Grant(
                        grantee=Grantee(type=GranteeType.CANONICAL_USER, id=owner_id),
                        permission=Permission.FULL_CONTROL,
                    )
                ],
            )
        if canned_acl == "public-read":
            return ACL(
                owner=Owner(id=owner_id),
                grants=[
                    Grant(
                        grantee=Grantee(type=GranteeType.CANONICAL_USER, id=owner_id),
                        permission=Permission.FULL_CONTROL,
                    ),
                    Grant(
                        grantee=Grantee(type=GranteeType.GROUP, uri=WellKnownGroups.ALL_USERS),
                        permission=Permission.READ,
                    ),
                ],
            )
        if canned_acl == "public-read-write":
            return ACL(
                owner=Owner(id=owner_id),
                grants=[
                    Grant(
                        grantee=Grantee(type=GranteeType.CANONICAL_USER, id=owner_id),
                        permission=Permission.FULL_CONTROL,
                    ),
                    Grant(
                        grantee=Grantee(type=GranteeType.GROUP, uri=WellKnownGroups.ALL_USERS),
                        permission=Permission.READ,
                    ),
                    Grant(
                        grantee=Grantee(type=GranteeType.GROUP, uri=WellKnownGroups.ALL_USERS),
                        permission=Permission.WRITE,
                    ),
                ],
            )
        if canned_acl == "authenticated-read":
            return ACL(
                owner=Owner(id=owner_id),
                grants=[
                    Grant(
                        grantee=Grantee(type=GranteeType.CANONICAL_USER, id=owner_id),
                        permission=Permission.FULL_CONTROL,
                    ),
                    Grant(
                        grantee=Grantee(type=GranteeType.GROUP, uri=WellKnownGroups.AUTHENTICATED_USERS),
                        permission=Permission.READ,
                    ),
                ],
            )
        if canned_acl == "log-delivery-write":
            return ACL(
                owner=Owner(id=owner_id),
                grants=[
                    Grant(
                        grantee=Grantee(type=GranteeType.CANONICAL_USER, id=owner_id),
                        permission=Permission.FULL_CONTROL,
                    ),
                    Grant(
                        grantee=Grantee(type=GranteeType.GROUP, uri=WellKnownGroups.LOG_DELIVERY),
                        permission=Permission.WRITE,
                    ),
                    Grant(
                        grantee=Grantee(type=GranteeType.GROUP, uri=WellKnownGroups.LOG_DELIVERY),
                        permission=Permission.READ_ACP,
                    ),
                ],
            )
        if canned_acl == "aws-exec-read":
            return ACL(
                owner=Owner(id=owner_id),
                grants=[
                    Grant(
                        grantee=Grantee(type=GranteeType.CANONICAL_USER, id=owner_id),
                        permission=Permission.FULL_CONTROL,
                    ),
                    Grant(
                        grantee=Grantee(type=GranteeType.GROUP, uri=WellKnownGroups.AWS_EC2),
                        permission=Permission.READ,
                    ),
                ],
            )
        if canned_acl == "bucket-owner-read":
            if not bucket:
                raise ValueError("bucket-owner-read requires bucket parameter")

            bucket_owner_id = await self.get_bucket_owner(bucket)

            grants = [
                Grant(
                    grantee=Grantee(type=GranteeType.CANONICAL_USER, id=owner_id),
                    permission=Permission.FULL_CONTROL,
                ),
            ]

            if bucket_owner_id and bucket_owner_id != owner_id:
                grants.append(
                    Grant(
                        grantee=Grantee(type=GranteeType.CANONICAL_USER, id=bucket_owner_id),
                        permission=Permission.READ,
                    )
                )

            return ACL(owner=Owner(id=owner_id), grants=grants)
        if canned_acl == "bucket-owner-full-control":
            if not bucket:
                raise ValueError("bucket-owner-full-control requires bucket parameter")

            bucket_owner_id = await self.get_bucket_owner(bucket)

            grants = [
                Grant(
                    grantee=Grantee(type=GranteeType.CANONICAL_USER, id=owner_id),
                    permission=Permission.FULL_CONTROL,
                ),
            ]

            if bucket_owner_id and bucket_owner_id != owner_id:
                grants.append(
                    Grant(
                        grantee=Grantee(type=GranteeType.CANONICAL_USER, id=bucket_owner_id),
                        permission=Permission.FULL_CONTROL,
                    )
                )

            return ACL(owner=Owner(id=owner_id), grants=grants)
        raise ValueError(f"Unknown canned ACL: {canned_acl}")

    def _grant_matches(self, grant: Grant, account_id: str | None) -> bool:
        """Check if grant applies to this account."""
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
        """Get object owner from objects table."""
        query = "SELECT main_account_id FROM objects WHERE bucket_name = $1 AND object_key = $2"
        row = await self.acl_repo.db.fetchrow(query, bucket, key)
        return str(row["main_account_id"]) if row else None

    async def check_permission(
        self,
        account_id: str | None,
        bucket: str,
        key: str | None,
        permission: Permission,
    ) -> bool:
        """Check if account has permission for bucket/object."""
        is_anonymous = account_id is None or account_id == "anonymous"
        if is_anonymous and key is not None and permission == Permission.READ:
            query = "SELECT is_public FROM buckets WHERE bucket_name = $1"
            row = await self.acl_repo.db.fetchrow(query, bucket)
            if row and row["is_public"]:
                return True

        acl = await self.get_effective_acl(bucket, key)

        logger.info(
            f"DEBUG_ACL: check_permission - account_id={account_id}, bucket={bucket}, key={key}, permission={permission.value}"
        )
        logger.info(f"DEBUG_ACL: ACL owner_id={acl.owner.id}")
        logger.info(f"DEBUG_ACL: ACL grants count={len(acl.grants)}")
        for i, grant in enumerate(acl.grants):
            logger.info(
                f"DEBUG_ACL: Grant {i}: type={grant.grantee.type.value}, id={grant.grantee.id}, uri={grant.grantee.uri}, permission={grant.permission.value}"
            )

        if account_id and acl.owner.id == account_id:
            logger.info("DEBUG_ACL: Access GRANTED (owner match): account_id matches owner_id")
            return True

        for grant in acl.grants:
            if self._grant_matches(grant, account_id) and self._permission_implies(grant.permission, permission):
                if permission == Permission.WRITE and key is not None:
                    bucket_owner = await self.get_bucket_owner(bucket)
                    object_owner = await self.get_object_owner(bucket, key)

                    if account_id == bucket_owner:
                        logger.info("DEBUG_ACL: Access GRANTED (grant match + bucket owner): user is bucket owner")
                        return True

                    if account_id == object_owner:
                        logger.info("DEBUG_ACL: Access GRANTED (grant match + object owner): user is object owner")
                        return True

                    logger.info("DEBUG_ACL: Access DENIED: WRITE grant but user is not bucket/object owner")
                    return False

                logger.info("DEBUG_ACL: Access GRANTED (grant match): grant matches account")
                return True

        logger.info("DEBUG_ACL: Access DENIED: no matching owner or grant")
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
        else:
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
