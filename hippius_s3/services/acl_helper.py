import asyncpg

from hippius_s3.models.acl import ACL
from hippius_s3.models.acl import Grant
from hippius_s3.models.acl import Grantee
from hippius_s3.models.acl import GranteeType
from hippius_s3.models.acl import Owner
from hippius_s3.models.acl import Permission
from hippius_s3.models.acl import WellKnownGroups


async def get_bucket_owner(db: asyncpg.Pool, bucket: str) -> str | None:
    query = "SELECT main_account_id FROM buckets WHERE bucket_name = $1"
    row = await db.fetchrow(query, bucket)
    return str(row["main_account_id"]) if row else None


async def canned_acl_to_acl(
    canned_acl: str, owner_id: str, db: asyncpg.Pool | None = None, bucket: str | None = None
) -> ACL:
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
        if not db:
            raise ValueError("bucket-owner-read requires database connection")

        bucket_owner_id = await get_bucket_owner(db, bucket)

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
        if not db:
            raise ValueError("bucket-owner-full-control requires database connection")

        bucket_owner_id = await get_bucket_owner(db, bucket)

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
