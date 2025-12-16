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


def has_public_read_acl(acl_json: dict | str | None) -> bool:
    if not acl_json:
        return False

    import json

    acl_data: dict = json.loads(acl_json) if isinstance(acl_json, str) else acl_json

    grants = acl_data.get("grants", [])
    if not isinstance(grants, list):
        return False

    for grant in grants:
        if not isinstance(grant, dict):
            continue

        grantee = grant.get("grantee", {})
        if not isinstance(grantee, dict):
            continue

        if (
            grantee.get("type") == "Group"
            and grantee.get("uri") == "http://acs.amazonaws.com/groups/global/AllUsers"
            and grant.get("permission") == "READ"
        ):
            return True

    return False


async def bucket_has_public_read_acl(db: asyncpg.Pool, bucket_name: str) -> bool:
    from hippius_s3.utils import get_query

    row = await db.fetchrow(get_query("get_bucket_acl_json"), bucket_name)
    if not row:
        return False

    acl_json = row.get("acl_json")
    return has_public_read_acl(acl_json)
