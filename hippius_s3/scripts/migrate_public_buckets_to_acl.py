import asyncio

import asyncpg

from hippius_s3.config import get_config
from hippius_s3.models.acl import ACL
from hippius_s3.models.acl import Grant
from hippius_s3.models.acl import Grantee
from hippius_s3.models.acl import GranteeType
from hippius_s3.models.acl import Owner
from hippius_s3.models.acl import Permission
from hippius_s3.repositories.acl_repository import ACLRepository


config = get_config()

ALL_USERS_URI = "http://acs.amazonaws.com/groups/global/AllUsers"


def create_public_read_acl(owner_id: str) -> ACL:
    owner = Owner(id=owner_id, display_name=None)
    grants = [
        Grant(
            grantee=Grantee(type=GranteeType.CANONICAL_USER, id=owner_id),
            permission=Permission.FULL_CONTROL,
        ),
        Grant(
            grantee=Grantee(type=GranteeType.GROUP, uri=ALL_USERS_URI),
            permission=Permission.READ,
        ),
    ]
    return ACL(owner=owner, grants=grants)


def create_private_acl(owner_id: str) -> ACL:
    owner = Owner(id=owner_id, display_name=None)
    grants = [
        Grant(
            grantee=Grantee(type=GranteeType.CANONICAL_USER, id=owner_id),
            permission=Permission.FULL_CONTROL,
        ),
    ]
    return ACL(owner=owner, grants=grants)


async def migrate_public_buckets() -> None:
    print(f"Connecting to database: {config.database_url}")
    db_pool = await asyncpg.create_pool(config.database_url)

    acl_repo = ACLRepository(db_pool)

    query = """
    SELECT bucket_name, owner_user_id, is_public
    FROM buckets
    ORDER BY bucket_name
    """
    buckets = await db_pool.fetch(query)

    print(f"Found {len(buckets)} buckets to process")

    public_count = 0
    private_count = 0
    skipped_count = 0

    for bucket in buckets:
        bucket_name = bucket["bucket_name"]
        owner_id = str(bucket["owner_user_id"])
        is_public = bucket["is_public"]

        existing_acl = await acl_repo.get_bucket_acl(bucket_name)
        if existing_acl:
            print(f"  Skipping {bucket_name}: ACL already exists")
            skipped_count += 1
            continue

        if is_public:
            acl = create_public_read_acl(owner_id)
            await acl_repo.set_bucket_acl(bucket_name, owner_id, acl)
            print(f"  ✓ {bucket_name}: Set public-read ACL (owner: {owner_id})")
            public_count += 1
        else:
            acl = create_private_acl(owner_id)
            await acl_repo.set_bucket_acl(bucket_name, owner_id, acl)
            print(f"  ✓ {bucket_name}: Set private ACL (owner: {owner_id})")
            private_count += 1

    await db_pool.close()

    print("\nMigration complete!")
    print(f"  Public buckets migrated: {public_count}")
    print(f"  Private buckets migrated: {private_count}")
    print(f"  Skipped (already have ACL): {skipped_count}")
    print(f"  Total: {len(buckets)}")


if __name__ == "__main__":
    asyncio.run(migrate_public_buckets())
