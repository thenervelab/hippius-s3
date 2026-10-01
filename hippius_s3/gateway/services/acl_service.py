import json
import logging
from typing import Any

import asyncpg
import redis.asyncio as redis
from pydantic import BaseModel
from redis.exceptions import RedisError

from hippius_s3.config import get_config
from hippius_s3.gateway.repositories.cached_acl_repository import CachedACLRepository
from hippius_s3.gateway.utils.accounts import is_sentinel_account_id
from hippius_s3.models.acl import ACL
from hippius_s3.models.acl import Grant
from hippius_s3.models.acl import GranteeType
from hippius_s3.models.acl import Permission
from hippius_s3.models.acl import WellKnownGroups
from hippius_s3.repositories.acl_repository import ACLRepository
from hippius_s3.services.public_prefix_policy import key_matches_public_prefix
from hippius_s3.services.public_prefix_policy import stored_object_key
from hippius_s3.services.service_accounts import WRITE_PERMISSIONS
from hippius_s3.services.service_accounts import is_service_account
from hippius_s3.utils import get_query


logger = logging.getLogger(__name__)

# Negative-cache TTL for "this bucket has no public prefixes". Allow lists are not
# cached: a stale allow list is an anonymous read of a prefix the owner just revoked.
# A stale empty entry is only an under-grant, and it expires.
_PREFIX_NEGATIVE_TTL_SECONDS = 60
_PREFIX_EMPTY = "[]"


class BucketLookup(BaseModel):
    """Result of resolving a bucket name to its owner / id / cache state in one query."""

    owner_id: str
    bucket_id: str
    is_cache_warm: bool
    # Tier 1's bucket-level Object Lock config, carried here so the PUT path can apply a default
    # retention without a second bucket read. This lookup is already Redis-cached per bucket name,
    # so a default-retention bucket costs the write path nothing extra. None = Object Lock is not
    # configured on this bucket, which is the overwhelmingly common case.
    object_lock: dict[str, Any] | None = None


def _coerce_object_lock(raw: Any) -> dict[str, Any] | None:
    """asyncpg returns JSONB as a str unless a codec is registered; accept either shape."""
    if raw is None:
        return None
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            value = json.loads(raw)
        except json.JSONDecodeError:
            return None
        return value if isinstance(value, dict) else None
    return None


class ACLService:
    acl_repo: ACLRepository | CachedACLRepository

    def __init__(self, db_pool: asyncpg.Pool, redis_client: redis.Redis | None = None, cache_ttl: int = 600):
        base_repo = ACLRepository(db_pool)
        self._redis = redis_client
        self._cache_ttl = cache_ttl
        if redis_client:
            self.acl_repo = CachedACLRepository(base_repo, redis_client, cache_ttl)
            logger.info(f"ACLService initialized with Redis caching (TTL={cache_ttl}s)")
        else:
            self.acl_repo = base_repo
            logger.info("ACLService initialized (direct DB queries, no caching)")

    @staticmethod
    def _bucket_meta_key(bucket: str) -> str:
        return f"hippius_acl:bucketmeta:{bucket}"

    async def _cache_get_bucket_meta(self, bucket: str) -> BucketLookup | None:
        # Best-effort: a Redis hiccup falls back to the DB so bucket resolution never hard-depends
        # on Redis being up — master-token / anonymous requests bypass check_permission and must
        # keep working during a redis-acl outage.
        if self._redis is None:
            return None
        try:
            cached = await self._redis.get(self._bucket_meta_key(bucket))
        except RedisError as exc:
            logger.warning(f"bucket-meta cache: redis GET failed, falling through to DB: {exc}")
            cached = None
        if cached:
            return BucketLookup.model_validate_json(cached)
        return None

    async def _cache_set_bucket_meta(self, bucket: str, lookup: BucketLookup) -> None:
        if self._redis is None:
            return
        try:
            await self._redis.setex(self._bucket_meta_key(bucket), self._cache_ttl, lookup.model_dump_json())
        except RedisError as exc:
            logger.warning(f"bucket-meta cache: redis SETEX failed (best-effort, continuing): {exc}")

    async def invalidate_bucket_meta(self, bucket: str) -> None:
        """Drop the cached owner/id for a bucket NAME. Required on DeleteBucket: the name is
        reusable by any account the moment the row is soft-deleted, so the entry outlives the
        bucket it describes."""
        if self._redis is None:
            return
        # Best-effort, mirroring CachedACLRepository.invalidate_bucket_acl: the soft-delete has
        # already committed upstream, so raising here would only turn a successful 204 into a 500
        # while leaving the same stale entry behind. Staleness stays bounded by the TTL.
        try:
            deleted = await self._redis.delete(self._bucket_meta_key(bucket))
        except RedisError as exc:
            logger.warning(f"bucket-meta cache: redis DELETE failed for {bucket} (best-effort, continuing): {exc}")
            return
        if deleted:
            logger.info(f"Invalidated bucket-meta cache for bucket {bucket}")

    async def canned_acl_to_acl(self, canned_acl: str, owner_id: str, bucket: str | None = None) -> ACL:
        """Convert canned ACL name to ACL object with grants."""
        from hippius_s3.services.acl_helper import canned_acl_to_acl as shared_canned_acl_to_acl

        return await shared_canned_acl_to_acl(canned_acl, owner_id, self.acl_repo.db, bucket)

    def _grant_matches(self, grant: Grant, account_id: str | None, access_key: str | None = None) -> bool:
        """Check if grant applies to this account or access key."""
        if grant.grantee.type == GranteeType.ACCESS_KEY:
            return grant.grantee.id == access_key
        if grant.grantee.type == GranteeType.CANONICAL_USER:
            # Same sentinel trap as the owner match in check_permission: a grant row naming
            # "anonymous" as a canonical user would be satisfied by every anonymous caller.
            if is_sentinel_account_id(account_id):
                return False
            return grant.grantee.id == account_id
        if grant.grantee.type == GranteeType.GROUP:
            if grant.grantee.uri == WellKnownGroups.ALL_USERS:
                return True
            if grant.grantee.uri == WellKnownGroups.AUTHENTICATED_USERS:
                return not is_sentinel_account_id(account_id)
        return False

    def _permission_implies(self, granted: Permission, required: Permission) -> bool:
        """Check if granted permission satisfies required permission."""
        if granted == Permission.FULL_CONTROL:
            return True
        return granted == required

    async def get_bucket_owner(self, bucket: str) -> str | None:
        """Get bucket owner from buckets table."""
        query = "SELECT main_account_id FROM buckets WHERE bucket_name = $1 AND deleted_at IS NULL"
        row = await self.acl_repo.db.fetchrow(query, bucket)
        return str(row["main_account_id"]) if row else None

    async def get_bucket_id(self, bucket: str) -> str | None:
        """Get bucket UUID from buckets table."""
        query = "SELECT bucket_id FROM buckets WHERE bucket_name = $1 AND deleted_at IS NULL"
        row = await self.acl_repo.db.fetchrow(query, bucket)
        return str(row["bucket_id"]) if row else None

    async def get_bucket_owner_and_id(self, bucket: str) -> BucketLookup | None:
        """Fetch owner_id / bucket_id / is_cache_warm in a single query.

        Preferred over calling get_bucket_owner() + get_bucket_id() separately
        on hot paths (e.g. acl_middleware). Returns None when the bucket does
        not exist.

        Redis-cached (hits only) to spare every request a buckets-table query. Non-existent
        buckets are never cached, so a freshly-created bucket is visible immediately. The cache
        is a pure optimization — a Redis outage transparently falls back to the DB.

        The key is a bucket NAME, not a bucket. Owner/id are immutable for one bucket's
        *lifetime*, but a name outlives it: uniqueness is enforced by the partial index
        `buckets_bucket_name_active_key` over `deleted_at IS NULL` rows, so any account can claim
        the name the instant the previous bucket is soft-deleted. A cached entry that survives
        that hands the previous owner the master-token bypass and the "private" canned-ACL owner
        match on someone else's bucket. Anything that creates or removes a bucket MUST call
        invalidate_bucket_meta — cache_invalidation_middleware does this for both. is_cache_warm
        can flip independently and lags by at most the TTL.
        """
        cached = await self._cache_get_bucket_meta(bucket)
        if cached is not None:
            return cached

        query = (
            "SELECT main_account_id, bucket_id, is_cache_warm, object_lock FROM buckets "
            "WHERE bucket_name = $1 AND deleted_at IS NULL"
        )
        row = await self.acl_repo.db.fetchrow(query, bucket)
        if row is None:
            return None
        lookup = BucketLookup(
            owner_id=str(row["main_account_id"]),
            bucket_id=str(row["bucket_id"]),
            is_cache_warm=bool(row["is_cache_warm"]),
            # Tolerant read: a caller selecting a narrower column list must not break the ACL
            # path, which every request goes through.
            object_lock=_coerce_object_lock(row.get("object_lock")),
        )

        await self._cache_set_bucket_meta(bucket, lookup)
        return lookup

    async def get_object_owner(self, bucket: str, key: str) -> str | None:
        """Get object owner (inherits from bucket owner)."""
        query = """
            SELECT b.main_account_id
            FROM objects o
            JOIN buckets b ON o.bucket_id = b.bucket_id
            WHERE b.bucket_name = $1 AND o.object_key = $2 AND b.deleted_at IS NULL
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
        bucket_owner_id: str | None = None,
        allow_public_prefix: bool = True,
        bucket_id: str | None = None,
    ) -> bool:
        """Check if account or access key has permission for bucket/object."""

        acl, object_acl = await self._load_effective_acl(bucket, key, bucket_owner_id)

        grants_summary = [
            f"{{type={g.grantee.type.value}, id={g.grantee.id or 'None'}, uri={g.grantee.uri or 'None'}, perm={g.permission.value}}}"
            for g in acl.grants
        ]

        # The owner match is the only check that grants FULL_CONTROL without a stored grant, and
        # `acl.owner.id` comes straight from `buckets.main_account_id`. Unauthenticated callers
        # carry the same sentinel id, so a legacy ownerless row (the routes that produced them
        # were closed in 3880fbec) would match here and grant on a bucket with no public flag and
        # no ACL row anywhere. Exclude the sentinel on both sides: nobody is the owner of a bucket
        # nobody owns.
        if account_id and not is_sentinel_account_id(account_id) and acl.owner.id == account_id:
            logger.info(
                f"ACL check: account={account_id}, access_key={access_key or 'None'}, bucket={bucket}, "
                f"key={key or 'None'}, required_perm={permission.value}, owner={acl.owner.id}, "
                f"grants={len(acl.grants)}{grants_summary}, result=GRANTED (owner match)"
            )
            return True

        # Nobody but the owner writes to a service account's bucket — enforced HERE, at
        # evaluation, not only where ACLs are written. A write-time check alone would leave every
        # grant that already exists live, and would miss any path that reaches the acl tables
        # another way (a direct DB write, a future endpoint, a restored backup). Refusing at the
        # point of use makes the ban retroactive and total: the grant may sit in the table, it is
        # simply never honoured.
        #
        # Placed after the owner match so the service account itself is unaffected, and scoped to
        # write permissions so public READ of our own datasets keeps working.
        # `bucket_owner_id` in preference to `acl.owner.id`: for an OBJECT acl the latter is
        # whoever stored the row (the writer), not who owns the bucket, and what is being
        # protected here is the bucket. Falls back when the caller did not resolve an owner.
        protected_owner = bucket_owner_id or acl.owner.id
        if permission in WRITE_PERMISSIONS and is_service_account(protected_owner, get_config().service_account_ids):
            logger.warning(
                f"ACL check: account={account_id}, access_key={access_key or 'None'}, bucket={bucket}, "
                f"key={key or 'None'}, required_perm={permission.value}, owner={protected_owner}, "
                f"grants={len(acl.grants)}{grants_summary}, "
                f"result=DENIED (write to a service-account bucket is never granted)"
            )
            return False

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

        # A public prefix grants READ of the current object only, and only when no
        # object ACL row exists. An object ACL replaces the bucket ACL; appending the
        # prefix onto that object (or onto the cached ACL object) would publish a key
        # the owner sealed, and would poison the ACL cache for every later request.
        # The lookup uses the stored key, not the raw copy-source string: `../` in
        # `x-amz-copy-source` must not hide the private ACL of the object it names.
        # `bucket_id` is the bucket being authorized. CopyObject must pass the source
        # bucket's id — the destination's prefixes must not publish the source.
        #
        # The prefix list comes first. Object-ACL misses are not cached, and a bucket
        # with no matching prefix would otherwise pay that query on every denied read.
        # When this key is already the stored key, the effective-ACL read above is the
        # seal lookup and is not repeated.
        stored_key = stored_object_key(key) if key is not None else ""
        if (
            allow_public_prefix
            and stored_key
            and stored_key != "/"
            and permission == Permission.READ
            and await self._prefix_read_allows(bucket, stored_key, bucket_id)
        ):
            if stored_key == key:
                sealed = object_acl is not None
            else:
                sealed = await self.acl_repo.get_object_acl(bucket, stored_key) is not None
            if not sealed:
                logger.info(
                    f"ACL check: account={account_id}, access_key={access_key or 'None'}, bucket={bucket}, "
                    f"key={key or 'None'}, required_perm={permission.value}, owner={acl.owner.id}, "
                    f"grants={len(acl.grants)}{grants_summary}, result=GRANTED (public prefix)"
                )
                return True

        logger.info(
            f"ACL check: account={account_id}, access_key={access_key or 'None'}, bucket={bucket}, "
            f"key={key or 'None'}, required_perm={permission.value}, owner={acl.owner.id}, "
            f"grants={len(acl.grants)}{grants_summary}, result=DENIED"
        )
        return False

    async def _prefix_read_allows(self, bucket: str, key: str, bucket_id: str | None) -> bool:
        resolved = bucket_id
        if resolved is None:
            lookup = await self.get_bucket_owner_and_id(bucket)
            if lookup is None:
                return False
            resolved = lookup.bucket_id
        prefixes = await self.list_public_prefixes(resolved)
        return key_matches_public_prefix(key, prefixes)

    def _prefix_cache_key(self, bucket_id: str) -> str:
        # Keyed by bucket id, not name. A name is reusable the moment the previous
        # bucket is soft-deleted; a name-keyed allow list would publish the next
        # tenant's objects under the previous tenant's prefixes.
        return f"hippius_acl:prefixes:{bucket_id}"

    async def list_public_prefixes(self, bucket_id: str) -> list[str]:
        bucket_id = str(bucket_id)
        if await self._prefix_negative_cached(bucket_id):
            return []
        rows = await self.acl_repo.db.fetch(get_query("list_bucket_public_prefixes"), bucket_id)
        prefixes = [str(row["prefix"]) for row in rows]
        if not prefixes:
            # SETEX runs after the read. A PutBucketPolicy that committed and deleted
            # this key in between can be overwritten by this empty sentinel. That is
            # an under-grant until the TTL, which is the direction this cache allows.
            await self._cache_empty_prefixes(bucket_id)
        return prefixes

    async def _prefix_negative_cached(self, bucket_id: str) -> bool:
        if self._redis is None:
            return False
        try:
            cached = await self._redis.get(self._prefix_cache_key(bucket_id))
        except RedisError as exc:
            logger.warning(f"prefix cache: redis GET failed, falling through to DB: {exc}")
            return False
        if isinstance(cached, bytes):
            cached = cached.decode("utf-8", "replace")
        # Anything other than the empty sentinel — including a list a future bug
        # might store — is not an allow. Fall through and read the table.
        return cached == _PREFIX_EMPTY

    async def _cache_empty_prefixes(self, bucket_id: str) -> None:
        if self._redis is None:
            return
        try:
            await self._redis.setex(self._prefix_cache_key(bucket_id), _PREFIX_NEGATIVE_TTL_SECONDS, _PREFIX_EMPTY)
        except RedisError as exc:
            logger.warning(f"prefix cache: redis SETEX failed (best-effort, continuing): {exc}")

    async def invalidate_public_prefixes(self, bucket_id: str) -> None:
        if self._redis is None:
            return
        try:
            await self._redis.delete(self._prefix_cache_key(str(bucket_id)))
        except RedisError as exc:
            logger.warning(f"prefix cache: redis DELETE failed for {bucket_id} (best-effort, continuing): {exc}")

    async def invalidate_public_prefixes_by_name(self, bucket: str) -> None:
        lookup = await self.get_bucket_owner_and_id(bucket)
        if lookup is None:
            return
        await self.invalidate_public_prefixes(lookup.bucket_id)

    async def _load_effective_acl(
        self, bucket: str, key: str | None, bucket_owner_id: str | None = None
    ) -> tuple[ACL, ACL | None]:
        """Effective ACL, and the object ACL row when this key has one.

        The object ACL is None when the key was looked up and has no row. Callers
        that authorize a different stored key still have to read that key.
        """
        if key:
            object_acl = await self.acl_repo.get_object_acl(bucket, key)
            if object_acl:
                return object_acl, object_acl

        acl = await self.acl_repo.get_bucket_acl(bucket)
        if acl:
            return acl, None

        if bucket_owner_id is None:
            owner_id = await self.get_bucket_owner(bucket)
            if not owner_id:
                raise ValueError(f"Bucket not found: {bucket}")
        else:
            owner_id = bucket_owner_id

        return await self.canned_acl_to_acl("private", owner_id, bucket), None

    async def get_effective_acl(self, bucket: str, key: str | None, bucket_owner_id: str | None = None) -> ACL:
        """Get effective ACL with inheritance (direct DB queries)."""
        acl, _object_acl = await self._load_effective_acl(bucket, key, bucket_owner_id)
        return acl

    async def invalidate_cache(self, bucket: str, key: str | None = None) -> None:
        """Invalidate ACL cache for bucket or object."""
        if isinstance(self.acl_repo, CachedACLRepository):
            if key:
                await self.acl_repo.invalidate_object_acl(bucket, key)
            else:
                await self.acl_repo.invalidate_bucket_acl(bucket)
