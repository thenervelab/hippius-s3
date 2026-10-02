"""Prefix READ inside ACLService.check_permission.

The grant has to use the bucket id the caller resolved. CopyObject authorizes the
source bucket; a helper that loaded prefixes for the path bucket would publish the
source under the destination's policy.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest

from hippius_s3.gateway.services import acl_service as acl_service_mod
from hippius_s3.gateway.services.acl_service import ACLService
from hippius_s3.gateway.services.acl_service import BucketLookup
from hippius_s3.models.acl import ACL
from hippius_s3.models.acl import Grant
from hippius_s3.models.acl import Grantee
from hippius_s3.models.acl import GranteeType
from hippius_s3.models.acl import Owner
from hippius_s3.models.acl import Permission
from hippius_s3.models.acl import WellKnownGroups


OWNER = "owner-acct"
OTHER = "other-acct"
SERVICE = "service-acct"


def _acl(owner: str, *grants: Grant) -> ACL:
    return ACL(owner=Owner(id=owner), grants=list(grants))


def _owner_grant(owner: str = OWNER) -> Grant:
    return Grant(grantee=Grantee(type=GranteeType.CANONICAL_USER, id=owner), permission=Permission.FULL_CONTROL)


def _all_users(permission: Permission) -> Grant:
    return Grant(grantee=Grantee(type=GranteeType.GROUP, uri=WellKnownGroups.ALL_USERS), permission=permission)


def _service(
    acl: ACL,
    *,
    object_acl: ACL | None = None,
    rows_for: dict[str, list[str]] | None = None,
    redis: Any = None,
) -> tuple[ACLService, list[str]]:
    fetched: list[str] = []

    async def fetch(_query: str, bucket_id: str) -> list[dict[str, str]]:
        fetched.append(str(bucket_id))
        prefixes = (rows_for or {}).get(str(bucket_id), [])
        return [{"prefix": prefix} for prefix in prefixes]

    svc = ACLService(AsyncMock())
    svc.acl_repo = SimpleNamespace(  # type: ignore[assignment]
        get_object_acl=AsyncMock(return_value=object_acl),
        get_bucket_acl=AsyncMock(return_value=acl),
        db=SimpleNamespace(fetch=AsyncMock(side_effect=fetch)),
    )
    svc._redis = redis
    return svc, fetched


async def _read(
    svc: ACLService, key: str, *, account: str | None = None, bucket_id: str = "bid-a", **extra: Any
) -> bool:
    return await svc.check_permission(
        account,
        "bucket-a",
        key,
        Permission.READ,
        bucket_owner_id=OWNER,
        bucket_id=bucket_id,
        **extra,
    )


@pytest.mark.asyncio
async def test_prefix_allows_only_a_strict_child_of_this_bucket() -> None:
    svc, fetched = _service(_acl(OWNER, _owner_grant()), rows_for={"bid-a": ["public"], "bid-b": ["sealed"]})

    assert await _read(svc, "public/a") is True
    assert await _read(svc, "public/") is True
    assert await _read(svc, "public/secret/x") is True
    assert await _read(svc, "public") is False
    assert await _read(svc, "publicity") is False
    assert await _read(svc, "sealed/x") is False
    assert await _read(svc, "public/../sealed/x") is False
    assert set(fetched) == {"bid-a"}


@pytest.mark.asyncio
async def test_prefixes_are_loaded_for_the_bucket_id_argument() -> None:
    svc, fetched = _service(_acl(OWNER, _owner_grant()), rows_for={"bid-a": ["public"]})

    assert await _read(svc, "public/a", bucket_id="bid-b") is False
    assert fetched == ["bid-b"]
    assert await _read(svc, "public/a", bucket_id="bid-a") is True


@pytest.mark.asyncio
async def test_a_cross_account_caller_can_read_the_prefix_and_nothing_else() -> None:
    svc, _fetched = _service(_acl(OWNER, _owner_grant()), rows_for={"bid-a": ["public"]})

    assert await _read(svc, "public/a", account=OTHER) is True
    assert await _read(svc, "sealed/x", account=OTHER) is False


@pytest.mark.asyncio
async def test_a_dot_segment_alias_still_sees_the_private_acl_on_the_stored_key() -> None:
    """`foo/../sealed/x` must not skip the object ACL that `sealed/x` carries.

    The prefix matcher collapses dot segments. Looking the object ACL up on the
    raw copy-source string returns no row, and the collapsed key then matches
    the prefix — publishing an object the owner sealed.
    """
    private = _acl(OWNER, _owner_grant())
    svc, fetched = _service(_acl(OWNER, _owner_grant()), rows_for={"bid-a": ["sealed"]})
    looked_up: list[str] = []

    async def get_object_acl(_bucket: str, key: str) -> ACL | None:
        looked_up.append(key)
        if key == "sealed/x":
            return private
        return None

    svc.acl_repo.get_object_acl = get_object_acl  # type: ignore[method-assign]

    assert await _read(svc, "foo/../sealed/x") is False
    assert await _read(svc, "sealed/./x") is False
    assert await _read(svc, "sealed/x") is False
    assert fetched == ["bid-a", "bid-a", "bid-a"]
    assert await _read(svc, "foo/../sealed/y") is True
    assert fetched == ["bid-a", "bid-a", "bid-a", "bid-a"]
    # The alias is sealed by the collapsed key. The direct key reuses the
    # effective-ACL read, so `sealed/x` is not looked up a fourth time.
    assert looked_up.count("sealed/x") == 3
    assert looked_up.count("foo/../sealed/x") == 1


@pytest.mark.asyncio
async def test_a_leading_slash_key_is_not_published_by_the_prefix_it_does_not_name() -> None:
    """Anonymous GET `/bucket//public/secret` is object key `/public/secret`.

    The route keeps the empty segment, so this is not the public child
    `public/secret`. Authorizing the stripped name reads the other object.
    `/bucket///public/secret` is `//public/secret` and takes the same path
    once the collapse runs twice.
    """
    svc, _fetched = _service(_acl(OWNER, _owner_grant()), rows_for={"bid-a": ["public"]})
    looked_up: list[str] = []

    async def get_object_acl(_bucket: str, key: str) -> ACL | None:
        looked_up.append(key)
        return None

    svc.acl_repo.get_object_acl = get_object_acl  # type: ignore[method-assign]

    assert await _read(svc, "/public/secret") is False
    assert await _read(svc, "//public/secret") is False
    # The prefix does not match, so the seal lookup is skipped. The effective ACL
    # still sees the key the router stored. The stripped name `public/secret` is
    # the public child and must not be consulted.
    assert looked_up.count("/public/secret") == 1
    assert looked_up.count("//public/secret") == 1
    assert "public/secret" not in looked_up


@pytest.mark.asyncio
async def test_an_object_acl_row_hides_the_prefix() -> None:
    svc, fetched = _service(
        _acl(OWNER, _owner_grant()),
        object_acl=_acl(OWNER, _owner_grant()),
        rows_for={"bid-a": ["public"]},
    )

    assert await _read(svc, "public/a") is False
    assert fetched == ["bid-a"]
    assert svc.acl_repo.get_object_acl.await_count == 1


@pytest.mark.asyncio
async def test_a_private_bucket_with_no_prefixes_reads_the_object_acl_once() -> None:
    svc, fetched = _service(_acl(OWNER, _owner_grant()), rows_for={"bid-a": []})

    assert await _read(svc, "public/a") is False
    assert fetched == ["bid-a"]
    assert svc.acl_repo.get_object_acl.await_count == 1


@pytest.mark.asyncio
async def test_a_matching_prefix_reuses_the_object_acl_read() -> None:
    svc, fetched = _service(_acl(OWNER, _owner_grant()), rows_for={"bid-a": ["public"]})

    assert await _read(svc, "public/a") is True
    assert fetched == ["bid-a"]
    assert svc.acl_repo.get_object_acl.await_count == 1


@pytest.mark.asyncio
async def test_version_pin_write_and_bucket_reads_do_not_consult_prefixes() -> None:
    svc, fetched = _service(_acl(OWNER, _owner_grant()), rows_for={"bid-a": ["public"]})

    assert await _read(svc, "public/a", allow_public_prefix=False) is False
    assert await svc.check_permission(None, "bucket-a", "public/a", Permission.WRITE, bucket_id="bid-a") is False
    assert await svc.check_permission(None, "bucket-a", "public/a", Permission.READ_ACP, bucket_id="bid-a") is False
    assert await svc.check_permission(None, "bucket-a", None, Permission.READ, bucket_id="bid-a") is False
    assert fetched == []


@pytest.mark.asyncio
async def test_the_owner_is_allowed_before_any_prefix_lookup() -> None:
    svc, fetched = _service(_acl(OWNER, _owner_grant()), rows_for={})

    assert await _read(svc, "sealed/x", account=OWNER) is True
    assert fetched == []


@pytest.mark.asyncio
async def test_all_users_write_does_not_grant_read_and_a_prefix_does_not_grant_write() -> None:
    svc, _fetched = _service(_acl(OWNER, _owner_grant(), _all_users(Permission.WRITE)), rows_for={"bid-a": ["public"]})

    assert await _read(svc, "public/a") is True
    assert await _read(svc, "sealed/x") is False
    assert await svc.check_permission(None, "bucket-a", "sealed/x", Permission.WRITE, bucket_id="bid-a") is True


@pytest.mark.asyncio
async def test_service_account_prefix_read_stays_open_and_write_stays_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        acl_service_mod,
        "get_config",
        lambda: SimpleNamespace(service_account_ids=frozenset({SERVICE})),
    )
    svc, fetched = _service(
        _acl(SERVICE, _owner_grant(SERVICE)),
        rows_for={"bid-a": ["public"]},
    )

    assert await svc.check_permission(
        None, "bucket-a", "public/a", Permission.READ, bucket_owner_id=SERVICE, bucket_id="bid-a"
    )
    assert (
        await svc.check_permission(
            OTHER, "bucket-a", "public/a", Permission.WRITE, bucket_owner_id=SERVICE, bucket_id="bid-a"
        )
        is False
    )
    assert fetched == ["bid-a"]


class _Redis:
    def __init__(self) -> None:
        self.store: dict[str, Any] = {}
        self.setexes: list[tuple[str, int, str]] = []

    async def get(self, key: str) -> Any:
        return self.store.get(key)

    async def setex(self, key: str, ttl: int, value: str) -> None:
        self.setexes.append((key, ttl, value))
        self.store[key] = value

    async def delete(self, key: str) -> int:
        self.store.pop(key, None)
        return 1


@pytest.mark.asyncio
async def test_only_an_empty_prefix_list_is_cached_and_the_key_is_the_bucket_id() -> None:
    redis = _Redis()
    svc, fetched = _service(_acl(OWNER, _owner_grant()), rows_for={"bid-a": []}, redis=redis)

    assert await svc.list_public_prefixes("bid-a") == []
    assert redis.setexes == [("hippius_acl:prefixes:bid-a", 60, "[]")]
    assert await svc.list_public_prefixes("bid-a") == []
    assert fetched == ["bid-a"]

    assert await _read(svc, "public/a") is False
    assert fetched == ["bid-a"]


@pytest.mark.asyncio
async def test_a_non_empty_list_is_not_cached_so_revoke_is_the_next_read() -> None:
    redis = _Redis()
    rows = {"bid-a": ["public"]}
    svc, _fetched = _service(_acl(OWNER, _owner_grant()), rows_for=rows, redis=redis)

    assert await _read(svc, "public/a") is True
    assert redis.setexes == []

    rows["bid-a"] = []
    assert await _read(svc, "public/a") is False


@pytest.mark.asyncio
async def test_a_cached_allow_list_is_ignored() -> None:
    redis = _Redis()
    redis.store["hippius_acl:prefixes:bid-a"] = b'["sealed"]'
    svc, fetched = _service(_acl(OWNER, _owner_grant()), rows_for={"bid-a": ["public"]}, redis=redis)

    assert await _read(svc, "public/a") is True
    assert await _read(svc, "sealed/x") is False
    assert fetched == ["bid-a", "bid-a"]


@pytest.mark.asyncio
async def test_invalidate_by_name_deletes_the_id_key() -> None:
    redis = _Redis()
    redis.store["hippius_acl:prefixes:bid-a"] = "[]"
    svc, _fetched = _service(_acl(OWNER, _owner_grant()), redis=redis)
    svc.get_bucket_owner_and_id = AsyncMock(  # type: ignore[method-assign]
        return_value=BucketLookup(owner_id=OWNER, bucket_id="bid-a", is_cache_warm=False)
    )

    await svc.invalidate_public_prefixes_by_name("bucket-a")

    assert "hippius_acl:prefixes:bid-a" not in redis.store
