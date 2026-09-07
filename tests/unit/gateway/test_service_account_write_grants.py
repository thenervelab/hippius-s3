"""Nobody but the owner writes to a service-account bucket.

Two layers, and the order matters. The write-time refusal in the ?acl / canned-ACL endpoints is
UX: it fails loudly instead of storing a grant that does nothing. The evaluation-time refusal in
ACLService.check_permission is the control: it makes the ban retroactive over grants that already
exist and total over any path that reaches the acl tables another way.
"""

from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from hippius_s3.gateway.services import acl_service as acl_service_mod
from hippius_s3.gateway.services.acl_service import ACLService
from hippius_s3.models.acl import ACL
from hippius_s3.models.acl import Grant
from hippius_s3.models.acl import Grantee
from hippius_s3.models.acl import GranteeType
from hippius_s3.models.acl import Owner
from hippius_s3.models.acl import Permission
from hippius_s3.models.acl import WellKnownGroups
from hippius_s3.services.service_accounts import forbidden_write_grants


SERVICE_ACCOUNT = "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY"
REGULAR_ACCOUNT = "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"
ALLOWLIST = frozenset({SERVICE_ACCOUNT})


def _grant(permission: Permission, *, group: str | None = None, account: str | None = None) -> Grant:
    if group is not None:
        return Grant(grantee=Grantee(type=GranteeType.GROUP, uri=group), permission=permission)
    return Grant(grantee=Grantee(type=GranteeType.CANONICAL_USER, id=account), permission=permission)


# ---------------------------------------------------------------------------
# The predicate
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("permission", [Permission.WRITE, Permission.WRITE_ACP, Permission.FULL_CONTROL])
@pytest.mark.parametrize(
    "grantee",
    [
        {"group": WellKnownGroups.ALL_USERS},
        {"group": WellKnownGroups.AUTHENTICATED_USERS},
        {"account": REGULAR_ACCOUNT},
    ],
)
def test_every_write_shaped_grant_to_a_stranger_is_forbidden(permission: Permission, grantee: dict) -> None:
    """FULL_CONTROL implies WRITE and WRITE_ACP lets the holder grant themselves WRITE, so all
    three have to be caught or the ban has a hole."""
    grants = [_grant(permission, **grantee)]
    assert forbidden_write_grants(SERVICE_ACCOUNT, grants, ALLOWLIST) == grants


@pytest.mark.parametrize("permission", [Permission.READ, Permission.READ_ACP])
def test_read_grants_are_untouched(permission: Permission) -> None:
    """Publishing our own datasets publicly is the point of several of these buckets."""
    grants = [_grant(permission, group=WellKnownGroups.ALL_USERS)]
    assert forbidden_write_grants(SERVICE_ACCOUNT, grants, ALLOWLIST) == []


def test_the_owners_own_full_control_grant_is_not_forbidden() -> None:
    """Every canned ACL includes it, `private` included. Rejecting it would make a service
    account unable to set any ACL at all."""
    grants = [_grant(Permission.FULL_CONTROL, account=SERVICE_ACCOUNT)]
    assert forbidden_write_grants(SERVICE_ACCOUNT, grants, ALLOWLIST) == []


def test_a_regular_users_bucket_is_unaffected() -> None:
    """This costs ordinary users nothing — public-read-write stays legal on their buckets."""
    grants = [
        _grant(Permission.WRITE, group=WellKnownGroups.ALL_USERS),
        _grant(Permission.FULL_CONTROL, account=SERVICE_ACCOUNT),
    ]
    assert forbidden_write_grants(REGULAR_ACCOUNT, grants, ALLOWLIST) == []


def test_empty_allowlist_forbids_nothing() -> None:
    grants = [_grant(Permission.WRITE, group=WellKnownGroups.ALL_USERS)]
    assert forbidden_write_grants(SERVICE_ACCOUNT, grants, frozenset()) == []


def test_access_key_grantee_is_refused() -> None:
    """An access key cannot be resolved back to an account here, so it cannot be shown to belong
    to the owner. The owner needs no grant at all (owner match precedes the grant list), so
    refusing costs them nothing and is the only safe reading."""
    grants = [
        Grant(
            grantee=Grantee(type=GranteeType.ACCESS_KEY, id="hip_somekey"),
            permission=Permission.WRITE,
        )
    ]
    assert forbidden_write_grants(SERVICE_ACCOUNT, grants, ALLOWLIST) == grants


def test_only_the_offending_grants_are_returned() -> None:
    """public-read-write on one of our buckets should report the WRITE, not the READ."""
    read = _grant(Permission.READ, group=WellKnownGroups.ALL_USERS)
    write = _grant(Permission.WRITE, group=WellKnownGroups.ALL_USERS)
    owner = _grant(Permission.FULL_CONTROL, account=SERVICE_ACCOUNT)
    assert forbidden_write_grants(SERVICE_ACCOUNT, [owner, read, write], ALLOWLIST) == [write]


# ---------------------------------------------------------------------------
# Evaluation time — the layer that eliminates the vector
# ---------------------------------------------------------------------------


def _service(monkeypatch: Any, acl: ACL, allowlist: frozenset[str] = ALLOWLIST) -> ACLService:
    monkeypatch.setattr(acl_service_mod, "get_config", lambda: MagicMock(service_account_ids=allowlist))
    svc = ACLService.__new__(ACLService)
    svc.get_effective_acl = AsyncMock(return_value=acl)  # ty: ignore[invalid-assignment]
    return svc


def _acl(owner: str, *grants: Grant) -> ACL:
    return ACL(owner=Owner(id=owner), grants=list(grants))


@pytest.mark.asyncio
@pytest.mark.parametrize("permission", [Permission.WRITE, Permission.WRITE_ACP])
async def test_a_stored_write_grant_on_a_service_account_bucket_is_never_honoured(
    monkeypatch: Any, permission: Permission
) -> None:
    """THE retroactive case. The grant row exists — written before this rule shipped, restored
    from a backup, or inserted directly. It must simply not work."""
    svc = _service(monkeypatch, _acl(SERVICE_ACCOUNT, _grant(permission, group=WellKnownGroups.ALL_USERS)))

    assert await svc.check_permission(REGULAR_ACCOUNT, "our-bucket", None, permission) is False


@pytest.mark.asyncio
async def test_an_anonymous_all_users_write_is_refused(monkeypatch: Any) -> None:
    """A public-read-write bucket of ours, hit with no credentials at all. Anonymous writes never
    reach the gateway's can_upload gate, so this evaluation is the only thing standing there."""
    svc = _service(monkeypatch, _acl(SERVICE_ACCOUNT, _grant(Permission.WRITE, group=WellKnownGroups.ALL_USERS)))

    assert await svc.check_permission("anonymous", "our-bucket", None, Permission.WRITE) is False


@pytest.mark.asyncio
async def test_a_named_grant_to_a_specific_partner_is_refused(monkeypatch: Any) -> None:
    svc = _service(monkeypatch, _acl(SERVICE_ACCOUNT, _grant(Permission.FULL_CONTROL, account=REGULAR_ACCOUNT)))

    assert await svc.check_permission(REGULAR_ACCOUNT, "our-bucket", None, Permission.WRITE) is False


@pytest.mark.asyncio
async def test_the_service_account_can_still_write_to_its_own_bucket(monkeypatch: Any) -> None:
    """The owner match precedes the ban. Breaking this would stop our own ingest dead."""
    svc = _service(monkeypatch, _acl(SERVICE_ACCOUNT))

    assert await svc.check_permission(SERVICE_ACCOUNT, "our-bucket", None, Permission.WRITE) is True


@pytest.mark.asyncio
@pytest.mark.parametrize("permission", [Permission.READ, Permission.READ_ACP])
async def test_public_reads_of_our_buckets_keep_working(monkeypatch: Any, permission: Permission) -> None:
    """The ban is scoped to write permissions; a public dataset must stay readable, anonymously."""
    svc = _service(monkeypatch, _acl(SERVICE_ACCOUNT, _grant(permission, group=WellKnownGroups.ALL_USERS)))

    assert await svc.check_permission("anonymous", "our-bucket", None, permission) is True


@pytest.mark.asyncio
async def test_a_regular_users_public_write_bucket_still_works(monkeypatch: Any) -> None:
    """Blast-radius guard: this rule must not change behaviour for anyone else's bucket."""
    svc = _service(monkeypatch, _acl(REGULAR_ACCOUNT, _grant(Permission.WRITE, group=WellKnownGroups.ALL_USERS)))

    assert await svc.check_permission(SERVICE_ACCOUNT, "their-bucket", None, Permission.WRITE) is True


@pytest.mark.asyncio
async def test_with_an_empty_allowlist_nothing_changes(monkeypatch: Any) -> None:
    """An unconfigured deployment must behave exactly as it did before this feature."""
    svc = _service(
        monkeypatch,
        _acl(SERVICE_ACCOUNT, _grant(Permission.WRITE, group=WellKnownGroups.ALL_USERS)),
        allowlist=frozenset(),
    )

    assert await svc.check_permission(REGULAR_ACCOUNT, "a-bucket", None, Permission.WRITE) is True
