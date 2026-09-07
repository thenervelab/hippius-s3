from typing import Collection
from typing import Iterable

from hippius_s3.models.acl import Grant
from hippius_s3.models.acl import GranteeType
from hippius_s3.models.acl import Permission


# Permissions that let a grantee put bytes in a bucket, or hand themselves the right to.
# FULL_CONTROL implies WRITE, and WRITE_ACP is a write grant one step removed — whoever holds it
# can grant themselves WRITE. All three have to be treated the same or the ban has a hole in it.
WRITE_PERMISSIONS = frozenset({Permission.WRITE, Permission.WRITE_ACP, Permission.FULL_CONTROL})


def is_service_account(address: str | None, service_account_ids: Collection[str]) -> bool:
    """True iff `address` is an internal Hippius account exempt from billing.

    Takes the allowlist as an argument rather than reading the config singleton so both
    callers — the gateway's account middleware and the uploader worker — resolve it from
    their own already-loaded config, and so the predicate stays trivially testable.

    The comparison is exact and case-sensitive: SS58 is base58, where case is significant,
    so normalising would let a near-miss address match an allowlisted one.
    """
    if not address:
        return False
    return address in service_account_ids


def forbidden_write_grants(
    owner_id: str | None,
    grants: Iterable[Grant],
    service_account_ids: Collection[str],
) -> list[Grant]:
    """Grants that would let anyone but the owner write to a service account's bucket.

    Empty for every bucket that is not owned by a service account, so this costs ordinary
    users nothing.

    The owner needs no grant to write to its own bucket — `check_permission` returns
    FULL_CONTROL on an owner match before it ever looks at the grant list — so a write grant
    on one of these buckets can only ever be widening access to someone else. That includes an
    ACCESS_KEY grantee, which cannot be resolved back to an account here: refusing it costs the
    owner nothing and is the only safe reading.

    READ and READ_ACP are untouched. Publishing our own datasets publicly is the point of
    several of these buckets; letting strangers write to them is not.
    """
    if not is_service_account(owner_id, service_account_ids):
        return []

    forbidden: list[Grant] = []
    for grant in grants:
        if grant.permission not in WRITE_PERMISSIONS:
            continue
        is_owner_grant = grant.grantee.type == GranteeType.CANONICAL_USER and grant.grantee.id == owner_id
        if is_owner_grant:
            continue
        forbidden.append(grant)
    return forbidden


def describe_grants(grants: Iterable[Grant]) -> str:
    """Render grants for an error message or log line. No account ids beyond what the caller sent."""
    return ", ".join(f"{g.permission.value} to {g.grantee.uri or g.grantee.id or g.grantee.type.value}" for g in grants)


class ServiceAccountProtected(Exception):
    """A bulk-destructive operation was aimed at an internal Hippius service account."""


def refuse_destructive_operation(
    address: str | None,
    service_account_ids: Collection[str],
    *,
    operation: str,
) -> None:
    """Raise if `operation` would mass-delete a service account's data.

    Guards the ADMIN and OPERATOR paths — account purge, nuke_user, purge_buckets, the purge
    worker — not ordinary S3 verbs. A service account deleting its own object through DeleteObject
    is normal use and unaffected; what this stops is one mistyped SS58 on a maintenance script
    taking out our own storage.

    NOTE THE ABSENT `force` PARAMETER, and do not add one. A boolean meaning "destroy the
    protected account anyway", sitting in the signature of functions that admin endpoints and
    background workers call, is one careless refactor away from being passed True by something
    that should never have it — and that failure is silent and unrecoverable. The escape hatch is
    declarative instead: remove the address from HIPPIUS_SERVICE_ACCOUNT_IDS and redeploy, which
    is a reviewed, recorded, attributable change, then run the operation. Same shape as the
    `terminated_at` carve-out for Object Lock COMPLIANCE.
    """
    if is_service_account(address, service_account_ids):
        raise ServiceAccountProtected(
            f"{operation} refused: {address} is a Hippius service account. "
            f"Remove it from HIPPIUS_SERVICE_ACCOUNT_IDS and redeploy before running this."
        )
