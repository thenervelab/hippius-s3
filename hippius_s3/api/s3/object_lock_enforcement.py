"""Object Lock Tier 2 enforcement: the single definition of "is this version locked".

WIRED TODAY: the unpin resolution query and the janitor's hard-delete ring, both in SQL, plus the
ops scripts that issue raw DELETEs. That set is what makes the durability promise real, because it
holds with no API code running at all.

NOT WIRED YET: the delete endpoints (`DELETE ?versionId` must answer 403; a versionId-less DELETE
must answer 200 and write a delete marker) and `DeleteObjects`. `deletion_refusal_reason` exists
for them and is tested, but nothing calls it yet — see `specs/s3-object-lock-tier2-handoff.md` §5
rows 1-3. Do not read this module as evidence that the API refuses a locked delete; it does not.

Two rules are easy to get wrong and are worth stating up front, because both are load-bearing:

1. **Retention and legal hold are INDEPENDENT.** Either one locks the version. A version whose
   retention has expired but which still carries a legal hold is locked; so is one with an active
   retention and no hold. Modelling them as a single "locked" flag loses this.

2. **A simple DELETE is not refused.** AWS refuses a *permanent* delete (`?versionId=`) with 403,
   but answers a versionId-less DELETE with 200 and writes a delete marker — the locked version
   survives underneath, untouched. Refusing both breaks ordinary clients against a lock they never
   asked about. See `specs/s3-object-lock-tier2-handoff.md` §1.
"""

from __future__ import annotations

from datetime import datetime
from datetime import timezone
from typing import Any
from typing import Final


# The canonical SQL spelling of is_version_locked, for the workers, where the guarantee has to hold
# with no API code running. asyncpg cannot parameterise a predicate, so each .sql file embeds this
# text rather than importing it. That makes several hand-synced copies, which is exactly the shape
# that rots — so test_sql_gates_embed_the_canonical_predicate asserts every gated query still
# contains it, and the integration suite cross-checks the SQL verdict against the Python one.
LOCKED_VERSION_SQL_PREDICATE: Final[str] = (
    "({alias}.object_lock_legal_hold "
    "OR ({alias}.object_lock_retain_until IS NOT NULL AND {alias}.object_lock_retain_until > now()))"
)

GOVERNANCE: Final[str] = "GOVERNANCE"
COMPLIANCE: Final[str] = "COMPLIANCE"

BYPASS_HEADER: Final[str] = "x-amz-bypass-governance-retention"


def is_version_locked(row: Any, *, now: datetime | None = None) -> bool:
    """True when this object version may not be permanently deleted or have its lock weakened.

    `row` is any mapping carrying `object_lock_legal_hold` and `object_lock_retain_until` — an
    asyncpg Record or a dict. A missing key is treated as unlocked so that call sites which
    select a narrower column list cannot silently start reporting everything as locked.
    """
    if _get(row, "object_lock_legal_hold"):
        return True
    retain_until = _get(row, "object_lock_retain_until")
    if retain_until is None:
        return False
    return retain_until > (now or datetime.now(timezone.utc))


def is_bypass_requested(headers: Any) -> bool:
    """Whether the caller asked to bypass GOVERNANCE retention.

    AWS requires the header AND the permission; this only reports the header. `truthy` is
    deliberately strict — AWS documents the value as `true`, and treating any present value as
    consent would let `x-amz-bypass-governance-retention: false` delete a locked object.
    """
    raw = headers.get(BYPASS_HEADER) if headers is not None else None
    return str(raw).strip().lower() == "true"


def may_bypass_governance(*, is_bucket_owner: bool, headers: Any) -> bool:
    """Both halves of an AWS governance bypass: the explicit header and the permission.

    The permission half is BUCKET OWNER ONLY, which is a deliberate deviation from AWS worth
    understanding rather than copying blindly. AWS gates this on `s3:BypassGovernanceRetention`,
    an IAM action; this codebase has no IAM, and the nearest ACL vocabulary (`WRITE_ACP`) is
    grantable to another account. Accepting `WRITE_ACP` would mean an owner who delegates ACL
    administration has also delegated "may destroy retained data", which is precisely the
    authority Object Lock exists to withhold. Owner-only is the conservative reading; it can be
    widened later without breaking anyone, whereas the reverse cannot.
    """
    return is_bucket_owner and is_bypass_requested(headers)


def deletion_refusal_reason(row: Any, *, is_bucket_owner: bool, headers: Any) -> str | None:
    """Why this version may not be permanently deleted, or None when it may.

    The returned string is for the log and the error message, not for the client to parse — S3
    answers a bare AccessDenied and does not disclose which protection applied.
    """
    if not is_version_locked(row):
        return None
    if _get(row, "object_lock_legal_hold"):
        return "object is under a legal hold"
    mode = _get(row, "object_lock_mode")
    if mode == COMPLIANCE:
        return "object is retained in COMPLIANCE mode until its retain-until date"
    if mode == GOVERNANCE:
        if may_bypass_governance(is_bucket_owner=is_bucket_owner, headers=headers):
            return None
        return "object is retained in GOVERNANCE mode; a bucket-owner bypass is required"
    # Retention with no recognised mode: the CHECK constraint makes this unreachable from the API,
    # but a direct DB write could still produce it. Refuse rather than fall through to "deletable"
    # — an unrecognised lock is the one case where guessing wrong destroys data.
    return "object carries an unrecognised retention mode"


def _get(row: Any, key: str) -> Any:
    try:
        return row[key]
    except (KeyError, IndexError, TypeError):
        return None


def request_is_bucket_owner(request: Any) -> bool:
    """Whether the CALLER owns the bucket, which is the permission half of a governance bypass.

    Compares the caller's own identity against the bucket owner the ACL middleware resolved. Do NOT
    substitute `request.state.main_account_id` for the left-hand side: that is the
    storage-attribution account, which falls back to the BUCKET OWNER, so comparing it against the
    owner is a tautology that hands every delegated caller a bypass. The same confusion between
    those two fields was a finding on the Tier 1 PR.

    Absent state (an internal caller, a test double) reads as "not the owner", so the bypass fails
    closed.
    """
    account = getattr(request.state, "account", None)
    caller = getattr(account, "main_account", None) if account is not None else None
    owner = getattr(request.state, "bucket_owner_id", None)
    return bool(caller) and bool(owner) and str(caller) == str(owner)
