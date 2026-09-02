"""Adversarial tests for the Object Lock enforcement predicate.

This module is the single definition of "may this version be permanently deleted", and every
enforcement point routes through it, so a wrong answer here is silent data loss (too permissive)
or a bucket nobody can ever clean up (too strict). These tests are written to break it rather
than to demonstrate it: boundaries, missing fields, hostile header values, and the combinations
where the two independent protections disagree.

The rules under test, from the AWS Object Lock guide:
- Retention and legal hold are INDEPENDENT. Either one locks. An expired retention with a live
  hold is still locked.
- COMPLIANCE cannot be bypassed by anyone, including the account root.
- GOVERNANCE can be bypassed, but only with BOTH the permission and the explicit header.
- A legal hold has no bypass at all.
"""

from __future__ import annotations

from datetime import datetime
from datetime import timedelta
from datetime import timezone
from typing import Any

import pytest

from hippius_s3.api.s3.object_lock_enforcement import COMPLIANCE
from hippius_s3.api.s3.object_lock_enforcement import GOVERNANCE
from hippius_s3.api.s3.object_lock_enforcement import deletion_refusal_reason
from hippius_s3.api.s3.object_lock_enforcement import is_bypass_requested
from hippius_s3.api.s3.object_lock_enforcement import is_version_locked
from hippius_s3.api.s3.object_lock_enforcement import may_bypass_governance


NOW = datetime(2026, 9, 2, 12, 0, 0, tzinfo=timezone.utc)
FUTURE = NOW + timedelta(days=365)
PAST = NOW - timedelta(days=1)


def _version(
    *,
    mode: str | None = None,
    retain_until: datetime | None = None,
    legal_hold: bool = False,
) -> dict[str, Any]:
    return {
        "object_lock_mode": mode,
        "object_lock_retain_until": retain_until,
        "object_lock_legal_hold": legal_hold,
    }


class TestIsVersionLocked:
    """The truth table. Retention and legal hold are independent, which is four states, not two."""

    @pytest.mark.parametrize(
        "row,expected,why",
        [
            (_version(), False, "no protection at all"),
            (_version(mode=COMPLIANCE, retain_until=FUTURE), True, "live compliance retention"),
            (_version(mode=GOVERNANCE, retain_until=FUTURE), True, "live governance retention"),
            (_version(mode=COMPLIANCE, retain_until=PAST), False, "retention has expired"),
            (_version(legal_hold=True), True, "legal hold with no retention at all"),
            (
                _version(mode=COMPLIANCE, retain_until=PAST, legal_hold=True),
                True,
                "EXPIRED retention but a live hold — the hold alone still locks it",
            ),
            (
                _version(mode=COMPLIANCE, retain_until=FUTURE, legal_hold=False),
                True,
                "live retention with the hold explicitly off",
            ),
        ],
    )
    def test_truth_table(self, row: dict, expected: bool, why: str) -> None:
        assert is_version_locked(row, now=NOW) is expected, why

    def test_retain_until_exactly_now_is_not_locked(self) -> None:
        """Boundary. The retention period has elapsed at exactly its expiry instant — AWS says the
        version 'can be overwritten or deleted' AFTER the period expires, and `>` rather than `>=`
        is what makes a lapsed lock actually lapse rather than hang for a tick."""
        assert is_version_locked(_version(mode=COMPLIANCE, retain_until=NOW), now=NOW) is False

    def test_one_microsecond_before_expiry_is_locked(self) -> None:
        row = _version(mode=COMPLIANCE, retain_until=NOW + timedelta(microseconds=1))
        assert is_version_locked(row, now=NOW) is True

    def test_missing_columns_read_as_unlocked_not_crash(self) -> None:
        """A caller selecting a narrower column list must not make everything look locked (which
        would wedge deletion fleet-wide) nor raise inside a delete path."""
        assert is_version_locked({}, now=NOW) is False
        assert is_version_locked({"object_lock_legal_hold": None}, now=NOW) is False

    def test_row_may_be_any_mapping(self) -> None:
        """asyncpg hands back a Record, tests hand dicts; both must work identically."""

        class RecordLike:
            def __init__(self, data: dict) -> None:
                self._d = data

            def __getitem__(self, key: str) -> Any:
                return self._d[key]

        row = RecordLike(_version(mode=GOVERNANCE, retain_until=FUTURE))
        assert is_version_locked(row, now=NOW) is True

    def test_defaults_to_wall_clock_when_now_omitted(self) -> None:
        """Production calls pass no `now`; a far-future retention must still read as locked."""
        far = datetime.now(timezone.utc) + timedelta(days=3650)
        assert is_version_locked(_version(mode=COMPLIANCE, retain_until=far)) is True


class TestBypassHeader:
    """`x-amz-bypass-governance-retention` is consent, so only an explicit `true` counts."""

    @pytest.mark.parametrize("value", ["true", "TRUE", "True", " true ", "tRuE"])
    def test_accepted_spellings(self, value: str) -> None:
        assert is_bypass_requested({"x-amz-bypass-governance-retention": value}) is True

    @pytest.mark.parametrize("value", ["false", "False", "1", "yes", "on", "", "0", "null", "TRUEISH"])
    def test_rejected_values(self, value: str) -> None:
        """`false` is the sharp one: treating any PRESENT header as consent would let a client
        that explicitly declined the bypass delete a retained object."""
        assert is_bypass_requested({"x-amz-bypass-governance-retention": value}) is False

    def test_absent_header_and_no_headers_at_all(self) -> None:
        assert is_bypass_requested({}) is False
        assert is_bypass_requested(None) is False


class TestMayBypassGovernance:
    """Both halves are required: the header AND being the bucket owner."""

    @pytest.mark.parametrize(
        "is_owner,header,expected",
        [
            (True, "true", True),
            (True, "false", False),
            (True, None, False),
            (False, "true", False),
            (False, None, False),
        ],
    )
    def test_both_halves_required(self, is_owner: bool, header: str | None, expected: bool) -> None:
        headers = {} if header is None else {"x-amz-bypass-governance-retention": header}
        assert may_bypass_governance(is_bucket_owner=is_owner, headers=headers) is expected

    def test_non_owner_with_header_is_refused(self) -> None:
        """The deliberate deviation from AWS: with no IAM, the permission half is owner-only, so a
        delegated WRITE_ACP grantee cannot destroy retained data. Widening this later is safe;
        having shipped it wide is not."""
        assert (
            may_bypass_governance(is_bucket_owner=False, headers={"x-amz-bypass-governance-retention": "true"}) is False
        )


class TestDeletionRefusalReason:
    """The decision actually used by the delete endpoints."""

    def test_unlocked_version_is_deletable(self) -> None:
        assert deletion_refusal_reason(_version(), is_bucket_owner=False, headers={}) is None

    def test_expired_retention_is_deletable(self) -> None:
        row = _version(mode=GOVERNANCE, retain_until=PAST)
        assert deletion_refusal_reason(row, is_bucket_owner=False, headers={}) is None

    def test_compliance_refuses_even_the_owner_with_the_bypass_header(self) -> None:
        """The defining property of COMPLIANCE: nobody, including the account root, can delete it.
        A bypass that worked here would make the mode a lie."""
        row = _version(mode=COMPLIANCE, retain_until=FUTURE)
        reason = deletion_refusal_reason(
            row, is_bucket_owner=True, headers={"x-amz-bypass-governance-retention": "true"}
        )
        assert reason is not None
        assert "COMPLIANCE" in reason

    def test_governance_refuses_without_bypass(self) -> None:
        row = _version(mode=GOVERNANCE, retain_until=FUTURE)
        assert deletion_refusal_reason(row, is_bucket_owner=True, headers={}) is not None

    def test_governance_allows_owner_with_bypass(self) -> None:
        row = _version(mode=GOVERNANCE, retain_until=FUTURE)
        reason = deletion_refusal_reason(
            row, is_bucket_owner=True, headers={"x-amz-bypass-governance-retention": "true"}
        )
        assert reason is None

    def test_governance_refuses_non_owner_with_bypass(self) -> None:
        row = _version(mode=GOVERNANCE, retain_until=FUTURE)
        assert (
            deletion_refusal_reason(row, is_bucket_owner=False, headers={"x-amz-bypass-governance-retention": "true"})
            is not None
        )

    def test_legal_hold_has_no_bypass(self) -> None:
        """A legal hold is not a retention mode and the governance bypass does not apply to it —
        an owner sending the bypass header must still be refused."""
        row = _version(legal_hold=True)
        reason = deletion_refusal_reason(
            row, is_bucket_owner=True, headers={"x-amz-bypass-governance-retention": "true"}
        )
        assert reason is not None
        assert "legal hold" in reason

    def test_legal_hold_beats_a_bypassable_governance_retention(self) -> None:
        """Both protections present, governance bypassable — the hold must still refuse. Checking
        retention first and returning early would delete an object under legal hold."""
        row = _version(mode=GOVERNANCE, retain_until=FUTURE, legal_hold=True)
        reason = deletion_refusal_reason(
            row, is_bucket_owner=True, headers={"x-amz-bypass-governance-retention": "true"}
        )
        assert reason is not None
        assert "legal hold" in reason

    def test_unrecognised_mode_fails_closed(self) -> None:
        """A CHECK constraint blocks this from the API, but a direct DB write could still produce
        it. An unrecognised lock is the one case where guessing wrong destroys data, so refuse."""
        row = _version(mode="WORM", retain_until=FUTURE)
        assert deletion_refusal_reason(row, is_bucket_owner=True, headers={}) is not None


class TestSqlGatesEmbedTheCanonicalPredicate:
    """Every gated query must carry the same lock predicate, spelled the same way.

    asyncpg cannot parameterise a predicate, so each .sql file embeds the text rather than
    importing it — several hand-synced copies, which is exactly the shape that rots. The
    integration suite cross-checks the SQL verdict against the Python one, but only for the unpin
    gate; this pins every other gated query too, cheaply and without a database.
    """

    GATED_QUERIES = (
        "get_chunk_backend_identifiers",
        "find_objects_ready_for_hard_delete",
        "find_versions_ready_for_reap",
    )

    @pytest.mark.parametrize("query_name", GATED_QUERIES)
    def test_query_contains_both_halves_of_the_predicate(self, query_name: str) -> None:
        from hippius_s3.utils import get_query

        sql = get_query(query_name)
        # Both protections, independently — a query carrying only the retention half would silently
        # let a legal-hold-only version be destroyed.
        assert "object_lock_legal_hold" in sql, f"{query_name} does not check the legal hold"
        assert "object_lock_retain_until" in sql, f"{query_name} does not check the retention"
        assert "now()" in sql, f"{query_name} does not compare the retention against the clock"

    @pytest.mark.parametrize("query_name", GATED_QUERIES)
    def test_predicate_uses_strict_inequality(self, query_name: str) -> None:
        """`>=` would keep a version locked for an instant past its expiry in SQL while Python
        released it — the two would disagree exactly at the boundary the unit tests pin."""
        from hippius_s3.utils import get_query

        sql = get_query(query_name)
        assert "object_lock_retain_until >= now()" not in sql, f"{query_name} uses >= at the boundary"
