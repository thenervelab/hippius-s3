"""A new version must carry the lock the request asked for — by header, or by bucket default.

This is what makes Tier 1's bucket configuration mean anything: without it, a bucket reports a
default retention that is never applied to a single object. It is also why the per-object
`x-amz-object-lock-*` headers were allowed to stop returning 501 — a header that is accepted and
then dropped is strictly worse than one that is refused, because the client believes its object is
retained.

Precedence follows AWS: explicit headers OVERRIDE the bucket default, and a bucket default is a
DURATION resolved against each version's creation time.
"""

from __future__ import annotations

from datetime import datetime
from datetime import timedelta
from datetime import timezone
from types import SimpleNamespace
from typing import Any

import pytest
from starlette.datastructures import Headers
from starlette.responses import Response

from hippius_s3.api.s3.objects.object_lock_endpoints import lock_for_new_version


def _request(headers: dict[str, str] | None = None, *, bucket_lock: dict[str, Any] | None = None) -> Any:
    return SimpleNamespace(
        headers=Headers(headers or {}),
        state=SimpleNamespace(bucket_object_lock=bucket_lock),
    )


# The minimum bucket config that means "this bucket opted in to Object Lock, with no default
# retention rule". Every explicit-header case needs it: lock headers are refused outright on a
# bucket that never opted in, so passing them without this tests the refusal, not the parsing.
LOCK_ENABLED: dict[str, Any] = {"enabled": True}


def _enabled(headers: dict[str, str] | None = None) -> Any:
    return _request(headers, bucket_lock=LOCK_ENABLED)


class TestBucketMustHaveOptedIn:
    """Lock headers on a bucket with no Object Lock configuration are REFUSED, not applied.

    Honouring them would let any caller holding plain WRITE pin an unreclaimable retention on a
    bucket that never opted in: COMPLIANCE has no bypass by design, and the SQL gates then hold
    the unpinner, the reaper, the hard-delete ring and the ops scripts off those bytes for the
    whole retain-until — up to 3650 days. Not even the bucket owner can undo it.

    Refusing rather than silently dropping matters just as much: a client whose retention header
    was ignored believes its object is protected when it is not.
    """

    @pytest.mark.parametrize(
        "headers,why",
        [
            (
                {
                    "x-amz-object-lock-mode": "COMPLIANCE",
                    "x-amz-object-lock-retain-until-date": "2036-01-01T00:00:00Z",
                },
                "COMPLIANCE retention — the unreclaimable one",
            ),
            (
                {
                    "x-amz-object-lock-mode": "GOVERNANCE",
                    "x-amz-object-lock-retain-until-date": "2036-01-01T00:00:00Z",
                },
                "GOVERNANCE retention",
            ),
            ({"x-amz-object-lock-legal-hold": "ON"}, "legal hold alone is also a lock"),
        ],
    )
    @pytest.mark.parametrize(
        "config,label",
        [
            (None, "no config at all"),
            ({}, "empty config"),
            ({"enabled": False}, "explicitly not enabled"),
            ({"enabled": False, "mode": "GOVERNANCE", "days": 30}, "disabled but carrying a stale rule"),
        ],
    )
    def test_lock_headers_are_refused(
        self, headers: dict[str, str], why: str, config: Any, label: str
    ) -> None:
        result = lock_for_new_version(_request(headers, bucket_lock=config))
        assert isinstance(result, Response), f"{why} accepted on a bucket with {label}"
        assert result.status_code == 400

    def test_legal_hold_off_is_not_lock_intent(self) -> None:
        """OFF asserts no protection, so it must not trip the gate on an ordinary bucket."""
        assert lock_for_new_version(_request({"x-amz-object-lock-legal-hold": "OFF"})) is None

    def test_ordinary_upload_to_an_ordinary_bucket_is_untouched(self) -> None:
        """The gate must cost the overwhelmingly common path nothing."""
        assert lock_for_new_version(_request()) is None

    def test_enabled_bucket_still_accepts_the_headers(self) -> None:
        """The other direction: opting in must actually work."""
        result = lock_for_new_version(
            _enabled(
                {
                    "x-amz-object-lock-mode": "COMPLIANCE",
                    "x-amz-object-lock-retain-until-date": "2036-01-01T00:00:00Z",
                }
            )
        )
        assert not isinstance(result, Response)
        assert result is not None and result[0] == "COMPLIANCE"


class TestExplicitHeaders:
    def test_no_headers_and_no_bucket_default_applies_nothing(self) -> None:
        """The overwhelmingly common case, and the one that must cost the write path nothing."""
        assert lock_for_new_version(_request()) is None

    def test_mode_and_date_are_persisted(self) -> None:
        until = datetime.now(timezone.utc) + timedelta(days=30)
        result = lock_for_new_version(
            _enabled(
                {
                    "x-amz-object-lock-mode": "COMPLIANCE",
                    "x-amz-object-lock-retain-until-date": until.isoformat().replace("+00:00", "Z"),
                }
            )
        )
        assert not isinstance(result, Response)
        assert result is not None
        mode, retain_until, hold = result
        assert mode == "COMPLIANCE"
        assert abs((retain_until - until).total_seconds()) < 2
        assert hold is False

    def test_legal_hold_alone_is_a_lock(self) -> None:
        """A hold needs no retention — it is an independent protection."""
        result = lock_for_new_version(_enabled({"x-amz-object-lock-legal-hold": "ON"}))
        assert result == (None, None, True)

    def test_legal_hold_off_alone_applies_nothing(self) -> None:
        assert lock_for_new_version(_request({"x-amz-object-lock-legal-hold": "OFF"})) is None

    @pytest.mark.parametrize(
        "headers,why",
        [
            ({"x-amz-object-lock-mode": "COMPLIANCE"}, "mode without a date"),
            ({"x-amz-object-lock-retain-until-date": "2099-01-01T00:00:00Z"}, "date without a mode"),
            (
                {"x-amz-object-lock-mode": "WORM", "x-amz-object-lock-retain-until-date": "2099-01-01T00:00:00Z"},
                "invalid mode",
            ),
            (
                {"x-amz-object-lock-mode": "COMPLIANCE", "x-amz-object-lock-retain-until-date": "nonsense"},
                "unparseable date",
            ),
            ({"x-amz-object-lock-legal-hold": "MAYBE"}, "invalid legal-hold status"),
        ],
    )
    def test_malformed_headers_are_rejected_not_ignored(self, headers: dict[str, str], why: str) -> None:
        """Rejection is the point: silently ignoring a malformed lock header is the Tier 0 bug."""
        result = lock_for_new_version(_enabled(headers))
        assert isinstance(result, Response), why
        assert result.status_code == 400

    def test_retention_beyond_the_cap_is_rejected(self) -> None:
        far = datetime.now(timezone.utc) + timedelta(days=365 * 500)
        result = lock_for_new_version(
            _enabled(
                {
                    "x-amz-object-lock-mode": "COMPLIANCE",
                    "x-amz-object-lock-retain-until-date": far.isoformat().replace("+00:00", "Z"),
                }
            )
        )
        assert isinstance(result, Response) and result.status_code == 400


class TestBucketDefaultRetention:
    def test_days_default_is_applied_to_a_new_version(self) -> None:
        result = lock_for_new_version(_request(bucket_lock={"enabled": True, "mode": "GOVERNANCE", "days": 30}))
        assert not isinstance(result, Response) and result is not None
        mode, retain_until, hold = result
        assert mode == "GOVERNANCE"
        expected = datetime.now(timezone.utc) + timedelta(days=30)
        assert abs((retain_until - expected).total_seconds()) < 5
        assert hold is False

    def test_years_default_is_applied(self) -> None:
        result = lock_for_new_version(_request(bucket_lock={"enabled": True, "mode": "COMPLIANCE", "years": 2}))
        assert not isinstance(result, Response) and result is not None
        _, retain_until, _ = result
        expected = datetime.now(timezone.utc) + timedelta(days=730)
        assert abs((retain_until - expected).total_seconds()) < 5

    def test_explicit_headers_override_the_bucket_default(self) -> None:
        """AWS: 'the object version's individual Object Lock settings override any bucket property
        retention settings.'"""
        until = datetime.now(timezone.utc) + timedelta(days=1)
        result = lock_for_new_version(
            _request(
                {
                    "x-amz-object-lock-mode": "GOVERNANCE",
                    "x-amz-object-lock-retain-until-date": until.isoformat().replace("+00:00", "Z"),
                },
                bucket_lock={"enabled": True, "mode": "COMPLIANCE", "years": 10},
            )
        )
        assert not isinstance(result, Response) and result is not None
        mode, retain_until, _ = result
        assert mode == "GOVERNANCE", "the bucket default overrode an explicit per-object retention"
        assert abs((retain_until - until).total_seconds()) < 2

    @pytest.mark.parametrize(
        "config,why",
        [
            (None, "no config at all"),
            ({}, "empty config"),
            ({"enabled": False, "mode": "COMPLIANCE", "days": 30}, "lock not enabled"),
            ({"enabled": True}, "enabled with no default rule — legal for Tier 1"),
            ({"enabled": True, "mode": "WORM", "days": 30}, "unrecognised mode"),
            ({"enabled": True, "mode": "COMPLIANCE"}, "mode with neither days nor years"),
        ],
    )
    def test_configs_that_apply_no_default(self, config: Any, why: str) -> None:
        """`enabled` alone is a valid Tier 1 state — it means 'lock is on' with no default rule, and
        must NOT silently invent a retention."""
        assert lock_for_new_version(_request(bucket_lock=config)) is None, why

    def test_legal_hold_header_still_applies_alongside_a_bucket_default(self) -> None:
        result = lock_for_new_version(
            _request(
                {"x-amz-object-lock-legal-hold": "ON"},
                bucket_lock={"enabled": True, "mode": "GOVERNANCE", "days": 7},
            )
        )
        assert not isinstance(result, Response) and result is not None
        mode, retain_until, hold = result
        assert mode == "GOVERNANCE" and hold is True, "the hold and the default retention must compose"
