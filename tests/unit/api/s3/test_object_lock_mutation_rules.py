"""Adversarial tests for retention mutation rules and the lock XML parsers.

`validate_retention_transition` is the WORM security model: it decides whether a lock may be
weakened. Every test here is an attempt to weaken one and be told no.
"""

from __future__ import annotations

from datetime import datetime
from datetime import timedelta
from datetime import timezone
from typing import Any

import pytest

from hippius_s3.api.s3.object_lock_enforcement import COMPLIANCE
from hippius_s3.api.s3.object_lock_enforcement import GOVERNANCE
from hippius_s3.api.s3.objects.object_lock_endpoints import legal_hold_to_xml
from hippius_s3.api.s3.objects.object_lock_endpoints import parse_legal_hold_body
from hippius_s3.api.s3.objects.object_lock_endpoints import parse_retention_body
from hippius_s3.api.s3.objects.object_lock_endpoints import retention_to_xml
from hippius_s3.api.s3.objects.object_lock_endpoints import validate_retention_transition


NOW = datetime(2026, 9, 2, 12, 0, 0, tzinfo=timezone.utc)
SOON = NOW + timedelta(days=30)
LATER = NOW + timedelta(days=365)
PAST = NOW - timedelta(days=1)
BYPASS = {"x-amz-bypass-governance-retention": "true"}


def _check(**kw: Any) -> Any:
    kw.setdefault("is_bucket_owner", True)
    kw.setdefault("headers", {})
    kw.setdefault("now", NOW)
    return validate_retention_transition(**kw)


class TestComplianceIsImmutable:
    """COMPLIANCE's defining property: nobody weakens it, not even the owner with a bypass."""

    def test_extension_allowed(self) -> None:
        assert _check(current_mode=COMPLIANCE, current_until=SOON, new_mode=COMPLIANCE, new_until=LATER) is None

    def test_shortening_refused_even_with_bypass(self) -> None:
        r = _check(current_mode=COMPLIANCE, current_until=LATER, new_mode=COMPLIANCE, new_until=SOON, headers=BYPASS)
        assert r is not None and r.status_code == 403

    def test_clearing_refused(self) -> None:
        r = _check(current_mode=COMPLIANCE, current_until=LATER, new_mode=None, new_until=None, headers=BYPASS)
        assert r is not None and r.status_code == 403

    def test_downgrade_to_governance_refused(self) -> None:
        """The escape hatch an attacker would actually reach for: flip the mode to the bypassable
        one, then bypass it. Refusing the mode change is what closes that."""
        r = _check(current_mode=COMPLIANCE, current_until=LATER, new_mode=GOVERNANCE, new_until=LATER, headers=BYPASS)
        assert r is not None and r.status_code == 403

    def test_expired_compliance_may_be_replaced(self) -> None:
        """Immutability lasts for the retention period, not forever."""
        assert _check(current_mode=COMPLIANCE, current_until=PAST, new_mode=GOVERNANCE, new_until=SOON) is None

    def test_equal_date_is_not_a_shortening(self) -> None:
        assert _check(current_mode=COMPLIANCE, current_until=LATER, new_mode=COMPLIANCE, new_until=LATER) is None


class TestGovernanceNeedsBypassToWeaken:
    def test_extension_needs_no_bypass(self) -> None:
        assert _check(current_mode=GOVERNANCE, current_until=SOON, new_mode=GOVERNANCE, new_until=LATER) is None

    @pytest.mark.parametrize(
        "new_mode,new_until,what",
        [
            (GOVERNANCE, SOON, "shortening"),
            (None, None, "clearing"),
            (COMPLIANCE, LATER, "mode change"),
        ],
    )
    def test_weakening_refused_without_bypass(self, new_mode: str | None, new_until: Any, what: str) -> None:
        r = _check(current_mode=GOVERNANCE, current_until=LATER, new_mode=new_mode, new_until=new_until)
        assert r is not None and r.status_code == 403, f"{what} was allowed with no bypass"

    def test_weakening_allowed_for_owner_with_bypass(self) -> None:
        assert (
            _check(current_mode=GOVERNANCE, current_until=LATER, new_mode=GOVERNANCE, new_until=SOON, headers=BYPASS)
            is None
        )

    def test_weakening_refused_for_non_owner_with_bypass(self) -> None:
        r = _check(
            current_mode=GOVERNANCE,
            current_until=LATER,
            new_mode=GOVERNANCE,
            new_until=SOON,
            is_bucket_owner=False,
            headers=BYPASS,
        )
        assert r is not None and r.status_code == 403

    def test_unrecognised_live_mode_fails_closed(self) -> None:
        r = _check(current_mode="WORM", current_until=LATER, new_mode=GOVERNANCE, new_until=SOON, headers=BYPASS)
        assert r is not None and r.status_code == 403


class TestRetentionBodyParsing:
    def test_valid_body(self) -> None:
        body = (
            b'<Retention xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
            b"<Mode>COMPLIANCE</Mode><RetainUntilDate>2027-01-01T00:00:00Z</RetainUntilDate></Retention>"
        )
        parsed, err = parse_retention_body(body)
        assert err is None and parsed is not None
        assert parsed["mode"] == COMPLIANCE
        assert parsed["retain_until"] == datetime(2027, 1, 1, tzinfo=timezone.utc)

    def test_bare_namespace_free_body_parses(self) -> None:
        body = b"<Retention><Mode>GOVERNANCE</Mode><RetainUntilDate>2027-01-01T00:00:00Z</RetainUntilDate></Retention>"
        parsed, err = parse_retention_body(body)
        assert err is None and parsed is not None and parsed["mode"] == GOVERNANCE

    def test_naive_timestamp_is_assumed_utc_not_left_naive(self) -> None:
        """A naive datetime compared against an aware now() raises TypeError — which would surface
        as a 500 on a DELETE long after this request succeeded."""
        parsed, err = parse_retention_body(
            b"<Retention><Mode>GOVERNANCE</Mode><RetainUntilDate>2027-01-01T00:00:00</RetainUntilDate></Retention>"
        )
        assert err is None and parsed is not None
        assert parsed["retain_until"].tzinfo is not None

    def test_empty_retention_is_an_explicit_clear_not_an_error(self) -> None:
        parsed, err = parse_retention_body(b"<Retention></Retention>")
        assert err is None and parsed == {"mode": None, "retain_until": None}

    @pytest.mark.parametrize(
        "body,why",
        [
            (b"", "empty body"),
            (
                b"<Retention><Mode>WORM</Mode><RetainUntilDate>2027-01-01T00:00:00Z</RetainUntilDate></Retention>",
                "invalid mode",
            ),
            (b"<Retention><Mode>GOVERNANCE</Mode></Retention>", "mode without a date"),
            (b"<Retention><RetainUntilDate>2027-01-01T00:00:00Z</RetainUntilDate></Retention>", "date without a mode"),
            (
                b"<Retention><Mode>GOVERNANCE</Mode><RetainUntilDate>not-a-date</RetainUntilDate></Retention>",
                "bad date",
            ),
            (b"<LegalHold><Status>ON</Status></LegalHold>", "wrong root element"),
            (b"<Retention><Mode>GOVERNANCE</Mode>", "malformed xml"),
        ],
    )
    def test_rejected_bodies(self, body: bytes, why: str) -> None:
        parsed, err = parse_retention_body(body)
        assert parsed is None and err is not None and err.status_code == 400, why

    def test_entity_expansion_is_refused(self) -> None:
        """Same hardening as every other client-XML entry point: a DTD entity must not resolve."""
        body = (
            b"<?xml version='1.0'?><!DOCTYPE r [<!ENTITY e 'COMPLIANCE'>]>"
            b"<Retention><Mode>&e;</Mode><RetainUntilDate>2027-01-01T00:00:00Z</RetainUntilDate></Retention>"
        )
        parsed, err = parse_retention_body(body)
        assert parsed is None and err is not None, "a DTD entity resolved into the retention mode"


class TestLegalHoldBodyParsing:
    @pytest.mark.parametrize("status,expected", [(b"ON", True), (b"OFF", False), (b"on", True), (b"off", False)])
    def test_valid(self, status: bytes, expected: bool) -> None:
        on, err = parse_legal_hold_body(b"<LegalHold><Status>" + status + b"</Status></LegalHold>")
        assert err is None and on is expected

    @pytest.mark.parametrize(
        "body", [b"", b"<LegalHold></LegalHold>", b"<LegalHold><Status>MAYBE</Status></LegalHold>", b"<Retention/>"]
    )
    def test_rejected(self, body: bytes) -> None:
        on, err = parse_legal_hold_body(body)
        assert on is None and err is not None and err.status_code == 400


class TestResponseRendering:
    def test_retention_round_trips(self) -> None:
        xml = retention_to_xml(COMPLIANCE, datetime(2027, 1, 1, tzinfo=timezone.utc))
        parsed, err = parse_retention_body(xml)
        assert err is None and parsed is not None
        assert parsed["mode"] == COMPLIANCE
        assert parsed["retain_until"] == datetime(2027, 1, 1, tzinfo=timezone.utc)

    @pytest.mark.parametrize("on", [True, False])
    def test_legal_hold_round_trips(self, on: bool) -> None:
        back, err = parse_legal_hold_body(legal_hold_to_xml(on))
        assert err is None and back is on
