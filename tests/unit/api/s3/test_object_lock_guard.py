"""Unit tests for the Object Lock guard helper.

The guard inspects an incoming request for any Object Lock query param or header and,
if present, returns a 501 NotImplemented S3 XML error. This is the building block for
the Tier 0 implementation — every Object Lock entry point delegates to it.

See specs/s3-object-lock.md.
"""

from __future__ import annotations

from typing import Any
from typing import Mapping
from unittest.mock import MagicMock

import pytest
from lxml import etree as ET  # ty: ignore[unresolved-import]

from hippius_s3.api.s3.object_lock_guard import maybe_object_lock_not_implemented_response


def _make_request(*, query: Mapping[str, str] | None = None, headers: Mapping[str, str] | None = None) -> Any:
    """Build a duck-typed FastAPI Request with just the fields the guard reads."""
    request = MagicMock()
    # FastAPI request.query_params behaves like a Mapping; the guard uses `in`.
    request.query_params = dict(query or {})
    # request.headers is case-insensitive in FastAPI; the guard normalises lower-case keys.
    headers_dict = {k.lower(): v for k, v in (headers or {}).items()}

    class _CIHeaders:
        def __init__(self, d: dict[str, str]) -> None:
            self._d = d

        def get(self, k: str, default: str | None = None) -> str | None:
            return self._d.get(k.lower(), default)

        def __iter__(self):  # type: ignore[no-untyped-def]
            return iter(self._d)

        def __contains__(self, k: object) -> bool:
            return isinstance(k, str) and k.lower() in self._d

        def items(self):  # type: ignore[no-untyped-def]
            return self._d.items()

    request.headers = _CIHeaders(headers_dict)
    return request


def _assert_not_implemented(resp: Any) -> None:
    assert resp is not None, "guard should have returned a 501 response"
    assert resp.status_code == 501, f"expected status 501, got {resp.status_code}"
    # Body is XML; parse and assert the standard S3 error fields.
    root = ET.fromstring(resp.body)
    code = root.findtext("Code")
    message = root.findtext("Message") or ""
    assert code == "NotImplemented", f"expected NotImplemented, got {code}"
    assert "Object Lock" in message or "object lock" in message.lower(), (
        f"expected the message to mention Object Lock; got {message!r}"
    )


def test_no_object_lock_signal_returns_none() -> None:
    resp = maybe_object_lock_not_implemented_response(_make_request())
    assert resp is None


def test_unrelated_query_params_pass_through() -> None:
    resp = maybe_object_lock_not_implemented_response(
        _make_request(query={"tagging": "", "uploads": ""}, headers={"x-amz-meta-foo": "bar"})
    )
    assert resp is None


@pytest.mark.parametrize("subresource", ["object-lock", "retention", "legal-hold"])
def test_query_subresources_trigger_501(subresource: str) -> None:
    resp = maybe_object_lock_not_implemented_response(_make_request(query={subresource: ""}))
    _assert_not_implemented(resp)


def test_retention_with_version_id_triggers_501() -> None:
    resp = maybe_object_lock_not_implemented_response(_make_request(query={"retention": "", "versionId": "v1"}))
    _assert_not_implemented(resp)


@pytest.mark.parametrize(
    "header_name,header_value",
    [
        ("x-amz-object-lock-mode", "GOVERNANCE"),
        ("x-amz-object-lock-retain-until-date", "2099-01-01T00:00:00Z"),
        ("x-amz-object-lock-legal-hold", "ON"),
    ],
)
def test_per_object_lock_headers_trigger_501(header_name: str, header_value: str) -> None:
    resp = maybe_object_lock_not_implemented_response(_make_request(headers={header_name: header_value}))
    _assert_not_implemented(resp)


def test_per_object_lock_header_case_insensitive() -> None:
    resp = maybe_object_lock_not_implemented_response(_make_request(headers={"X-Amz-Object-Lock-Mode": "GOVERNANCE"}))
    _assert_not_implemented(resp)


def test_bucket_object_lock_enabled_header_true_triggers_501() -> None:
    resp = maybe_object_lock_not_implemented_response(
        _make_request(headers={"x-amz-bucket-object-lock-enabled": "true"})
    )
    _assert_not_implemented(resp)


def test_bucket_object_lock_enabled_header_True_triggers_501() -> None:
    """Case-insensitive value: AWS clients send 'true' or 'True'."""
    resp = maybe_object_lock_not_implemented_response(
        _make_request(headers={"x-amz-bucket-object-lock-enabled": "True"})
    )
    _assert_not_implemented(resp)


def test_bucket_object_lock_enabled_header_false_is_noop() -> None:
    """Header present but value is 'false' — same as not setting it."""
    resp = maybe_object_lock_not_implemented_response(
        _make_request(headers={"x-amz-bucket-object-lock-enabled": "false"})
    )
    assert resp is None


def test_bypass_governance_header_alone_does_not_trigger() -> None:
    """Tier 0 contract: x-amz-bypass-governance-retention is a no-op without locks."""
    resp = maybe_object_lock_not_implemented_response(
        _make_request(headers={"x-amz-bypass-governance-retention": "true"})
    )
    assert resp is None


def test_error_body_has_request_id_and_host_id() -> None:
    resp = maybe_object_lock_not_implemented_response(_make_request(query={"object-lock": ""}))
    _assert_not_implemented(resp)
    root = ET.fromstring(resp.body)
    assert root.findtext("RequestId"), "missing RequestId in error XML"
    assert root.findtext("HostId"), "missing HostId in error XML"
