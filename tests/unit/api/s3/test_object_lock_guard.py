"""Unit tests for the Object Lock guard helper.

The guard catches requests that touch the **Tier 2** surface (per-object retention,
per-object legal hold, per-object x-amz-object-lock-* headers) and returns a 501
NotImplemented response. Bucket-level Object Lock is Tier 1 and handled by
`bucket_object_lock_endpoint` — the guard intentionally does NOT trip on those.

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
    request.query_params = dict(query or {})
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
    root = ET.fromstring(resp.body)
    code = root.findtext("Code")
    assert code == "NotImplemented", f"expected NotImplemented, got {code}"


def test_no_object_lock_signal_returns_none() -> None:
    resp = maybe_object_lock_not_implemented_response(_make_request())
    assert resp is None


def test_unrelated_query_params_pass_through() -> None:
    resp = maybe_object_lock_not_implemented_response(
        _make_request(query={"tagging": "", "uploads": ""}, headers={"x-amz-meta-foo": "bar"})
    )
    assert resp is None


@pytest.mark.parametrize("subresource", ["retention", "legal-hold"])
def test_tier2_query_subresources_trigger_501(subresource: str) -> None:
    resp = maybe_object_lock_not_implemented_response(_make_request(query={subresource: ""}))
    _assert_not_implemented(resp)


def test_retention_with_version_id_triggers_501() -> None:
    resp = maybe_object_lock_not_implemented_response(_make_request(query={"retention": "", "versionId": "v1"}))
    _assert_not_implemented(resp)


def test_bucket_object_lock_subresource_does_not_trigger() -> None:
    """Tier 1 endpoint handles ?object-lock; the guard must let it through."""
    resp = maybe_object_lock_not_implemented_response(_make_request(query={"object-lock": ""}))
    assert resp is None


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


def test_bucket_object_lock_enabled_header_does_not_trigger() -> None:
    """Tier 1 CreateBucket handler reads this header; the guard must let it through."""
    resp = maybe_object_lock_not_implemented_response(
        _make_request(headers={"x-amz-bucket-object-lock-enabled": "true"})
    )
    assert resp is None


def test_bypass_governance_header_alone_does_not_trigger() -> None:
    """No-op without any lock state — guard must not trip on it."""
    resp = maybe_object_lock_not_implemented_response(
        _make_request(headers={"x-amz-bypass-governance-retention": "true"})
    )
    assert resp is None


def test_error_body_has_request_id_and_host_id() -> None:
    resp = maybe_object_lock_not_implemented_response(_make_request(query={"retention": ""}))
    _assert_not_implemented(resp)
    root = ET.fromstring(resp.body)
    assert root.findtext("RequestId"), "missing RequestId in error XML"
    assert root.findtext("HostId"), "missing HostId in error XML"
