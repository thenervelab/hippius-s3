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
def test_tier2_query_subresources_no_longer_trigger(subresource: str) -> None:
    """Tier 2 implements ?retention and ?legal-hold, so the guard must let them reach the router.

    Was a 501 assertion under Tier 0. Flipping it is the point of the feature — but the guard must
    only stop trapping a surface once a real handler exists, or the request is silently ignored
    instead, which is worse than the 501 it replaced.
    """
    resp = maybe_object_lock_not_implemented_response(_make_request(query={subresource: ""}))
    assert resp is None


def test_retention_with_version_id_no_longer_triggers() -> None:
    resp = maybe_object_lock_not_implemented_response(_make_request(query={"retention": "", "versionId": "v1"}))
    assert resp is None


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
def test_per_object_lock_headers_still_501_where_unsupported(header_name: str, header_value: str) -> None:
    """The guard is PER PATH: refused by default, allowed only where the caller honours them.

    Default-deny is the load-bearing half. The write paths that persist a lock opt IN explicitly
    (PutObject, CopyObject, CreateMultipartUpload); everything else — the read and delete routes,
    and the bucket routes — still refuses, because a lock header there names nothing that any write
    could apply, and accepting it would tell the client its object is retained when nothing was
    written at all.
    """
    resp = maybe_object_lock_not_implemented_response(_make_request(headers={header_name: header_value}))
    _assert_not_implemented(resp)


@pytest.mark.parametrize(
    "header_name,header_value",
    [
        ("x-amz-object-lock-mode", "GOVERNANCE"),
        ("x-amz-object-lock-retain-until-date", "2099-01-01T00:00:00Z"),
        ("x-amz-object-lock-legal-hold", "ON"),
    ],
)
def test_per_object_lock_headers_pass_where_supported(header_name: str, header_value: str) -> None:
    """PutObject, CopyObject and CreateMultipartUpload persist them, so they opt in and the guard
    must let them through — otherwise the implemented feature is unreachable. Pinned in both
    directions because the opt-in is a single keyword argument per call site, and silently losing
    it turns a working feature back into a 501 with nothing else failing."""
    resp = maybe_object_lock_not_implemented_response(
        _make_request(headers={header_name: header_value}), object_lock_headers_supported=True
    )
    assert resp is None


def test_per_object_lock_header_case_insensitive_still_501_where_unsupported() -> None:
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
    """Exercises the error builder directly.

    Tier 2 implements every surface this guard used to trap, so no request reaches it today. The
    builder is kept and tested because it is where the next genuinely-unimplemented Object Lock
    surface (Batch Operations, the enable-on-existing-bucket token) gets refused, and an S3 error
    missing RequestId/HostId breaks SDK error handling.
    """
    from hippius_s3.api.s3.object_lock_guard import _not_implemented

    resp = _not_implemented()
    _assert_not_implemented(resp)
    root = ET.fromstring(resp.body)
    assert root.findtext("RequestId"), "missing RequestId in error XML"
    assert root.findtext("HostId"), "missing HostId in error XML"


def test_every_write_path_that_persists_a_lock_opts_into_the_guard() -> None:
    """The opt-in is one keyword argument per call site, and losing it fails nothing else.

    `object_lock_headers_supported=True` is what lets a lock header reach a write path that
    actually persists it. Drop it from a call site and that path silently answers 501 again: the
    feature disappears, no unit test breaks, and the only symptom is a client being told Object
    Lock is not implemented. That is the same shape as the regression this PR fixes on the
    multipart path, so it is asserted against the real source rather than trusted.

    Read/delete and bucket routes are deliberately NOT in this list — a lock header there names
    nothing a write could apply, so default-deny is correct for them.
    """
    import re
    from pathlib import Path

    repo = Path(__file__).resolve().parents[4]
    expected = {
        # file -> how many guard call sites must pass the opt-in EXPLICITLY.
        # The value may be a literal True or a condition (multipart passes `is_initiate`, because
        # that handler also serves CompleteMultipartUpload, which cannot apply a lock and so must
        # keep refusing the headers). What matters is that the keyword is passed at all — omitting
        # it silently restores the 501.
        "hippius_s3/api/s3/objects/router.py": 1,  # put_object, which also dispatches CopyObject
        "hippius_s3/api/s3/multipart.py": 1,  # CreateMultipartUpload
    }
    for rel, count in expected.items():
        source = (repo / rel).read_text()
        calls = re.findall(r"maybe_object_lock_not_implemented_response\((.*?)\)\n", source, re.S)
        opted_in = [c for c in calls if "object_lock_headers_supported=" in c]
        assert len(opted_in) == count, (
            f"{rel} should have {count} guard call(s) passing object_lock_headers_supported, "
            f"found {len(opted_in)}. Losing it makes a path that persists locks answer 501 again."
        )
