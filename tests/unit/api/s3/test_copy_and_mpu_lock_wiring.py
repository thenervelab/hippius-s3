"""The two decisions CopyObject makes about Object Lock, isolated from the copy machinery.

An e2e proves the outcome; these pin the two branches that are easy to regress silently and
expensive to reach through a real copy:

1. Lock intent disqualifies the same-bucket ALIAS optimisation. An alias is a second name on one
   object_id with no version of its own, so a lock written there lands on the SOURCE — an object the
   caller never named becomes undeletable. The alias must also survive for unlocked copies, which is
   the overwhelmingly common case, so both directions are asserted.

2. The lock is applied only to a copy that actually succeeded, and only when there is intent.
"""

from __future__ import annotations

from datetime import datetime
from datetime import timedelta
from datetime import timezone
from types import SimpleNamespace
from typing import Any

import pytest
from starlette.responses import Response

from hippius_s3.api.s3.objects import copy_object_endpoint as mod


FUTURE = datetime.now(timezone.utc) + timedelta(days=30)


class _Repo:
    """Stands in for ObjectRepository, returning one destination row."""

    row: Any = {"object_id": "obj-dest", "current_object_version": 7}

    def __init__(self, _db: Any) -> None: ...

    async def get_by_path(self, _b: str, _k: str) -> Any:
        return _Repo.row


@pytest.fixture
def captured(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    """Capture every store_version_lock call the copy path makes."""
    calls: list[dict[str, Any]] = []

    async def _store(_db: Any, **kw: Any) -> None:
        calls.append(kw)

    monkeypatch.setattr(mod, "store_version_lock", _store)
    monkeypatch.setattr(mod, "ObjectRepository", _Repo)
    _Repo.row = {"object_id": "obj-dest", "current_object_version": 7}
    return calls


@pytest.mark.asyncio
class TestApplyLockToCopy:
    async def test_no_intent_writes_nothing(self, captured: list[dict[str, Any]]) -> None:
        """The common case: an ordinary copy must not touch the lock columns at all."""
        resp = Response(status_code=200)
        out = await mod._apply_lock_to_copy(None, resp, None, "bkt", "k")
        assert out is resp
        assert captured == []

    @pytest.mark.parametrize("status", [400, 403, 404, 500, 501])
    async def test_a_failed_copy_is_never_locked(self, status: int, captured: list[dict[str, Any]]) -> None:
        """Locking a copy that did not happen would pin a retention onto whatever content the
        destination key already had — including an unrelated object."""
        await mod._apply_lock_to_copy(None, Response(status_code=status), ("GOVERNANCE", FUTURE, False), "bkt", "k")
        assert captured == [], f"a {status} copy still wrote a lock"

    async def test_successful_copy_locks_the_destination_version(self, captured: list[dict[str, Any]]) -> None:
        await mod._apply_lock_to_copy(None, Response(status_code=200), ("COMPLIANCE", FUTURE, False), "bkt", "k")
        assert len(captured) == 1
        assert captured[0]["object_id"] == "obj-dest"
        assert captured[0]["object_version"] == 7
        assert captured[0]["mode"] == "COMPLIANCE"
        assert captured[0]["legal_hold"] is False

    async def test_legal_hold_only_intent_is_applied(self, captured: list[dict[str, Any]]) -> None:
        await mod._apply_lock_to_copy(None, Response(status_code=200), (None, None, True), "bkt", "k")
        assert captured[0]["mode"] is None and captured[0]["legal_hold"] is True

    async def test_missing_destination_row_does_not_raise(self, captured: list[dict[str, Any]]) -> None:
        """Fail loudly in the log, not by 500-ing a copy that already succeeded and was returned."""
        _Repo.row = None
        resp = Response(status_code=200)
        out = await mod._apply_lock_to_copy(None, resp, ("GOVERNANCE", FUTURE, False), "bkt", "k")
        assert out is resp
        assert captured == []


@pytest.mark.asyncio
class TestAliasDisqualification:
    """Lock intent must disqualify the same-bucket ALIAS optimisation.

    An alias is a second NAME on one object_id with no version of its own, so a lock written for the
    copy lands on the SOURCE — an object the caller never named becomes undeletable. Driven through
    the real handler with the copy machinery stubbed, so removing the guard from the source fails
    this; a truth table restating the condition would not.
    """

    @staticmethod
    def _wire(monkeypatch: pytest.MonkeyPatch, headers: dict[str, str]) -> dict[str, Any]:
        seen: dict[str, Any] = {"alias_called": False, "streamed": False}

        async def _resolve(**_kw: Any) -> Any:
            bucket = {"bucket_id": "same-bucket"}
            src_row = {"storage_version": 5, "multipart": False}
            return {"id": "u"}, bucket, bucket, src_row

        async def _alias(*_a: Any, **_k: Any) -> Any:
            seen["alias_called"] = True
            return Response(status_code=200)

        async def _stream(*_a: Any, **_k: Any) -> Any:
            seen["streamed"] = True
            return Response(status_code=200)

        async def _fast(*_a: Any, **_k: Any) -> Any:
            seen["streamed"] = True
            return Response(status_code=200)

        async def _eligible(**_kw: Any) -> Any:
            return False, None, "forced streaming"

        monkeypatch.setattr(mod, "resolve_copy_resources", _resolve)
        monkeypatch.setattr(mod, "handle_same_bucket_copy", _alias)
        monkeypatch.setattr(mod, "handle_streaming_copy", _stream)
        monkeypatch.setattr(mod, "execute_v5_fast_path_copy", _fast)
        monkeypatch.setattr(mod, "should_use_v5_fast_path", _eligible)
        monkeypatch.setattr(mod, "is_multipart_object", lambda _r: False)
        monkeypatch.setattr(mod, "parse_copy_source", lambda _h: ("same-bucket", "src", None))
        monkeypatch.setattr(mod, "require_supported_storage_version", lambda v: v)
        return seen

    @staticmethod
    def _request(headers: dict[str, str]) -> Any:
        return SimpleNamespace(
            headers=headers,
            state=SimpleNamespace(main_account_id="acct", bucket_object_lock={"enabled": True}),
        )

    @pytest.mark.parametrize(
        "headers,label",
        [
            (
                {"x-amz-object-lock-mode": "GOVERNANCE", "x-amz-object-lock-retain-until-date": "2036-01-01T00:00:00Z"},
                "governance retention",
            ),
            (
                {"x-amz-object-lock-mode": "COMPLIANCE", "x-amz-object-lock-retain-until-date": "2036-01-01T00:00:00Z"},
                "compliance retention",
            ),
            ({"x-amz-object-lock-legal-hold": "ON"}, "legal hold alone"),
        ],
    )
    async def test_any_lock_intent_disqualifies_the_alias(
        self, headers: dict[str, str], label: str, monkeypatch: pytest.MonkeyPatch, captured: list[dict[str, Any]]
    ) -> None:
        seen = self._wire(monkeypatch, headers)
        await mod.handle_copy_object("same-bucket", "dst", self._request(headers), None, None)
        assert not seen["alias_called"], (
            f"a same-bucket copy carrying {label} was aliased — the lock would land on the source "
            f"object, which the caller never named"
        )
        assert seen["streamed"], "expected a real byte copy instead of the alias"

    async def test_unlocked_same_bucket_copy_still_aliases(
        self, monkeypatch: pytest.MonkeyPatch, captured: list[dict[str, Any]]
    ) -> None:
        """The optimisation must survive for the case that is almost all of them."""
        seen = self._wire(monkeypatch, {})
        await mod.handle_copy_object("same-bucket", "dst", self._request({}), None, None)
        assert seen["alias_called"], "an ordinary same-bucket copy lost the alias optimisation"
