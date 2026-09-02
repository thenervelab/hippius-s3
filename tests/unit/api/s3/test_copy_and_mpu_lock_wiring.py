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


REAL_GET_BY_PATH_COLUMNS = frozenset(
    {
        "object_id",
        "bucket_id",
        "object_key",
        "size_bytes",
        "content_type",
        "created_at",
        "metadata",
        "md5_hash",
        "append_version",
        "multipart",
        "storage_version",
        "object_version",
        "encryption_version",
        "enc_suite_id",
        "enc_chunk_size_bytes",
        "kek_id",
        "wrapped_dek",
        "is_delete_marker",
        "bucket_name",
    }
)


def test_the_destination_row_does_not_carry_current_object_version() -> None:
    """Pins why the lock version comes off the RESPONSE, not off a re-read of the destination.

    `get_object_by_path.sql` projects `object_version`. `current_object_version` appears in that
    file only inside the version-resolution subquery's predicate, so a grep suggests it exists as
    an output column and it does not. Reading it off the returned Record raises KeyError — which,
    on the copy path, fires AFTER the destination has been overwritten and made live: the client
    is told the copy failed while an unprotected copy sits at the key.

    This is the second time that exact confusion has shipped (see
    test_multipart_reserve_row_contract, where the MPU reserve row DOES call it
    `current_object_version`). The two rows genuinely disagree, which is what makes it easy to get
    wrong, so it is asserted against the real SQL rather than remembered.
    """
    from pathlib import Path

    sql = (Path(__file__).resolve().parents[4] / "hippius_s3/sql/queries/get_object_by_path.sql").read_text()
    projection = sql[sql.rindex("SELECT") : sql.index("FROM", sql.rindex("SELECT"))]
    assert "object_version" in projection
    assert "current_object_version" not in projection, (
        "get_object_by_path now projects current_object_version — the copy path's comment and the "
        "reserve-row contract test both need revisiting together"
    )


@pytest.fixture
def captured(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    """Capture every store_version_lock call the copy path makes."""
    calls: list[dict[str, Any]] = []

    async def _store(_db: Any, **kw: Any) -> None:
        calls.append(kw)

    monkeypatch.setattr(mod, "store_version_lock", _store)
    return calls


def _copied(version_id: str | None = "7", status: int = 200) -> Response:
    """A copy response shaped like the real one: the version it wrote, reported as a header."""
    headers = {"x-amz-version-id": version_id} if version_id is not None else {}
    return Response(status_code=status, headers=headers)


@pytest.mark.asyncio
class TestApplyLockToCopy:
    async def test_no_intent_writes_nothing(self, captured: list[dict[str, Any]]) -> None:
        """The common case: an ordinary copy must not touch the lock columns at all."""
        resp = _copied()
        assert await mod._apply_lock_to_copy(None, resp, None, "obj-1") is resp
        assert captured == []

    @pytest.mark.parametrize("status", [400, 403, 404, 500, 501])
    async def test_a_failed_copy_is_never_locked(self, status: int, captured: list[dict[str, Any]]) -> None:
        """Locking a copy that did not happen would pin a retention onto whatever content the
        destination key already had — including an unrelated object."""
        await mod._apply_lock_to_copy(None, _copied(status=status), ("GOVERNANCE", FUTURE, False), "obj-1")
        assert captured == [], f"a {status} copy still wrote a lock"

    async def test_lock_lands_on_the_version_the_copy_wrote(self, captured: list[dict[str, Any]]) -> None:
        """Not on whatever is current afterwards. A concurrent PUT to the same key between the copy
        and a re-read would otherwise leave this copy unlocked and pin the retention onto the
        unrelated write — permanently, under COMPLIANCE."""
        await mod._apply_lock_to_copy(None, _copied("42"), ("COMPLIANCE", FUTURE, False), "obj-1")
        assert len(captured) == 1
        assert captured[0]["object_id"] == "obj-1"
        assert captured[0]["object_version"] == 42
        assert captured[0]["mode"] == "COMPLIANCE"
        assert captured[0]["legal_hold"] is False

    async def test_legal_hold_only_intent_is_applied(self, captured: list[dict[str, Any]]) -> None:
        await mod._apply_lock_to_copy(None, _copied(), (None, None, True), "obj-1")
        assert captured[0]["mode"] is None and captured[0]["legal_hold"] is True

    async def test_a_response_without_a_version_does_not_raise(self, captured: list[dict[str, Any]]) -> None:
        """Fail loudly in the log, not by 500-ing a copy that already succeeded and was returned.
        The alias path reports no version, and any future path that forgets to must not crash."""
        resp = _copied(version_id=None)
        assert await mod._apply_lock_to_copy(None, resp, ("GOVERNANCE", FUTURE, False), "obj-1") is resp
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

        class _Repo:
            """Only the columns get_object_by_path actually projects — deliberately NOT
            `current_object_version`, which the real row does not carry. Inventing it here is what
            let an earlier version of these tests pass green over a 500 on every locked copy."""

            def __init__(self, _db: Any) -> None: ...

            async def get_by_path(self, _b: str, _k: str) -> Any:
                return {"object_id": "obj-dest", "object_version": 3, "storage_version": 5}

        monkeypatch.setattr(mod, "ObjectRepository", _Repo)
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
