"""The two decisions CopyObject makes about Object Lock, isolated from the copy machinery.

An e2e proves the outcome; these pin the two branches that are easy to regress silently and
expensive to reach through a real copy:

1. Lock intent disqualifies the same-bucket ALIAS optimisation. An alias is a second name on one
   object_id with no version of its own, so a lock written there lands on the SOURCE — an object the
   caller never named becomes undeletable. The alias must also survive for unlocked copies, which is
   the overwhelmingly common case, so both directions are asserted.

2. Lock intent sends the copy through the streaming writer, which stores the lock in the same
   transaction that makes the destination version serveable, and never through the v5 fast path,
   whose version is serveable before any lock could be written.
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
        self, headers: dict[str, str], label: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        seen = self._wire(monkeypatch, headers)
        await mod.handle_copy_object("same-bucket", "dst", self._request(headers), None, None)
        assert not seen["alias_called"], (
            f"a same-bucket copy carrying {label} was aliased — the lock would land on the source "
            f"object, which the caller never named"
        )
        assert seen["streamed"], "expected a real byte copy instead of the alias"

    async def test_unlocked_same_bucket_copy_still_aliases(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The optimisation must survive for the case that is almost all of them."""
        seen = self._wire(monkeypatch, {})
        await mod.handle_copy_object("same-bucket", "dst", self._request({}), None, None)
        assert seen["alias_called"], "an ordinary same-bucket copy lost the alias optimisation"


@pytest.mark.asyncio
class TestLockedCopyPath:
    """A locked copy must be written by the streaming writer, carrying the lock, so the version is
    never serveable unlocked. The v5 fast path makes its version serveable from its first autocommit
    statement, so a lock could only follow it — a window in which a key allowed to DELETE
    ?versionId= destroys the copy before it is retained."""

    LOCK = {"x-amz-object-lock-mode": "COMPLIANCE", "x-amz-object-lock-retain-until-date": "2036-01-01T00:00:00Z"}

    @staticmethod
    def _wire(monkeypatch: pytest.MonkeyPatch, *, multipart: bool) -> dict[str, Any]:
        seen: dict[str, Any] = {"fast": False, "stream_lock": "not called"}

        async def _resolve(**_kw: Any) -> Any:
            return {"id": "u"}, {"bucket_id": "src-bucket"}, {"bucket_id": "dst-bucket"}, {"storage_version": 5}

        async def _stream(*_a: Any, **kw: Any) -> Any:
            seen["stream_lock"] = kw.get("lock")
            return Response(status_code=200)

        async def _fast(*_a: Any, **_k: Any) -> Any:
            seen["fast"] = True
            return Response(status_code=200)

        async def _eligible(**_kw: Any) -> Any:
            return True, [], ""

        class _Repo:
            def __init__(self, _db: Any) -> None: ...

            async def get_by_path(self, _b: str, _k: str) -> Any:
                return None

        monkeypatch.setattr(mod, "ObjectRepository", _Repo)
        monkeypatch.setattr(mod, "resolve_copy_resources", _resolve)
        monkeypatch.setattr(mod, "handle_streaming_copy", _stream)
        monkeypatch.setattr(mod, "execute_v5_fast_path_copy", _fast)
        monkeypatch.setattr(mod, "should_use_v5_fast_path", _eligible)
        monkeypatch.setattr(mod, "is_multipart_object", lambda _r: multipart)
        monkeypatch.setattr(mod, "parse_copy_source", lambda _h: ("src-bucket", "src", None))
        monkeypatch.setattr(mod, "require_supported_storage_version", lambda v: v)
        return seen

    @staticmethod
    def _request(headers: dict[str, str]) -> Any:
        return SimpleNamespace(
            headers=headers,
            state=SimpleNamespace(main_account_id="acct", bucket_object_lock={"enabled": True}),
        )

    @pytest.mark.parametrize("multipart", [False, True])
    async def test_a_locked_copy_streams_with_the_lock(self, multipart: bool, monkeypatch: pytest.MonkeyPatch) -> None:
        seen = self._wire(monkeypatch, multipart=multipart)
        await mod.handle_copy_object("dst-bucket", "dst", self._request(self.LOCK), None, None)
        assert not seen["fast"], "a locked copy took the v5 fast path"
        mode, retain_until, legal_hold = seen["stream_lock"]
        assert mode == "COMPLIANCE"
        assert retain_until == datetime(2036, 1, 1, tzinfo=timezone.utc)
        assert not legal_hold

    async def test_an_unlocked_eligible_copy_keeps_the_fast_path(self, monkeypatch: pytest.MonkeyPatch) -> None:
        seen = self._wire(monkeypatch, multipart=False)
        await mod.handle_copy_object("dst-bucket", "dst", self._request({}), None, None)
        assert seen["fast"], "an ordinary copy lost the v5 fast path"
        assert seen["stream_lock"] == "not called"
