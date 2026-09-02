"""The delete endpoints must refuse a permanent delete of a locked version — and only that one.

AWS's two delete shapes behave differently on a locked version, and getting them backwards is the
usual implementation bug:

- `DELETE key?versionId=X`  -> 403 AccessDenied. Permanent, refused.
- `DELETE key` (no version) -> 200/204 and a DELETE MARKER. The locked version survives beneath it.

The second is what stops an ordinary client breaking against a lock it never asked about, so these
tests assert both directions, and assert that a refused delete enqueues NO unpin — the status code
is the symptom, the unpin is the harm.
"""

from __future__ import annotations

from datetime import datetime
from datetime import timedelta
from datetime import timezone
from types import SimpleNamespace
from typing import Any

import pytest

from hippius_s3.api.s3.objects import delete_object_endpoint


FUTURE = datetime.now(timezone.utc) + timedelta(days=365)
PAST = datetime.now(timezone.utc) - timedelta(days=1)
BYPASS = {"x-amz-bypass-governance-retention": "true"}


class _LockDb:
    """Serves the row-locked read with lock columns, and records what was asked of it."""

    def __init__(self, *, lock: dict[str, Any], locked_live_versions: int = 0) -> None:
        self.lock = lock
        self.locked_live_versions = locked_live_versions
        self.calls: list[str] = []

    def transaction(self) -> Any:
        db = self

        class _Tx:
            async def __aenter__(self) -> None:
                db.calls.append("BEGIN")

            async def __aexit__(self, *_: Any) -> None:
                db.calls.append("COMMIT")

        return _Tx()

    async def fetchrow(self, query: str, *args: Any) -> Any:
        self.calls.append(query)
        if query == "lock_object_and_get_version":
            return {
                "object_id": "obj-1",
                "current_object_version": 1,
                "object_version": 1,
                "is_delete_marker": False,
                "alias_count": 0,
                **self.lock,
            }
        if query == "count_locked_versions":
            return {"locked_count": self.locked_live_versions}
        if query == "soft_delete_object_version":
            return {"object_version": 1}
        if query == "repoint_current_version_after_delete":
            return {"object_version": 0}
        return None

    async def execute(self, *_a: Any, **_k: Any) -> str:
        return "OK"


def _request(*, is_owner: bool = True, headers: dict[str, str] | None = None) -> Any:
    return SimpleNamespace(
        state=SimpleNamespace(
            main_account_id="owner-acct",
            ray_id="ray-1",
            account=SimpleNamespace(main_account="owner-acct" if is_owner else "other-acct"),
            bucket_owner_id="owner-acct",
        ),
        query_params={"versionId": "1"},
        headers=headers or {},
    )


@pytest.fixture(autouse=True)
def _wiring(monkeypatch: pytest.MonkeyPatch) -> list[Any]:
    """Identity get_query, and capture any unpin the endpoint tries to enqueue."""
    enqueued: list[Any] = []
    monkeypatch.setattr(delete_object_endpoint, "get_query", lambda name: name)

    async def _capture(*_a: Any, **kw: Any) -> None:
        enqueued.append(kw)

    monkeypatch.setattr(delete_object_endpoint, "enqueue_object_unpin", _capture)
    return enqueued


def _locked(mode: str | None = "COMPLIANCE", until: datetime | None = FUTURE, hold: bool = False) -> dict[str, Any]:
    return {
        "object_lock_mode": mode,
        "object_lock_retain_until": until,
        "object_lock_legal_hold": hold,
    }


@pytest.mark.asyncio
class TestVersionedDeleteIsRefused:
    async def test_compliance_locked_version_is_403(self, _wiring: list[Any]) -> None:
        db = _LockDb(lock=_locked())
        resp = await delete_object_endpoint.delete_object_version("bkt", "k", 1, _request(), db)

        assert resp.status_code == 403
        assert b"AccessDenied" in bytes(resp.body)

    async def test_refused_delete_enqueues_no_unpin(self, _wiring: list[Any]) -> None:
        """The status code is the symptom; an unpin is the actual destruction of backend bytes."""
        db = _LockDb(lock=_locked())
        await delete_object_endpoint.delete_object_version("bkt", "k", 1, _request(), db)

        assert _wiring == [], "a refused delete still enqueued an unpin"

    async def test_refused_delete_does_not_soft_delete_the_version(self, _wiring: list[Any]) -> None:
        db = _LockDb(lock=_locked())
        await delete_object_endpoint.delete_object_version("bkt", "k", 1, _request(), db)

        assert "soft_delete_object_version" not in db.calls

    async def test_legal_hold_alone_is_403(self, _wiring: list[Any]) -> None:
        db = _LockDb(lock=_locked(mode=None, until=None, hold=True))
        resp = await delete_object_endpoint.delete_object_version("bkt", "k", 1, _request(), db)
        assert resp.status_code == 403

    async def test_expired_retention_with_live_hold_is_403(self, _wiring: list[Any]) -> None:
        db = _LockDb(lock=_locked(mode="GOVERNANCE", until=PAST, hold=True))
        resp = await delete_object_endpoint.delete_object_version("bkt", "k", 1, _request(), db)
        assert resp.status_code == 403

    async def test_compliance_is_403_even_for_the_owner_with_bypass(self, _wiring: list[Any]) -> None:
        db = _LockDb(lock=_locked())
        resp = await delete_object_endpoint.delete_object_version(
            "bkt", "k", 1, _request(is_owner=True, headers=BYPASS), db
        )
        assert resp.status_code == 403

    async def test_governance_is_403_without_bypass(self, _wiring: list[Any]) -> None:
        db = _LockDb(lock=_locked(mode="GOVERNANCE"))
        resp = await delete_object_endpoint.delete_object_version("bkt", "k", 1, _request(), db)
        assert resp.status_code == 403

    async def test_governance_is_403_for_non_owner_even_with_bypass(self, _wiring: list[Any]) -> None:
        db = _LockDb(lock=_locked(mode="GOVERNANCE"))
        resp = await delete_object_endpoint.delete_object_version(
            "bkt", "k", 1, _request(is_owner=False, headers=BYPASS), db
        )
        assert resp.status_code == 403

    async def test_legal_hold_is_403_even_with_owner_bypass(self, _wiring: list[Any]) -> None:
        """The governance bypass does not apply to a legal hold — it is not a retention mode."""
        db = _LockDb(lock=_locked(mode=None, until=None, hold=True))
        resp = await delete_object_endpoint.delete_object_version(
            "bkt", "k", 1, _request(is_owner=True, headers=BYPASS), db
        )
        assert resp.status_code == 403


@pytest.mark.asyncio
class TestVersionedDeleteIsAllowed:
    async def test_unlocked_version_deletes_normally(self, _wiring: list[Any]) -> None:
        db = _LockDb(lock=_locked(mode=None, until=None))
        resp = await delete_object_endpoint.delete_object_version("bkt", "k", 1, _request(), db)

        assert resp.status_code == 204
        assert "soft_delete_object_version" in db.calls
        assert len(_wiring) == 1, "an unlocked delete must still enqueue its unpin"

    async def test_expired_retention_deletes_normally(self, _wiring: list[Any]) -> None:
        """A lock must LAPSE — otherwise every locked object is undeletable forever."""
        db = _LockDb(lock=_locked(mode="COMPLIANCE", until=PAST))
        resp = await delete_object_endpoint.delete_object_version("bkt", "k", 1, _request(), db)
        assert resp.status_code == 204

    async def test_governance_owner_with_bypass_deletes(self, _wiring: list[Any]) -> None:
        db = _LockDb(lock=_locked(mode="GOVERNANCE"))
        resp = await delete_object_endpoint.delete_object_version(
            "bkt", "k", 1, _request(is_owner=True, headers=BYPASS), db
        )
        assert resp.status_code == 204
        assert len(_wiring) == 1

    async def test_governance_owner_without_the_header_is_refused(self, _wiring: list[Any]) -> None:
        """Being the owner is not consent on its own; AWS requires the explicit header too."""
        db = _LockDb(lock=_locked(mode="GOVERNANCE"))
        resp = await delete_object_endpoint.delete_object_version(
            "bkt", "k", 1, _request(is_owner=True, headers={}), db
        )
        assert resp.status_code == 403

    async def test_bypass_header_false_is_refused(self, _wiring: list[Any]) -> None:
        db = _LockDb(lock=_locked(mode="GOVERNANCE"))
        resp = await delete_object_endpoint.delete_object_version(
            "bkt", "k", 1, _request(is_owner=True, headers={"x-amz-bypass-governance-retention": "false"}), db
        )
        assert resp.status_code == 403
