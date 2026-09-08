"""Service accounts cannot be purged, suspended or mass-deleted.

The threat here is not an attacker — it is one mistyped SS58 on a maintenance script, or an
admin call aimed at the wrong account. Every gate below is therefore placed BEFORE the first
side effect, and none of them takes a bypass parameter.
"""

import inspect
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from hippius_s3.services.service_accounts import ServiceAccountProtected
from hippius_s3.services.service_accounts import refuse_destructive_operation


SERVICE_ACCOUNT = "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY"
REGULAR_ACCOUNT = "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"
ALLOWLIST = frozenset({SERVICE_ACCOUNT})


# ---------------------------------------------------------------------------
# The guard
# ---------------------------------------------------------------------------


def test_refuses_a_service_account() -> None:
    with pytest.raises(ServiceAccountProtected, match="nuke_user refused"):
        refuse_destructive_operation(SERVICE_ACCOUNT, ALLOWLIST, operation="nuke_user")


def test_allows_a_regular_account() -> None:
    refuse_destructive_operation(REGULAR_ACCOUNT, ALLOWLIST, operation="nuke_user")


def test_allows_everything_when_the_allowlist_is_empty() -> None:
    refuse_destructive_operation(SERVICE_ACCOUNT, frozenset(), operation="nuke_user")


def test_the_message_names_the_escape_hatch() -> None:
    """An operator who hits this at 3am needs to be told how to proceed deliberately, or they
    will go looking for a --force flag that must not exist."""
    with pytest.raises(ServiceAccountProtected) as exc:
        refuse_destructive_operation(SERVICE_ACCOUNT, ALLOWLIST, operation="purge_buckets")
    assert "HIPPIUS_SERVICE_ACCOUNT_IDS" in str(exc.value)


def test_the_guard_takes_no_bypass_parameter() -> None:
    """The point of the declarative escape hatch. A boolean meaning "destroy the protected
    account anyway", living in a signature that admin endpoints and background workers call, is
    one careless refactor away from being passed True by something that should never have it —
    and that failure is silent and unrecoverable. Removing the address from the allowlist and
    redeploying is the only way through, because it is reviewed, recorded and attributable.
    """
    params = set(inspect.signature(refuse_destructive_operation).parameters)
    assert not params & {"force", "bypass", "override", "yes_really", "allow_service_accounts"}


# ---------------------------------------------------------------------------
# The purge worker — the second gate, behind the admin endpoint's
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_purge_worker_refuses_a_service_account_job() -> None:
    """A job can predate the allowlist, or an address can be added while a job sits queued —
    neither is visible to the admin endpoint. The worker must refuse before it deletes anything.
    """
    from hippius_s3.workers import purger

    db_pool = MagicMock()
    db_pool.fetch = AsyncMock(side_effect=AssertionError("must not read buckets for a service account"))
    db_pool.execute = AsyncMock(side_effect=AssertionError("must not delete anything"))
    config = MagicMock(service_account_ids=ALLOWLIST)
    job = {"job_id": "j1", "account_id": SERVICE_ACCOUNT, "deleted_objects": 0, "deleted_bytes": 0}

    with pytest.raises(ServiceAccountProtected):
        await purger._purge_account(db_pool, MagicMock(), MagicMock(), job, config)


@pytest.mark.asyncio
async def test_a_refused_purge_job_is_marked_failed_and_not_retried(monkeypatch: Any) -> None:
    """process_one_job's top-level handler must record the refusal on the job row. A job left
    'running' would be reclaimed after its lease and retried forever."""
    from hippius_s3.workers import purger

    updates: list[tuple] = []

    db_pool = MagicMock()
    db_pool.fetchrow = AsyncMock(
        return_value={"job_id": "j1", "account_id": SERVICE_ACCOUNT, "deleted_objects": 0, "deleted_bytes": 0}
    )

    async def record_execute(sql: str, *params: Any) -> None:
        updates.append((sql, params))

    db_pool.execute = record_execute
    monkeypatch.setattr(purger, "get_metrics_collector", lambda: MagicMock())

    handled = await purger.process_one_job(
        db_pool, MagicMock(), MagicMock(), MagicMock(service_account_ids=ALLOWLIST, purger_lease_seconds=60)
    )

    assert handled is True
    assert len(updates) == 1
    sql, params = updates[0]
    assert "state = 'failed'" in sql
    assert "ServiceAccountProtected" in params[1]


@pytest.mark.asyncio
async def test_purge_worker_still_purges_a_regular_account(monkeypatch: Any) -> None:
    """Blast-radius guard: the protection must not stop ordinary purges."""
    from hippius_s3.workers import purger

    db_pool = MagicMock()
    db_pool.fetch = AsyncMock(return_value=[])
    db_pool.execute = AsyncMock()
    db_pool.fetchrow = AsyncMock(return_value=None)
    config = MagicMock(service_account_ids=ALLOWLIST)
    job = {"job_id": "j1", "account_id": REGULAR_ACCOUNT, "deleted_objects": 0, "deleted_bytes": 0}

    deleted_objects, deleted_bytes = await purger._purge_account(db_pool, MagicMock(), MagicMock(), job, config)

    assert (deleted_objects, deleted_bytes) == (0, 0)


# ---------------------------------------------------------------------------
# Admin endpoints
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("endpoint", ["suspend", "purge"])
async def test_admin_refuses_before_writing_anything(monkeypatch: Any, endpoint: str) -> None:
    """purge_account_data upserts a `full` suspension BEFORE creating the job, so a check placed
    after that point would already have taken our own ingest offline. Assert nothing is written.
    """
    from fastapi import HTTPException

    from hippius_s3.api import admin

    monkeypatch.setattr(admin, "get_config", lambda: MagicMock(service_account_ids=ALLOWLIST))

    db = MagicMock()
    db.fetchrow = AsyncMock(side_effect=AssertionError("must not write for a service account"))
    request = MagicMock()

    with pytest.raises(HTTPException) as exc:
        if endpoint == "suspend":
            await admin.suspend_account(request, admin.SuspendBody(mode="full"), SERVICE_ACCOUNT, db)
        else:
            await admin.purge_account_data(request, SERVICE_ACCOUNT, db)

    assert exc.value.status_code == 403
    assert exc.value.detail["code"] == "ServiceAccountProtected"


@pytest.mark.asyncio
async def test_admin_still_suspends_a_regular_account(monkeypatch: Any) -> None:
    from hippius_s3.api import admin

    monkeypatch.setattr(admin, "get_config", lambda: MagicMock(service_account_ids=ALLOWLIST))
    monkeypatch.setattr(admin, "_write_suspension_cache", AsyncMock())

    db = MagicMock()
    db.fetchrow = AsyncMock(return_value={"mode": "full"})

    result = await admin.suspend_account(MagicMock(), admin.SuspendBody(mode="full"), REGULAR_ACCOUNT, db)

    assert result.state == "suspended"


# ---------------------------------------------------------------------------
# Global sweeps — no --address to refuse, so they filter rows instead
# ---------------------------------------------------------------------------


GLOBAL_SWEEPS = [
    "hippius_s3/scripts/purge_source_versions.py",
    "hippius_s3/scripts/delete_legacy_object_versions.py",
    "hippius_s3/scripts/cleanup_migration_versions.py",
]

PER_ACCOUNT_SCRIPTS = [
    "hippius_s3/scripts/nuke_user.py",
    "hippius_s3/scripts/purge_buckets.py",
]


# Destructive-looking but deliberately UNGUARDED, with the reason. Guarding these would be
# actively wrong: the first two act only on data the owner has ALREADY deleted or superseded —
# refusing them would leave our own released storage pinned and paid for forever — and the third
# removes multipart_uploads headers that have zero `parts` rows, i.e. no chunk data behind them,
# which is exactly what AbortMultipartUpload already does.
DELIBERATELY_UNGUARDED = {
    "hippius_s3/scripts/backfill_soft_delete_unpins.py": "acts only on rows already soft-deleted",
    "hippius_s3/scripts/backfill_superseded_version_unpins.py": "acts only on superseded versions",
    "hippius_s3/scripts/sweep_partless_multipart_uploads.py": "removes MPU headers with no parts behind them",
}


def test_every_destructive_script_is_accounted_for() -> None:
    """scripts/CLAUDE.md claims EVERY destructive path is guarded. This is what keeps that claim
    honest: a new bulk-delete script has to be classified here, or this fails. The original miss
    was cleanup_migration_versions.py — structurally identical to purge_source_versions.py, and
    silently unguarded while the docs said otherwise.
    """
    import pathlib

    guarded = set(GLOBAL_SWEEPS) | set(PER_ACCOUNT_SCRIPTS) | set(DELIBERATELY_UNGUARDED)
    unclassified = []
    for path in pathlib.Path("hippius_s3/scripts").glob("*.py"):
        source = path.read_text()
        # A bulk destructive script is one that DELETEs rows or enqueues unpins wholesale.
        destroys = "DELETE FROM" in source or "enqueue_unpin_request" in source
        if destroys and str(path) not in guarded:
            unclassified.append(str(path))

    assert not unclassified, (
        f"destructive script(s) with no service-account guard: {unclassified}. "
        f"Add the guard and list it above, or explain why it cannot touch account data."
    )


@pytest.mark.parametrize("script", GLOBAL_SWEEPS)
def test_global_sweeps_exclude_service_accounts_in_sql(script: str) -> None:
    """These take no --address, so a per-account refusal has nothing to refuse — the only way to
    protect our rows is to exclude them from the candidate query itself. Pinning the predicate's
    presence because losing it produces no error, just a sweep that quietly eats our data.
    """
    source = open(script).read()
    assert "main_account_id <> ALL(" in source
    assert "service_account_ids" in source


@pytest.mark.parametrize("script", PER_ACCOUNT_SCRIPTS)
def test_per_account_scripts_guard_before_touching_the_database(script: str) -> None:
    """The refusal has to precede the connect: a script that validates the user exists first
    would already have run against production before deciding not to."""
    source = open(script).read()
    guard = source.index("refuse_destructive_operation(")
    connect = source.index("asyncpg.connect(")
    assert guard < connect, f"{script}: guard must run before the DB connection"


@pytest.mark.parametrize("script", GLOBAL_SWEEPS + PER_ACCOUNT_SCRIPTS)
def test_every_script_refuses_to_run_without_the_allowlist_wired(script: str) -> None:
    """These are run by hand via `kubectl exec`, so which pod the operator picked decides whether
    the guard exists at all. An UNSET variable must abort — otherwise `<> ALL('{}')` matches every
    row and `refuse_destructive_operation` returns silently, and the protection reports nothing
    and does nothing.
    """
    source = open(script).read()
    assert "require_service_account_env(" in source, f"{script}: must fail closed on a missing allowlist"
    env_check = source.index("require_service_account_env(")
    connect = source.index("asyncpg.connect(")
    assert env_check < connect, f"{script}: env check must precede the DB connection"


def test_env_check_distinguishes_unset_from_empty(monkeypatch: Any) -> None:
    """Empty is a deliberate 'nothing is protected here'. Unset means nobody told this process,
    which is a different and more dangerous state — conflating them is the whole bug."""
    from hippius_s3.services.service_accounts import require_service_account_env

    monkeypatch.setenv("HIPPIUS_SERVICE_ACCOUNT_IDS", "")
    require_service_account_env("test")  # explicitly empty: allowed

    monkeypatch.delenv("HIPPIUS_SERVICE_ACCOUNT_IDS", raising=False)
    with pytest.raises(ServiceAccountProtected, match="not set in this environment"):
        require_service_account_env("test")
