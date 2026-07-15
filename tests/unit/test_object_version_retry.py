"""Adversarial unit tests for retry_on_object_version_conflict.

The helper wraps the version-reservation of every object write path (simple PUT,
multipart initiate, copy fast-path, upsert_with_cid) so a snapshot-stale
object_versions_pkey collision with a concurrent create_migration_version is
retried in a fresh statement. These pin the branch logic without a DB:
- succeed on first try (no wasted retries),
- retry on object_versions_pkey and converge,
- stay bounded and re-raise on persistent collision (never loop forever),
- NOT retry a unique violation on a different constraint (don't mask real bugs),
- never swallow unrelated exceptions.
"""

import asyncpg
import pytest

from hippius_s3.writer.db import OBJECT_VERSION_PK_CONSTRAINT
from hippius_s3.writer.db import retry_on_object_version_conflict


def _unique_violation(constraint: str) -> asyncpg.exceptions.UniqueViolationError:
    exc = asyncpg.exceptions.UniqueViolationError("duplicate key value violates unique constraint")
    exc.constraint_name = constraint
    return exc


@pytest.mark.asyncio
async def test_returns_first_result_without_retrying() -> None:
    calls = 0

    async def reserve() -> str:
        nonlocal calls
        calls += 1
        return "row"

    assert await retry_on_object_version_conflict(reserve) == "row"
    assert calls == 1


@pytest.mark.asyncio
async def test_retries_on_object_versions_pkey_then_succeeds() -> None:
    calls = 0

    async def reserve() -> str:
        nonlocal calls
        calls += 1
        if calls < 3:
            raise _unique_violation(OBJECT_VERSION_PK_CONSTRAINT)
        return "row-3"

    assert await retry_on_object_version_conflict(reserve, attempts=3) == "row-3"
    assert calls == 3


@pytest.mark.asyncio
async def test_reraises_after_exhausting_attempts_and_stays_bounded() -> None:
    calls = 0

    async def reserve() -> str:
        nonlocal calls
        calls += 1
        raise _unique_violation(OBJECT_VERSION_PK_CONSTRAINT)

    with pytest.raises(asyncpg.exceptions.UniqueViolationError):
        await retry_on_object_version_conflict(reserve, attempts=3)
    assert calls == 3


@pytest.mark.asyncio
async def test_single_attempt_reraises_immediately() -> None:
    calls = 0

    async def reserve() -> str:
        nonlocal calls
        calls += 1
        raise _unique_violation(OBJECT_VERSION_PK_CONSTRAINT)

    with pytest.raises(asyncpg.exceptions.UniqueViolationError):
        await retry_on_object_version_conflict(reserve, attempts=1)
    assert calls == 1


@pytest.mark.asyncio
async def test_does_not_retry_a_different_constraint() -> None:
    # A unique violation on any OTHER constraint (e.g. objects_pkey) is not the migration race —
    # it must surface immediately instead of being retried and masked.
    calls = 0

    async def reserve() -> str:
        nonlocal calls
        calls += 1
        raise _unique_violation("objects_pkey")

    with pytest.raises(asyncpg.exceptions.UniqueViolationError):
        await retry_on_object_version_conflict(reserve, attempts=3)
    assert calls == 1


@pytest.mark.asyncio
async def test_does_not_swallow_unrelated_errors() -> None:
    calls = 0

    async def reserve() -> str:
        nonlocal calls
        calls += 1
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        await retry_on_object_version_conflict(reserve, attempts=3)
    assert calls == 1
