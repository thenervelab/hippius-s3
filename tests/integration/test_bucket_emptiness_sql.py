"""DeleteBucket's emptiness check and the multipart abort/complete claims, against real Postgres.

`bucket_emptiness` replaced a `list_objects` probe that only saw keys whose newest
version is live content. A versioned bucket whose keys were all hidden behind delete markers read as
empty, so DeleteBucket soft-deleted it and orphaned every non-current version under it — Object-Locked
ones included. Each case below is a bucket state that probe got wrong or that the fix must not
over-count.

`abort_multipart_upload` is here too because it is the other way data left a bucket through a
non-delete permission: a completed upload's parts cascade from its multipart_uploads row.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from typing import Any
from typing import AsyncGenerator

import asyncpg
import pytest
import pytest_asyncio

from hippius_s3.utils import get_query


pytestmark = pytest.mark.asyncio


class Ctx:
    def __init__(self, conn: asyncpg.Connection, bucket_id: uuid.UUID) -> None:
        self.conn = conn
        self.bucket_id = bucket_id


@pytest_asyncio.fixture
async def ctx(pg_conn: asyncpg.Connection) -> AsyncGenerator[Ctx, None]:
    bucket_id = uuid.uuid4()
    account = f"emptiness-{bucket_id}"
    await pg_conn.execute("INSERT INTO users (main_account_id) VALUES ($1) ON CONFLICT DO NOTHING", account)
    await pg_conn.execute(
        "INSERT INTO buckets (bucket_id, bucket_name, created_at, main_account_id, versioning_status) "
        "VALUES ($1, $2, now(), $3, 'Enabled')",
        bucket_id,
        f"emptiness-{bucket_id}",
        account,
    )
    try:
        yield Ctx(pg_conn, bucket_id)
    finally:
        await pg_conn.execute("DELETE FROM buckets WHERE bucket_id = $1", bucket_id)
        await pg_conn.execute("DELETE FROM users WHERE main_account_id = $1", account)


async def _object(
    ctx: Ctx,
    *,
    key: str,
    versions: list[dict[str, Any]],
    object_deleted: bool = False,
    upload_completed: bool = True,
) -> tuple[uuid.UUID, uuid.UUID]:
    """One object, one upload row, one part per version. Returns (object_id, upload_id).

    A version dict may set: size (default 10), md5 (default 'm'), marker, deleted, retain_until, hold.
    """
    object_id = uuid.uuid4()
    upload_id = uuid.uuid4()
    async with ctx.conn.transaction():
        await ctx.conn.execute(
            "INSERT INTO objects (object_id, bucket_id, object_key, created_at, current_object_version, deleted_at) "
            "VALUES ($1, $2, $3, now(), $4, $5)",
            object_id,
            ctx.bucket_id,
            key,
            max(v["version"] for v in versions),
            datetime.now(timezone.utc) if object_deleted else None,
        )
        await ctx.conn.execute(
            "INSERT INTO multipart_uploads (upload_id, bucket_id, object_key, initiated_at, is_completed, object_id) "
            "VALUES ($1, $2, $3, now(), $4, $5)",
            upload_id,
            ctx.bucket_id,
            key,
            upload_completed,
            object_id,
        )
        for v in versions:
            marker = v.get("marker", False)
            retain_until = v.get("retain_until")
            await ctx.conn.execute(
                "INSERT INTO object_versions (object_id, object_version, storage_version, size_bytes, md5_hash, "
                "content_type, is_delete_marker, deleted_at, object_lock_mode, object_lock_retain_until, "
                "object_lock_legal_hold) VALUES ($1, $2, 5, $3, $4, 'text/plain', $5, $6, $7, $8, $9)",
                object_id,
                v["version"],
                0 if marker else v.get("size", 10),
                None if marker else v.get("md5", "m"),
                marker,
                datetime.now(timezone.utc) if v.get("deleted") else None,
                "COMPLIANCE" if retain_until else None,
                retain_until,
                v.get("hold", False),
            )
            if not marker:
                await ctx.conn.execute(
                    "INSERT INTO parts (part_id, upload_id, object_id, object_version, part_number, size_bytes, "
                    "etag, uploaded_at) VALUES ($1, $2, $3, $4, 1, 10, 'e', now())",
                    uuid.uuid4(),
                    upload_id,
                    object_id,
                    v["version"],
                )
    return object_id, upload_id


async def _has_versions(ctx: Ctx) -> bool:
    row = await ctx.conn.fetchrow(get_query("bucket_emptiness"), ctx.bucket_id)
    return bool(row["has_versions"])


async def _list_objects_sees_any(ctx: Ctx) -> bool:
    """The probe DeleteBucket used before."""
    return bool(await ctx.conn.fetch(get_query("list_objects"), ctx.bucket_id, None, None, 1, None))


LATER = datetime.now(timezone.utc) + timedelta(days=30)
EARLIER = datetime.now(timezone.utc) - timedelta(days=1)


class TestBucketEmptiness:
    async def test_empty_bucket_is_empty(self, ctx: Ctx) -> None:
        assert await _has_versions(ctx) is False

    async def test_listable_key_counts(self, ctx: Ctx) -> None:
        await _object(ctx, key="k", versions=[{"version": 1}])
        assert await _has_versions(ctx) is True

    async def test_versions_hidden_behind_a_delete_marker_count(self, ctx: Ctx) -> None:
        """The regression: invisible to ListObjects, but the data version is still there."""
        await _object(ctx, key="k", versions=[{"version": 1}, {"version": 2, "marker": True}])
        assert await _list_objects_sees_any(ctx) is False, "precondition: ListObjects hides this key"
        assert await _has_versions(ctx) is True

    async def test_locked_version_behind_a_marker_counts(self, ctx: Ctx) -> None:
        await _object(ctx, key="k", versions=[{"version": 1, "retain_until": LATER}, {"version": 2, "marker": True}])
        assert await _list_objects_sees_any(ctx) is False
        assert await _has_versions(ctx) is True

    async def test_a_lone_delete_marker_counts(self, ctx: Ctx) -> None:
        """AWS: delete markers must be removed too before a bucket is empty."""
        await _object(ctx, key="k", versions=[{"version": 1, "deleted": True}, {"version": 2, "marker": True}])
        assert await _has_versions(ctx) is True

    async def test_a_locked_aborted_upload_placeholder_does_not_count(self, ctx: Ctx) -> None:
        """CreateMultipartUpload applies the lock to its reserved row; an abort then leaves that row
        with no data. Counting it would let any key that can initiate an upload make the bucket
        undeletable for the full retention, with nothing S3-visible the owner could remove."""
        await _object(ctx, key="k", versions=[{"version": 1, "size": 0, "md5": None, "hold": True}])
        assert await _has_versions(ctx) is False

    async def test_a_soft_deleted_object_without_a_lock_does_not_count(self, ctx: Ctx) -> None:
        """What an unversioned DELETE leaves behind until the hard-delete ring runs. Counting it
        would make 'delete every object, then the bucket' fail until the janitor catches up."""
        await _object(ctx, key="k", versions=[{"version": 1}], object_deleted=True)
        assert await _has_versions(ctx) is False

    async def test_open_upload_is_reported_separately(self, ctx: Ctx) -> None:
        """A PUT or MPU still being written holds an open upload row; it must block DeleteBucket
        even though its reserved version is not yet serveable."""
        await _object(ctx, key="k", versions=[{"version": 1, "size": 0, "md5": None}], upload_completed=False)
        row = await ctx.conn.fetchrow(get_query("bucket_emptiness"), ctx.bucket_id)
        assert row["has_versions"] is False
        assert row["has_open_uploads"] is True

    async def test_every_version_deleted_by_version_id_does_not_count(self, ctx: Ctx) -> None:
        await _object(ctx, key="k", versions=[{"version": 1, "deleted": True}, {"version": 2, "deleted": True}])
        assert await _has_versions(ctx) is False

    async def test_an_aborted_upload_placeholder_does_not_count(self, ctx: Ctx) -> None:
        """An abort on a brand-new key retains its zero-byte reserved version and leaves the object
        live. No client can see or delete it, so counting it would wedge the bucket forever."""
        await _object(ctx, key="k", versions=[{"version": 1, "size": 0, "md5": None}])
        assert await _has_versions(ctx) is False

    async def test_a_zero_byte_object_counts(self, ctx: Ctx) -> None:
        """Zero bytes but a real md5: a completed empty object, not a placeholder."""
        await _object(ctx, key="k", versions=[{"version": 1, "size": 0, "md5": "d41d8cd98f00b204e9800998ecf8427e"}])
        assert await _has_versions(ctx) is True

    async def test_other_buckets_do_not_leak_in(self, ctx: Ctx) -> None:
        other = uuid.uuid4()
        await ctx.conn.execute(
            "INSERT INTO buckets (bucket_id, bucket_name, created_at, main_account_id) VALUES ($1, $2, now(), $3)",
            other,
            f"emptiness-other-{other}",
            f"emptiness-{ctx.bucket_id}",
        )
        try:
            await _object(Ctx(ctx.conn, other), key="k", versions=[{"version": 1}], upload_completed=False)
            row = await ctx.conn.fetchrow(get_query("bucket_emptiness"), ctx.bucket_id)
            assert (row["has_versions"], row["has_open_uploads"]) == (False, False)
        finally:
            await ctx.conn.execute("DELETE FROM buckets WHERE bucket_id = $1", other)


class TestAbortMultipartUploadQuery:
    async def test_open_upload_is_deleted_with_its_parts(self, ctx: Ctx) -> None:
        _, upload_id = await _object(
            ctx, key="k", versions=[{"version": 1, "size": 0, "md5": None}], upload_completed=False
        )
        assert await ctx.conn.fetchrow(get_query("abort_multipart_upload"), upload_id) is not None
        assert await ctx.conn.fetchval("SELECT count(*) FROM parts WHERE upload_id = $1", upload_id) == 0

    async def test_completed_upload_and_its_parts_survive(self, ctx: Ctx) -> None:
        """The parts of a completed upload are the object. Deleting the row would cascade them away
        and destroy a committed version — through a permission that is not DeleteObject."""
        _, upload_id = await _object(ctx, key="k", versions=[{"version": 1, "retain_until": LATER}])
        assert await ctx.conn.fetchrow(get_query("abort_multipart_upload"), upload_id) is None
        assert await ctx.conn.fetchval("SELECT count(*) FROM parts WHERE upload_id = $1", upload_id) == 1


async def _second_connection() -> asyncpg.Connection:
    return await asyncpg.connect(dsn=os.environ["DATABASE_URL"])


class TestDeleteBucketLock:
    async def test_the_bucket_lock_waits_for_an_in_flight_create(self, ctx: Ctx) -> None:
        """A create still in its transaction holds FOR KEY SHARE on the bucket through its foreign
        key. DeleteBucket's FOR UPDATE must wait for it, and the emptiness check that follows must
        see the committed row — not the empty bucket it would have seen a moment earlier."""
        writer = await _second_connection()
        try:
            tx = writer.transaction()
            await tx.start()
            await writer.execute(
                "INSERT INTO multipart_uploads (upload_id, bucket_id, object_key, initiated_at, is_completed) "
                "VALUES ($1, $2, 'in-flight', now(), FALSE)",
                uuid.uuid4(),
                ctx.bucket_id,
            )

            async def delete_bucket_check() -> Any:
                async with ctx.conn.transaction():
                    assert await ctx.conn.fetchrow(get_query("lock_bucket_for_delete"), ctx.bucket_id)
                    return await ctx.conn.fetchrow(get_query("bucket_emptiness"), ctx.bucket_id)

            check = asyncio.create_task(delete_bucket_check())
            await asyncio.sleep(0.3)
            assert not check.done(), "the bucket lock must wait for the in-flight create"
            await tx.commit()
            row = await asyncio.wait_for(check, timeout=5)
            assert row["has_open_uploads"] is True
        finally:
            await writer.close()


class TestAbortCompleteRace:
    async def test_complete_first_leaves_abort_nothing_to_claim(self, ctx: Ctx) -> None:
        _, upload_id = await _object(
            ctx, key="k", versions=[{"version": 1, "size": 0, "md5": None}], upload_completed=False
        )
        completer = await _second_connection()
        try:
            tx = completer.transaction()
            await tx.start()
            flipped = await completer.fetchval(
                "UPDATE multipart_uploads SET is_completed = TRUE "
                "WHERE upload_id = $1 AND is_completed = FALSE RETURNING upload_id",
                upload_id,
            )
            assert flipped == upload_id

            abort = asyncio.create_task(ctx.conn.fetchrow(get_query("abort_multipart_upload"), upload_id))
            await asyncio.sleep(0.3)
            assert not abort.done(), "the abort must queue behind the completion's row lock"
            await tx.commit()
            assert await asyncio.wait_for(abort, timeout=5) is None
            assert await ctx.conn.fetchval("SELECT count(*) FROM parts WHERE upload_id = $1", upload_id) == 1
        finally:
            await completer.close()

    async def test_abort_first_makes_the_completion_flip_find_nothing(self, ctx: Ctx) -> None:
        _, upload_id = await _object(
            ctx, key="k", versions=[{"version": 1, "size": 0, "md5": None}], upload_completed=False
        )
        assert await ctx.conn.fetchrow(get_query("abort_multipart_upload"), upload_id) is not None
        flipped = await ctx.conn.fetchval(
            "UPDATE multipart_uploads SET is_completed = TRUE "
            "WHERE upload_id = $1 AND is_completed = FALSE RETURNING upload_id",
            upload_id,
        )
        assert flipped is None, "mpu_complete raises UploadNoLongerOpen on this and rolls back"

    async def test_a_null_is_completed_row_is_not_aborted(self, ctx: Ctx) -> None:
        """Not known to be open, so not deleted: its parts may be a committed object's data."""
        _, upload_id = await _object(ctx, key="k", versions=[{"version": 1}])
        await ctx.conn.execute("UPDATE multipart_uploads SET is_completed = NULL WHERE upload_id = $1", upload_id)
        assert await ctx.conn.fetchrow(get_query("abort_multipart_upload"), upload_id) is None


class TestApiAbortClaim:
    async def test_an_upload_in_progress_is_claimed(self, ctx: Ctx) -> None:
        _, upload_id = await _object(
            ctx, key="k", versions=[{"version": 1, "size": 0, "md5": None}], upload_completed=False
        )
        assert await ctx.conn.fetchrow(get_query("claim_upload_for_abort"), upload_id) is not None
        assert await ctx.conn.fetchval("SELECT count(*) FROM parts WHERE upload_id = $1", upload_id) == 0

    async def test_a_finished_put_still_in_its_tail_is_not_claimed(self, ctx: Ctx) -> None:
        """A simple PUT commits its serveable version with an OPEN upload row and flips it only
        after the address is written. The row is listed by ListMultipartUploads meanwhile, and
        aborting it cascaded away a finished — possibly Object-Locked — object."""
        _, upload_id = await _object(
            ctx, key="k", versions=[{"version": 1, "hold": True}], upload_completed=False
        )
        assert await ctx.conn.fetchrow(get_query("claim_upload_for_abort"), upload_id) is None
        assert await ctx.conn.fetchval("SELECT count(*) FROM parts WHERE upload_id = $1", upload_id) == 1

    async def test_a_completed_upload_is_not_claimed(self, ctx: Ctx) -> None:
        _, upload_id = await _object(ctx, key="k", versions=[{"version": 1, "size": 0, "md5": None}])
        assert await ctx.conn.fetchrow(get_query("claim_upload_for_abort"), upload_id) is None
