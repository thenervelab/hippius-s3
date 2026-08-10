"""A shorter re-upload must not leave surplus part_chunks rows, against real Postgres.

`parts` is updated in place on a re-upload (same part_id, new size and etag) while part_chunks
rows were only ever inserted. A 9 MiB attempt followed by a 5 MiB one therefore left a third row
describing a chunk that is neither on disk nor part of the object.

That row is not cosmetic. It never gains a chunk_backend row, and
`janitor_evictable_candidates.sql` admits a part only when EVERY part_chunks row has a live one —
so the part stays on the ingest SSD permanently, and `find_underreplicated_live_chunks` reports it
forever. Reads are unaffected (they plan from parts/meta), which is exactly why it went unnoticed:
the only symptom is disk that never comes back.

Against real Postgres rather than mocked, because the whole behaviour is in the SQL of
`insert_part_chunk_placeholders.sql` — the data-modifying CTE, the `chunk_index >= cardinality`
bound, and the `ON DELETE CASCADE` on chunk_backend that makes deleting a backed row unsafe.
A mock accepts any statement.
"""

from __future__ import annotations

import uuid
from typing import Any

import asyncpg
import pytest

from hippius_s3.services.parts_service import upsert_part_placeholder


pytestmark = pytest.mark.asyncio


async def _mk_object(conn: asyncpg.Connection) -> tuple[str, str, str]:
    """A bucket + object + upload row, enough for parts' foreign keys. Rolled back by pg_tx."""
    account = f"acct-{uuid.uuid4().hex[:8]}"
    bucket_id = str(uuid.uuid4())
    object_id = str(uuid.uuid4())
    upload_id = str(uuid.uuid4())
    await conn.execute("INSERT INTO users (main_account_id) VALUES ($1)", account)
    await conn.execute(
        "INSERT INTO buckets (bucket_id, bucket_name, created_at, main_account_id) VALUES ($1, $2, now(), $3)",
        bucket_id,
        f"b-{uuid.uuid4().hex[:8]}",
        account,
    )
    await conn.execute(
        "INSERT INTO objects (object_id, bucket_id, object_key, created_at, current_object_version) "
        "VALUES ($1, $2, $3, now(), 1)",
        object_id,
        bucket_id,
        f"k-{uuid.uuid4().hex[:8]}",
    )
    await conn.execute(
        "INSERT INTO object_versions (object_id, object_version, storage_version, size_bytes, content_type) "
        "VALUES ($1, 1, 5, 0, 'application/octet-stream')",
        object_id,
    )
    await conn.execute(
        "INSERT INTO multipart_uploads (upload_id, bucket_id, object_key, initiated_at, object_id) "
        "VALUES ($1, $2, $3, now(), $4)",
        upload_id,
        bucket_id,
        "k",
        object_id,
    )
    return object_id, upload_id, bucket_id


async def _upload(conn: asyncpg.Connection, object_id: str, upload_id: str, sizes: list[int]) -> None:
    await upsert_part_placeholder(
        conn,
        object_id=object_id,
        upload_id=upload_id,
        part_number=1,
        size_bytes=sum(sizes),
        etag=uuid.uuid4().hex,
        chunk_size_bytes=4 * 1024 * 1024,
        object_version=1,
        chunk_cipher_sizes=sizes,
    )


async def _chunks(conn: asyncpg.Connection, object_id: str) -> list[tuple[int, int]]:
    rows = await conn.fetch(
        """
        SELECT pc.chunk_index, pc.cipher_size_bytes
        FROM part_chunks pc JOIN parts p ON p.part_id = pc.part_id
        WHERE p.object_id = $1 ORDER BY pc.chunk_index
        """,
        object_id,
    )
    return [(int(r["chunk_index"]), int(r["cipher_size_bytes"])) for r in rows]


async def test_a_shorter_reupload_drops_the_surplus_chunk_rows(pg_tx: asyncpg.Connection) -> None:
    """The reported case: 3 chunks then 2. The third row must not survive."""
    object_id, upload_id, _ = await _mk_object(pg_tx)

    await _upload(pg_tx, object_id, upload_id, [4194332, 4194332, 1048604])
    assert len(await _chunks(pg_tx, object_id)) == 3

    await _upload(pg_tx, object_id, upload_id, [4194332, 1048604])

    assert [c[0] for c in await _chunks(pg_tx, object_id)] == [0, 1], (
        "the 9 MiB attempt's third chunk row outlived the 5 MiB attempt that replaced it"
    )


async def test_the_surviving_rows_describe_the_winning_attempt(pg_tx: asyncpg.Connection) -> None:
    """`ON CONFLICT DO NOTHING` kept the FIRST attempt's cipher sizes on the overlapping rows.

    Invisible whenever both attempts encrypt to the same length, and wrong for any re-upload that
    does not — the row then describes ciphertext the disk no longer holds.
    """
    object_id, upload_id, _ = await _mk_object(pg_tx)

    await _upload(pg_tx, object_id, upload_id, [1111, 2222])
    await _upload(pg_tx, object_id, upload_id, [3333, 4444])

    assert await _chunks(pg_tx, object_id) == [(0, 3333), (1, 4444)]


async def test_a_longer_reupload_keeps_every_row(pg_tx: asyncpg.Connection) -> None:
    """The bound is `chunk_index >= count`: growing must delete nothing."""
    object_id, upload_id, _ = await _mk_object(pg_tx)

    await _upload(pg_tx, object_id, upload_id, [10, 20])
    await _upload(pg_tx, object_id, upload_id, [10, 20, 30])

    assert [c[0] for c in await _chunks(pg_tx, object_id)] == [0, 1, 2]


async def test_a_stale_row_already_on_a_backend_is_kept_not_cascaded_away(pg_tx: asyncpg.Connection) -> None:
    """The deliberate exception, and the reason the delete is not unconditional.

    chunk_backend.chunk_id is ON DELETE CASCADE, so deleting a stale row whose ciphertext already
    reached a backend would take the backend_identifier with it and strand that object with
    nothing left to name it for the unpinner — and UnpinChainRequest is object/version-scoped, so
    no request can ask for one chunk back. Keeping the row keeps the part pinned, which is the
    pre-existing behaviour and strictly better than an unreclaimable remote object.
    """
    object_id, upload_id, _ = await _mk_object(pg_tx)
    await _upload(pg_tx, object_id, upload_id, [10, 20, 30])

    stale_id = await pg_tx.fetchval(
        """
        SELECT pc.id FROM part_chunks pc JOIN parts p ON p.part_id = pc.part_id
        WHERE p.object_id = $1 AND pc.chunk_index = 2
        """,
        object_id,
    )
    await pg_tx.execute(
        "INSERT INTO chunk_backend (chunk_id, backend, backend_identifier, deleted) VALUES ($1, 'arion', $2, false)",
        stale_id,
        "already-uploaded-identifier",
    )

    await _upload(pg_tx, object_id, upload_id, [10, 20])

    assert [c[0] for c in await _chunks(pg_tx, object_id)] == [0, 1, 2], "a backed stale row must survive"
    assert (
        await pg_tx.fetchval("SELECT backend_identifier FROM chunk_backend WHERE chunk_id = $1", stale_id)
        == "already-uploaded-identifier"
    ), "the backend pointer was cascaded away, stranding the object on the backend"


async def test_a_soft_deleted_backend_row_does_not_protect_a_stale_chunk(pg_tx: asyncpg.Connection) -> None:
    """`deleted = true` means the unpinner already reclaimed it, so there is nothing left to strand.

    It also matters for the pin: the evictable gate counts only LIVE chunk_backend rows, so a row
    protected by a soft-deleted one would pin the part forever with no way out.
    """
    object_id, upload_id, _ = await _mk_object(pg_tx)
    await _upload(pg_tx, object_id, upload_id, [10, 20, 30])

    stale_id = await pg_tx.fetchval(
        """
        SELECT pc.id FROM part_chunks pc JOIN parts p ON p.part_id = pc.part_id
        WHERE p.object_id = $1 AND pc.chunk_index = 2
        """,
        object_id,
    )
    await pg_tx.execute(
        "INSERT INTO chunk_backend (chunk_id, backend, backend_identifier, deleted) VALUES ($1, 'arion', $2, true)",
        stale_id,
        "already-unpinned",
    )

    await _upload(pg_tx, object_id, upload_id, [10, 20])

    assert [c[0] for c in await _chunks(pg_tx, object_id)] == [0, 1]


async def test_the_part_becomes_evictable_again(pg_tx: asyncpg.Connection) -> None:
    """The consequence the whole fix exists for, asserted as the janitor's gate states it.

    `janitor_evictable_candidates.sql` admits a part only when NO part_chunks row lacks a live
    chunk_backend row. Before the fix the surplus row failed that for the life of the object.
    """
    object_id, upload_id, _ = await _mk_object(pg_tx)
    await _upload(pg_tx, object_id, upload_id, [4194332, 4194332, 1048604])

    for idx in (0, 1):
        cid = await pg_tx.fetchval(
            """
            SELECT pc.id FROM part_chunks pc JOIN parts p ON p.part_id = pc.part_id
            WHERE p.object_id = $1 AND pc.chunk_index = $2
            """,
            object_id,
            idx,
        )
        await pg_tx.execute(
            "INSERT INTO chunk_backend (chunk_id, backend, backend_identifier, deleted) "
            "VALUES ($1, 'arion', $2, false)",
            cid,
            f"ident-{idx}",
        )

    await _upload(pg_tx, object_id, upload_id, [4194332, 1048604])

    has_unbacked = await pg_tx.fetchval(
        """
        SELECT EXISTS (
            SELECT 1 FROM part_chunks pc
            JOIN parts p ON p.part_id = pc.part_id
            WHERE p.object_id = $1
              AND NOT EXISTS (
                  SELECT 1 FROM chunk_backend cb
                  WHERE cb.chunk_id = pc.id AND cb.backend = 'arion' AND NOT cb.deleted
              )
        )
        """,
        object_id,
    )
    assert has_unbacked is False, "a surplus row with no backend keeps the part unevictable forever"


async def test_a_first_upload_deletes_nothing(pg_tx: asyncpg.Connection) -> None:
    """The overwhelmingly common path pays a statement but must never remove a row."""
    object_id, upload_id, _ = await _mk_object(pg_tx)

    await _upload(pg_tx, object_id, upload_id, [10, 20, 30])

    assert [c[0] for c in await _chunks(pg_tx, object_id)] == [0, 1, 2]


async def test_a_part_number_two_is_untouched_by_part_one(pg_tx: asyncpg.Connection) -> None:
    """The delete is scoped by part_id; a sibling part's rows are not its business."""
    object_id, upload_id, _ = await _mk_object(pg_tx)

    await upsert_part_placeholder(
        pg_tx,
        object_id=object_id,
        upload_id=upload_id,
        part_number=2,
        size_bytes=60,
        etag=uuid.uuid4().hex,
        chunk_size_bytes=4 * 1024 * 1024,
        object_version=1,
        chunk_cipher_sizes=[10, 20, 30],
    )
    await _upload(pg_tx, object_id, upload_id, [10, 20, 30])
    await _upload(pg_tx, object_id, upload_id, [10])

    rows: list[Any] = await pg_tx.fetch(
        """
        SELECT p.part_number, count(*) AS n
        FROM part_chunks pc JOIN parts p ON p.part_id = pc.part_id
        WHERE p.object_id = $1 GROUP BY p.part_number ORDER BY p.part_number
        """,
        object_id,
    )
    assert [(int(r["part_number"]), int(r["n"])) for r in rows] == [(1, 1), (2, 3)]
