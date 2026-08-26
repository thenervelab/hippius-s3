"""Every version-scoped statement in the append path must exclude tombstoned versions.

`append_stream` reserves a version, streams and encrypts the body (a window that can be seconds
long), then finalizes with a compare-and-swap. A versioned DELETE landing anywhere inside that
window tombstones the row. If finalize does not exclude tombstones it still finds the row, still
bumps size/md5/append_version, and returns 200 — for bytes that no read path will ever serve, since
every resolver filters `deleted_at`. Worse, the fresh `chunk_backend` rows it writes permanently
fail the reaper's "no live backend copy" gate, so the version, its parts and its FS bytes leak
forever while the DELETE's unpin destroys everything around them.

This is a STRUCTURAL bug, which is why it is tested structurally. There are FIVE version-scoped
statements in `append_stream`; only the reservation had the filter, and nothing detected the other
four because each one is individually plausible. Writing this guard is what found the fifth — the
post-CAS re-read, which reported a tombstoned version as AppendPreconditionFailed ("retry with
current version N") for an object that no longer existed.

A behavioural test would need to drive the whole encrypt+FS+MPU pipeline to reach the finalize, and
would still only cover whichever statement that particular path happened to hit. Reading the
statements out of the source covers all of them, and fails the moment a sixth is added without the
filter.
"""

from __future__ import annotations

import inspect
import re

from hippius_s3.writer.object_writer import ObjectWriter


def _append_stream_source() -> str:
    return inspect.getsource(ObjectWriter.append_stream)


def _version_scoped_statements(source: str) -> list[str]:
    """Every SQL literal in append_stream that reads or writes ONE row of `object_versions`.

    Scoped to `object_versions` deliberately. The same function also pins (object_id,
    object_version) against `parts`, which has no `deleted_at` column at all — a guard that swept
    those in would be unsatisfiable, and the usual way out is to loosen it until it catches nothing.
    """
    # Python implicit concatenation splits several of these across adjacent literals
    # (`"SELECT ... " \n "WHERE ..."`). Merge those pairs first or each half is inspected alone and
    # neither carries both the table name and the version predicate — the guard would then find
    # nothing to check and pass silently.
    source = re.sub(r'"\s*\n\s*"', "", source)
    literals = re.findall(r'"""(?:.|\n)*?"""|"(?:[^"\\\n]|\\.)*"', source)
    return [
        lit
        for lit in literals
        if "object_versions" in lit and re.search(r"object_version\s*=\s*\$2", lit, re.IGNORECASE)
    ]


def test_the_append_path_still_has_the_statements_this_guards() -> None:
    """If a refactor renames the parameters or restructures the WHERE clauses, the guard below
    would pass vacuously. Fail loudly instead."""
    found = _version_scoped_statements(_append_stream_source())

    assert len(found) >= 5, (
        f"expected at least the reservation, AP-1 precheck, finalize lock, finalize CAS and the "
        f"post-CAS re-read; found {len(found)}. The extraction has gone stale — update it rather "
        f"than deleting the guard, or this suite silently stops checking anything."
    )


def test_every_version_scoped_append_statement_excludes_tombstones() -> None:
    """The regression. Three of these four shipped without `deleted_at IS NULL`."""
    offenders = [
        stmt for stmt in _version_scoped_statements(_append_stream_source()) if "deleted_at IS NULL" not in stmt
    ]

    assert not offenders, (
        "these append statements pin a single object version without excluding tombstoned rows, "
        "so a versioned DELETE landing mid-append is invisible to them:\n\n"
        + "\n\n---\n\n".join(s.strip() for s in offenders)
    )


def test_a_tombstoned_finalize_cleans_up_rather_than_leaking_the_part() -> None:
    """The other half of the fix, and the reason it is not a one-line SQL change.

    Adding the filter makes finalize raise ObjectNotFound. The finalize block's only handler was
    `except AppendPreconditionFailed`, so ObjectNotFound escaped uncaught and the part row plus its
    FS directory were orphaned — trading a silent-success bug for a silent leak. ObjectNotFound must
    get the same cleanup a CAS failure gets.
    """
    source = _append_stream_source()

    assert re.search(r"except ObjectNotFound:\s*\n(?:\s*#.*\n)*\s*await _cleanup_part\(", source), (
        "finalize must clean up the written part when the version was tombstoned mid-append"
    )
    assert re.search(
        r"except ObjectNotFound:(?:.|\n){0,600}?await _delete_part_row\(\)",
        source,
    ), "finalize must also remove the parts row when the version was tombstoned mid-append"
