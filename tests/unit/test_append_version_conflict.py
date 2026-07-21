"""AP-1: the pre-write append-version conflict predicate.

Before the expensive encrypt+FS write, append re-reads append_version and rejects fast if a concurrent
append already changed it — instead of paying the full write only to fail the finalize CAS.
"""

from __future__ import annotations

from hippius_s3.writer.object_writer import _append_version_conflict


def test_matching_version_is_not_a_conflict() -> None:
    assert _append_version_conflict(5, 5) is False


def test_changed_version_is_a_conflict() -> None:
    assert _append_version_conflict(6, 5) is True


def test_missing_row_is_not_a_conflict() -> None:
    # No row yet (None) → defer to the downstream reservation/finalize path, don't reject here.
    assert _append_version_conflict(None, 5) is False
