"""Drift guards for config defaults that the docs describe (DOC-2 / DOC-3).

CLAUDE.md, todo.md, and reader/CLAUDE.md historically claimed the streaming prefetch depth
"defaults to 0" and the download-coalesce lock TTL defaults to 120. Both were wrong — the wired
config defaults are 16 and 600. These tests pin the real defaults so a future edit can't silently
re-diverge the code from the corrected docs.
"""

from __future__ import annotations

from hippius_s3.config import Config


def test_stream_prefetch_default_is_16(monkeypatch) -> None:
    # The streamer function-parameter fallback is 0, but the runtime/config default is 16.
    monkeypatch.delenv("HTTP_STREAM_PREFETCH_CHUNKS", raising=False)
    assert Config().http_stream_prefetch_chunks == 16


def test_download_coalesce_lock_ttl_default_is_600(monkeypatch) -> None:
    monkeypatch.delenv("DOWNLOAD_COALESCE_LOCK_TTL", raising=False)
    assert Config().download_coalesce_lock_ttl_seconds == 600
