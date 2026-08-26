"""The plaintext-digest headers HEAD and GET emit.

An adversarial review of PR #453 mutation-tested the original change: every SQL mutation was
caught, and every PYTHON mutation slipped through green — deleting the header block from either
endpoint, renaming the header, and above all sourcing the digest from `arion_file_hash` (the exact
conflation the integration test's docstring says must never happen, guarded only at the SQL layer).
The emission layer had no coverage at all. These tests are that coverage.
"""

from __future__ import annotations

from typing import Any

import pytest

from hippius_s3.api.s3.common import body_blake3_headers


DIGEST = "6437b3ac38465133ffb63b75273a8db548c558465d79db03fd359c6cd5bd9d85"
DIGEST_HDR = "X-Hippius-Body-Blake3"
SCOPE_HDR = "X-Hippius-Body-Blake3-Scope"


def _row(**over: Any) -> dict[str, Any]:
    base = {"body_blake3": DIGEST, "multipart": False, "append_version": 0}
    base.update(over)
    return base


def test_simple_put_is_scoped_full() -> None:
    out = body_blake3_headers(_row())
    assert out[DIGEST_HDR] == DIGEST
    assert out[SCOPE_HDR] == "full"


def test_multipart_is_scoped_first_chunk() -> None:
    """MPU hashes only chunk 0 of part 1 — a client must not verify the whole body against it."""
    assert body_blake3_headers(_row(multipart=True))[SCOPE_HDR] == "first-chunk"


def test_appended_object_is_scoped_prefix() -> None:
    """The dangerous case: append never rehashes, so the value is stale but non-NULL.

    Without the qualifier this is indistinguishable from a current whole-body digest, sitting
    beside a Content-Length that has already grown past it.
    """
    assert body_blake3_headers(_row(append_version=3))[SCOPE_HDR] == "prefix"


def test_append_on_multipart_reports_prefix_not_first_chunk() -> None:
    """`prefix` is the weaker, truthful claim when both apply."""
    assert body_blake3_headers(_row(multipart=True, append_version=1))[SCOPE_HDR] == "prefix"


def test_scope_always_accompanies_the_digest() -> None:
    """Emitting the digest alone would restore the bug the scope exists to prevent."""
    for row in (_row(), _row(multipart=True), _row(append_version=2)):
        out = body_blake3_headers(row)
        assert (DIGEST_HDR in out) == (SCOPE_HDR in out) is True


@pytest.mark.parametrize(
    "value",
    [
        None,
        "",
        "pending",
        DIGEST[:63],
        DIGEST + "a",
        DIGEST.upper(),
        "g" * 64,
        f"{DIGEST[:32]}\r\nX-Injected: 1",
        123,
    ],
    ids=["null", "empty", "sentinel", "short", "long", "uppercase", "non-hex", "crlf", "not-a-str"],
)
def test_anything_that_is_not_a_lowercase_hex_digest_emits_nothing(value: Any) -> None:
    """The column is plain `text` with no CHECK.

    A CRLF or non-latin-1 value would raise at header-encoding time and turn a GET into a 500, so
    the digest is validated rather than trusted. Omitting beats emitting a sentinel a client could
    mistake for a real digest.
    """
    assert body_blake3_headers(_row(body_blake3=value)) == {}


def test_the_digest_is_not_sourced_from_the_arion_identifier() -> None:
    """Pins the conflation that slipped past the SQL-layer guard.

    X-Hippius-Arion-File-Hash is chunk_backend.backend_identifier — where the ENCRYPTED bytes live.
    This header is BLAKE3 of the PLAINTEXT. Both are 64 hex chars, so a refactor swapping one for
    the other is invisible to any test that only checks the shape of the value.
    """
    arion_identifier = "a" * 64
    out = body_blake3_headers({"body_blake3": DIGEST, "arion_file_hash": arion_identifier})
    assert out[DIGEST_HDR] == DIGEST
    assert out[DIGEST_HDR] != arion_identifier

    # ...and a row carrying ONLY the arion identifier must produce nothing at all.
    assert body_blake3_headers({"arion_file_hash": arion_identifier}) == {}


def test_missing_discriminators_default_to_full() -> None:
    """A row lacking multipart/append_version must not crash — it degrades to the common case."""
    assert body_blake3_headers({"body_blake3": DIGEST})[SCOPE_HDR] == "full"
