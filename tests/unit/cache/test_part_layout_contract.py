"""Cross-language contract for the on-disk part layout (api side).

The api derives a part's directory in `fs_store.part_path` and the drain agent derives it again
in `PartKey::relative_dir`; nothing connects the two but the fact that they happen to agree. That
agreement is not cosmetic — it is what makes two of this branch's safety properties work at all:

- `publish_part` takes `flock(LOCK_EX)` on the part directory and the drain's
  `remove_part_dir_exclusive` takes the same lock non-blockingly before `remove_dir_all`. They
  contend only because both `open(2)` the SAME directory. A one-character drift on either side
  (`v{n}` -> `version_{n}`, say) leaves both locks working perfectly on two different inodes, the
  mutual exclusion silently gone, and every existing test still green.
- A staged chunk must NOT be counted by the drain's completeness gate, which accepts exactly
  `chunk_<u32>.bin`. That holds because of how the staged name is SHAPED, so the shape is pinned
  here rather than left to a comment.

The companion assertions live in `crates/hippius-drain-agent/src/localfs.rs`
(`the_part_layout_matches_the_cross_language_golden`), against this same fixture. Drift on either
side fails one of the two tests. Same discipline as
`tests/unit/test_upload_chain_request_wire.py` for the drain -> uploader queue payload.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from hippius_s3.cache.fs_store import FileSystemPartsStore


GOLDEN = Path(__file__).resolve().parents[3] / "tests" / "fixtures" / "part_layout.golden.json"


def _golden() -> dict:
    return json.loads(GOLDEN.read_text())


@pytest.fixture()
def store(tmp_path) -> FileSystemPartsStore:
    return FileSystemPartsStore(str(tmp_path))


def test_every_golden_case_matches_what_the_api_derives(store, tmp_path) -> None:
    golden = _golden()
    assert golden["cases"], "an empty fixture would make this test vacuous"

    for case in golden["cases"]:
        part_dir = Path(store.part_path(case["object_id"], case["object_version"], case["part_number"]))

        assert part_dir.relative_to(tmp_path).as_posix() == case["relative_dir"]
        assert store._chunk_file(part_dir, case["chunk_index"]).name == case["chunk_file"]
        assert store._meta_file(part_dir).name == golden["meta_file"]
        assert store._staged_chunk_file(part_dir, case["chunk_index"], case["attempt_id"]).name == case["staged_file"]


def test_a_staged_name_is_the_chunk_name_plus_the_infix_and_attempt(store, tmp_path) -> None:
    """The shape, not just the literal — this is what keeps the drain's chunk parse from counting it.

    `parse_chunk_index` accepts only exactly `chunk_<u32>.bin`, so a staged file is excluded
    precisely because the index is followed by `.bin` AND MORE. Asserted as a relationship so a
    future rename that kept the fixture in sync but dropped the `.bin` (e.g. `chunk_0.staged.x`)
    still fails here.
    """
    for case in _golden()["cases"]:
        part_dir = Path(store.part_path(case["object_id"], case["object_version"], case["part_number"]))
        chunk = store._chunk_file(part_dir, case["chunk_index"]).name
        staged = store._staged_chunk_file(part_dir, case["chunk_index"], case["attempt_id"]).name

        assert staged == f"{chunk}{_golden()['staged_infix']}{case['attempt_id']}"
        assert staged.startswith(f"{chunk}.")
        assert staged != chunk


def test_the_staged_infix_is_not_tmp_shaped() -> None:
    """A `.tmp.` in the staged name would hand it to the two millisecond-scale tmp sweepers.

    The drain reaps `*.tmp.*` on the ingest SSD at CEPHOR_RECLAIM_GRACE_SECS (1h) and the janitor
    at TMP_FILE_MAX_AGE_SECONDS (30m), both on the premise that a write temp lives for
    milliseconds. A staged chunk is held for the WHOLE of one UploadPart, so a tmp-shaped name
    would have a live multi-GB upload's data deleted under it.
    """
    assert ".tmp." not in _golden()["staged_infix"]
