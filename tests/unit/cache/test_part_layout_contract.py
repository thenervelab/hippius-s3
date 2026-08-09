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
import re
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


@pytest.mark.asyncio
async def test_meta_json_carries_exactly_the_keys_the_drain_deserializes(store) -> None:
    """`meta.json`'s KEYS are a contract, not just its filename.

    The agent's `MetaJson` has no serde default and no rename, so a key the api renamed becomes
    `InvalidData` on every part that node tries to drain — the whole node stops replicating rather
    than one part failing. Pinned as the exact set, since an extra key is as breaking as a missing
    one would be if `MetaJson` ever gained `deny_unknown_fields`.
    """
    golden = _golden()
    object_id = "466916c0-d61b-4518-b81b-9576b574270a"
    expected = golden["meta_payload"]
    await store.set_meta(
        object_id,
        1,
        1,
        chunk_size=expected["chunk_size"],
        num_chunks=expected["num_chunks"],
        size_bytes=expected["size_bytes"],
    )

    written = json.loads((Path(store.part_path(object_id, 1, 1)) / golden["meta_file"]).read_text())

    assert written == expected


def test_the_api_write_temp_is_shaped_so_the_agent_sweeps_it(store, tmp_path) -> None:
    """The other half of the retention contract: the api's write temps MUST be tmp-shaped.

    Staged chunks are deliberately not (see below), but a half-written chunk or meta from a killed
    api worker has nothing else to reclaim it — the drain's sweep is the only reaper, and it matches
    on `.tmp.`. If the api's temp suffix drifted, those files would accumulate on the ingest SSD
    with no reaper and no error. The uuid4 tail cannot be pinned, so the infix is, plus the tail's
    alphabet: a tail that grew a separator could turn a temp into something else's name.
    """
    golden = _golden()
    infix = golden["write_temp"]["api_infix"]
    part_dir = Path(store.part_path("466916c0-d61b-4518-b81b-9576b574270a", 1, 1))

    for target in (store._chunk_file(part_dir, 0), store._meta_file(part_dir)):
        generated = store._unique_tmp(target).name
        prefix = f"{target.name}{infix}"
        assert generated.startswith(prefix), generated
        assert re.fullmatch(r"[0-9a-f]+", generated.removeprefix(prefix)), generated

    assert golden["write_temp"]["api_chunk_example"].startswith(f"chunk_0.bin{infix}")
    assert golden["write_temp"]["api_meta_example"].startswith(f"{golden['meta_file']}{infix}")


def test_the_staged_infix_is_not_tmp_shaped() -> None:
    """A `.tmp.` in the staged name would hand it to the two millisecond-scale tmp sweepers.

    The drain reaps `*.tmp.*` on the ingest SSD at CEPHOR_RECLAIM_GRACE_SECS (1h) and the janitor
    at TMP_FILE_MAX_AGE_SECONDS (30m), both on the premise that a write temp lives for
    milliseconds. A staged chunk is held for the WHOLE of one UploadPart, so a tmp-shaped name
    would have a live multi-GB upload's data deleted under it.
    """
    assert ".tmp." not in _golden()["staged_infix"]
