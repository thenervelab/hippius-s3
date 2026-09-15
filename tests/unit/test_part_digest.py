"""The uploader's part digest must equal the drain's, byte for byte — one golden pins both.

The Rust side asserts the same values in crates/hippius-drain-core/src/redrive.rs
(`the_digest_matches_the_python_uploaders_fold`). If either fold drifts, one of the two fails.
"""

from hippius_s3.workers.part_digest import chunk_hash
from hippius_s3.workers.part_digest import part_digest


CHUNK_ZERO = "03047aba0943318f2da44328856c1a7100c9239c4839cfe7fe36cdb2a2a255a2"
CHUNK_ONE = "f54ac4fc59ff7f7010e4d2433baf48beee4300b92a58b92e15b80b6472f440ff"
GOLDEN_TWO = "3e85e400a0b5249473ce9325322b8d66d91b32008603a7a52c1c9a44b6efe5d6"
GOLDEN_EMPTY = "4897c99081ad24f71fb73a2489ecb5ac5e5c2f2f2d14f64783ed2233c58185c8"


def test_chunk_hash_is_lowercase_hex_sha256() -> None:
    assert chunk_hash(b"chunk zero") == CHUNK_ZERO
    assert chunk_hash(b"chunk one") == CHUNK_ONE


def test_the_fold_matches_the_drains_golden() -> None:
    assert part_digest([CHUNK_ZERO, CHUNK_ONE]) == GOLDEN_TWO
    assert part_digest([]) == GOLDEN_EMPTY


def test_the_fold_separates_order_and_count() -> None:
    assert part_digest([CHUNK_ONE, CHUNK_ZERO]) != GOLDEN_TWO
    assert part_digest([CHUNK_ZERO]) != GOLDEN_TWO
