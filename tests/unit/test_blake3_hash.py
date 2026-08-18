from hippius_s3.blake3_hash import hex_of
from hippius_s3.blake3_hash import new_hasher


def test_hex_of_matches_published_abc_vector() -> None:
    # https://github.com/BLAKE3-team/BLAKE3 — test vector for "abc"
    assert hex_of(b"abc") == "6437b3ac38465133ffb63b75273a8db548c558465d79db03fd359c6cd5bd9d85"


def test_incremental_hasher_matches_one_shot() -> None:
    hasher = new_hasher()
    hasher.update(b"hel")
    hasher.update(b"lo")
    assert hasher.hexdigest() == hex_of(b"hello")
