from __future__ import annotations


# The account middleware stamps unauthenticated callers with the literal id "anonymous" rather
# than None, so every downstream comparison against a real SS58 address sees an ordinary string.
# That is only safe while no *stored* owner column can hold one of these values — if it can, the
# sentinel matches itself and the caller becomes the owner. Kept as a set (not just the one
# string) because ownerless rows in the wild also carry '' and the textual nulls.
SENTINEL_ACCOUNT_IDS = frozenset({"", "anonymous", "none", "null", "undefined"})


def is_sentinel_account_id(account_id: str | None) -> bool:
    """True when `account_id` identifies nobody — an unauthenticated caller or an ownerless row."""
    if account_id is None:
        return True
    return account_id.strip().lower() in SENTINEL_ACCOUNT_IDS
