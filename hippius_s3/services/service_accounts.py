from typing import Collection


def is_service_account(address: str | None, service_account_ids: Collection[str]) -> bool:
    """True iff `address` is an internal Hippius account exempt from billing.

    Takes the allowlist as an argument rather than reading the config singleton so both
    callers — the gateway's account middleware and the uploader worker — resolve it from
    their own already-loaded config, and so the predicate stays trivially testable.

    The comparison is exact and case-sensitive: SS58 is base58, where case is significant,
    so normalising would let a near-miss address match an allowlisted one.
    """
    if not address:
        return False
    return address in service_account_ids
