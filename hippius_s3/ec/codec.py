from __future__ import annotations

from typing import List

from pyeclib.ec_iface import ECDriver  # type: ignore


def encode_rs_systematic(blocks: List[bytes], k: int, m: int, symbol_size: int) -> tuple[bytes, ...]:
    """Encode with systematic Reed–Solomon over GF(2^8).

    - Data blocks: k
    - Parity blocks: m (>=1)
    - Each block is treated as a vector of ``symbol_size`` bytes; shorter inputs are zero-padded.

    Returns m parity blocks.
    """
    if k <= 0 or m <= 0 or symbol_size <= 0:
        return tuple(b"" for _ in range(max(0, m)))

    # Normalize number of data blocks
    data: List[bytes] = list(blocks[:k]) + [b""] * max(0, k - len(blocks))

    # Right-pad data fragments to symbol_size and concatenate into a single buffer of size k*symbol_size
    padded = [b.ljust(symbol_size, b"\x00")[:symbol_size] for b in data]
    joined = b"".join(padded)
    driver = ECDriver(k=int(k), m=int(m), ec_type="isa_l_rs_vand", chksum_type="none")
    fragments = driver.encode(joined)
    if not isinstance(fragments, list) or len(fragments) < k + m:
        raise RuntimeError("PyECLib encode returned unexpected fragments")
    parity_frags = fragments[k : k + m]
    return tuple(bytes(pf) for pf in parity_frags)
