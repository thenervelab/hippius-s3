from hashlib import blake2b
from typing import Optional

import base58


SS58_CHECKSUM_PREFIX = b"SS58PRE"
SS58_ADDRESS_PATTERN = r"^[1-9A-HJ-NP-Za-km-z]{47,48}$"


def ss58_decode(address: str, valid_ss58_format: Optional[int] = None) -> str:
    """Decode an SS58 address to a hex account ID. Raises ValueError on invalid input."""
    if address.startswith("0x"):
        return address

    if address == "":
        raise ValueError("Empty address provided")

    address_decoded = base58.b58decode(address)

    if address_decoded[0] & 0b0100_0000:
        ss58_format_length = 2
        ss58_format = (
            ((address_decoded[0] & 0b0011_1111) << 2)
            | (address_decoded[1] >> 6)
            | ((address_decoded[1] & 0b0011_1111) << 8)
        )
    else:
        ss58_format_length = 1
        ss58_format = address_decoded[0]

    if ss58_format in [46, 47]:
        raise ValueError(f"{ss58_format} is a reserved SS58 format")

    if valid_ss58_format is not None and ss58_format != valid_ss58_format:
        raise ValueError("Invalid SS58 format")

    # Checksum length depends on total decoded length
    length = len(address_decoded)
    if length in [3, 4, 6, 10]:
        checksum_length = 1
    elif length in [5, 7, 11, 34 + ss58_format_length, 35 + ss58_format_length]:
        checksum_length = 2
    elif length in [8, 12]:
        checksum_length = 3
    elif length in [9, 13]:
        checksum_length = 4
    elif length in [14]:
        checksum_length = 5
    elif length in [15]:
        checksum_length = 6
    elif length in [16]:
        checksum_length = 7
    elif length in [17]:
        checksum_length = 8
    else:
        raise ValueError("Invalid address length")

    checksum = blake2b(SS58_CHECKSUM_PREFIX + address_decoded[0:-checksum_length]).digest()

    if checksum[0:checksum_length] != address_decoded[-checksum_length:]:
        raise ValueError("Invalid checksum")

    return address_decoded[ss58_format_length : len(address_decoded) - checksum_length].hex()


def is_valid_ss58_address(value: str, valid_ss58_format: Optional[int] = None) -> bool:
    """Check if a value is a valid SS58 formatted address."""
    if value.startswith("0x"):
        return False

    try:
        ss58_decode(value, valid_ss58_format=valid_ss58_format)
    except ValueError:
        return False

    return True
