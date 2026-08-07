from __future__ import annotations

import hmac


# One definition, imported by both the fetcher and the endpoint. A header name duplicated as two
# string literals is how a fail-closed handshake silently stops matching: each side's own tests
# keep passing because each side agrees with itself.
#
# The X-Hippius- prefix is load-bearing, not cosmetic: the gateway strips every inbound
# x-hippius-* header before forwarding (gateway/services/forward_service.py), so a client
# cannot forge this one. That strip is what makes a shared secret sufficient here.
PEER_AUTH_HEADER = "X-Hippius-Peer-Auth"


def peer_auth_matches(presented: str | None, expected: str) -> bool:
    """Constant-time comparison of a presented peer secret against the configured one.

    Returns False when either side is empty, so an unset secret can never authenticate — the
    serve path must fail closed rather than degrade to "no auth required".
    """
    if not presented or not expected:
        return False
    # Compared as BYTES, never as str. `hmac.compare_digest` supports only ASCII when handed str,
    # and Starlette decodes header values as latin-1, so a header carrying any byte >= 0x80 arrives
    # here as a non-ASCII str and raises TypeError — surfacing as a 500 that confirms this route is
    # mounted, which is the existence oracle the endpoint's 404 exists to deny.
    #
    # errors="replace" cannot raise, and it cannot manufacture a match either. Not because of
    # anything about the secret's alphabet — nothing constrains it — but because the replace
    # path is unreachable from a header at all: latin-1 maps bytes 0x00-0xFF to U+0000-U+00FF
    # and back exactly, so anything decoded from the wire re-encodes to the identical bytes.
    # It is here so this function's "never raises" contract also holds for a caller that hands
    # it a str from somewhere other than a request header.
    return hmac.compare_digest(presented.encode("latin-1", errors="replace"), expected.encode("utf-8"))
