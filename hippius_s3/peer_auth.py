from __future__ import annotations

import hmac
import re


# 64 hex characters, matching the shape CLAUDE.md already documents for HIPPIUS_SERVICE_KEY and
# HIPPIUS_AUTH_ENCRYPTION_KEY. Neither of those is enforced in code; this one is, because a
# malformed value here fails on the READ path rather than at the point of use.
_SECRET_PATTERN = re.compile(r"\A[0-9a-fA-F]{64}\Z")


class PeerSecretError(ValueError):
    """`HIPPIUS_INTERNAL_PEER_SECRET` is set to something the peer client cannot send."""


def validate_peer_secret(secret: str) -> None:
    """Refuse at boot a secret that would fail on every peer read.

    `httpx` ascii-encodes header values and raises `UnicodeEncodeError` doing it — a `ValueError`,
    which `PeerChunkFetcher.__call__`'s `except (httpx.HTTPError, OSError)` does not catch. So a
    non-ASCII secret does not degrade the peer tier to pool reads the way every other failure on
    that path does; it raises out of the GET and fails the request outright.

    Widening that except would be the wrong fix: it would turn an operator typo into a silently
    dark peer tier, visible only as `chunk_reads_by_tier_total{tier=peer}` going flat. Failing at
    startup puts the fault where someone is already looking.

    An EMPTY secret is legal and is not a misconfiguration — it is how `factory()` decides not to
    mount the peer route at all, and it is what prod runs today. Rejecting it here would turn this
    guard into a fleet-wide boot failure on the deploy that introduced it.

    Raises:
        PeerSecretError: naming the variable and the expected shape, and never echoing the
            rejected value — boot failures are logged, and a near-miss would leak an
            almost-correct secret to everyone with log access.
    """
    if not secret:
        return
    if not _SECRET_PATTERN.match(secret):
        raise PeerSecretError(
            "HIPPIUS_INTERNAL_PEER_SECRET must be 64 hex characters "
            "(generate with `openssl rand -hex 32`); leave it unset to disable peer serving"
        )


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
