import logging
from urllib.parse import unquote

from fastapi import Request


logger = logging.getLogger(__name__)

# One-shot, so a misconfigured server says so once rather than per request.
_WARNED_NO_RAW_PATH = False


def decoded_path(request: Request) -> str:
    """The request path, percent-decoded exactly once, with nothing dropped.

    NOT `request.url.path`. Starlette builds that with `urlsplit` over the already-decoded URL,
    and `urlsplit` treats `#` as the fragment delimiter — so a key sent as `report%23v1.txt`
    decodes to `report#v1.txt` and then loses everything from the `#`, leaving `report`. The
    truncated key reached storage, so `report#v1.txt` and `report#v2.txt` both landed as
    `report` and the second silently overwrote the first: a 200 OK on both, one object destroyed,
    nothing in the logs. It also meant `#` could never be caught by the object-key character
    checks, despite being in OBJECT_KEY_AVOID_CHARS all along.

    `scope["raw_path"]` is the undecoded bytes as the client sent them, so decoding it here keeps
    the `#`. This is the same discipline `sigv4.py` already uses to canonicalize.

    Every middleware that makes a routing or security decision on "the first path segment" must
    use this, not `request.url.path` — two middlewares disagreeing on where a path starts is how
    exempt-path bypasses get built.
    """
    raw = request.scope.get("raw_path")
    if raw is None:
        # Not every ASGI server populates raw_path. Falling back to the truncating path is still
        # better than 500-ing; uvicorn (what we run) always provides it.
        #
        # Logged, and once, because this fallback SILENTLY DEGRADES the guards built on top of
        # it. `request.url.path` is already truncated at `#`, so input_validation's refusal of
        # delimiters in the path can never fire on one, and every key-view check goes back to
        # judging a shortened key — the precise blindness this function exists to remove. A
        # server swap is the kind of change nobody would connect to a quiet re-opening of that
        # hole months later, so it says so the first time rather than never.
        global _WARNED_NO_RAW_PATH
        if not _WARNED_NO_RAW_PATH:
            _WARNED_NO_RAW_PATH = True
            logger.error(
                "ASGI server did not populate scope['raw_path']; falling back to the truncating "
                "request.url.path. Path-delimiter and object-key guards are DEGRADED until this "
                "is fixed."
            )
        return request.url.path
    return unquote(raw.decode("utf-8", "surrogateescape"))


def routing_path(request: Request) -> str:
    """The request path as the api will receive it. The only view a security check may judge.

    `decoded_path` for the characters the client actually sent, then `forwarded_path` for the two
    rewrites httpx performs on the way out. Every layer that decides "is this path exempt from
    auth", "which bucket is this", or "is this a reserved name" has to agree with the api about
    where the path starts, and the api's answer is the one that matters — it is what actually
    routes. Layers disagreeing on that is how `/docs/../anybucket/key` skipped authentication
    while being served from `anybucket`.
    """
    return forwarded_path(decoded_path(request))


def first_path_segment(request: Request) -> str:
    """The first segment of `routing_path`. `""` for `/`.

    Deliberately the routing view and not the path as sent: a helper that answered "the first
    segment the client typed" is the wrong question for every caller it has ever had, and having it
    available under a plausible name is how the bypass gets rebuilt.
    """
    return routing_path(request).strip("/").split("/", 1)[0]


def collapse_dot_segments(path: str) -> str:
    """`.` and `..` segments removed, exactly as httpx removes them (RFC 3986 §5.2.4).

    `ForwardService` hands a URL string built from `scope["path"]` to httpx, and httpx collapses
    dot segments before the request leaves the gateway — so `/anybucket/../internal/parts/...`
    arrives at the api as `/internal/parts/...`, and `/bucket/a/../b.txt` is stored as `b.txt`.

    Deliberately a segment-for-segment mirror of `httpx._urlparse.normalize_path` (0.28.x) rather
    than `posixpath.normpath`, whose extra rewrites (`//` collapse, trailing-slash drops) would
    make this diverge from what the forwarder actually sends;
    `test_collapse_matches_what_httpx_forwards` pins the parity against the installed httpx.

    This is the *destination* view — what a surviving request will be stored as. For the view a
    routing or security check needs, use `forwarded_path`: httpx also truncates the request target
    at `#`/`?`, and a check that ignores that judges a longer path than the api will route on.
    """
    if "." not in path:
        return path
    output: list[str] = []
    for segment in path.split("/"):
        if segment == ".":
            continue
        if segment == "..":
            # `output != [""]` keeps the leading slash: "/.." must collapse to "/", not "".
            if output and output != [""]:
                output.pop()
            continue
        output.append(segment)
    # httpx never sends an empty request target: a path that collapses to nothing ("/..",
    # "/a/..") goes out as "/", so it must be judged as "/" here too.
    return "/".join(output) or "/"


def forwarded_path(path: str) -> str:
    """The path exactly as the api will receive it: truncated at `#`/`?`, then dot-collapsed.

    Every routing or security decision keyed off "the first path segment" must run on this, not on
    the path as sent. `ForwardService` interpolates `scope["path"]` into a URL *string*
    (`f"{backend_url}{path}"`) and httpx re-parses it, so httpx rewrites it twice:

    - **`#`/`?` truncate the request target.** `scope["path"]` is already percent-decoded, so a
      client's `%23`/`%3F` is a literal `#`/`?` in that string and httpx reads it as the
      fragment/query delimiter. `/internal%23x/parts/1` has first segment `internal#x`, which is
      not `internal`, so it passed the denylist in `input_validation` — and reached the api as
      `GET /internal`, its S3 catch-all on the reserved bucket name `internal`. Truncation has
      nothing to do with dot segments, so it must be applied to every path, not only ones
      containing `.`.
    - **`.`/`..` collapse** — see `collapse_dot_segments`.

    NOT modelled, deliberately: httpx forwards percent-escapes in that string untouched and the api
    decodes them a *second* time, so `/%69nternal/parts/1` goes on the wire verbatim and is read by
    the api as `/internal/parts/1`. Decoding again here would decode object keys twice as well,
    turning a key sent as `a%252Fb` — rejected today for containing `%` — into the accepted key
    `a/b`. `input_validation` therefore refuses `%` in the first segment outright, and
    `test_percent_escapes_diverge_which_is_why_they_are_refused_instead` pins the divergence so
    that closing it forces a look at that rule.
    """
    # Whichever delimiter comes first ends the target; everything after it, dot segments included,
    # is never forwarded. httpx sends "/" rather than an empty target.
    delimiters = [index for index in (path.find("#"), path.find("?")) if index >= 0]
    if delimiters:
        path = path[: min(delimiters)] or "/"
    return collapse_dot_segments(path)
