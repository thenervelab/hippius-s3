from urllib.parse import unquote

from fastapi import Request


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
        return request.url.path
    return unquote(raw.decode("utf-8", "surrogateescape"))


def first_path_segment(request: Request) -> str:
    """The first path segment, decoded. `""` for `/`."""
    return decoded_path(request).strip("/").split("/", 1)[0]


def collapse_dot_segments(path: str) -> str:
    """The path exactly as the api will receive it: `.` and `..` segments collapsed.

    The api never sees the path a client sent. `ForwardService` hands `scope["path"]` to httpx,
    and httpx removes dot segments (RFC 3986 §5.2.4) before the request leaves the gateway — so
    `/anybucket/../internal/parts/...` arrives at the api as `/internal/parts/...`. A security
    check keyed off "the first path segment" of the UNcollapsed path therefore judges a different
    request than the one it lets through. Any such check must run on this function's output.

    Deliberately a segment-for-segment mirror of `httpx._urlparse.normalize_path` (0.28.x) rather
    than `posixpath.normpath`, whose extra rewrites (`//` collapse, trailing-slash drops) would
    make this diverge from what the forwarder actually sends;
    `test_collapse_matches_what_httpx_forwards` pins the parity against the installed httpx.
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
