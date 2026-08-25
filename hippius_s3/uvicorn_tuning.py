def raise_receive_high_water(limit_bytes: int) -> None:
    """Widen uvicorn's per-connection body buffer from its hardcoded 64 KiB.

    Uvicorn pauses the transport whenever more than HIGH_WATER_LIMIT of un-consumed
    request body is buffered, and resumes only when the app's next `receive` drains
    it. 64 KiB means the wire stops every time the handler coroutine does anything
    besides awaiting receive — for a streaming PUT that is one pause/resume round
    trip per 64 KiB, whose fixed cost (loop wakeup + instrumented ASGI dispatch)
    was measured at ~160 µs and capped a single connection at ~181 MB/s while
    every thread sat idle. A ~1 MiB buffer rides through the handler's off-loop
    hops instead; the price is up to `limit_bytes` of RAM per in-flight upload.

    The constant is a module global with no setting, and both protocol impls
    `from`-import their own copy, so all three module namespaces must be set —
    patching only `flow_control` changes nothing. The impls read the name at
    request time, so this works whenever it runs before traffic; each uvicorn
    worker imports the app (and calls this) during its own startup.
    """
    if limit_bytes <= 0:
        return
    from uvicorn.protocols.http import flow_control
    from uvicorn.protocols.http import h11_impl
    from uvicorn.protocols.http import httptools_impl

    flow_control.HIGH_WATER_LIMIT = limit_bytes  # ty: ignore[invalid-assignment]
    h11_impl.HIGH_WATER_LIMIT = limit_bytes  # ty: ignore[invalid-assignment]
    httptools_impl.HIGH_WATER_LIMIT = limit_bytes  # ty: ignore[invalid-assignment]
