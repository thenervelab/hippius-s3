from __future__ import annotations

import base64
from typing import List

from fastapi import FastAPI
from pydantic import BaseModel
from pyeclib.ec_iface import ECDriver  # type: ignore


app = FastAPI()


class ReconstructRequest(BaseModel):
    k: int
    m: int
    missing_index: int
    parity: List[str]
    others: List[str]


class ReconstructResponse(BaseModel):
    reconstructed: str


class EncodeRequest(BaseModel):
    k: int
    m: int
    symbol_size: int
    blocks: List[str]


class EncodeResponse(BaseModel):
    parity: List[str]


@app.get("/health")
def health() -> dict:
    try:
        ECDriver(k=2, m=1, ec_type="isa_l_rs_vand", chksum_type="none")
        return {"status": "ok", "backend": "isa_l_rs_vand"}
    except Exception as e:  # pragma: no cover
        return {"status": "error", "error": str(e)}


@app.post("/reconstruct", response_model=ReconstructResponse)
def reconstruct(req: ReconstructRequest) -> ReconstructResponse:
    driver = ECDriver(k=int(req.k), m=int(req.m), ec_type="isa_l_rs_vand", chksum_type="none")
    parity = [base64.b64decode(p) for p in req.parity]
    others = [base64.b64decode(o) for o in req.others]

    # Pad to the same symbol size
    symbol_size = 0
    for b in others:
        symbol_size = max(symbol_size, len(b))
    for p in parity:
        symbol_size = max(symbol_size, len(p))

    def pad(b: bytes) -> bytes:
        return b.ljust(symbol_size, b"\x00")[:symbol_size]

    data_frags = []
    it = iter(others)
    for i in range(int(req.k)):
        if i == int(req.missing_index):
            data_frags.append(pad(b""))
        else:
            try:
                data_frags.append(pad(next(it)))
            except StopIteration:
                data_frags.append(pad(b""))
    frags = data_frags + [pad(p) for p in parity]
    erasures = [int(req.missing_index)]
    out = driver.reconstruct(frags, erasures)
    rec = bytes(out[0]) if out else b""
    return ReconstructResponse(reconstructed=base64.b64encode(rec).decode("ascii"))


@app.post("/encode", response_model=EncodeResponse)
def encode(req: EncodeRequest) -> EncodeResponse:
    driver = ECDriver(k=int(req.k), m=int(req.m), ec_type="isa_l_rs_vand", chksum_type="none")
    blocks = [base64.b64decode(b) for b in req.blocks]
    symbol_size = int(req.symbol_size)
    padded = [b.ljust(symbol_size, b"\x00")[:symbol_size] for b in blocks[: int(req.k)]]
    joined = b"".join(padded)
    fragments = driver.encode(joined)
    parity = fragments[int(req.k) : int(req.k) + int(req.m)]
    return EncodeResponse(parity=[base64.b64encode(p).decode("ascii") for p in parity])
