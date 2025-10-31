# ------------------------------------------------------------
# Dockerfile for building a PyECLib environment with ISA-L
# that supports ec_type="isa_l_rs_vand"
# ------------------------------------------------------------

FROM ubuntu:24.04

# System deps
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y \
    build-essential autoconf automake libtool pkg-config nasm \
    python3 python3-pip python3-dev git curl \
    && rm -rf /var/lib/apt/lists/*

# ------------------------------------------------------------
# Build ISA-L (Intel's SIMD Reed–Solomon library)
# ------------------------------------------------------------
WORKDIR /opt
RUN git clone https://github.com/intel/isa-l.git && \
    cd isa-l && \
    ./autogen.sh && ./configure && make -j$(nproc) && make install

# ------------------------------------------------------------
# Build liberasurecode (will auto-detect ISA-L)
# ------------------------------------------------------------
RUN git clone https://github.com/openstack/liberasurecode.git && \
    cd liberasurecode && \
    ./autogen.sh && ./configure && make -j$(nproc) && make install

# ------------------------------------------------------------
# Build PyECLib (links against liberasurecode)
# ------------------------------------------------------------
RUN pip install --upgrade pip setuptools wheel && \
    pip install pyeclib==1.7.0 fastapi==0.115.0 uvicorn==0.30.6 pydantic==2.9.2

# ------------------------------------------------------------
# Quick self-test to verify ISA-L backend loads
# (avoid heredoc for broader Docker compatibility)
# ------------------------------------------------------------
RUN python3 -c "from pyeclib.ec_iface import ECDriver; ECDriver(k=4, m=2, ec_type='isa_l_rs_vand'); print('ISA-L backend available')"

WORKDIR /workspace
COPY app.py /workspace/app.py
EXPOSE 8000
CMD ["uvicorn","app:app","--host","0.0.0.0","--port","8000"]
