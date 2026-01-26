ARG BASE_IMAGE=ghcr.io/thenervelab/hippius-s3/base:latest
FROM ${BASE_IMAGE}

ARG TARGETARCH
RUN apt-get update && apt-get install -y \
    curl \
    ca-certificates \
    && curl -fsSL -o /usr/local/bin/dbmate https://github.com/amacneil/dbmate/releases/latest/download/dbmate-linux-${TARGETARCH:-amd64} \
    && chmod +x /usr/local/bin/dbmate \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY . .

COPY start-api.sh /start-api.sh
RUN chmod +x /start-api.sh

CMD ["/start-api.sh"]
