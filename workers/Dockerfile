ARG BASE_IMAGE=ghcr.io/thenervelab/hippius-s3/base:latest
FROM ${BASE_IMAGE}

COPY . .

COPY start-worker.sh /start-worker.sh
RUN chmod +x /start-worker.sh

CMD ["/start-worker.sh"]
