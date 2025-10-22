FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY . /app/hippius-s3/
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir /app/hippius-s3

COPY migrator/entrypoint.py /app/entrypoint.py

ENTRYPOINT ["python", "/app/entrypoint.py"]
