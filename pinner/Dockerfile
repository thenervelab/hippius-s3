FROM python:3.13-slim

# Set working directory
WORKDIR /app

# Install only the specific dependencies needed for pinner
RUN pip install --no-cache-dir \
    redis==5.1.1 \
    hippius==0.2.37 \
    python-dotenv==1.0.0

# Copy source code
COPY hippius_s3/ ./hippius_s3/
COPY pinner/ ./pinner/

# Set Python path
ENV PYTHONPATH=/app

# Run the pinner service
CMD ["python", "pinner/run_pinner_in_loop.py"]
