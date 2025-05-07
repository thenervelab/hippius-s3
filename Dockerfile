FROM python:3.11-slim

WORKDIR /app

# Install dependencies including dbmate, gcc, and build tools
RUN apt-get update && apt-get install -y \
    curl \
    ca-certificates \
    gcc \
    g++ \
    make \
    && curl -fsSL -o /usr/local/bin/dbmate https://github.com/amacneil/dbmate/releases/latest/download/dbmate-linux-amd64 \
    && chmod +x /usr/local/bin/dbmate \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy the entire project
COPY . .

# Install Python dependencies
RUN pip install --no-cache-dir -e .

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Create a startup script that runs dbmate and then uvicorn
RUN echo '#!/bin/bash\nset -e\n\n# Apply database migrations\necho "Applying database migrations..."\ndbmate wait\ndbmate up\n\n# Run the application\nexec uvicorn hippius_s3.main:app --host 0.0.0.0 --port 8000 --reload' > /start.sh && \
    chmod +x /start.sh

# Run the startup script
CMD ["/start.sh"]
