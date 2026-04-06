# Stage 1: Builder
FROM python:3.13-slim AS builder

WORKDIR /tmp

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    git \
    && rm -rf /var/lib/apt/lists/*

# Create virtual environment
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Copy requirements and install (includes git dependencies)
COPY services/websocket/requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Stage 2: Runtime
FROM python:3.13-slim

WORKDIR /app

# Install only curl for health checks
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy virtual environment from builder
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Create non-root user
RUN useradd -m -u 1000 websocket-service

# Copy application code and frontend
COPY services/websocket/app /app/app
COPY services/websocket/frontend /app/frontend

# Set ownership
RUN chown -R websocket-service:websocket-service /app

# Switch to non-root user
USER websocket-service

# Expose port
EXPOSE 8006

# Health check
HEALTHCHECK --interval=30s --timeout=10s --retries=3 --start-period=10s \
    CMD curl -f http://localhost:8006/live || exit 1

# Run application
CMD ["python", "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8006"]
