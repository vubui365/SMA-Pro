# ── SMA v9999 Dockerfile ──────────────────────────────────────
FROM python:3.11-slim

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libffi-dev libssl-dev curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# App files
COPY server_v9999.py .
COPY index_v9999.html .

# Data directory
RUN mkdir -p /data

# Env defaults (override in docker-compose)
ENV PORT=9999 \
    WS_PORT=10000 \
    DB_PATH=/data/sma.db \
    SMA_ADMIN_PW="" \
    POLYGON_API_KEY="" \
    REDIS_URL="redis://redis:6379/0"

EXPOSE 9999 10000

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -sf http://localhost:9999/api/health || exit 1

CMD ["python", "server_v9999.py"]
