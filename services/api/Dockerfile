# Dockerfile  (repo ROOT — used by Render)
FROM python:3.10-slim

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       build-essential \
       libpq-dev \
       curl \
    && rm -rf /var/lib/apt/lists/*

COPY services/api/requirements.txt ./requirements.txt

RUN pip install --upgrade pip

# CPU-only torch — much smaller than the default CUDA build
RUN pip install --no-cache-dir torch --index-url https://download.pytorch.org/whl/cpu

RUN pip install --no-cache-dir -r requirements.txt

# Pre-download the embedding model at BUILD time (not startup)
# NOTE: Cross-encoder reranker (~300MB) is intentionally excluded here.
# Render free tier has 512MB RAM — loading both models at startup causes OOM (exit 137).
# Set RERANK_MODEL="" in Render env vars to disable it, or upgrade to a paid plan.
RUN python - <<'PY'
from sentence_transformers import SentenceTransformer
SentenceTransformer("all-MiniLM-L6-v2")
print("Embedding model pre-loaded OK")
PY

COPY services/api/ .

EXPOSE 10000

# KEY FIX: Render injects $PORT (usually 10000). Hardcoding 8000 causes
# "No open ports detected" because Render scans $PORT, not 8000.
CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port ${PORT:-10000} --workers 1"]