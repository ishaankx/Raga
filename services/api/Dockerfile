# services/api/Dockerfile
# Build context = services/api/ so all paths are relative to that folder.
# In Render: Root Directory = services/api, Dockerfile Path = ./Dockerfile

FROM python:3.10-slim

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       build-essential \
       libpq-dev \
       curl \
    && rm -rf /var/lib/apt/lists/*

# requirements.txt is in services/api/ — just copy it directly
COPY requirements.txt .

RUN pip install --upgrade pip

# CPU-only torch to stay within Render free tier memory
RUN pip install --no-cache-dir torch --index-url https://download.pytorch.org/whl/cpu

RUN pip install --no-cache-dir -r requirements.txt

# Pre-download embedding model at build time (not at startup)
# Cross-encoder excluded — uses ~300MB RAM, exceeds Render free 512MB limit
RUN python - <<'PY'
from sentence_transformers import SentenceTransformer
SentenceTransformer("all-MiniLM-L6-v2")
print("Embedding model pre-loaded OK")
PY

# Copy all files in services/api/ into the container (main.py etc.)
COPY . .

EXPOSE 10000

# $PORT is injected by Render — must use it or Render kills container
CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port ${PORT:-10000} --workers 1"]