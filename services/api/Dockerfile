# services/api/Dockerfile
FROM python:3.10-slim
WORKDIR /app

RUN apt-get update && apt-get install -y build-essential git curl wget && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --upgrade pip

# install CPU-only torch from official CPU wheels (downloads the correct wheel)
RUN pip install --no-cache-dir torch -f https://download.pytorch.org/whl/cpu/torch_stable.html

# then install the rest of requirements (without torch)
RUN pip install --no-cache-dir -r requirements.txt

# Pre-download the embedding model to avoid HF pulls at runtime (optional)
RUN python - <<'PY'
from sentence_transformers import SentenceTransformer
print("Preloading embedding model all-MiniLM-L6-v2")
SentenceTransformer("all-MiniLM-L6-v2")
print("done")
PY

COPY . .

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]