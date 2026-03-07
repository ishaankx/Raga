# services/ingestion/ingest.py
import os
import logging
from typing import List
from qdrant_client import QdrantClient
from qdrant_client.http.models import PointStruct, VectorParams, Distance
from sentence_transformers import SentenceTransformer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ingest")

# Config (override with env or .env.dev)
QDRANT_URL = os.getenv("QDRANT_URL")
QDRANT_API_KEY = os.getenv("QDRANT_API_KEY")
EMBED_MODEL = os.getenv("EMBEDDING_MODEL", "all-MiniLM-L6-v2")
COLLECTION = os.getenv("COLLECTION_NAME", "cinntra_docs")
# default to repository path for dev convenience
DATA_DIR = os.getenv("INGEST_DATA_DIR", "./services/ingestion/data")
BATCH_SIZE = int(os.getenv("INGEST_BATCH_SIZE", "128"))
CHUNK_SIZE_WORDS = int(os.getenv("INGEST_CHUNK_SIZE_WORDS", "400"))

logger.info("Ingest config: QDRANT_URL=%s EMBED_MODEL=%s COLLECTION=%s DATA_DIR=%s",
            QDRANT_URL, EMBED_MODEL, COLLECTION, DATA_DIR)

# Ensure data dir exists (especially inside Docker)
os.makedirs(DATA_DIR, exist_ok=True)

# clients
client = QdrantClient(url=QDRANT_URL, api_key=QDRANT_API_KEY or None)
embedder = SentenceTransformer(EMBED_MODEL)


def chunk_text_by_words(text: str, chunk_size: int = CHUNK_SIZE_WORDS) -> List[str]:
    words = text.split()
    chunks = []
    for i in range(0, len(words), chunk_size):
        chunks.append(" ".join(words[i:i+chunk_size]))
    return chunks


def ensure_collection():
    try:
        cols = [c.name for c in client.get_collections().collections]
    except Exception:
        logger.exception("Failed to query collections from Qdrant")
        cols = []
    if COLLECTION not in cols:
        dim = embedder.get_sentence_embedding_dimension()
        logger.info("Creating collection %s with dim=%s", COLLECTION, dim)
        client.create_collection(
            collection_name=COLLECTION,
            vectors_config=VectorParams(size=dim, distance=Distance.COSINE)
        )
    else:
        logger.info("Collection %s already exists", COLLECTION)


def ingest():
    ensure_collection()
    points = []
    next_id = 1

    for fname in sorted(os.listdir(DATA_DIR)):
        if not fname.lower().endswith(".txt"):
            continue
        path = os.path.join(DATA_DIR, fname)
        logger.info("Reading file: %s", path)
        with open(path, "r", encoding="utf-8") as f:
            text = f.read().strip()
        if not text:
            continue
        chunks = chunk_text_by_words(text, CHUNK_SIZE_WORDS)
        for i, c in enumerate(chunks):
            vec = embedder.encode(c).tolist()
            payload = {"source": fname, "chunk_index": i, "text": c}
            points.append(PointStruct(id=next_id, vector=vec, payload=payload))
            next_id += 1
            if len(points) >= BATCH_SIZE:
                logger.info("Uploading batch of %d points...", len(points))
                client.upsert(collection_name=COLLECTION, points=points)
                points = []
    if points:
        logger.info("Uploading final batch of %d points...", len(points))
        client.upsert(collection_name=COLLECTION, points=points)

    logger.info("Ingest done. Total indexed: approx %d", next_id - 1)


if __name__ == "__main__":
    ingest()