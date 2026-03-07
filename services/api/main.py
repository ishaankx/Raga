# services/api/main.py
import os
import logging
import re
import json
from typing import List, Any, Dict, Optional
from decimal import Decimal
from datetime import date, datetime

from fastapi import FastAPI, HTTPException, Response, Body, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from qdrant_client import QdrantClient
from qdrant_client.http.models import Distance, VectorParams
from sentence_transformers import SentenceTransformer

try:
    from sentence_transformers.cross_encoder import CrossEncoder
except Exception:
    CrossEncoder = None

from google import genai
import psycopg2
from psycopg2.extras import RealDictCursor
from prometheus_client import Counter, Histogram, generate_latest

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("raga-api")

# ─────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────
QDRANT_URL      = os.getenv("QDRANT_URL", "http://qdrant:6333")
EMBED_MODEL     = os.getenv("EMBEDDING_MODEL", "all-MiniLM-L6-v2")
COLLECTION      = os.getenv("COLLECTION_NAME", "cinntra_docs")
GEMINI_API_KEY  = os.getenv("GEMINI_API_KEY")
GEMINI_MODEL    = os.getenv("GEMINI_MODEL", "gemini-3.1-flash-lite-preview")
DATABASE_URL    = os.getenv("DATABASE_URL", "postgresql://cinntra:cinntra@postgres:5432/cinntra")
RERANK_MODEL    = os.getenv("RERANK_MODEL", "cross-encoder/ms-marco-MiniLM-L-6-v2")
MAX_CONTEXT_CHARS = int(os.getenv("MAX_CONTEXT_CHARS", "3000"))
DOC_K           = int(os.getenv("DOC_K", "4"))


# ─────────────────────────────────────────────
# Client init
# ─────────────────────────────────────────────
if not GEMINI_API_KEY:
    logger.warning("GEMINI_API_KEY not set — LLM calls will be unavailable.")
    genai_client = None
else:
    try:
        genai_client = genai.Client(api_key=GEMINI_API_KEY)
        logger.info("GenAI client configured with model: %s", GEMINI_MODEL)
    except Exception:
        logger.exception("Failed to configure GenAI client")
        genai_client = None

app = FastAPI(title="Cinntra RAG + Analytics API")


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """
    Catch-all handler: any unhandled exception returns a proper JSON 500
    instead of an empty body, so the frontend can display the real message.
    """
    logger.exception("Unhandled exception on %s %s", request.method, request.url.path)
    return JSONResponse(
        status_code=500,
        content={"detail": f"{type(exc).__name__}: {exc}"},
    )
QDRANT_API_KEY = os.getenv("QDRANT_API_KEY")
qdrant = QdrantClient(url=QDRANT_URL, api_key=os.getenv("QDRANT_API_KEY") or None)
embedder = SentenceTransformer(EMBED_MODEL)

reranker = None
if CrossEncoder is not None and RERANK_MODEL:
    try:
        reranker = CrossEncoder(RERANK_MODEL)
        logger.info("Loaded reranker: %s", RERANK_MODEL)
    except Exception:
        logger.exception("Reranker load failed; disabled.")

# Ensure collection exists
try:
    existing = [c.name for c in qdrant.get_collections().collections]
    if COLLECTION not in existing:
        qdrant.create_collection(
            collection_name=COLLECTION,
            vectors_config=VectorParams(
                size=embedder.get_sentence_embedding_dimension(),
                distance=Distance.COSINE,
            ),
        )
        logger.info("Created Qdrant collection: %s", COLLECTION)
except Exception:
    logger.exception("Qdrant collection init failed (QDRANT_URL=%s)", QDRANT_URL)
    raise

# ─────────────────────────────────────────────
# Pydantic models
# ─────────────────────────────────────────────
class QueryRequest(BaseModel):
    q: str
    k: int = 5


# ─────────────────────────────────────────────
# Prometheus
# ─────────────────────────────────────────────
REQ_COUNT   = Counter("api_requests_total", "Total API requests", ["endpoint"])
REQ_LATENCY = Histogram("api_request_latency_seconds", "Request latency", ["endpoint"])


@app.get("/metrics")
def metrics():
    return Response(content=generate_latest(), media_type="text/plain; version=0.0.4; charset=utf-8")


# ─────────────────────────────────────────────
# DB helpers
# ─────────────────────────────────────────────
def _serialize_value(v: Any) -> Any:
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, (date, datetime)):
        return v.isoformat()
    return v


def serialize_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [{k: _serialize_value(v) for k, v in row.items()} for row in rows]


def run_sql(query: str, params: tuple = None) -> List[Dict[str, Any]]:
    conn = psycopg2.connect(os.getenv("DATABASE_URL", DATABASE_URL))
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params or ())
            try:
                rows = cur.fetchall()
            except psycopg2.ProgrammingError:
                rows = []
            return [dict(r) for r in rows]
    finally:
        conn.close()


def get_column_type(table: str, column: str) -> Optional[str]:
    rows = run_sql(
        "SELECT data_type FROM information_schema.columns WHERE table_name=%s AND column_name=%s LIMIT 1",
        (table, column),
    )
    return (rows[0].get("data_type") or "").lower() if rows else None


def dedupe_invoices(invoices: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Keep first occurrence of each invoice_no."""
    seen: set = set()
    out: List[Dict[str, Any]] = []
    for inv in invoices:
        key = inv.get("invoice_no") or json.dumps(inv, sort_keys=True)
        if key not in seen:
            seen.add(key)
            out.append(inv)
    return out


def try_sql_with_rewrites(sql: str) -> Dict[str, Any]:
    """Execute SQL; on integer/text mismatch attempt schema-aware rewrites."""
    try:
        return {"ok": True, "rows": run_sql(sql)}
    except Exception as e:
        err = str(e)
        logger.warning("SQL execution failed: %s", err)
        if "operator does not exist" in err and ("integer = text" in err or "text = integer" in err):
            if "c.id = i.customer" in sql:
                inv_type = get_column_type("invoices", "customer")
                if inv_type and inv_type.startswith(("character", "text", "varchar")):
                    attempt = sql.replace("c.id = i.customer", "c.name = i.customer")
                    try:
                        return {"ok": True, "rows": run_sql(attempt), "note": "rewrote join to c.name = i.customer"}
                    except Exception:
                        pass
                    attempt = sql.replace("c.id = i.customer", "c.id = CAST(i.customer AS INTEGER)")
                    try:
                        return {"ok": True, "rows": run_sql(attempt), "note": "cast i.customer to integer"}
                    except Exception:
                        pass
        return {"ok": False, "error": err}


# ─────────────────────────────────────────────
# Vector retrieval + reranking
# ─────────────────────────────────────────────
def vector_retrieve(q: str, k: int):
    """
    Return (texts, sources, top_rerank_score, top_vector_score).
    top_rerank_score: cross-encoder score if reranker loaded, else 0.0
    top_vector_score: raw Qdrant cosine similarity of best hit (always available)
    """
    try:
        qvec = embedder.encode(q).tolist()
        res  = qdrant.query_points(collection_name=COLLECTION, query=qvec, limit=k)
        pts  = res.points or []
    except Exception:
        logger.exception("Qdrant retrieve failed")
        pts = []

    docs    = [p.payload.get("text", "") for p in pts]
    sources = [p.payload.get("source", "unknown") for p in pts]
    top_vector_score = float(pts[0].score) if pts else 0.0

    if not docs:
        return [], [], 0.0, 0.0

    if reranker:
        pairs = [[q, d] for d in docs]
        try:
            scores = reranker.predict(pairs)
        except Exception:
            scores = [0.0] * len(docs)
        ranked = sorted(zip(docs, sources, scores), key=lambda x: x[2], reverse=True)
        top    = ranked[:3]
        docs    = [t[0] for t in top]
        sources = [t[1] for t in top]
        top_rerank_score = float(top[0][2]) if top else 0.0
    else:
        top_rerank_score = 0.0

    return docs, sources, top_rerank_score, top_vector_score


def build_context(sql_summary: dict, docs: List[str]) -> str:
    parts = []
    if sql_summary:
        parts.append("=== Structured Data (from database) ===")
        parts.append(f"Customer: {sql_summary.get('customer', 'N/A')}")
        parts.append(f"Total outstanding: {sql_summary.get('total_outstanding', 0)}")
        parts.append(f"Invoice count: {sql_summary.get('invoice_count', 0)}")
        for inv in sql_summary.get("recent_invoices", []):
            parts.append(
                f"  • {inv.get('invoice_no')} | {inv.get('amount')} {inv.get('currency')} "
                f"| {inv.get('status')} | due {inv.get('due_date')} | {inv.get('notes') or ''}"
            )
        parts.append("")

    for i, d in enumerate(docs):
        snippet = d[:800] + "..." if len(d) > 800 else d
        parts.append(f"=== Knowledge Doc {i+1} ===\n{snippet}")

    ctx = "\n".join(parts).strip()
    return ctx[:MAX_CONTEXT_CHARS] + "\n\n[TRUNCATED]" if len(ctx) > MAX_CONTEXT_CHARS else ctx


# ─────────────────────────────────────────────
# LLM helpers
# ─────────────────────────────────────────────
def llm_call(prompt: str) -> str:
    """Single GenAI call. Raises RuntimeError if client not available."""
    if genai_client is None:
        raise RuntimeError("GEMINI_API_KEY not configured")
    try:
        response   = genai_client.models.generate_content(model=GEMINI_MODEL, contents=prompt)
        text = getattr(response, "text", None)
        if not text:
            text = str(response)
        return text.strip()
    except Exception:
        logger.exception("LLM call failed")
        raise


def llm_answer_from_context(q: str, context: str) -> tuple[str, List[str]]:
    """Returns (answer_text, citations)."""
    prompt = f"""You are an enterprise ERP/CRM assistant.
Use ONLY the Context below to answer the question.
If the answer is not in the context, reply exactly: I don't know.

Context:
{context}

Question:
{q}

Return ONLY valid JSON with these keys:
- "answer": string (clear, human-readable, use bullet points or line breaks if listing items)
- "citations": list of strings like "doc1:filename" or "sql:invoices"
"""
    raw = llm_call(prompt)
    m   = re.search(r"\{.*\}", raw, re.S)
    if m:
        try:
            parsed = json.loads(m.group(0))
            return parsed.get("answer", raw), parsed.get("citations", [])
        except Exception:
            pass
    return raw, []


def classify_intent(q: str) -> str:
    """
    Use Gemini to classify the query into one of four modes:
      rag           — knowledge/explanation question
      hybrid        — customer-specific structured + knowledge question
      nl2sql        — user wants to see the SQL only
      nl2sql_execute — user wants data returned from a query
    Falls back to simple keyword heuristics if LLM unavailable.
    """
    if genai_client is not None:
        prompt = f"""Classify the following user query into EXACTLY ONE of these four categories.
Respond with only the category name, nothing else.

Categories:
- rag: General knowledge, explanations, policies, "how does X work", "what is X", "explain"
- hybrid: Invoice or customer question that mentions a specific company name OR asks about outstanding/overdue amounts for a customer
- nl2sql: User explicitly wants to see the SQL query (e.g. "write SQL", "generate query", "show me the SQL")
- nl2sql_execute: User wants data retrieved from structured tables (e.g. "show me all invoices", "list customers", "what is the total revenue", "top customers by amount")

Query: {q}

Category:"""
        try:
            mode = llm_call(prompt).strip().lower().split()[0]
            if mode in ("rag", "hybrid", "nl2sql", "nl2sql_execute"):
                return mode
        except Exception:
            pass

    # Keyword fallback
    t = q.lower()
    if any(w in t for w in ["write sql", "generate sql", "show sql", "give me the sql", "sql for"]):
        return "nl2sql"
    if any(w in t for w in ["total outstanding", "total due", "outstanding invoices", "overdue", "top customers", "revenue"]):
        return "nl2sql_execute"
    if any(w in t for w in ["explain", "what is", "how does", "define", "describe", "policy", "terms"]):
        return "rag"
    return "hybrid"


def nl_to_sql(nl: str, schema_hint: str = "") -> str:
    """Translate natural language to a single safe SELECT statement."""
    prompt = f"""You are an assistant that translates natural language to a single safe SQL SELECT statement.

Database schema:
- customers(id INTEGER, name TEXT)
- invoices(invoice_no TEXT, customer TEXT, amount NUMERIC, currency TEXT, issue_date DATE, due_date DATE, status TEXT, notes TEXT)

Business rules — apply these automatically based on the intent of the query:
- "overdue", "past due date", "past their due date" → due_date < CURRENT_DATE AND (status IS NULL OR LOWER(status) != 'paid')
- "outstanding" or "unpaid" → status IS NULL OR LOWER(status) != 'paid'
- "paid" → status IS NOT NULL AND LOWER(status) = 'paid'
- ALWAYS use SELECT DISTINCT invoice_no, customer, amount, currency, issue_date, due_date, status, notes (never SELECT *) to avoid duplicates.

SQL rules:
- Return ONLY the SQL SELECT statement — no explanation, no backticks, no comments.
- One statement only, ending with a semicolon.
- Do NOT use INSERT/UPDATE/DELETE/CREATE/DROP.
- If the request cannot be answered as a SELECT, return: I_CANNOT_GENERATE_SQL

Schema hint: {schema_hint}
Request: {nl}
"""
    try:
        sql = llm_call(prompt).strip()
        # Strip markdown fences and language hints (```sql ... ```)
        sql = re.sub(r"```(?:sql)?", "", sql, flags=re.IGNORECASE).strip()
        # Strip inline SQL comments on the first line
        sql = re.sub(r"^--[^\n]*\n", "", sql, flags=re.MULTILINE).strip()
        # Keep only up to and including the first semicolon
        if ";" in sql:
            sql = sql.split(";")[0].strip() + ";"
        logger.info("nl_to_sql generated: %s", sql[:200])
        return sql
    except Exception:
        logger.exception("nl_to_sql failed")
        return ""


def safe_sql_check(sql: str) -> bool:
    if not sql:
        return False
    s = sql.strip().lower()
    if not s.startswith("select"):
        return False
    forbidden = ["insert ", "update ", "delete ", "drop ", "create ", "alter ", "truncate ", "grant ", "revoke "]
    return not any(k in s for k in forbidden)


def compute_confidence(
    sql_used: bool,
    rerank_top_score: float,
    llm_used: bool,
    vector_score: float = 0.0,
) -> float:
    """
    Confidence breakdown:
      0.15  base  — pipeline ran at all
      0.35  SQL   — structured DB data was found and used
      0.25  LLM   — an LLM answer was generated (not a fallback string)
      0.25  reranker score (cross-encoder logits, shifted to 0-1 range)
        OR
      0.15  vector similarity (Qdrant cosine 0-1, lower weight — less discriminative)
    Max possible: 1.0
    """
    score = 0.15  # base

    if sql_used:
        score += 0.35
    if llm_used:                          # was inverted before — fixed
        score += 0.25

    if rerank_top_score and rerank_top_score != 0.0:
        # Cross-encoder logits: ~-3 = irrelevant, 0 = borderline, +7 = very relevant
        rs_norm = max(0.0, min(1.0, (float(rerank_top_score) + 3.0) / 10.0))
        score += 0.25 * rs_norm
    elif vector_score and vector_score > 0.0:
        # Qdrant cosine similarity fallback (range 0..1)
        score += 0.15 * max(0.0, min(1.0, float(vector_score)))

    return round(min(1.0, score), 3)


def format_rows_as_text(rows: List[Dict]) -> str:
    """Convert DB rows to readable text. Safely handles None / non-string values."""
    if not rows:
        return "No results found."
    # Single-column single-row (e.g. COUNT or SUM): show as "label: value"
    if len(rows) == 1 and len(rows[0]) == 1:
        key, val = next(iter(rows[0].items()))
        return f"{key}: {val if val is not None else '—'}"
    lines = []
    for r in rows:
        parts = []
        for k, v in r.items():
            safe_v = "—" if v is None else str(v)
            parts.append(f"{k}: {safe_v}")
        lines.append("  •  " + "  |  ".join(parts))
    return "\n".join(lines)


# ─────────────────────────────────────────────
# JSON safety helper
# ─────────────────────────────────────────────
def _jsonable(obj: Any) -> Any:
    """
    Recursively coerce any object that FastAPI cannot serialize by default.
    Handles Decimal, date/datetime, and nested structures.
    """
    if isinstance(obj, dict):
        return {k: _jsonable(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_jsonable(i) for i in obj]
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()
    return obj


# ─────────────────────────────────────────────
# Core handlers (used by /smart_query and legacy endpoints)
# ─────────────────────────────────────────────
def handle_rag(q: str, k: int) -> dict:
    docs, sources, top_rerank_score, top_vector_score = vector_retrieve(q, k)
    context = build_context({}, docs)

    if not docs:
        return {
            "question": q,
            "answer": "No relevant documents found in the knowledge base.",
            "sources": [],
            "confidence": 0.0,
            "retrieved": [],
        }

    if genai_client is None:
        return {
            "question": q,
            "answer": "LLM not configured (GEMINI_API_KEY missing).",
            "sources": sources[:3],
            "confidence": compute_confidence(False, top_rerank_score, False, top_vector_score),
            "retrieved": docs[:3],
        }

    answer, citations = llm_answer_from_context(q, context)
    return {
        "question": q,
        "answer": answer,
        "sources": citations or sources[:3],
        "confidence": compute_confidence(False, top_rerank_score, True, top_vector_score),
        "retrieved": docs[:3],
    }


def handle_hybrid(q: str, k: int) -> dict:
    # Try to find a named customer in the query
    try:
        customer_rows = run_sql("SELECT name FROM customers")
        customers     = [r.get("name") for r in customer_rows if r.get("name")]
    except Exception:
        customers = []

    customer_found = next((n for n in customers if n and n.lower() in q.lower()), None)

    sql_summary: dict = {}
    if customer_found:
        try:
            invoices = dedupe_invoices(run_sql(
                "SELECT invoice_no, amount, currency, issue_date, due_date, status, notes "
                "FROM invoices WHERE customer = %s ORDER BY due_date DESC",
                (customer_found,),
            ))
            total_outstanding = sum(
                float(inv.get("amount") or 0)
                for inv in invoices
                if (inv.get("status") or "").lower() != "paid"
            )
            sql_summary = {
                "customer": customer_found,
                "total_outstanding": round(total_outstanding, 2),
                "invoice_count": len(invoices),
                "recent_invoices": serialize_rows(invoices[:10]),
            }
        except Exception:
            logger.exception("hybrid SQL fetch failed for customer=%s", customer_found)
            sql_summary = {"error": "SQL query failed"}

    # If we have clean SQL data, try to answer deterministically
    if sql_summary and sql_summary.get("invoice_count", 0) > 0:
        outstanding = [
            inv for inv in sql_summary.get("recent_invoices", [])
            if (inv.get("status") or "").lower() != "paid"
        ]
        if outstanding:
            lines = [
                f"• {i.get('invoice_no')} — {i.get('amount')} {i.get('currency')} — "
                f"due {i.get('due_date')} — {i.get('notes') or 'no notes'}"
                for i in outstanding
            ]
            answer = f"Outstanding invoices for {customer_found}:\n" + "\n".join(lines)
            answer += f"\n\nTotal outstanding: {sql_summary['total_outstanding']} (unpaid invoices)"
        else:
            answer = f"No outstanding invoices found for {customer_found}."

        return {
            "question": q,
            "answer": answer,
            "customer_detected": customer_found,
            "sql_summary": sql_summary,
            "sources": [f"sql:invoices:{customer_found}"],
            "retrieved": [],
            "confidence": compute_confidence(True, 0.0, False, 0.0),
        }

    # Fallback: enrich with vector docs + LLM
    docs, sources, top_rerank_score, top_vector_score = vector_retrieve(q, k)
    context = build_context(sql_summary, docs)

    if genai_client is None:
        return {
            "question": q,
            "answer": "LLM not configured (GEMINI_API_KEY missing).",
            "sql_summary": sql_summary,
            "sources": sources[:3],
            "retrieved": docs[:3],
            "confidence": compute_confidence(bool(sql_summary), top_rerank_score, False, top_vector_score),
        }

    answer, citations = llm_answer_from_context(q, context)
    return {
        "question": q,
        "answer": answer,
        "customer_detected": customer_found,
        "sql_summary": sql_summary,
        "sources": citations or sources[:3],
        "retrieved": docs[:3],
        "confidence": compute_confidence(bool(sql_summary), top_rerank_score, True, top_vector_score),
    }


def handle_nl2sql(q: str, execute: bool = False) -> dict:
    sql = nl_to_sql(q)

    if not sql or "I_CANNOT_GENERATE_SQL" in sql:
        return {
            "question": q,
            "answer": "Could not generate a valid SQL query for this request.",
            "sql": None,
        }

    if not safe_sql_check(sql):
        return {
            "question": q,
            "answer": "Generated SQL failed safety check (non-SELECT or forbidden keywords).",
            "sql": sql,
        }

    if not execute:
        return {
            "question": q,
            "answer": f"Here is the generated SQL:\n\n{sql}",
            "sql": sql,
        }

    result = try_sql_with_rewrites(sql)
    if result["ok"]:
        rows = serialize_rows(result["rows"])
        # Deduplicate by invoice_no if the result set contains that column,
        # guarding against duplicate rows from joins or dirty data in the DB.
        if rows and "invoice_no" in rows[0]:
            rows = dedupe_invoices(rows)
        answer = format_rows_as_text(rows)
        return {
            "question": q,
            "answer": answer,
            "sql": sql,
            "rows": rows,
            "note": result.get("note"),
        }
    else:
        return {
            "question": q,
            "answer": f"SQL execution failed: {result['error']}",
            "sql": sql,
        }


# ─────────────────────────────────────────────
# SMART QUERY — unified endpoint
# ─────────────────────────────────────────────
@app.post("/smart_query")
def smart_query(req: QueryRequest):
    """
    Unified endpoint. Classifies user intent via LLM and routes to the
    appropriate handler. Always returns a consistent response shape.
    """
    q = (req.q or "").strip()
    k = req.k or DOC_K

    if not q:
        raise HTTPException(status_code=400, detail="Query 'q' cannot be empty.")

    REQ_COUNT.labels("smart_query").inc()
    start = datetime.utcnow()

    try:
        mode = classify_intent(q)
        logger.info("smart_query | mode=%s | q=%s", mode, q[:80])

        if mode == "rag":
            result = handle_rag(q, k)
        elif mode == "hybrid":
            result = handle_hybrid(q, k)
        elif mode == "nl2sql":
            result = handle_nl2sql(q, execute=False)
        else:  # nl2sql_execute
            result = handle_nl2sql(q, execute=True)

        elapsed = (datetime.utcnow() - start).total_seconds()
        REQ_LATENCY.labels("smart_query").observe(elapsed)

        result["mode"] = mode
        # Ensure the result is JSON-safe before returning
        return _jsonable(result)

    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("smart_query error | q=%s", q[:80])
        return JSONResponse(
            status_code=500,
            content={
                "question": q,
                "mode": "error",
                "answer": None,
                "error": f"{type(exc).__name__}: {exc}",
            },
        )


# ─────────────────────────────────────────────
# Legacy endpoints (kept for backward compat)
# ─────────────────────────────────────────────
@app.post("/query")
def query_endpoint(req: QueryRequest):
    REQ_COUNT.labels("query").inc()
    with REQ_LATENCY.labels("query").time():
        qvec = embedder.encode(req.q).tolist()
        try:
            res  = qdrant.query_points(collection_name=COLLECTION, query=qvec, limit=req.k)
            hits = res.points or []
        except Exception:
            raise HTTPException(status_code=503, detail="Vector DB query failed")
        return {
            "query": req.q,
            "results": [{"id": h.id, "score": h.score, "payload": h.payload} for h in hits],
        }


@app.post("/answer")
def answer_endpoint(req: QueryRequest):
    REQ_COUNT.labels("answer").inc()
    with REQ_LATENCY.labels("answer").time():
        result = handle_rag(req.q, req.k or DOC_K)
        result["mode"] = "rag"
        return result


@app.post("/hybrid_answer")
def hybrid_answer_endpoint(req: QueryRequest):
    REQ_COUNT.labels("hybrid_answer").inc()
    with REQ_LATENCY.labels("hybrid_answer").time():
        result = handle_hybrid(req.q, req.k or DOC_K)
        result["mode"] = "hybrid"
        return result


@app.post("/nl2sql")
def nl2sql_endpoint(payload: Dict[str, Any] = Body(...)):
    q = (payload.get("q") or "").strip()
    if not q:
        raise HTTPException(status_code=400, detail="Missing 'q'")
    result = handle_nl2sql(q, execute=False)
    result["mode"] = "nl2sql"
    return result


@app.post("/nl2sql/execute")
def nl2sql_execute_endpoint(payload: Dict[str, Any] = Body(...)):
    # Accept either {"q": "..."} or {"q": "...", "sql": "..."}
    q   = (payload.get("q") or "").strip()
    sql = (payload.get("sql") or "").strip()

    if sql:
        # User provided SQL directly — validate and execute
        if not safe_sql_check(sql):
            raise HTTPException(status_code=400, detail="SQL failed safety check.")
        result = try_sql_with_rewrites(sql)
        if result["ok"]:
            rows = serialize_rows(result["rows"])
            return {"query": q, "sql": sql, "rows": rows, "note": result.get("note"), "mode": "nl2sql_execute"}
        else:
            raise HTTPException(status_code=500, detail=result["error"])

    if not q:
        raise HTTPException(status_code=400, detail="Provide 'q' or 'sql'.")

    result = handle_nl2sql(q, execute=True)
    result["mode"] = "nl2sql_execute"
    return result


# ─────────────────────────────────────────────
# Analytics endpoints
# ─────────────────────────────────────────────
@app.get("/invoices/{customer}")
def invoices_for_customer(customer: str):
    try:
        rows = run_sql(
            "SELECT invoice_no, amount, currency, issue_date, due_date, status, notes "
            "FROM invoices WHERE customer = %s ORDER BY due_date DESC",
            (customer,),
        )
        return {"customer": customer, "invoices": serialize_rows(rows)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/analytics/overdue")
def analytics_overdue():
    try:
        rows = run_sql(
            "SELECT invoice_no, customer, amount, currency, issue_date, due_date, status, notes "
            "FROM invoices WHERE (status IS NULL OR LOWER(status) != 'paid') AND due_date < CURRENT_DATE "
            "ORDER BY due_date ASC"
        )
        return {"count": len(rows), "overdue": serialize_rows(rows)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/analytics/top_customers")
def analytics_top_customers(limit: int = 10):
    try:
        rows = run_sql(
            "SELECT customer, COALESCE(SUM(amount),0) AS total_outstanding "
            "FROM invoices WHERE (status IS NULL OR LOWER(status) != 'paid') "
            "GROUP BY customer ORDER BY total_outstanding DESC LIMIT %s",
            (limit,),
        )
        return {"count": len(rows), "top_customers": serialize_rows(rows)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/analytics/revenue_month")
def analytics_revenue_month(year: int = None, month: int = None):
    try:
        if year is None or month is None:
            today = datetime.utcnow().date()
            year, month = today.year, today.month
        rows = run_sql(
            "SELECT COALESCE(SUM(amount), 0) AS total_revenue "
            "FROM invoices "
            "WHERE status IS NOT NULL AND LOWER(status) = 'paid' "
            "AND EXTRACT(YEAR FROM issue_date) = %s "
            "AND EXTRACT(MONTH FROM issue_date) = %s",
            (year, month),
        )
        return {"year": year, "month": month, "total_revenue": float(rows[0].get("total_revenue") or 0)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))