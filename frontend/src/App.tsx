// frontend/src/App.tsx
import React, { useState, useRef, useEffect } from "react";
import { post } from "./api";

// ─── Types ────────────────────────────────────────────────────────────────────
type Mode = "rag" | "hybrid" | "nl2sql" | "nl2sql_execute";

interface Invoice {
  invoice_no: string;
  amount: number | string;
  currency: string;
  due_date: string;
  status: string;
  notes?: string;
}

interface ApiResponse {
  mode?: Mode;
  answer?: string;
  question?: string;
  sql?: string;
  rows?: Record<string, any>[];
  sources?: string[];
  retrieved?: string[];
  confidence?: number;
  sql_summary?: {
    customer?: string;
    total_outstanding?: number;
    invoice_count?: number;
    recent_invoices?: Invoice[];
  };
  error?: string;
  note?: string;
}

interface HistoryEntry {
  id: number;
  q: string;
  response: ApiResponse;
  ts: number;
}

// ─── Constants ─────────────────────────────────────────────────────────────────
const MODE_META: Record<Mode, { label: string; color: string; bg: string; desc: string }> = {
  rag:            { label: "Knowledge",    color: "#2563eb", bg: "#eff6ff", desc: "Answered from document knowledge base" },
  hybrid:         { label: "Hybrid",       color: "#7c3aed", bg: "#f5f3ff", desc: "Answered using database + knowledge base" },
  nl2sql:         { label: "SQL Preview",  color: "#d97706", bg: "#fffbeb", desc: "Generated SQL from your question" },
  nl2sql_execute: { label: "Data Query",   color: "#059669", bg: "#ecfdf5", desc: "Retrieved live data from the database" },
};

const EXAMPLES = [
  "Show outstanding invoices for Acme Corp",
  "What is the total amount Beta Ltd owes?",
  "Explain how late payment fees are calculated",
  "List the top 5 customers by outstanding balance",
  "Generate SQL to find all overdue invoices",
  "Which invoices are past their due date?",
];

// ─── Helpers ──────────────────────────────────────────────────────────────────
function ModeBadge({ mode }: { mode?: Mode }) {
  if (!mode || !MODE_META[mode]) return null;
  const { label, color, bg } = MODE_META[mode];
  return (
    <span
      style={{
        display: "inline-block",
        padding: "2px 10px",
        borderRadius: "999px",
        fontSize: "11px",
        fontWeight: 700,
        letterSpacing: "0.05em",
        textTransform: "uppercase",
        color,
        background: bg,
        border: `1px solid ${color}30`,
      }}
    >
      {label}
    </span>
  );
}

function ConfidencePill({ value }: { value?: number }) {
  if (value === undefined || value === null) return null;
  const pct = Math.round(value * 100);
  const color = pct >= 70 ? "#059669" : pct >= 40 ? "#d97706" : "#dc2626";
  return (
    <span style={{ fontSize: "11px", color, fontWeight: 600 }}>
      {pct}% confidence
    </span>
  );
}

function Collapsible({ title, children, defaultOpen = false }: { title: string; children: React.ReactNode; defaultOpen?: boolean }) {
  const [open, setOpen] = useState(defaultOpen);
  return (
    <div style={{ borderTop: "1px solid #e5e7eb", marginTop: "12px" }}>
      <button
        onClick={() => setOpen(o => !o)}
        style={{
          width: "100%",
          textAlign: "left",
          padding: "8px 0",
          background: "none",
          border: "none",
          cursor: "pointer",
          fontSize: "12px",
          fontWeight: 600,
          color: "#6b7280",
          display: "flex",
          alignItems: "center",
          gap: "6px",
        }}
      >
        <span style={{ fontSize: "10px" }}>{open ? "▼" : "▶"}</span>
        {title}
      </button>
      {open && <div style={{ paddingBottom: "8px" }}>{children}</div>}
    </div>
  );
}

// ─── Invoice Table ────────────────────────────────────────────────────────────
function InvoiceTable({ invoices }: { invoices: Invoice[] }) {
  if (!invoices || invoices.length === 0) return <p style={{ color: "#6b7280", fontSize: "13px" }}>No invoices.</p>;
  const outstanding = invoices.filter(i => (i.status || "").toLowerCase() !== "paid");
  return (
    <div style={{ overflowX: "auto" }}>
      <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "12px" }}>
        <thead>
          <tr style={{ background: "#f9fafb" }}>
            {["Invoice", "Amount", "Currency", "Due Date", "Status", "Notes"].map(h => (
              <th key={h} style={{ textAlign: "left", padding: "6px 10px", color: "#6b7280", fontWeight: 600, whiteSpace: "nowrap", borderBottom: "1px solid #e5e7eb" }}>{h}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {invoices.map((inv, idx) => {
            const isOverdue = inv.status?.toLowerCase() !== "paid" && inv.due_date < new Date().toISOString().slice(0, 10);
            return (
              <tr key={idx} style={{ borderBottom: "1px solid #f3f4f6" }}>
                <td style={{ padding: "6px 10px", fontFamily: "monospace", fontWeight: 600 }}>{inv.invoice_no}</td>
                <td style={{ padding: "6px 10px", textAlign: "right" }}>{Number(inv.amount).toLocaleString()}</td>
                <td style={{ padding: "6px 10px", color: "#6b7280" }}>{inv.currency}</td>
                <td style={{ padding: "6px 10px", color: isOverdue ? "#dc2626" : "#374151" }}>{inv.due_date}</td>
                <td style={{ padding: "6px 10px" }}>
                  <span style={{
                    padding: "2px 8px", borderRadius: "4px", fontSize: "11px", fontWeight: 600,
                    background: inv.status?.toLowerCase() === "paid" ? "#d1fae5" : isOverdue ? "#fee2e2" : "#fef3c7",
                    color: inv.status?.toLowerCase() === "paid" ? "#065f46" : isOverdue ? "#991b1b" : "#92400e",
                  }}>
                    {inv.status || "open"}
                  </span>
                </td>
                <td style={{ padding: "6px 10px", color: "#6b7280" }}>{inv.notes || "—"}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
      {outstanding.length > 0 && (
        <div style={{ marginTop: "8px", fontSize: "12px", color: "#374151", fontWeight: 600 }}>
          {outstanding.length} unpaid invoice{outstanding.length !== 1 ? "s" : ""} totalling{" "}
          {outstanding.reduce((s, i) => s + Number(i.amount || 0), 0).toLocaleString()} {outstanding[0]?.currency || ""}
        </div>
      )}
    </div>
  );
}

// ─── Generic Rows Table ───────────────────────────────────────────────────────
function RowsTable({ rows }: { rows: Record<string, any>[] }) {
  if (!rows || rows.length === 0) return <p style={{ color: "#6b7280", fontSize: "13px" }}>No results.</p>;
  const cols = Object.keys(rows[0]);
  return (
    <div style={{ overflowX: "auto" }}>
      <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "12px" }}>
        <thead>
          <tr style={{ background: "#f9fafb" }}>
            {cols.map(c => (
              <th key={c} style={{ textAlign: "left", padding: "6px 10px", color: "#6b7280", fontWeight: 600, borderBottom: "1px solid #e5e7eb" }}>{c}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, i) => (
            <tr key={i} style={{ borderBottom: "1px solid #f3f4f6" }}>
              {cols.map(c => (
                <td key={c} style={{ padding: "6px 10px", fontFamily: typeof row[c] === "number" ? "monospace" : "inherit" }}>
                  {row[c] === null || row[c] === undefined ? "—" : String(row[c])}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

// ─── Response Card ─────────────────────────────────────────────────────────────
function ResponseCard({ resp }: { resp: ApiResponse }) {
  if (resp.error) {
    return (
      <div style={{ padding: "16px", background: "#fef2f2", border: "1px solid #fecaca", borderRadius: "8px" }}>
        <p style={{ color: "#dc2626", fontWeight: 600, margin: "0 0 8px", fontSize: "13px" }}>
          ⚠ Error
        </p>
        <pre style={{ fontSize: "12px", color: "#7f1d1d", margin: "0 0 10px", whiteSpace: "pre-wrap", wordBreak: "break-word" }}>
          {resp.error}
        </pre>
        <p style={{ fontSize: "11px", color: "#9f1239", margin: 0 }}>
          Run <code style={{ background: "#fee2e2", padding: "1px 4px", borderRadius: "3px" }}>docker compose logs api -f</code> to see the full server stack trace.
        </p>
      </div>
    );
  }

  const mode = resp.mode as Mode | undefined;
  const meta = mode && MODE_META[mode];
  const invoices = resp.sql_summary?.recent_invoices;

  return (
    <div style={{ background: "#fff", border: "1px solid #e5e7eb", borderRadius: "10px", overflow: "hidden" }}>
      {/* Header */}
      <div style={{ padding: "12px 16px", borderBottom: "1px solid #f3f4f6", display: "flex", alignItems: "center", gap: "10px", background: meta?.bg || "#f9fafb" }}>
        <ModeBadge mode={mode} />
        {meta && <span style={{ fontSize: "11px", color: "#6b7280" }}>{meta.desc}</span>}
        <div style={{ marginLeft: "auto" }}>
          <ConfidencePill value={resp.confidence} />
        </div>
      </div>

      {/* Answer */}
      <div style={{ padding: "16px" }}>
        <div
          style={{
            fontSize: "14px",
            lineHeight: "1.7",
            color: "#111827",
            whiteSpace: "pre-wrap",
            wordBreak: "break-word",
          }}
        >
          {resp.answer || "No answer returned."}
        </div>

        {/* Invoice table for hybrid */}
        {invoices && invoices.length > 0 && (
          <Collapsible title={`Invoice Details (${invoices.length} records)`} defaultOpen={true}>
            <InvoiceTable invoices={invoices} />
          </Collapsible>
        )}

        {/* SQL block */}
        {resp.sql && (
          <Collapsible title="Generated SQL" defaultOpen={mode === "nl2sql"}>
            <div style={{ position: "relative" }}>
              <pre style={{
                background: "#0f172a",
                color: "#e2e8f0",
                padding: "12px 14px",
                borderRadius: "6px",
                fontSize: "12px",
                overflowX: "auto",
                fontFamily: "'JetBrains Mono', 'Fira Code', monospace",
                lineHeight: "1.6",
              }}>
                {resp.sql}
              </pre>
              <button
                onClick={() => navigator.clipboard.writeText(resp.sql!)}
                style={{
                  position: "absolute", top: "8px", right: "8px",
                  background: "#334155", border: "none", color: "#94a3b8",
                  padding: "3px 8px", borderRadius: "4px", fontSize: "11px", cursor: "pointer",
                }}
              >
                Copy
              </button>
            </div>
          </Collapsible>
        )}

        {/* Query results table */}
        {resp.rows && resp.rows.length > 0 && (
          <Collapsible title={`Query Results (${resp.rows.length} rows)`} defaultOpen={true}>
            <RowsTable rows={resp.rows} />
          </Collapsible>
        )}

        {/* Sources */}
        {resp.sources && resp.sources.length > 0 && (
          <Collapsible title="Sources">
            <ul style={{ margin: 0, padding: "0 0 0 16px" }}>
              {resp.sources.map((s, i) => (
                <li key={i} style={{ fontSize: "12px", color: "#6b7280", padding: "2px 0" }}>{s}</li>
              ))}
            </ul>
          </Collapsible>
        )}

        {/* Retrieved docs */}
        {resp.retrieved && resp.retrieved.length > 0 && (
          <Collapsible title={`Retrieved Chunks (${resp.retrieved.length})`}>
            <div style={{ maxHeight: "160px", overflowY: "auto" }}>
              {resp.retrieved.map((r, i) => (
                <div key={i} style={{
                  fontSize: "11px", color: "#6b7280", padding: "6px 8px",
                  background: "#f9fafb", borderRadius: "4px", marginBottom: "4px",
                  fontFamily: "monospace",
                }}>
                  {r.slice(0, 240)}{r.length > 240 ? "…" : ""}
                </div>
              ))}
            </div>
          </Collapsible>
        )}

        {resp.note && (
          <p style={{ marginTop: "10px", fontSize: "11px", color: "#9ca3af" }}>Note: {resp.note}</p>
        )}
      </div>
    </div>
  );
}

// ─── Main App ─────────────────────────────────────────────────────────────────
export default function App() {
  const [q, setQ] = useState("");
  const [k, setK] = useState(4);
  const [loading, setLoading] = useState(false);
  const [history, setHistory] = useState<HistoryEntry[]>([]);
  const [selectedId, setSelectedId] = useState<number | null>(null);
  const textareaRef = useRef<HTMLTextAreaElement>(null);
  const idCounter = useRef(0);

  const selected = history.find(h => h.id === selectedId);

  async function ask() {
    const query = q.trim();
    if (!query || loading) return;
    setLoading(true);
    try {
      const data: ApiResponse = await post("/smart_query", { q: query, k });
      const entry: HistoryEntry = { id: ++idCounter.current, q: query, response: data, ts: Date.now() };
      setHistory(prev => [entry, ...prev].slice(0, 100));
      setSelectedId(entry.id);
    } catch (err: any) {
      // The backend now returns JSON even on 500 — try to extract the real detail
      let errorMsg: string = err.message || String(err);
      // If the error text is "500 ...: {}" it means the body was empty (old code);
      // if it's "500 ...: {"detail": "..."}" our new handler is working.
      const detailMatch = errorMsg.match(/"detail"\s*:\s*"([^"]+)"/);
      if (detailMatch) errorMsg = detailMatch[1];

      const entry: HistoryEntry = {
        id: ++idCounter.current,
        q: query,
        response: { error: errorMsg },
        ts: Date.now(),
      };
      setHistory(prev => [entry, ...prev].slice(0, 100));
      setSelectedId(entry.id);
    } finally {
      setLoading(false);
    }
  }

  function handleKeyDown(e: React.KeyboardEvent<HTMLTextAreaElement>) {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      ask();
    }
  }

  return (
    <div style={{ display: "flex", height: "100vh", fontFamily: "'Inter', -apple-system, sans-serif", background: "#f8fafc" }}>

      {/* ── Sidebar ─────────────────────────────────────────────────────────── */}
      <aside style={{
        width: "260px", minWidth: "260px", background: "#0f172a", color: "#e2e8f0",
        display: "flex", flexDirection: "column", overflow: "hidden",
      }}>
        <div style={{ padding: "20px 16px 12px", borderBottom: "1px solid #1e293b" }}>
          <h1 style={{ margin: 0, fontSize: "16px", fontWeight: 700, color: "#f1f5f9", letterSpacing: "-0.3px" }}>
            Cinntra
          </h1>
          <p style={{ margin: "2px 0 0", fontSize: "11px", color: "#64748b" }}>RAG + Analytics Assistant</p>
        </div>

        {/* Examples */}
        <div style={{ padding: "12px 16px", borderBottom: "1px solid #1e293b" }}>
          <p style={{ margin: "0 0 8px", fontSize: "10px", fontWeight: 700, letterSpacing: "0.08em", color: "#475569", textTransform: "uppercase" }}>
            Quick Examples
          </p>
          <ul style={{ margin: 0, padding: 0, listStyle: "none" }}>
            {EXAMPLES.map((ex, i) => (
              <li key={i}>
                <button
                  onClick={() => { setQ(ex); textareaRef.current?.focus(); }}
                  style={{
                    width: "100%", textAlign: "left", background: "none", border: "none",
                    color: "#94a3b8", fontSize: "12px", padding: "5px 4px", cursor: "pointer",
                    borderRadius: "4px", lineHeight: "1.4",
                  }}
                  onMouseEnter={e => (e.currentTarget.style.color = "#e2e8f0")}
                  onMouseLeave={e => (e.currentTarget.style.color = "#94a3b8")}
                >
                  {ex}
                </button>
              </li>
            ))}
          </ul>
        </div>

        {/* History */}
        <div style={{ flex: 1, overflowY: "auto", padding: "12px 0" }}>
          <p style={{ margin: "0 16px 8px", fontSize: "10px", fontWeight: 700, letterSpacing: "0.08em", color: "#475569", textTransform: "uppercase" }}>
            History
          </p>
          {history.length === 0 && (
            <p style={{ margin: "0 16px", fontSize: "12px", color: "#475569" }}>No queries yet.</p>
          )}
          {history.map(h => {
            const mode = h.response.mode as Mode | undefined;
            const isActive = h.id === selectedId;
            return (
              <button
                key={h.id}
                onClick={() => setSelectedId(h.id)}
                style={{
                  width: "100%", textAlign: "left", background: isActive ? "#1e293b" : "none",
                  border: "none", padding: "8px 16px", cursor: "pointer",
                  borderLeft: isActive ? "2px solid #3b82f6" : "2px solid transparent",
                }}
              >
                <div style={{ display: "flex", alignItems: "center", gap: "6px", marginBottom: "2px" }}>
                  {mode && (
                    <span style={{
                      fontSize: "9px", fontWeight: 700, letterSpacing: "0.06em", textTransform: "uppercase",
                      color: MODE_META[mode]?.color, background: MODE_META[mode]?.bg,
                      padding: "1px 5px", borderRadius: "3px",
                    }}>
                      {MODE_META[mode]?.label}
                    </span>
                  )}
                  <span style={{ fontSize: "10px", color: "#475569" }}>
                    {new Date(h.ts).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}
                  </span>
                </div>
                <div style={{ fontSize: "12px", color: isActive ? "#e2e8f0" : "#94a3b8", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                  {h.q}
                </div>
              </button>
            );
          })}
        </div>
      </aside>

      {/* ── Main Panel ──────────────────────────────────────────────────────── */}
      <main style={{ flex: 1, display: "flex", flexDirection: "column", overflow: "hidden" }}>

        {/* Top bar */}
        <div style={{
          padding: "12px 24px", borderBottom: "1px solid #e5e7eb", background: "#fff",
          display: "flex", alignItems: "center", gap: "12px",
        }}>
          <span style={{ fontSize: "13px", color: "#374151", fontWeight: 500 }}>
            Just ask a question — the system will detect whether to use documents, the database, or generate SQL automatically.
          </span>
          <div style={{ marginLeft: "auto", display: "flex", alignItems: "center", gap: "8px" }}>
            <label style={{ fontSize: "12px", color: "#6b7280" }}>k =</label>
            <input
              type="number" min={1} max={20} value={k}
              onChange={e => setK(Number(e.target.value))}
              style={{
                width: "52px", border: "1px solid #d1d5db", borderRadius: "6px",
                padding: "4px 6px", fontSize: "12px", textAlign: "center",
              }}
            />
          </div>
        </div>

        {/* Response area */}
        <div style={{ flex: 1, overflowY: "auto", padding: "24px", display: "flex", flexDirection: "column", gap: "16px" }}>
          {!selected && !loading && (
            <div style={{
              margin: "auto", textAlign: "center", color: "#9ca3af", maxWidth: "400px",
            }}>
              <div style={{ fontSize: "40px", marginBottom: "12px" }}>⚡</div>
              <p style={{ fontSize: "14px", lineHeight: "1.6" }}>
                Ask anything about your invoices, customers, or knowledge base. The system automatically routes your question to the right pipeline.
              </p>
            </div>
          )}

          {loading && (
            <div style={{ display: "flex", alignItems: "center", gap: "10px", color: "#6b7280", padding: "16px 0" }}>
              <div style={{
                width: "16px", height: "16px", borderRadius: "50%",
                border: "2px solid #e5e7eb", borderTopColor: "#3b82f6",
                animation: "spin 0.7s linear infinite",
              }} />
              <span style={{ fontSize: "13px" }}>Routing and processing your query…</span>
            </div>
          )}

          {selected && !loading && (
            <div>
              <div style={{ marginBottom: "12px" }}>
                <p style={{ margin: 0, fontSize: "13px", color: "#6b7280", fontWeight: 500 }}>Your query</p>
                <p style={{ margin: "4px 0 0", fontSize: "15px", color: "#111827", fontWeight: 600 }}>{selected.q}</p>
              </div>
              <ResponseCard resp={selected.response} />
            </div>
          )}
        </div>

        {/* Input */}
        <div style={{
          padding: "16px 24px", background: "#fff", borderTop: "1px solid #e5e7eb",
        }}>
          <div style={{
            display: "flex", gap: "10px", alignItems: "flex-end",
            background: "#f8fafc", border: "1px solid #e5e7eb", borderRadius: "10px",
            padding: "10px 14px",
          }}>
            <textarea
              ref={textareaRef}
              value={q}
              onChange={e => setQ(e.target.value)}
              onKeyDown={handleKeyDown}
              rows={2}
              placeholder="Ask about invoices, customers, or anything in your knowledge base… (Enter to send)"
              style={{
                flex: 1, resize: "none", border: "none", background: "none", outline: "none",
                fontSize: "14px", color: "#111827", lineHeight: "1.5", fontFamily: "inherit",
              }}
            />
            <button
              onClick={ask}
              disabled={loading || !q.trim()}
              style={{
                background: loading || !q.trim() ? "#e5e7eb" : "#1d4ed8",
                color: loading || !q.trim() ? "#9ca3af" : "#fff",
                border: "none", borderRadius: "7px", padding: "8px 20px",
                fontWeight: 600, fontSize: "13px", cursor: loading || !q.trim() ? "not-allowed" : "pointer",
                transition: "background 0.15s",
                whiteSpace: "nowrap",
              }}
            >
              {loading ? "…" : "Ask"}
            </button>
          </div>
          <p style={{ margin: "6px 0 0", fontSize: "11px", color: "#9ca3af", textAlign: "center" }}>
            Shift+Enter for newline · Enter to send · Automatically routes between Knowledge, Hybrid, SQL Preview, and Data Query modes
          </p>
        </div>
      </main>

      <style>{`
        @keyframes spin { to { transform: rotate(360deg); } }
        * { box-sizing: border-box; }
        ::-webkit-scrollbar { width: 4px; height: 4px; }
        ::-webkit-scrollbar-track { background: transparent; }
        ::-webkit-scrollbar-thumb { background: #cbd5e1; border-radius: 2px; }
      `}</style>
    </div>
  );
}