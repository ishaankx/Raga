// frontend/src/api.ts

/**
 * POST to a backend endpoint.
 * All paths are normalised so the Vite proxy forwards them correctly.
 */
export async function post(path: string, body?: unknown, timeoutMs = 45_000): Promise<any> {
  const normalised = path.startsWith("/api")
    ? path
    : `/api${path.startsWith("/") ? path : "/" + path}`;

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const res = await fetch(normalised, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: body === undefined ? undefined : JSON.stringify(body),
      signal: controller.signal,
    });

    const text = await res.text();
    clearTimeout(timer);

    if (!res.ok) {
      let message = text;
      try {
        const json = JSON.parse(text || "{}");
        message = json.detail ?? json.message ?? JSON.stringify(json);
      } catch { /* leave as raw text */ }
      throw new Error(`${res.status} ${res.statusText}: ${message}`);
    }

    if (!text) return null;
    try {
      return JSON.parse(text);
    } catch {
      return text;
    }
  } catch (err: any) {
    clearTimeout(timer);
    if (err.name === "AbortError") throw new Error("Request timed out — the model may be slow, try again.");
    throw err;
  }
}

/** Convenience wrapper for the unified smart query endpoint. */
export async function smartQuery(q: string, k = 4) {
  return post("/smart_query", { q, k });
}