/**
 * Forgeyn port proxy — THE SLOW-JVM PORT-BIND LAW.
 *
 * Render's free tier (0.1 CPU) gives a Kotlin/Ktor/Exposed/Flyway JVM far too little CPU to
 * finish its boot inside Render's port-bind startup window, so every deploy was marked
 * update_failed ~40s in — before the JVM ever listened. This proxy binds the public port the
 * moment the container starts (Bun binds in milliseconds), forwards everything to the JVM on
 * its loopback port, and answers honestly (503 "orchestrator booting") until the JVM is up.
 *
 * It restores the public Host-header allowlist the JVM's own guard can no longer see (the JVM
 * now only ever receives loopback traffic from this proxy). Authentication is untouched — the
 * JVM's bearer/JWKS floor still gates every real request.
 */
const UPSTREAM = process.env.PORT_PROXY_UPSTREAM || "http://127.0.0.1:3001";
const PORT = Number(process.env.PORT_PROXY_PORT || 10000);
const ALLOWED_HOSTS = (process.env.PORT_PROXY_ALLOWED_HOSTS || "")
  .split(",")
  .map((h) => h.trim().toLowerCase())
  .filter(Boolean);

function hostAllowed(host: string): boolean {
  if (ALLOWED_HOSTS.length === 0) return true; // unset = no edge host check configured
  const bare = host.toLowerCase().split(":")[0];
  return ALLOWED_HOSTS.includes(bare) || ALLOWED_HOSTS.includes(host.toLowerCase());
}

Bun.serve({
  port: PORT,
  fetch: async (req) => {
    const host = req.headers.get("host") || "";
    if (!hostAllowed(host)) {
      return new Response(
        JSON.stringify({
          ok: false,
          error: {
            code: "forbidden",
            kind: "permanent",
            message: "Host header not in the proxy's allowed list.",
          },
        }),
        { status: 403, headers: { "content-type": "application/json" } },
      );
    }
    const url = new URL(req.url);
    const upstream = UPSTREAM + url.pathname + url.search;
    try {
      const init: RequestInit & { duplex?: "half" } = {
        method: req.method,
        headers: req.headers,
      };
      if (req.method !== "GET" && req.method !== "HEAD") {
        init.body = req.body;
        init.duplex = "half";
      }
      const res = await fetch(upstream, init);
      return new Response(res.body, {
        status: res.status,
        statusText: res.statusText,
        headers: res.headers,
      });
    } catch (e) {
      return new Response(
        JSON.stringify({
          ok: false,
          error: {
            code: "unavailable",
            kind: "transient",
            message: "Orchestrator is booting or unreachable (honest transient state).",
            detail: String(e),
          },
        }),
        { status: 503, headers: { "content-type": "application/json" } },
      );
    }
  },
});

console.log(`[port-proxy] listening on :${PORT} -> ${UPSTREAM} (allowed hosts: ${ALLOWED_HOSTS.join(",") || "any"})`);
