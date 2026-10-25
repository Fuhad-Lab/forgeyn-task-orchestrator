import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

/**
 * Forgeyn port proxy — THE SLOW-JVM PORT-BIND LAW, pure-Java edition.
 *
 * Render's free tier (0.1 CPU) cannot boot the Kotlin/Ktor/Exposed/Flyway orchestrator JVM
 * inside Render's port-bind startup window. This tiny single-file proxy JVM binds the public
 * port within ~1s of container start, forwards every request to the orchestrator on its
 * loopback port, answers honest 503s until the orchestrator is up, and restores the public
 * Host-header allowlist the orchestrator can no longer see (it only receives loopback traffic).
 *
 * It also runs THE RENDER VISIBILITY BEACON: failed deploys leave no readable logs, so every
 * 15s the proxy reports its state plus the orchestrator's log tail to a Supabase diagnostics
 * table (opt-in via DIAG_REST_URL / DIAG_REST_KEY).
 *
 * Env: PROXY_PORT (10000), UPSTREAM_HOST (127.0.0.1), UPSTREAM_PORT (3001),
 *      PORT_PROXY_ALLOWED_HOSTS (csv; empty = allow any), DIAG_REST_URL, DIAG_REST_KEY,
 *      DIAG_SERVICE_TAG, DIAG_JVM_LOG (/app/jvm.log).
 */
public final class PortProxy {
    private static final int PROXY_PORT = intEnv("PROXY_PORT", 10000);
    private static final String UPSTREAM_HOST = env("UPSTREAM_HOST", "127.0.0.1");
    private static final int UPSTREAM_PORT = intEnv("UPSTREAM_PORT", 3001);
    private static final String BEACON_URL = env("DIAG_REST_URL", "");
    private static final String BEACON_KEY = env("DIAG_REST_KEY", "");
    private static final String SERVICE_TAG = env("DIAG_SERVICE_TAG", "forgeyn-task-orchestrator");
    private static final Path JVM_LOG = Path.of(env("DIAG_JVM_LOG", "/app/jvm.log"));

    private static final Set<String> ALLOWED_HOSTS = new HashSet<>();
    private static final HttpClient CLIENT = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(5))
        .build();

    public static void main(String[] args) throws Exception {
        for (String h : env("PORT_PROXY_ALLOWED_HOSTS", "").split(",")) {
            String t = h.trim().toLowerCase();
            if (!t.isEmpty()) ALLOWED_HOSTS.add(t);
        }

        HttpServer server = HttpServer.create(new InetSocketAddress("0.0.0.0", PROXY_PORT), 256);
        server.createContext("/", PortProxy::handle);
        server.setExecutor(java.util.concurrent.Executors.newFixedThreadPool(8));
        server.start();
        System.out.println("[port-proxy] listening on :" + PROXY_PORT + " -> " + UPSTREAM_HOST + ":" + UPSTREAM_PORT
            + " (allowed hosts: " + (ALLOWED_HOSTS.isEmpty() ? "any" : String.join(",", ALLOWED_HOSTS)) + ")");

        if (!BEACON_URL.isEmpty() && !BEACON_KEY.isEmpty()) {
            Thread beacon = new Thread(PortProxy::beaconLoop, "visibility-beacon");
            beacon.setDaemon(true);
            beacon.start();
        }
    }

    private static void handle(HttpExchange ex) throws IOException {
        try {
            String host = ex.getRequestHeaders().getFirst("Host");
            if (host != null && !ALLOWED_HOSTS.isEmpty()) {
                String bare = host.toLowerCase().split(":")[0];
                if (!ALLOWED_HOSTS.contains(bare) && !ALLOWED_HOSTS.contains(host.toLowerCase())) {
                    respond(ex, 403, "{\"ok\":false,\"error\":{\"code\":\"forbidden\",\"kind\":\"permanent\","
                        + "\"message\":\"Host header not in the proxy's allowed list.\"}}");
                    return;
                }
            }

            String uri = "http://" + UPSTREAM_HOST + ":" + UPSTREAM_PORT + ex.getRequestURI();
            HttpRequest.Builder rb = HttpRequest.newBuilder(URI.create(uri))
                .timeout(Duration.ofSeconds(120))
                .version(HttpClient.Version.HTTP_1_1);
            String method = ex.getRequestMethod();
            for (var e : ex.getRequestHeaders().entrySet()) {
                if (isHopOrRestricted(e.getKey())) continue;
                for (String v : e.getValue()) rb.header(e.getKey(), v);
            }
            if (!method.equals("GET") && !method.equals("HEAD")) {
                rb.method(method, HttpRequest.BodyPublishers.ofInputStream(() -> {
                    try { return ex.getRequestBody(); } catch (Exception e) { return null; }
                }));
            } else {
                rb.method(method, HttpRequest.BodyPublishers.noBody());
            }

            HttpResponse<InputStream> res = CLIENT.send(rb.build(), HttpResponse.BodyHandlers.ofInputStream());
            ex.getResponseHeaders().clear();
            for (var e : res.headers().map().entrySet()) {
                if (isHopOrRestricted(e.getKey())) continue;
                for (String v : e.getValue()) ex.getResponseHeaders().add(e.getKey(), v);
            }
            ex.sendResponseHeaders(res.statusCode(), 0);
            try (InputStream in = res.body(); OutputStream out = ex.getResponseBody()) {
                in.transferTo(out);
            }
        } catch (Exception e) {
            respond(ex, 503, "{\"ok\":false,\"error\":{\"code\":\"unavailable\",\"kind\":\"transient\","
                + "\"message\":\"Orchestrator is booting or unreachable (honest transient state).\","
                + "\"detail\":\"" + escape(String.valueOf(e)) + "\"}}");
        }
    }

    private static boolean isHopOrRestricted(String name) {
        String n = name == null ? "" : name.toLowerCase();
        return n.equals("host") || n.equals("connection") || n.equals("content-length")
            || n.equals("expect") || n.equals("upgrade") || n.equals("transfer-encoding");
    }

    private static void respond(HttpExchange ex, int code, String body) throws IOException {
        byte[] b = body.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", "application/json");
        ex.sendResponseHeaders(code, b.length);
        try (OutputStream out = ex.getResponseBody()) { out.write(b); }
    }

    private static void beaconLoop() {
        while (true) {
            String upstreamState;
            try {
                var r = CLIENT.send(
                    HttpRequest.newBuilder(URI.create("http://" + UPSTREAM_HOST + ":" + UPSTREAM_PORT + "/api/v1/health"))
                        .timeout(Duration.ofSeconds(3)).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
                upstreamState = "health:" + r.statusCode();
            } catch (Exception e) {
                upstreamState = "unreachable:" + String.valueOf(e).replaceAll("\\s+", " ").substring(0, Math.min(120, String.valueOf(e).length()));
            }
            try {
                String json = "{\"service\":\"" + SERVICE_TAG + "\","
                    + "\"note\":\"port=" + PROXY_PORT + " upstream=" + escape(upstreamState)
                    + " ts=" + java.time.Instant.now() + "\","
                    + "\"jvm_log_tail\":\"" + escape(tailLog()) + "\"}";
                HttpRequest req = HttpRequest.newBuilder(URI.create(BEACON_URL))
                    .timeout(Duration.ofSeconds(5))
                    .header("Content-Type", "application/json")
                    .header("apikey", BEACON_KEY)
                    .header("Authorization", "Bearer " + BEACON_KEY)
                    .POST(HttpRequest.BodyPublishers.ofString(json))
                    .build();
                CLIENT.send(req, HttpResponse.BodyHandlers.discarding());
            } catch (Exception ignored) {
                /* the beacon must never break the proxy */
            }
            try { Thread.sleep(15_000); } catch (InterruptedException ie) { return; }
        }
    }

    private static String tailLog() {
        try {
            byte[] all = Files.readAllBytes(JVM_LOG);
            int from = Math.max(0, all.length - 6000);
            return new String(all, from, all.length - from, StandardCharsets.UTF_8);
        } catch (Exception e) {
            return "<unreadable:" + e + ">";
        }
    }

    private static String escape(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t");
    }

    private static String env(String k, String d) {
        String v = System.getenv(k);
        return (v == null || v.isBlank()) ? d : v.trim();
    }

    private static int intEnv(String k, int d) {
        try { return Integer.parseInt(env(k, String.valueOf(d))); } catch (Exception e) { return d; }
    }
}
