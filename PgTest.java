import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.sql.Connection;
import java.util.List;
import org.postgresql.ds.PGSimpleDataSource;

/**
 * THE PGJDBC REPRODUCTION PROBE — runs the exact connection paths the orchestrator uses,
 * on Render's network, and reports per-variant timings and FULL exception chains.
 * Serves the report over HTTP :10000 (and re-runs on every request for fresh evidence).
 */
public final class PgTest {
    private static final String URL_BASE =
        "jdbc:postgresql://aws-0-us-west-2.pooler.supabase.com:5432/postgres"
            + "?user=forgeyn_orchestrator.phvqvypgzrdflbbwzyat&password=FrgT0rch_2026_8xQzLmVn4Rp7&sslmode=require";

    public static void main(String[] args) throws Exception {
        com.sun.net.httpserver.HttpServer server =
            com.sun.net.httpserver.HttpServer.create(new InetSocketAddress("0.0.0.0", 10000), 16);
        server.createContext("/", ex -> {
            byte[] body = runAll().getBytes(java.nio.charset.StandardCharsets.UTF_8);
            ex.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
            ex.sendResponseHeaders(200, body.length);
            try (var out = ex.getResponseBody()) { out.write(body); }
        });
        server.setExecutor(java.util.concurrent.Executors.newFixedThreadPool(2));
        server.start();
        System.out.println("pgtest serving on 10000");
    }

    private static String runAll() {
        StringBuilder sb = new StringBuilder();
        sb.append("ts=").append(java.time.Instant.now()).append("\n");
        sb.append("java=").append(System.getProperty("java.version"))
            .append(" egd=").append(System.getProperty("java.security.egd", "<unset>")).append("\n\n");

        // 0. Java's own DNS + raw socket (the layer under pgjdbc)
        try {
            var addrs = java.net.InetAddress.getAllByName("aws-0-us-west-2.pooler.supabase.com");
            for (var a : addrs) sb.append("dns: ").append(a).append("\n");
            for (var a : addrs) {
                long t0 = System.nanoTime();
                try (Socket s = new Socket()) {
                    s.connect(new InetSocketAddress(a, 5432), 8000);
                    sb.append(String.format("raw-socket %s -> CONNECTED in %dms%n", a, (System.nanoTime() - t0) / 1_000_000));
                } catch (Exception e) {
                    sb.append(String.format("raw-socket %s -> FAIL in %dms: %s%n", a, (System.nanoTime() - t0) / 1_000_000, e));
                }
            }
        } catch (Exception e) {
            sb.append("dns FAIL: ").append(e).append("\n");
        }
        sb.append("\n");

        // 1..3. the pgjdbc variants
        variant(sb, "A: URL as-is (sslmode=require)", URL_BASE);
        variant(sb, "B: + connectTimeout=60", URL_BASE + "&connectTimeout=60");
        variant(sb, "C: + sslfactory=LibPQFactory", URL_BASE + "&connectTimeout=60&sslfactory=org.postgresql.ssl.LibPQFactory");
        variant(sb, "D: sslmode=disable", URL_BASE.replaceAll("sslmode=require", "sslmode=disable") + "&connectTimeout=60");
        return sb.toString();
    }

    private static void variant(StringBuilder sb, String label, String url) {
        sb.append("== ").append(label).append("\n");
        long t0 = System.nanoTime();
        try {
            PGSimpleDataSource ds = new PGSimpleDataSource();
            ds.setURL(url);
            String user = param(url, "user"), pass = param(url, "password");
            if (user != null) ds.setUser(user);
            if (pass != null) ds.setPassword(pass);
            try (Connection c = ds.getConnection()) {
                sb.append(String.format("   OK in %dms: user=%s db=%s%n",
                    (System.nanoTime() - t0) / 1_000_000, c.getMetaData().getUserName(), c.getCatalog()));
                try (var st = c.createStatement(); var rs = st.executeQuery("select 1")) {
                    rs.next();
                    sb.append("   select 1 = ").append(rs.getInt(1)).append("\n");
                }
            }
        } catch (Throwable t) {
            sb.append(String.format("   FAIL in %dms%n", (System.nanoTime() - t0) / 1_000_000));
            Throwable cur = t;
            int depth = 0;
            while (cur != null && depth++ < 8) {
                sb.append("   ").append(cur.getClass().getName()).append(": ").append(cur.getMessage()).append("\n");
                for (var ste : cur.getStackTrace()) {
                    if (ste.getClassName().startsWith("org.postgresql") || ste.getClassName().startsWith("java.net")
                        || ste.getClassName().startsWith("javax.net") || ste.getClassName().startsWith("sun.security")) {
                        sb.append("      at ").append(ste).append("\n");
                    }
                }
                cur = cur.getCause();
            }
        }
        sb.append("\n");
    }

    private static String param(String url, String name) {
        String q = url.substring(url.indexOf('?') + 1);
        for (String pair : q.split("&")) {
            if (pair.startsWith(name + "=")) return pair.substring(name.length() + 1);
        }
        return null;
    }
}
