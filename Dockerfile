# ============================================================
# Forgeyn prebuilt runtime — the Render companion deployment image.
#
# The jar is built and committed by the maintainer (./gradlew :current:jar -x test) — see
# Dockerfile for the full from-source multi-stage build. This variant exists because Render's
# builder cannot reliably run the Gradle toolchain (distribution downloads fail there); it also
# makes deploys fast (~1 min) and byte-pinned to the committed jar.
#
# THE SLOW-JVM PORT-BIND LAW (2026-10-25): Render's free tier (0.1 CPU) cannot boot the
# Kotlin/Ktor/Exposed/Flyway orchestrator JVM inside Render's port-bind startup window, so
# every deploy was marked update_failed ~40s in — before the JVM ever listened (proven by the
# forgeyn-net-diag probe service: the Supabase pooler IS reachable from Render; a fresh service
# binds and goes live in ~40s). THE FIX: a tiny pure-Java port proxy (PortProxy.java, compiled
# in the build stage — no cross-runtime dependency: a bun binary copied from another image
# failed to run on this base) binds the public port within ~1s of container start, forwards
# everything to the orchestrator on its loopback port, answers honest 503s until the
# orchestrator is up, and restores the public Host allowlist the orchestrator can no longer see.
#
# The proxy also runs THE RENDER VISIBILITY BEACON: failed deploys leave no readable logs, so
# it reports its state + the orchestrator's jvm.log tail to the forgeyn_deploy_beacon Supabase
# table every 15s (opt-in via DIAG_REST_URL / DIAG_REST_KEY env).
# ============================================================
FROM eclipse-temurin:21 AS build
WORKDIR /build
COPY PortProxy.java .
RUN javac -d /build/classes PortProxy.java

FROM eclipse-temurin:21-jre
WORKDIR /app

# The Forgeyn server workflow configuration (work_item_schemas — the lifecycle gates), imported
# at boot by the server-config-import step.
COPY forgeyn-config /app/forgeyn-config

# The fat jar (service-loader files merged — see current/build.gradle.kts mergeServiceFiles).
COPY mcp-task-orchestrator-current.jar /app/orchestrator.jar

# The instant-bind pure-Java port proxy (public port -> orchestrator loopback port).
COPY --from=build /build/classes /app/proxy-classes

ENV AGENT_CONFIG_DIR=/app/forgeyn-config
ENV MCP_TRANSPORT=http
ENV LOG_LEVEL=INFO
ENV MCP_HTTP_HOST=127.0.0.1
ENV MCP_HTTP_PORT=3001
ENV PROXY_PORT=10000
ENV DATABASE_PATH=data/current-tasks.db
ENV READINESS_FILE=/tmp/mcp-task-orchestrator.ready
EXPOSE 10000

# THE 512MB LAW: Render free tier gives the container 512MB total. Both JVMs run lean
# (proxy: -Xmx32m; orchestrator: -Xmx256m + capped metaspace/code-cache + SerialGC; measured
# RSS ≈ 170MB for the orchestrator, ~60MB for the proxy) with headroom for the pool connections.
#
# The supervisor loop is dash-compatible (Ubuntu's /bin/sh has no `wait -n` — that killed the
# container at launch once). It exits when EITHER process dies — Render then restarts the
# container (self-healing). The orchestrator's output is captured to /app/jvm.log so the proxy's
# visibility beacon can tail it.
CMD ["sh", "-c", "java -Xmx32m -XX:+UseSerialGC -XX:TieredStopAtLevel=1 -cp /app/proxy-classes PortProxy & BP=$!; (while :; do if [ -f /app/jvm.log ] && [ $(wc -c < /app/jvm.log) -gt 1048576 ]; then tail -c 65536 /app/jvm.log > /app/jvm.log.tmp && mv /app/jvm.log.tmp /app/jvm.log; fi; echo \"=== JVM BOOT $(date -u +%FT%TZ) ===\" >> /app/jvm.log; java -Xms32m -Xmx256m -XX:MaxMetaspaceSize=128m -XX:ReservedCodeCacheSize=48m -XX:+UseSerialGC -XX:+PerfDisableSharedMem -XX:TieredStopAtLevel=1 -Djava.net.preferIPv4Stack=true -Dfile.encoding=UTF-8 -Djava.awt.headless=true -Duser.timezone=UTC -jar orchestrator.jar >> /app/jvm.log 2>&1; echo \"=== JVM EXIT code=$? at $(date -u +%FT%TZ) ===\" >> /app/jvm.log; sleep 3; done) & JP=$!; while :; do sleep 5; if ! kill -0 $BP 2>/dev/null; then echo PROXY_DIED >> /app/jvm.log; kill $JP 2>/dev/null; exit 1; fi; done"]
