FROM eclipse-temurin:21-jre
WORKDIR /app
COPY forgeyn-config /app/forgeyn-config
COPY mcp-task-orchestrator-current.jar /app/orchestrator.jar
ENV AGENT_CONFIG_DIR=/app/forgeyn-config
ENV MCP_TRANSPORT=http
ENV LOG_LEVEL=INFO
ENV MCP_HTTP_HOST=0.0.0.0
ENV MCP_HTTP_PORT=10000
EXPOSE 10000
CMD ["java", "-Djava.security.egd=file:/dev/./urandom", "-Xms32m", "-Xmx256m", "-XX:MaxMetaspaceSize=128m", "-XX:ReservedCodeCacheSize=48m", "-XX:+UseSerialGC", "-XX:+PerfDisableSharedMem", "-XX:TieredStopAtLevel=1", "-Djava.net.preferIPv4Stack=true", "-Dfile.encoding=UTF-8", "-Djava.awt.headless=true", "-Duser.timezone=UTC", "-jar", "orchestrator.jar"]
