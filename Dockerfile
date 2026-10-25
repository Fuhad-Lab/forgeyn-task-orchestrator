FROM eclipse-temurin:21-jre
WORKDIR /app
COPY test-classes/PgTest.class /app/PgTest.class
COPY mcp-task-orchestrator-current.jar /app/orchestrator.jar
EXPOSE 10000
CMD ["java", "-Djava.security.egd=file:/dev/./urandom", "-cp", "/app:/app/orchestrator.jar", "PgTest"]
