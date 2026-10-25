FROM oven/bun:1
RUN apt-get update && apt-get install -y --no-install-recommends postgresql-client && rm -rf /var/lib/apt/lists/*
COPY diag.js /app/diag.js
EXPOSE 10000
CMD ["bun", "/app/diag.js"]
