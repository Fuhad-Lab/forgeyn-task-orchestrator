FROM oven/bun:1
COPY diag.js /app/diag.js
EXPOSE 10000
CMD ["bun", "/app/diag.js"]
