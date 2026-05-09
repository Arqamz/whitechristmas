# ── Stage 1: Build ────────────────────────────────────────────────────────────
FROM golang:1.24-bookworm AS builder

WORKDIR /app

# Cache module downloads separately from source
COPY src/api/go.mod src/api/go.sum ./
RUN go mod download

COPY src/api/ ./
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o api .

# ── Stage 2: Runtime ─────────────────────────────────────────────────────────
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    wget \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/api /usr/local/bin/api

EXPOSE 8081

HEALTHCHECK --interval=10s --timeout=5s --retries=5 --start-period=10s \
  CMD wget -qO- http://localhost:8081/health || exit 1

ENTRYPOINT ["/usr/local/bin/api"]
