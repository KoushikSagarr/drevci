# Multi-stage Dockerfile for Drev CI daemon (drevd)
# Stage 1: Build binary
FROM golang:1.25-alpine AS builder

WORKDIR /app

# Install build dependencies
RUN apk add --no-cache git ca-certificates

# Cache dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy source code and compile statically
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /app/bin/drevd ./cmd/drevd
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /app/bin/drev ./cmd/drev

# Stage 2: Runtime image
FROM alpine:3.21

# Install runtime dependencies:
# - ca-certificates: HTTPS API requests and webhooks
# - git: cloning repositories in pipeline workspaces
# - tzdata: accurate timestamp logging
# - wget: healthcheck probes
RUN apk add --no-cache ca-certificates git tzdata wget

# Create standard directories for persistent data, logs, and job workspaces
RUN mkdir -p /var/lib/drev/data /var/lib/drev/logs /var/lib/drev/workspaces

# Copy binaries from builder
COPY --from=builder /app/bin/drevd /usr/local/bin/drevd
COPY --from=builder /app/bin/drev /usr/local/bin/drev

# Default configuration paths
ENV DREV_DB_PATH=/var/lib/drev/data/drev.db
ENV DREV_LOG_DIR=/var/lib/drev/logs
ENV TMPDIR=/var/lib/drev/workspaces
ENV HOST=0.0.0.0
ENV PORT=9090

EXPOSE 9090

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
  CMD wget -qO- http://127.0.0.1:9090/api/v1/health || exit 1

ENTRYPOINT ["drevd"]
CMD ["--host", "0.0.0.0", "--port", "9090", "--db", "/var/lib/drev/data/drev.db", "--log-dir", "/var/lib/drev/logs"]
