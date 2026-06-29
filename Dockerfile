# =============================================================================
# Distributed Task Scheduler - Multi-Stage Docker Build
# =============================================================================
# Build targets:
#   - scheduler: HTTP API + cron engine
#   - worker:    Job processing worker pool
# =============================================================================

# ------------------------------------------------------------------------------
# Stage 1: Build environment
# ------------------------------------------------------------------------------
FROM golang:1.22-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git ca-certificates tzdata

# Set timezone for reproducible builds
ENV TZ=UTC

WORKDIR /build

# Copy and download Go module dependencies first (for layer caching)
COPY go.mod go.sum ./
RUN go mod download && go mod verify

# Copy source code
COPY . .

# ------------------------------------------------------------------------------
# Stage 2: Scheduler binary
# ------------------------------------------------------------------------------
FROM builder AS scheduler-builder

# Build scheduler with optimized flags
# -ldflags "-w -s" strips debug info for smaller binary
# -trimpath removes build paths from binary
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-w -s -X main.version=$(git describe --tags --always 2>/dev/null || echo dev) -X main.buildTime=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    -trimpath \
    -o /bin/scheduler \
    ./cmd/scheduler

# ------------------------------------------------------------------------------
# Stage 3: Worker binary
# ------------------------------------------------------------------------------
FROM builder AS worker-builder

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-w -s -X main.version=$(git describe --tags --always 2>/dev/null || echo dev) -X main.buildTime=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    -trimpath \
    -o /bin/worker \
    ./cmd/worker

# ------------------------------------------------------------------------------
# Stage 4: Final scheduler image (distroless)
# ------------------------------------------------------------------------------
FROM gcr.io/distroless/static:nonroot AS scheduler

LABEL maintainer="rajeshwarrao1253"
LABEL description="Distributed Task Scheduler - Scheduler Node"
LABEL org.opencontainers.image.title="Task Scheduler"
LABEL org.opencontainers.image.description="Production-grade distributed job scheduler"

# Copy CA certificates for HTTPS calls
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo

# Copy binary
COPY --from=scheduler-builder /bin/scheduler /scheduler

# Use non-root user for security
USER nonroot:nonroot

EXPOSE 8080 9090

HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD ["/scheduler", "-health-check"] || exit 1

ENTRYPOINT ["/scheduler"]

# ------------------------------------------------------------------------------
# Stage 5: Final worker image (distroless)
# ------------------------------------------------------------------------------
FROM gcr.io/distroless/static:nonroot AS worker

LABEL maintainer="rajeshwarrao1253"
LABEL description="Distributed Task Scheduler - Worker Node"
LABEL org.opencontainers.image.title="Task Worker"
LABEL org.opencontainers.image.description="Distributed task scheduler worker pool"

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo

COPY --from=worker-builder /bin/worker /worker

USER nonroot:nonroot

HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD ["/worker", "-health-check"] || exit 1

ENTRYPOINT ["/worker"]