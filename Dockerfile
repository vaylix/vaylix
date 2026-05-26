# =========================
# Builder Stage
# =========================

FROM rust:1.95.0-bookworm AS builder

WORKDIR /app

ENV VAYLIX_BIND=0.0.0.0
ENV VAYLIX_PORT=9173
ENV VAYLIX_MAX_CONNECTIONS=256
ENV VAYLIX_WAL_SYNC=flush

# Copy manifests first for dependency caching
COPY Cargo.toml Cargo.lock ./

COPY crates ./crates

# Build release binary
RUN cargo build --release -p server


# =========================
# Runtime Stage
# =========================

FROM debian:bookworm-slim

WORKDIR /app

# Copy server binary
COPY --from=builder \
    /app/target/release/vaylix \
    /usr/local/bin/vaylix

EXPOSE 9173

CMD sh -c 'vaylix \
  --bind "$VAYLIX_BIND" \
  --port "$VAYLIX_PORT" \
  --max-connections "$VAYLIX_MAX_CONNECTIONS" \
  --wal-sync "$VAYLIX_WAL_SYNC"'