# =========================
# Builder Stage
# =========================

FROM rust:1.95.0-bookworm AS builder

WORKDIR /app

ENV VAYLIX_BIND=0.0.0.0
ENV VAYLIX_PORT=9173
ENV VAYLIX_MAX_CONNECTIONS=256
ENV VAYLIX_WAL_SYNC=flush
ENV VAYLIX_DATA_DIR=/var/lib/vaylix
ENV VAYLIX_USER=vaylix
ENV VAYLIX_PASSWORD=vaylix

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

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy server binary
COPY --from=builder \
    /app/target/release/vaylix \
    /usr/local/bin/vaylix

EXPOSE 9173

VOLUME ["/var/lib/vaylix"]

CMD sh -c 'vaylix \
  --bind "$VAYLIX_BIND" \
  --port "$VAYLIX_PORT" \
  --max-connections "$VAYLIX_MAX_CONNECTIONS" \
  --wal-sync "$VAYLIX_WAL_SYNC" \
  --data-dir "$VAYLIX_DATA_DIR" \
  --user "$VAYLIX_USER" \
  --password "$VAYLIX_PASSWORD"'
