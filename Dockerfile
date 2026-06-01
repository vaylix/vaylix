# syntax=docker/dockerfile:1.7

FROM rust:1.95.0-bookworm AS chef

RUN cargo install cargo-chef --locked

WORKDIR /app


FROM chef AS planner

COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

RUN cargo chef prepare --recipe-path recipe.json


FROM chef AS builder

COPY --from=planner /app/recipe.json recipe.json

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/app/target \
    cargo chef cook --release --package server --recipe-path recipe.json

COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/app/target \
    cargo build --release -p server \
    && mkdir -p /out \
    && cp target/release/vaylix /out/vaylix

RUN mkdir -p /runtime/var/lib/vaylix/backups \
    && chown -R 65532:65532 /runtime/var/lib/vaylix \
    && chmod -R u+rwX,g+rwX /runtime/var/lib/vaylix


FROM gcr.io/distroless/cc-debian12:nonroot AS runtime

ENV VAYLIX_BIND=0.0.0.0
ENV VAYLIX_PORT=9173
ENV VAYLIX_MAX_CONNECTIONS=256
ENV VAYLIX_WAL_SYNC=flush
ENV VAYLIX_DATA_DIR=/var/lib/vaylix
ENV VAYLIX_BACKUP_DIR=/var/lib/vaylix/backups
ENV VAYLIX_USER=vaylix
ENV VAYLIX_PASSWORD=vaylix

COPY --from=builder --chown=65532:65532 /runtime/var/lib/vaylix /var/lib/vaylix
COPY --from=builder --chown=65532:65532 /out/vaylix /usr/local/bin/vaylix

EXPOSE 9173

VOLUME ["/var/lib/vaylix"]

USER 65532:65532

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD ["/usr/local/bin/vaylix", "healthcheck", "--kind", "liveness"]

ENTRYPOINT ["/usr/local/bin/vaylix"]
