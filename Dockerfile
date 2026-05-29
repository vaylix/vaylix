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


FROM debian:bookworm-slim AS runtime

ENV VAYLIX_BIND=0.0.0.0
ENV VAYLIX_PORT=9173
ENV VAYLIX_MAX_CONNECTIONS=256
ENV VAYLIX_WAL_SYNC=flush
ENV VAYLIX_DATA_DIR=/var/lib/vaylix
ENV VAYLIX_BACKUP_DIR=/var/lib/vaylix/backups
ENV VAYLIX_USER=vaylix
ENV VAYLIX_PASSWORD=vaylix

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /out/vaylix /usr/local/bin/vaylix

EXPOSE 9173

VOLUME ["/var/lib/vaylix"]

CMD ["vaylix"]
