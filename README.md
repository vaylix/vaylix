# Vaylix

Vaylix is a Rust key/value database project built around a strict transport boundary. It currently provides a single-node, string-to-string database with a framed binary protocol, a Tokio multi-client server, default-on authentication, default-on transport compression, optional TLS, and encrypted-at-rest persistence.

The project is intentionally structured as a serious systems codebase rather than a demo:
- protocol and engine responsibilities are separated
- errors carry stable codes and friendly names
- persistence behavior is explicit
- CI enforces formatting, linting, and tests

## Current Scope

Implemented today:
- `String -> String` data model
- custom framed binary transport protocol
- shared transport crate for client and server
- authentication enabled by default, with an explicit local-only disable flag
- optional TLS via `--ssl`, `--tls-cert`, and `--tls-key`
- zstd transport compression enabled by default, with an explicit disable flag
- WAL + snapshot durability
- server-managed storage keyring with rotation support
- request rate limiting and command quotas
- REPL client with `plain`, `table`, and `json` output
- session transaction commands: `MULTI`, `EXEC`, `DISCARD`
- atomic single-node `EXEC` commits
- append-only audit logging

Not implemented yet:
- replication
- sharding
- distributed ACID semantics or cluster commit coordination

## Workspace

- `crates/command` — parser, lexer, command metadata
- `crates/transport` — framing, codec, wire protocol, sync/async I/O
- `crates/engine` — state, WAL, snapshots, recovery, storage encryption
- `crates/server` — Tokio server, auth, TLS, quotas, rate limits, engine runtime
- `crates/client` — REPL, connection strings, TLS client, output rendering

## Build

```bash
cargo build --workspace
```

Release binaries:

```bash
cargo build --release -p server
cargo build --release -p client
```

## Run

Start the server:

```bash
cargo run -p server -- \
  --bind 127.0.0.1 \
  --port 9173 \
  --user vaylix \
  --password vaylix
```

Start the client:

```bash
cargo run -p client -- \
  --host 127.0.0.1 \
  --port 9173
```

URL-based client connection:

```bash
cargo run -p client -- \
  --url 'vaylix://vaylix:vaylix@127.0.0.1:9173?output=table'
```

Enable TLS when certificate material is available:

```bash
cargo run -p server -- \
  --bind 127.0.0.1 \
  --port 9173 \
  --ssl \
  --tls-cert ./certs/server.crt \
  --tls-key ./certs/server.key

cargo run -p client -- \
  --url 'vaylix://vaylix:vaylix@127.0.0.1:9173?ssl=true' \
  --tls-ca-cert ./certs/ca.crt
```

Authentication and compression are enabled by default. For trusted local testing only, use `--disable-auth` or `--disable-compression` on the matching side.

## Docker Persistence

The server stores durable state under `/var/lib/vaylix`. Mount that path for persistence:

```bash
docker run \
  -p 9173:9173 \
  -v vaylix-data:/var/lib/vaylix \
  -e VAYLIX_USER=vaylix \
  -e VAYLIX_PASSWORD=vaylix \
  ghcr.io/<owner>/vaylix:latest
```

The data directory contains the snapshot, WAL, manifest, server-managed storage keyring, and `audit.log`.
The current durable storage format is version `2` and uses encrypted MessagePack payloads for engine state, WAL entries, manifests, and the storage keyring.

## Security and Operational Notes

- Authentication is enabled by default. `--disable-auth` exists for local/trusted testing only.
- TLS is disabled by default and enabled with `--ssl`.
- Transport compression is enabled by default. `--disable-compression` exists for compatibility and diagnostics.
- At-rest encryption is managed by the server; there is no raw `--data-key` flag.
- Audit logging is enabled by default under the data directory.
- Development defaults are convenient, not production-safe.
- Vaylix is not a distributed database yet. Do not rely on replication, sharding, or distributed ACID behavior until those features are implemented and tested.

## Quality Gates

Local validation:

```bash
cargo fmt --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
cargo audit
```

The PR workflow runs the same checks against `main`.

## Roadmap Constraints

Vaylix is being shaped for a larger future system. That means current changes should preserve room for:
- replication
- sharding
- stronger transactional guarantees
- richer audit pipelines
- transport compression negotiation for future mixed-version clients

The authoritative project context lives in [LLM.md](LLM.md).
