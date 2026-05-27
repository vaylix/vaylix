# Vaylix

Vaylix is a Rust key/value database project built around a strict transport boundary. It currently provides a single-node, string-to-string database with a framed binary protocol, a Tokio multi-client server, default-on authentication with RBAC, default-on transport compression, optional TLS/mTLS, and encrypted-at-rest persistence.

The project is intentionally structured as a serious systems codebase rather than a demo:
- protocol and engine responsibilities are separated
- errors carry stable codes and friendly names
- persistence behavior is explicit
- CI enforces formatting, linting, and tests

## Current Scope

Implemented today:
- `String -> String` data model
- custom framed binary transport protocol v2 with startup capability negotiation
- shared transport crate for client and server
- authentication enabled by default, with an explicit local-only disable flag
- in-server RBAC with encrypted user/role metadata
- optional TLS via `--ssl`, `--tls-cert`, and `--tls-key`
- optional mTLS by adding `--tls-client-ca` on the server and a client certificate/key on the client
- zstd transport compression enabled by default, with an explicit disable flag
- negotiated protocol capabilities for compression, request deadlines, server metrics, pipelining, and trace context
- WAL + snapshot durability
- logical `BACKUP` and `RESTORE` commands for portable dumps
- server-managed storage keyring with rotation support
- structured `INFO` output grouped by server, transport, storage, persistence, security, runtime, and metrics keys
- request rate limiting and command quotas
- REPL client with `plain`, `table`, and `json` output
- pretty client-side `HELP` output with command usage examples
- session transaction commands: `MULTI`, `EXEC`, `DISCARD`
- atomic single-node `EXEC` commits
- append-only audit logging

Not implemented yet:
- replication
- sharding
- distributed ACID semantics or cluster commit coordination

## Workspace

- `crates/command` — parser, lexer, command metadata
- `crates/transport` — framing, negotiation, codec, wire protocol, sync/async I/O
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

Require client certificates with mTLS:

```bash
cargo run -p server -- \
  --bind 127.0.0.1 \
  --port 9173 \
  --ssl \
  --tls-cert ./certs/server.crt \
  --tls-key ./certs/server.key \
  --tls-client-ca ./certs/client-ca.crt

cargo run -p client -- \
  --url 'vaylix://vaylix:vaylix@127.0.0.1:9173?ssl=true' \
  --tls-ca-cert ./certs/server-ca.crt \
  --tls-client-cert ./certs/client.crt \
  --tls-client-key ./certs/client.key
```

Authentication and compression are enabled by default. For trusted local testing only, use `--disable-auth` or `--disable-compression` on the matching side.

## Client Commands

Run `help` in the REPL for a formatted command reference with usage examples. Common operational commands:

```text
INFO
BACKUP
RESTORE <logical-dump-json>
SAVE
SNAPSHOT
METRICS
CREATE USER <username> PASSWORD <password>
CREATE ROLE <role>
GRANT ROLE <role> TO <username>
GRANT PERMISSION <permission> TO <role>
SHOW USERS
SHOW ROLES
WHOAMI
```

`INFO` returns deterministic section-prefixed keys such as `server.version`, `transport.protocol_version`, `storage.key_count`, `persistence.wal_size_bytes`, `security.tls_enabled`, `runtime.idle_timeout_seconds`, and `metrics.requests_total`.

`BACKUP` produces a logical JSON dump of live keys and absolute expiration timestamps. `RESTORE` accepts that JSON dump and replaces the current keyspace through one WAL-backed atomic engine batch. This is the Vaylix equivalent of a simple `pg_dump` / `pg_restore` flow:

```text
vaylix> backup
{"version":1,"created_at_ms":...,"entries":[...]}

vaylix> restore "{\"version\":1,\"created_at_ms\":...,\"entries\":[...]}"
1
```

The logical backup is online: the server stays running, and the engine worker takes a consistent in-memory view after purging expired keys. Requests behind the backup wait their turn in the engine queue. For physical persistence of the local node, use `SAVE` or `SNAPSHOT`; those write the encrypted snapshot and flush/rotate the WAL.

## RBAC

RBAC is handled by the existing server binary and client protocol. No separate admin binary is required.

On first startup, the configured `--user` / `--password` account is bootstrapped as an administrator. After that, users and roles are loaded from encrypted metadata under the data directory. Available permissions are:

```text
read write admin backup restore metrics snapshot
```

Example:

```text
vaylix> create user alice password "secret"
OK
vaylix> create role readonly
OK
vaylix> grant permission read to readonly
OK
vaylix> grant role readonly to alice
OK
vaylix> show users
alice  roles=readonly disabled=false
vaylix> whoami
username     vaylix
permissions  admin,backup,metrics,read,restore,snapshot,write
```

RBAC checks happen in the server before engine execution. The engine remains unaware of users, roles, and credentials.

## Docker Persistence

The server stores durable state under `/var/lib/vaylix`. Mount that path for persistence:

```bash
docker run \
  -p 9173:9173 \
  -v vaylix-data:/var/lib/vaylix \
  -e VAYLIX_USER=vaylix \
  -e VAYLIX_PASSWORD=vaylix \
  ghcr.io/vaylix/vaylix:latest
```

Stable releases are also tagged by version, for example `ghcr.io/vaylix/vaylix:0.2.0`.

The data directory contains the snapshot, WAL, manifest, server-managed storage keyring, and `audit.log`.
The current durable storage format is version `2` and uses encrypted MessagePack payloads for engine state, WAL entries, manifests, and the storage keyring.

## Security and Operational Notes

- Authentication is enabled by default. `--disable-auth` exists for local/trusted testing only.
- RBAC is enabled whenever authentication is enabled. `--disable-auth` bypasses both authentication and authorization.
- TLS is disabled by default and enabled with `--ssl`.
- mTLS is enabled by setting `--tls-client-ca` on the server. The client must then provide `--tls-client-cert` and `--tls-client-key`.
- Transport compression is enabled by default and negotiated during protocol startup. `--disable-compression` exists for compatibility and diagnostics.
- At-rest encryption is managed by the server; there is no raw `--data-key` flag. WAL, snapshots, and auth/RBAC metadata use encrypted storage envelopes.
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
- replication-aware transport sessions and protocol conformance fixtures

The authoritative project context lives in [LLM.md](LLM.md).
