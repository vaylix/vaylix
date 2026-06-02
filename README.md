# Vaylix

Vaylix is a Rust key/value database built around a strict transport boundary:

```text
client -> transport -> TCP/TLS -> transport -> server -> engine
```

The current server stores `String -> String` data with segmented WAL plus encrypted snapshot persistence. It includes a shared framed binary transport, a Tokio multi-client server, authentication with RBAC, optional TLS/mTLS, default-on frame compression, logical backup/restore commands, offline PITR-oriented storage subcommands, maintenance mode, hash-chained audit logging, and Raft-style HA replication with automatic leader election and quorum-backed writes.

Detailed architecture context lives in [LLM.md](LLM.md).
Benchmark guidance lives in [BENCHMARKING.md](BENCHMARKING.md).

## Downloads

Release binaries are published from tagged releases:

- Server and client archives: <https://github.com/vaylix/vaylix/releases>
- Server image: `ghcr.io/vaylix/vaylix:latest`
- Versioned server image example: `ghcr.io/vaylix/vaylix:0.7.0`

Release builds also publish SBOMs and keyless Sigstore/cosign attestations.

## Run with Docker

```bash
docker pull ghcr.io/vaylix/vaylix:latest

docker run --rm \
  -p 9173:9173 \
  -v vaylix-data:/var/lib/vaylix \
  -e VAYLIX_USER=vaylix \
  -e VAYLIX_PASSWORD=vaylix \
  -e VAYLIX_SNAPSHOT_INTERVAL_SECONDS=300 \
  ghcr.io/vaylix/vaylix:latest
```

Mount `/var/lib/vaylix` for persistence. The data directory contains snapshots, WAL, manifests, the storage keyring, encrypted auth/RBAC metadata, backups, and the audit log.

The published container uses a Debian 13 distroless runtime. It starts through the image-internal `vaylix-init` binary as root only long enough to create and repair ownership for `VAYLIX_DATA_DIR` and `VAYLIX_BACKUP_DIR`, then drops privileges and execs the server as UID/GID `65532`. This keeps Linux bind mounts working without manual host `chown` while keeping the database process unprivileged.

The server data directory defaults to `/var/lib/vaylix` in every runtime. Use `--data-dir` or `VAYLIX_DATA_DIR` only when an operator-controlled mount policy requires a different durable path. The interactive client still stores local history under the OS user data directory.

Useful runtime environment variables for containers:

- `VAYLIX_BIND`
- `VAYLIX_PORT`
- `VAYLIX_MAX_CONNECTIONS`
- `VAYLIX_DATA_DIR`
- `VAYLIX_BACKUP_DIR`
- `VAYLIX_RUNTIME_UID`
- `VAYLIX_RUNTIME_GID`
- `VAYLIX_USER`
- `VAYLIX_PASSWORD`
- `VAYLIX_SSL`
- `VAYLIX_TLS_CERT`
- `VAYLIX_TLS_KEY`
- `VAYLIX_TLS_CLIENT_CA`
- `VAYLIX_WAL_SYNC`
- `VAYLIX_LOG_REQUESTS`
- `VAYLIX_WAL_SEGMENT_SIZE_BYTES`
- `VAYLIX_WAL_RETAIN_SEGMENTS`
- `VAYLIX_SNAPSHOT_INTERVAL_SECONDS`
- `VAYLIX_EXPIRATION_SWEEP_INTERVAL_SECONDS`
- `VAYLIX_IDLE_TIMEOUT_SECONDS`
- `VAYLIX_DISABLE_AUTH`
- `VAYLIX_DISABLE_COMPRESSION`
- `VAYLIX_MAX_REQUEST_PAYLOAD_BYTES`
- `VAYLIX_MAX_KEY_BYTES`
- `VAYLIX_MAX_VALUE_BYTES`
- `VAYLIX_MAX_KEYS_PER_BATCH`
- `VAYLIX_MAX_TRANSACTION_QUEUE_LEN`
- `VAYLIX_REQUESTS_PER_SECOND`
- `VAYLIX_REQUEST_BURST`
- `VAYLIX_AUDIT_LOG_PATH`
- `VAYLIX_AUDIT_COMMANDS`
- `VAYLIX_SLOW_COMMAND_THRESHOLD_MS`
- `VAYLIX_AUTH_FAILURE_WINDOW_SECONDS`
- `VAYLIX_AUTH_FAILURE_LIMIT`
- `VAYLIX_AUTH_LOCKOUT_SECONDS`
- `VAYLIX_TRANSACTION_MAX_SECONDS`
- `VAYLIX_REPLICATION_ROLE`
- `VAYLIX_NODE_ID`
- `VAYLIX_REPLICATION_GROUP_ID`
- `VAYLIX_REPLICATION_ADVERTISE_ADDR`
- `VAYLIX_REPLICATION_UPSTREAM`
- `VAYLIX_REPLICATION_USER`
- `VAYLIX_REPLICATION_PASSWORD`
- `VAYLIX_WRITE_ACK_MODE`
- `VAYLIX_REPLICATION_ACK_TIMEOUT_MS`
- `VAYLIX_REPLICATION_POLL_INTERVAL_MS`
- `VAYLIX_REPLICATION_FETCH_BATCH_SIZE`
- `VAYLIX_REPLICATION_STALE_AFTER_SECONDS`
- `VAYLIX_REPLICATION_HEARTBEAT_INTERVAL_MS`
- `VAYLIX_REPLICATION_ELECTION_TIMEOUT_MIN_MS`
- `VAYLIX_REPLICATION_ELECTION_TIMEOUT_MAX_MS`
- `VAYLIX_CLUSTER_PEERS`

## High Availability

Vaylix 0.7.x supports a single-region HA topology using the existing `vaylix` server binary. Nodes keep stable identities, exchange vote/heartbeat/append RPCs over the normal framed transport, elect one leader, reject mutating commands on non-leaders, and commit writes according to the configured acknowledgement policy.

Recommended production shape:

- Run three voting nodes with stable `--node-id` values.
- Set `--replication-advertise-addr <host:port>` on each node.
- Set identical `--cluster-peers node-1@host1:9173,node-2@host2:9173,node-3@host3:9173` on each node.
- Seed at least one node with `--replication-role leader`; cluster startup still elects the active leader through the consensus path.
- Keep the default `--write-ack-mode replica`, or use `--write-ack-mode majority`; both map to quorum commit. `--write-ack-mode local` is explicitly weaker and is not HA-safe.

Example node:

```bash
vaylix \
  --bind 0.0.0.0 \
  --port 9173 \
  --data-dir /var/lib/vaylix \
  --node-id node-1 \
  --replication-role leader \
  --replication-advertise-addr node-1.internal:9173 \
  --cluster-peers node-1@node-1.internal:9173,node-2@node-2.internal:9173,node-3@node-3.internal:9173 \
  --write-ack-mode majority
```

Operational commands:

- `health` returns machine-readable readiness state, role, and reason.
- `show cluster` returns term, leader, quorum, sync policy, and member state.
- `show replication` returns replication progress, follower lag, and commit position.
- `cluster join <node-id> <host:port>` and `cluster remove <node-id>` update membership from the leader.

Followers may serve local stale reads; clients that require linearizable read-after-write behavior should route reads to the current leader reported by `show cluster` / `show replication`.

The engine uses a sharded in-memory store for live `String -> String` data while preserving deterministic snapshots, backups, and replication payloads. Leader and standalone nodes also maintain a committed read index for `GET`, `MGET`, `EXISTS`, and `TTL`. The index advances only after the configured WAL durability and HA acknowledgement boundary has completed, so fast-path reads do not expose an uncommitted local tail.

Leader writes are coordinated through a single HA write coordinator. Concurrent eligible writes share one ordered local WAL batch and one replicated frontier, but responses are still held until the configured local durability and quorum/all acknowledgement boundary completes.

## Run from Binaries

Start a local server:

```bash
vaylix --bind 127.0.0.1 --port 9173 --data-dir ./vaylix-data --user vaylix --password vaylix
```

Connect with the client:

```bash
vaylix-client --url 'vaylix://vaylix:vaylix@127.0.0.1:9173'
```

Enable TLS when certificate material is available:

```bash
vaylix \
  --bind 127.0.0.1 \
  --port 9173 \
  --data-dir ./vaylix-data \
  --ssl \
  --tls-cert ./certs/server.crt \
  --tls-key ./certs/server.key

vaylix-client \
  --url 'vaylix://vaylix:vaylix@127.0.0.1:9173?ssl=true' \
  --tls-ca-cert ./certs/ca.crt
```

Require mTLS by adding `--tls-client-ca` on the server and `--tls-client-cert` / `--tls-client-key` on the client.

## Build from Source

```bash
cargo build --workspace
cargo test --workspace
```

Release binaries:

```bash
cargo build --release -p server
cargo build --release -p client
```

Benchmarking:

```bash
cargo bench-engine
cargo bench-transport
cargo bench-load -- managed-single-node --server-bin target/debug/vaylix --duration-seconds 30
```

Offline storage and PITR operations:

```bash
vaylix storage verify --data-dir /var/lib/vaylix
vaylix storage migrate --data-dir /var/lib/vaylix
vaylix pitr inspect --data-dir /var/lib/vaylix
vaylix pitr restore \
  --source-dir /var/lib/vaylix \
  --target-dir /tmp/vaylix-restore \
  --to-sequence 1234
```

Quality gates:

```bash
cargo fmt --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
cargo audit
```

## Essential Notes

- Authentication and RBAC are enabled by default. `--disable-auth` is for trusted local testing only.
- Development credentials default to `vaylix / vaylix`; production deployments should override them.
- When a persisted auth store already exists, non-default `--user` / `--password` or `VAYLIX_USER` / `VAYLIX_PASSWORD` values are reconciled into the env-managed bootstrap admin on startup. Changing those startup credentials rotates that admin and retires the previous env-managed admin.
- Compression is enabled by default and can be disabled for diagnostics with `--disable-compression`.
- Request-level server logging is disabled by default on the hot path. Enable `--log-requests` / `VAYLIX_LOG_REQUESTS=true` only when request tracing is needed.
- Security/operator audit events remain enabled by default. Generic per-command audit lines are opt-in through `--audit-commands` / `VAYLIX_AUDIT_COMMANDS=true` because full command auditing is intentionally expensive on read-heavy in-memory workloads.
- TLS is opt-in with `--ssl`; production deployments should provide TLS certificates.
- TLS certificates are validated at startup for basic expiry/loadability, and the server reloads configured TLS material on Unix `SIGHUP`.
- `METRICS` uses an OpenTelemetry-aligned metric contract under the `vaylix.*` namespace, and `METRICS PROM` exposes Prometheus-safe names translated from that contract.
- Backups created with `BACKUP TO <path>` are sandboxed under `--backup-dir` / `VAYLIX_BACKUP_DIR`, defaulting to `<data-dir>/backups`.
- WAL is segmented under `<data-dir>/wal`. The write path keeps the active WAL segment open, runs append/flush/sync work through a dedicated WAL I/O worker, and can group concurrent writes behind the configured `VAYLIX_WAL_SYNC` durability boundary without acknowledging a write before that boundary completes.
- `maintenance on` switches the node into persisted read-only admin mode until `maintenance off`.
- Audit JSONL records are SHA-256 hash chained and verified on startup. This is tamper-evident logging, not non-repudiation without external anchoring.
- HA writes should use the default quorum acknowledgement mode. `local` acknowledgement is for explicitly weaker development or disaster-recovery workflows.
- Vaylix does not implement sharding, MVCC, distributed ACID transactions, or linearizable follower reads.
