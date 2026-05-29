# Vaylix

Vaylix is a Rust key/value database built around a strict transport boundary:

```text
client -> transport -> TCP/TLS -> transport -> server -> engine
```

The current server is single-node and stores `String -> String` data with segmented WAL plus encrypted snapshot persistence. It includes a shared framed binary transport, a Tokio multi-client server, authentication with RBAC, optional TLS/mTLS, default-on frame compression, logical backup/restore commands, offline PITR-oriented storage subcommands, maintenance mode, and hash-chained audit logging.

Detailed architecture context lives in [LLM.md](LLM.md).

## Downloads

Release binaries are published from tagged releases:

- Server and client archives: <https://github.com/vaylix/vaylix/releases>
- Server image: `ghcr.io/vaylix/vaylix:latest`
- Versioned server image example: `ghcr.io/vaylix/vaylix:0.2.0`

Release builds also publish SBOMs and keyless Sigstore/cosign attestations.

## Run with Docker

```bash
docker pull ghcr.io/vaylix/vaylix:latest

docker run --rm \
  -p 9173:9173 \
  -v vaylix-data:/var/lib/vaylix \
  -e VAYLIX_USER=vaylix \
  -e VAYLIX_PASSWORD=vaylix \
  ghcr.io/vaylix/vaylix:latest
```

Mount `/var/lib/vaylix` for persistence. The data directory contains snapshots, WAL, manifests, the storage keyring, encrypted auth/RBAC metadata, backups, and the audit log.

## Run from Binaries

Start a local server:

```bash
vaylix --bind 127.0.0.1 --port 9173 --user vaylix --password vaylix
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
- Compression is enabled by default and can be disabled for diagnostics with `--disable-compression`.
- TLS is opt-in with `--ssl`; production deployments should provide TLS certificates.
- TLS certificates are validated at startup for basic expiry/loadability, and the server reloads configured TLS material on Unix `SIGHUP`.
- Backups created with `BACKUP TO <path>` are sandboxed under `--backup-dir` / `VAYLIX_BACKUP_DIR`, defaulting to `<data-dir>/backups`.
- WAL is segmented under `<data-dir>/wal`, snapshots no longer discard all retained WAL history, and PITR restore is currently an offline operation that writes a new target data directory.
- `maintenance on` switches the node into persisted read-only admin mode until `maintenance off`.
- Audit JSONL records are SHA-256 hash chained and verified on startup. This is tamper-evident logging, not non-repudiation without external anchoring.
- Vaylix is not distributed yet. Replication, sharding, MVCC, and distributed ACID semantics remain roadmap items.
