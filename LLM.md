# Vaylix Project Context

This file is the authoritative non-sensitive project context for humans and AI agents working in this repository. Any change to protocol behavior, CLI semantics, persistence format, authentication, TLS, workflows, or operational defaults must update this file in the same change.

## Project Summary

Vaylix is a Rust database workspace centered on a transport-first architecture:

`client -> transport -> TCP/TLS -> transport -> server -> engine`

The current implementation is a UTF-8-key / opaque-byte-value key/value database with:

- a custom framed binary protocol v2 with startup capability negotiation
- a shared transport crate used by both client and server
- a Tokio multi-client server
- authenticated client connections with in-server RBAC
- optional TLS and mTLS client/server transport
- segmented encrypted-at-rest WAL and encrypted snapshots
- offline PITR-oriented storage inspection, migration, verification, and restore subcommands
- append-only audit logging
- default-on negotiated outbound frame-level zstd compression
- deterministic command parsing and explicit error codes
- protocol-level OTel-aligned metrics with Prometheus text export through `METRICS PROM`
- Raft-style HA replication with automatic leader election and quorum-backed write acknowledgement

The long-term target is broader:

- scale from replicated single-region deployments to sharded deployments
- keep the transport layer evolvable enough for replication traffic and cluster coordination
- harden transactional behavior toward stronger ACID guarantees than the current session-queued model
- add richer auditability and replication-oriented protocol sessions without breaking engine layering

## Workspace Layout

- `crates/command`
  - lexer, parser, command metadata, parser errors
- `crates/transport`
  - frame layout, opcodes, request/response types, codec, sync/async framed I/O
- `crates/engine`
  - sharded in-memory state, expirations, segmented WAL, snapshots, manifest, recovery, storage encryption, key rotation
- `crates/server`
  - Tokio listener, authentication, RBAC, TLS accept, session handling, quotas, rate limiting, maintenance mode, engine worker runtime
- `crates/client`
  - REPL, URL parsing, TLS client connection, output rendering

## Current Data Model

- User-visible model: UTF-8 keys with opaque byte values
- In-memory map: sharded `DashMap<String, StoredValue>`
- Stored value fields: byte payload, absolute expiration timestamp, and monotonic `u64` version
- Expirations: per-key absolute timestamps in milliseconds stored beside the value
- CAS: `SET <key> <value> IF VERSION <version>` performs a deterministic non-mutating failure on version mismatch
- Leader writes: eligible standalone commands are batched by a dedicated HA write coordinator into one local WAL batch and one replicated frontier before acknowledgement
- Supported command families:
  - auth
  - ping
  - get/getdel/getex
  - set/setnx
  - mget/mset
  - del/exists
  - incr/decr
  - expire/ttl/persist
  - rename/renamenx
  - scan/dbsize/info/metrics/metrics-prom/list/count
  - clear/save/snapshot
  - backup/restore
  - backup-to-file/backup-verify/restore-from-file/restore-check
  - maintenance on/off/status
  - health/show cluster/show replication/cluster join/cluster remove
  - create/drop user and role
  - alter user password
  - grant/revoke role and permission
  - show users/show roles/show grants/whoami
  - multi/exec/discard

## Transaction and ACID Status

Current state:

- writes are durable through WAL + snapshot
- command execution within the engine is serialized through a dedicated engine worker
- session transactions are queued with `MULTI` / `EXEC` / `DISCARD`
- `EXEC` commits as one atomic WAL-backed batch on a single node
- transactions are bounded by a server-side lifetime limit and are discarded on timeout
- sequence-tagged or pipelined requests are rejected while a transaction is active on a connection

Not yet true:

- MVCC
- distributed transactions
- formal isolation levels beyond serialized leader execution

Design direction:

- keep transaction boundaries explicit in transport and server layers
- keep transaction commits bound to deterministic WAL order and replicated commit position
- avoid protocol choices that assume one process owns all future execution forever

Agents should describe the current implementation honestly. Do not claim full ACID today.

## Transport Protocol

- Framed binary protocol
- Protocol magic: `VTP2`
- Protocol version: `2`
- Frame header includes:
  - magic
  - version
  - flags
  - payload length
  - frame checksum
- Requests contain:
  - `request_id: UUID`
  - opcode
  - optional metadata: deadline milliseconds, trace id, sequence number
  - payload
- Responses contain:
  - `request_id: UUID`
  - status
  - payload
- Remote errors are structured:
  - stable error code
  - friendly error name
  - message

### Startup Negotiation

Every client connection sends a required startup hello before command frames. The client hello carries protocol version, client name/version, supported capabilities, desired compression, maximum frame size, and auth intent. The server hello returns accepted capabilities, selected compression, effective maximum frame size, server id, and structured startup rejection details when negotiation fails.

Current negotiated capabilities:

- `zstd`
- `request_deadline`
- `server_metrics`
- `pipelining`
- `trace_context`

Protocol `0.2.x`, `0.3.x`, and `0.4.x` intentionally reject pre-v2 frames. `0.1.0` clients and servers are not wire-compatible with `0.2.0+`.
Within protocol v2, `0.3.0` changes successful `EXEC` responses from a lossy string list to a structured typed result payload. `0.2.x` clients are therefore not transaction-wire-compatible with `0.3.0+` servers.
Parser hardening is covered by deterministic malformed-frame tests and a bounded `cargo-fuzz` target under `fuzz/`.

### Request IDs

Request IDs are UUIDs, not random integers. This removes the old local counter/random collision concerns and supports pipelined response correlation.

## TLS

TLS is supported but disabled by default.

Client behavior:

- the client uses plaintext TCP by default
- `--ssl` opens a TLS connection
- connection URL query `ssl=true` also enables TLS
- system root store by default
- optional custom CA via `--tls-ca-cert`
- optional mTLS client identity via `--tls-client-cert` and `--tls-client-key`
- connection URL query params `client_cert=/path/to/client.crt` and `client_key=/path/to/client.key` can also provide mTLS material

Server inputs:

- `--ssl`
- `--tls-cert`
- `--tls-key`
- `--tls-client-ca`

When `--ssl` is enabled on the server, both `--tls-cert` and `--tls-key` are required. Plain TCP remains useful for local development and private test networks, but production deployments should enable TLS.

When `--tls-client-ca` is provided, the server requires clients to present a certificate chaining to that CA. mTLS is additive to username/password authentication; it should not be described as replacing application-level auth.

Important server runtime settings are also exposed as environment variables for container use. Current env-backed server inputs include:

- `VAYLIX_BIND`
- `VAYLIX_PORT`
- `VAYLIX_MAX_CONNECTIONS`
- `VAYLIX_DATA_DIR`
- `VAYLIX_BACKUP_DIR`
- `VAYLIX_USER`
- `VAYLIX_PASSWORD`
- `VAYLIX_SSL`
- `VAYLIX_TLS_CERT`
- `VAYLIX_TLS_KEY`
- `VAYLIX_TLS_CLIENT_CA`
- `VAYLIX_WAL_SYNC`
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
- `VAYLIX_SLOW_COMMAND_THRESHOLD_MS`
- `VAYLIX_AUTH_FAILURE_WINDOW_SECONDS`
- `VAYLIX_AUTH_FAILURE_LIMIT`
- `VAYLIX_AUTH_LOCKOUT_SECONDS`
- `VAYLIX_TRANSACTION_MAX_SECONDS`

Operational hardening:

- TLS startup fails if the certificate is already expired or the certificate/key pair cannot be loaded together.
- TLS metadata such as certificate expiry and last reload timestamps is surfaced in `INFO`.
- On Unix, the server reloads the configured TLS certificate, key, and client CA on `SIGHUP`. Reload failures keep the previous live TLS config.

## Authentication and RBAC

Authentication is enabled by default.

Development defaults:

- username: `vaylix`
- password: `vaylix`

These defaults exist for local development only. Production deployments should always override them.
Use `--disable-auth` only for local/trusted testing. When auth is disabled on the server, commands execute without an `AUTH` handshake and RBAC checks are bypassed.

Password policy and auth throttling:

- `CREATE USER` and `ALTER USER PASSWORD` require at least 12 characters with at least one ASCII letter and one ASCII digit.
- The bootstrap default credentials remain accepted only for initial development bootstrap compatibility.
- The server enforces auth failure windows and temporary lockouts per `(username, peer)` tuple using:
  - `--auth-failure-window-seconds`
  - `--auth-failure-limit`
  - `--auth-lockout-seconds`

RBAC is implemented inside the existing server binary and transport protocol. There is no separate admin binary. On first startup, the configured `--user` / `--password` account is bootstrapped as an admin user. Once `<data-dir>/auth.bin` exists, users and roles are loaded from encrypted RBAC metadata. Non-default configured bootstrap credentials are reconciled into the persisted admin account on startup, and the default `vaylix / vaylix` bootstrap user is retired if it still accepts the default password.

Current permissions:

- `read`
- `write`
- `admin`
- `backup`
- `restore`
- `metrics`
- `snapshot`
- `clear`
- `user_admin`
- `role_admin`

Current admin commands:

- `create user <username> password <password>`
- `alter user <username> password <password>`
- `drop user <username>`
- `create role <role>`
- `drop role <role>`
- `grant role <role> to <username>`
- `revoke role <role> from <username>`
- `grant permission <permission> to <role>`
- `grant permission <permission> on <pattern> to <role>`
- `revoke permission <permission> from <role>`
- `revoke permission <permission> on <pattern> from <role>`
- `show users`
- `show roles`
- `show grants`
- `show grants for user <username>`
- `show grants for role <role>`
- `whoami`

Permission grants are pattern-scoped. The legacy syntax `grant permission <permission> to <role>` is an alias for `grant permission <permission> on * to <role>`. Patterns are glob-like over keys, not SQL schemas or tables. Key-bearing commands require every key to match the relevant permission grant. The `admin` permission bypasses pattern checks. Destructive and administrative operations use explicit permissions: `CLEAR` / `FLUSHDB` require `clear`, restore commands require `restore`, user management requires `user_admin`, and role/grant management requires `role_admin`.

Grant inspection rules:

- `show grants` returns the current authenticated user's roles and resolved grants
- `show grants for user <username>` requires `user_admin` or `admin`
- `show grants for role <role>` requires `role_admin` or `admin`

Authorization happens in the server after authentication and before engine execution. The engine must remain unaware of users, credentials, roles, and permissions. Existing authenticated sessions remain valid after password rotation; new authentication attempts use the rotated password.

Client connection string format:

- `vaylix://user:password@host:port`

Supported query parameters:

- `ssl=true`
- `output=plain|table|json`
- `ca_cert=/path/to/ca.pem`
- `client_cert=/path/to/client.crt`
- `client_key=/path/to/client.key`
- `auth=false`
- `compression=none|zstd`

CLI flags override URL-derived values when both are provided.

## Persistence

Durability model:

- encrypted snapshot
- segmented WAL replay on startup
- manifest metadata for snapshot state
- storage format version `3`
- MessagePack-based engine serialization inside encrypted snapshot, WAL, manifest, and keyring files

Snapshot flow:

1. purge expired keys
2. optionally rotate the active storage key if rotation is due
3. seal the active WAL segment
4. serialize state
5. encrypt the snapshot payload
6. write temp file
7. fsync temp file
8. atomic rename
9. write manifest
10. fsync manifest
11. open a new active WAL segment
12. prune sealed segments older than retention

Recovery flow:

1. load or create the storage keyring
2. load and verify manifest
3. decrypt and deserialize snapshot
4. replay and verify retained WAL segments in order

WAL management:

- WAL lives under `<data-dir>/wal/`
- active segment name: `active-<start_sequence>.wal`
- sealed segment name: `<start_sequence>-<end_sequence>.wal`
- the write path keeps the active segment open through a stateful writer instead of reopening and rediscovering the active segment for every append
- WAL append/flush/sync work runs behind a dedicated WAL I/O worker while sequence assignment remains in the engine coordinator
- in-memory WAL entry term/checksum identities are cached for replication metadata lookups
- WAL recovery rejects ambiguous layouts with multiple active segment files
- Snapshot, manifest, and keyring replacement share a private durable engine-store helper
- Unix snapshot, manifest, keyring, WAL segment, and cluster-state rename/create paths sync the parent directory after atomic replacement
- eligible standalone writes may be appended as bounded batches; `flush` and `sync` modes must not acknowledge a write before the configured batch durability boundary completes
- runtime controls:
  - `--wal-segment-size-bytes`
  - `--wal-retain-segments`

Legacy `wal.log` from the pre-segmented layout is no longer accepted on normal startup. Operators must migrate legacy storage explicitly with the server binary subcommands described below.

### Logical Backup and Restore

Vaylix also supports logical backups through database commands:

- `BACKUP`
- `RESTORE <logical-dump-json>`
- `BACKUP TO <path>`
- `BACKUP VERIFY <logical-dump-json>`
- `BACKUP VERIFY FROM <path>`
- `RESTORE FROM <path>`
- `RESTORE CHECK <logical-dump-json>`
- `RESTORE CHECK FROM <path>`

`BACKUP` returns a JSON dump containing format version, creation timestamp, source engine metadata, live key/byte-value entries, stored value versions, and absolute expiration timestamps. It is online: the server remains available, but the engine worker serializes the backup against one consistent purged in-memory view, so later engine requests wait in queue until the dump is produced.

`RESTORE` accepts the logical JSON dump and replaces the current keyspace with live dump entries through one WAL-backed atomic engine batch. Entries whose absolute expiration timestamp is already in the past are skipped. This is separate from physical `SAVE` / `SNAPSHOT`, which persist the local node’s encrypted snapshot and flush/rotate the WAL.

`BACKUP TO <path>` and `RESTORE FROM <path>` operate on server-local files under the configured backup directory only. The server resolves and canonicalizes paths, rejects `..` traversal, and rejects paths outside the backup directory. `RESTORE CHECK` validates backup JSON, backup version, entry schema, string fields, and expired-entry handling without mutating engine state or WAL; it returns the count of live entries that would be restored.

When `BACKUP TO <path>` writes a server-side dump, the server also writes `<path>.manifest.json`. The sidecar manifest records backup version, creation timestamp, source engine version, source sequence, entry count, dump byte length, SHA-256 digest, and hash algorithm. `BACKUP VERIFY <logical-dump-json>` validates inline backup JSON and returns deterministic entries such as `status=ok`, `entries=<n>`, and `sha256=<hash>`. `BACKUP VERIFY FROM <path>` verifies both the sidecar manifest and the dump before validating backup contents.

Backup directory:

- server arg/env: `--backup-dir` / `VAYLIX_BACKUP_DIR`
- default: `<data-dir>/backups`

### Offline Storage and PITR Operations

The existing `vaylix` server binary also provides offline subcommands for storage maintenance:

- `vaylix storage migrate --data-dir <dir>`
- `vaylix storage verify --data-dir <dir>`
- `vaylix pitr inspect --data-dir <dir>`
- `vaylix pitr restore --source-dir <dir> --target-dir <dir> (--to-sequence <u64> | --to-timestamp-ms <u64>)`

Current PITR scope:

- offline-first only
- restore writes a new target data directory and does not mutate the source directory in place
- restore replays the latest valid snapshot plus retained WAL segments up to the requested sequence or timestamp

### Logical Backup and Restore

Vaylix also supports logical backups through database commands:

- `BACKUP`
- `RESTORE <logical-dump-json>`
- `BACKUP TO <path>`
- `BACKUP VERIFY <logical-dump-json>`
- `BACKUP VERIFY FROM <path>`
- `RESTORE FROM <path>`
- `RESTORE CHECK <logical-dump-json>`
- `RESTORE CHECK FROM <path>`

`BACKUP` returns a JSON dump containing format version, creation timestamp, source engine metadata, live key/byte-value entries, stored value versions, and absolute expiration timestamps. It is online: the server remains available, but the engine worker serializes the backup against one consistent purged in-memory view, so later engine requests wait in queue until the dump is produced.

`RESTORE` accepts the logical JSON dump and replaces the current keyspace with live dump entries through one WAL-backed atomic engine batch. Entries whose absolute expiration timestamp is already in the past are skipped. This is separate from physical `SAVE` / `SNAPSHOT`, which persist the local node’s encrypted snapshot and flush/rotate the WAL.

`BACKUP TO <path>` and `RESTORE FROM <path>` operate on server-local files under the configured backup directory only. The server resolves and canonicalizes paths, rejects `..` traversal, and rejects paths outside the backup directory. `RESTORE CHECK` validates backup JSON, backup version, entry schema, string fields, and expired-entry handling without mutating engine state or WAL; it returns the count of live entries that would be restored.

When `BACKUP TO <path>` writes a server-side dump, the server also writes `<path>.manifest.json`. The sidecar manifest records backup version, creation timestamp, source engine version, source sequence, entry count, dump byte length, SHA-256 digest, and hash algorithm. `BACKUP VERIFY <logical-dump-json>` validates inline backup JSON and returns deterministic entries such as `status=ok`, `entries=<n>`, and `sha256=<hash>`. `BACKUP VERIFY FROM <path>` verifies both the sidecar manifest and the dump before validating backup contents.

Backup directory:

- server arg/env: `--backup-dir` / `VAYLIX_BACKUP_DIR`
- default: `<data-dir>/backups`

### Storage Encryption

At-rest encryption is server-managed. There is no user-facing `--data-key` flag anymore.

Current model:

- the server loads or creates a local storage keyring under the data directory
- the active storage key is used to encrypt new snapshots and WAL entries
- keys can be rotated by the server and old keys remain available for decryption of older persisted data

This is meant to keep persistence concerns under server control rather than exposing raw key material as a CLI requirement.

Older pre-version-2 storage files are not migrated automatically. Recovery must fail closed with an unsupported-format or decode error rather than silently reading incompatible persisted data.

## Audit Logging

Audit logging is implemented as append-only JSON lines under the data directory by default.

- default path: `<data-dir>/audit.log`
- optional override: `--audit-log-path`
- generic every-command audit is opt-in with `--audit-commands` / `VAYLIX_AUDIT_COMMANDS=true`

Each event records:

- audit format version
- monotonically increasing audit sequence
- SHA-256 hash algorithm name
- previous event hash
- current event hash
- timestamp
- connection id
- peer address
- authenticated username if present
- request id
- opcode name
- response status
- error code when applicable
- latency in milliseconds
- event type
- sanitized details map

The audit chain uses a fixed zero genesis hash for the first event. On startup, the server verifies existing audit lines and fails closed if a line has malformed JSON, invalid sequence, mismatched previous hash, mismatched event hash, or an unsupported hash algorithm. This makes local tampering detectable, but it is not non-repudiation: a local attacker who can rewrite the entire log can recompute a fresh chain unless the latest hash is anchored externally.

Passwords and payload contents are not written to the audit log. Semantic event types are recorded for authentication success/failure and RBAC/auth mutations such as create/drop user, password rotation, create/drop role, grant/revoke role, and grant/revoke permission. Generic command audit lines are disabled by default for the read/write hot path; enable `--audit-commands` when operators need every command represented in the hash chain. Slow command audit events are emitted when command latency is at or above `--slow-command-threshold-ms` / `VAYLIX_SLOW_COMMAND_THRESHOLD_MS`; the default is `100`, and `0` disables slow-command events.

## Scalability Direction

Current state:

- Raft-style HA replication over the main transport
- follower/candidate/leader roles with persisted term, voted-for, and member metadata
- pre-vote, majority election, heartbeats, append entries, snapshot install, and automatic leader failover
- quorum-backed write acknowledgement by default through the `replica` / `majority` mode
- follower-side catch-up through append entries, retained WAL history, and snapshot bootstrap fallback
- consensus vote and heartbeat RPCs from unknown non-voter node IDs are rejected before term or membership mutation
- leader commit waiters are notified on commit-index advancement rather than relying only on fixed polling sleeps
- no sharding

Architectural target:

- replication traffic should reuse transport framing rather than invent a second ad hoc wire path
- request routing should remain decoupled from the engine so a shard-router or replica applier can be introduced later
- storage and protocol identifiers should remain stable enough for cluster metadata and log replication

Do not document sharding, MVCC, distributed transactions, or linearizable follower reads as implemented today. They remain roadmap constraints, not delivered features.

## Compression

Transport compression is enabled by default for outbound frames in the current client/server binaries:

- default mode: `zstd`
- default threshold: `1024` bytes
- compression is selected during startup negotiation
- readers decompress automatically based on the frame flag
- frame checksums validate the on-wire compressed payload
- large async zstd compression/decompression is offloaded from network tasks
- `--disable-compression` disables outbound compression on that process

Still missing:

- compression policy coordination between mixed-version peers
- replication-stream tuning

## Abuse Controls and Runtime Guards

Current runtime protections:

- request-level server logs are disabled by default on the hot path; use `--log-requests` / `VAYLIX_LOG_REQUESTS=true` for request tracing
- per-session token-bucket rate limiting
- request payload size limits
- key/value size limits
- key-count limits for batch commands
- transaction queue length limits
- transaction lifetime limits
- authentication failure lockouts
- idle connection timeouts

## Server Runtime

- Tokio multi-thread runtime
- concurrent client sessions
- engine work is funneled through a dedicated engine worker
- protocol startup negotiation is required before command execution
- optional background snapshotter
- optional background expiration sweeper
- persisted maintenance mode with read-only admin behavior
- plaintext TCP by default
- TLS accept path when `--ssl` is enabled
- Unix `SIGHUP` TLS reload support
- auth and compression enabled by default with explicit disable flags

### Maintenance Mode

Protocol admin commands:

- `maintenance on`
- `maintenance off`
- `maintenance status`

Maintenance mode is persisted with a sentinel file under the data directory so it survives restart.

Current behavior:

- allowed: reads, `INFO`, `METRICS`, `METRICS PROM`, backup verification flows, `SHOW *`, `WHOAMI`, and `maintenance status`
- rejected: mutating writes, `MULTI` / `EXEC`, restore flows, and auth/RBAC mutation commands

## Structured INFO

`INFO` returns deterministic key/value entries with section prefixes rather than one unstructured blob. Current sections:

- `server.*`
- `transport.*`
- `storage.*`
- `persistence.*`
- `security.*`
- `runtime.*`
- `metrics.*`

Examples include `server.version`, `transport.protocol_version`, `storage.key_count`, `persistence.wal_size_bytes`, `security.tls_enabled`, `runtime.idle_timeout_seconds`, and `metrics.vaylix.server.request.count`.

Runtime/security examples also include quota and operational settings such as `runtime.max_key_bytes`, `runtime.max_value_bytes`, `runtime.max_keys_per_batch`, `runtime.max_transaction_queue_len`, `runtime.requests_per_second`, `runtime.request_burst`, `runtime.backup_dir`, `runtime.slow_command_threshold_ms`, `security.auth_enabled`, `security.rbac_enabled`, `security.mtls_enabled`, and `transport.compression_mode`.
They also include operational hardening fields such as `runtime.transaction_max_seconds`, `runtime.auth_failure_window_seconds`, `runtime.auth_failure_limit`, `runtime.auth_lockout_seconds`, `runtime.wal_segment_size_bytes`, `runtime.wal_retain_segments`, `runtime.maintenance_mode`, `security.cert_not_after_ms`, `security.cert_days_remaining`, `security.last_tls_reload_success_at_ms`, and `security.last_tls_reload_failure_at_ms`.

`METRICS` returns deterministic key/value metric entries using OpenTelemetry-aligned dotted names under the `vaylix.*` namespace, for example `vaylix.server.request.count` and `vaylix.server.connection.active`. The contract follows OpenTelemetry metric naming rules: dotted namespacing, no `_total` suffixes, and fixed instrument semantics and units per metric.

`METRICS PROM` returns the same metric contract translated into Prometheus-safe text exposition names by replacing dots with underscores, for example `vaylix_server_request_count`. There is intentionally no separate HTTP listener in this pass.

## Client Runtime

- interactive REPL
- local history uses the OS user data directory through `directories::ProjectDirs`
- local-only commands:
  - `help`
  - `exit`
- output modes:
  - `plain`
  - `table`
  - `json`

The interactive client should print command results cleanly. Per-command transport logs are intentionally suppressed in normal output.
The local `help` command is formatted as a readable command reference with grammar-aligned usage strings rather than a single-line command list.

## Packaging, Docker, and Data Directory

- default native/local server data directory: `./default.vaylix`
- default container data directory: `/var/lib/vaylix`
- intended Docker volume mount:
  - `-v vaylix-data:/var/lib/vaylix`
- runtime container base: Debian 13 distroless `gcr.io/distroless/cc-debian13`
- image bootstrap binary: `/usr/local/bin/vaylix-init`
- server runtime identity after bootstrap: UID/GID `65532`

This path is the durable storage root for:

- snapshots
- WAL segments
- manifest
- storage keyring
- encrypted auth/RBAC metadata
- logical backup files under the backup directory

The image starts `vaylix-init` as root only to create `VAYLIX_DATA_DIR` / `VAYLIX_BACKUP_DIR`, recursively repair data-directory ownership for Linux bind mounts, then call `setgid`, `setuid`, and `exec` the real `vaylix` server command. Do not replace this with a shell entrypoint; the runtime image is distroless and intentionally has no shell or package manager.

Do not reintroduce per-user OS project directories for server storage. Native/local server runs default to `./default.vaylix`; Docker images override `VAYLIX_DATA_DIR` to `/var/lib/vaylix`. Client-local state such as REPL history may continue to use user-space project directories.

## CI and Release

Pull request CI runs:

- `cargo fmt --check`
- `cargo clippy --locked --workspace --all-targets --all-features -- -D warnings`
- `cargo test --locked --workspace`
- `cargo audit --file Cargo.lock`

Release workflow goal:

- publish multi-OS client binaries
- publish multi-OS server binaries
- publish a multi-arch server image to GHCR with both `latest` and the release version tag, for example `0.9.0`
- publish SBOMs for release archives and Docker images
- use keyless Sigstore/cosign signing and attestations through GitHub OIDC

## Current Gaps

- full distributed ACID semantics are not implemented
- no sharding yet
- no MVCC or linearizable follower-read mode yet
- PITR is offline-first and there is no online WAL archive/PITR workflow yet
- backup/restore remains logical JSON based; there is no separate streaming backup utility yet
- no TLS certificate automation or rotation workflow yet
- TLS is opt-in rather than mandatory

## Guidance for Agents

- keep transport concerns out of the engine
- keep docs honest about current capability vs roadmap
- do not reintroduce a user-facing raw `data_key` CLI argument
- prefer UUID-based request tracking consistently
- add tests for any protocol, persistence, auth, TLS, or workflow change
- Native and local server runs default to `./default.vaylix`.
- Docker images override the server data root to `/var/lib/vaylix`.
- `--data-dir` and `VAYLIX_DATA_DIR` remain the authoritative overrides in both environments.
