# Vaylix Project Context

This file is the authoritative non-sensitive project context for humans and AI agents working in this repository. Any change to protocol behavior, CLI semantics, persistence format, authentication, TLS, workflows, or operational defaults must update this file in the same change.

## Project Summary

Vaylix is a Rust database workspace centered on a transport-first architecture:

`client -> transport -> TCP/TLS -> transport -> server -> engine`

The current implementation is a single-node, string-to-string key/value database with:
- a custom framed binary protocol v2 with startup capability negotiation
- a shared transport crate used by both client and server
- a Tokio multi-client server
- authenticated client connections with in-server RBAC
- optional TLS and mTLS client/server transport
- encrypted-at-rest WAL and snapshots
- append-only audit logging
- default-on negotiated outbound frame-level zstd compression
- deterministic command parsing and explicit error codes

The long-term target is broader:
- scale from a single node to replicated and sharded deployments
- keep the transport layer evolvable enough for replication traffic and cluster coordination
- harden transactional behavior toward stronger ACID guarantees than the current session-queued model
- add richer auditability and replication-oriented protocol sessions without breaking engine layering

## Workspace Layout

- `crates/command`
  - lexer, parser, command metadata, parser errors
- `crates/transport`
  - frame layout, opcodes, request/response types, codec, sync/async framed I/O
- `crates/engine`
  - in-memory state, expirations, WAL, snapshots, manifest, recovery, storage encryption, key rotation
- `crates/server`
  - Tokio listener, authentication, RBAC, TLS accept, session handling, quotas, rate limiting, engine worker runtime
- `crates/client`
  - REPL, URL parsing, TLS client connection, output rendering

## Current Data Model

- User-visible model: `String -> String`
- In-memory map: `BTreeMap<String, String>`
- Expirations: per-key absolute timestamps in milliseconds
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
  - scan/dbsize/info/metrics/list/count
  - clear/save/snapshot
  - backup/restore
  - backup-to-file/restore-from-file/restore-check
  - create/drop user and role
  - alter user password
  - grant/revoke role and permission
  - show users/show roles/whoami
  - multi/exec/discard

## Transaction and ACID Status

Current state:
- writes are durable through WAL + snapshot
- command execution within the engine is serialized through a dedicated engine worker
- session transactions are queued with `MULTI` / `EXEC` / `DISCARD`
- `EXEC` commits as one atomic WAL-backed batch on a single node

Not yet true:
- MVCC
- distributed transactions
- formal isolation levels
- replication-aware commit coordination

Design direction:
- keep transaction boundaries explicit in transport and server layers
- move toward WAL-backed atomic commit groups and stronger isolation in engine internals
- avoid protocol choices that assume single-node execution forever

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

Protocol `0.2.x` intentionally rejects pre-v2 frames. `0.1.0` clients and servers are not wire-compatible with `0.2.0`.

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

## Authentication and RBAC

Authentication is enabled by default.

Development defaults:
- username: `vaylix`
- password: `vaylix`

These defaults exist for local development only. Production deployments should always override them.
Use `--disable-auth` only for local/trusted testing. When auth is disabled on the server, commands execute without an `AUTH` handshake and RBAC checks are bypassed.

RBAC is implemented inside the existing server binary and transport protocol. There is no separate admin binary. On first startup, the configured `--user` / `--password` account is bootstrapped as an admin user. Once `<data-dir>/auth.bin` exists, users and roles are loaded from encrypted RBAC metadata instead of being recreated from CLI defaults.

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
- `whoami`

Permission grants are pattern-scoped. The legacy syntax `grant permission <permission> to <role>` is an alias for `grant permission <permission> on * to <role>`. Patterns are glob-like over keys, not SQL schemas or tables. Key-bearing commands require every key to match the relevant permission grant. The `admin` permission bypasses pattern checks. Destructive and administrative operations use explicit permissions: `CLEAR` / `FLUSHDB` require `clear`, restore commands require `restore`, user management requires `user_admin`, and role/grant management requires `role_admin`.

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
- WAL replay on startup
- manifest metadata for snapshot state
- storage format version `2`
- MessagePack-based engine serialization inside encrypted snapshot, WAL, manifest, and keyring files

Snapshot flow:
1. purge expired keys
2. optionally rotate the active storage key if rotation is due
3. serialize state
4. encrypt the snapshot payload
5. write temp file
6. fsync temp file
7. atomic rename
8. write manifest
9. fsync manifest
10. truncate WAL

Recovery flow:
1. load or create the storage keyring
2. load and verify manifest
3. decrypt and deserialize snapshot
4. replay and verify WAL entries

### Logical Backup and Restore

Vaylix also supports logical backups through database commands:
- `BACKUP`
- `RESTORE <logical-dump-json>`
- `BACKUP TO <path>`
- `RESTORE FROM <path>`
- `RESTORE CHECK <logical-dump-json>`
- `RESTORE CHECK FROM <path>`

`BACKUP` returns a JSON dump containing format version, creation timestamp, source engine metadata, live string key/value entries, and absolute expiration timestamps. It is online: the server remains available, but the engine worker serializes the backup against one consistent purged in-memory view, so later engine requests wait in queue until the dump is produced.

`RESTORE` accepts the logical JSON dump and replaces the current keyspace with live dump entries through one WAL-backed atomic engine batch. Entries whose absolute expiration timestamp is already in the past are skipped. This is separate from physical `SAVE` / `SNAPSHOT`, which persist the local node’s encrypted snapshot and flush/rotate the WAL.

`BACKUP TO <path>` and `RESTORE FROM <path>` operate on server-local files under the configured backup directory only. The server resolves and canonicalizes paths, rejects `..` traversal, and rejects paths outside the backup directory. `RESTORE CHECK` validates backup JSON, backup version, entry schema, string fields, and expired-entry handling without mutating engine state or WAL; it returns the count of live entries that would be restored.

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

The audit chain uses a fixed zero genesis hash for the first event. On startup, the server verifies existing audit lines and fails closed if a line has malformed JSON, invalid sequence, mismatched previous hash, mismatched event hash, or an unsupported hash algorithm. This makes local tampering detectable, but it is not non-repudiation: a local attacker who can rewrite the entire log can recompute a fresh chain unless the latest hash is anchored externally.

Passwords and payload contents are not written to the audit log.

## Scalability Direction

Current state:
- single node only
- no replication
- no sharding

Architectural target:
- replication traffic should reuse transport framing rather than invent a second ad hoc wire path
- request routing should remain decoupled from the engine so a shard-router or replica applier can be introduced later
- storage and protocol identifiers should remain stable enough for cluster metadata and log replication

Do not document distributed support as implemented today. It is a roadmap constraint, not a delivered feature.

## Compression

Transport compression is enabled by default for outbound frames in the current client/server binaries:
- default mode: `zstd`
- default threshold: `0`
- compression is selected during startup negotiation
- readers decompress automatically based on the frame flag
- frame checksums validate the on-wire compressed payload
- `--disable-compression` disables outbound compression on that process

Still missing:
- compression policy coordination between mixed-version peers
- replication-stream tuning

## Abuse Controls and Runtime Guards

Current runtime protections:
- per-session token-bucket rate limiting
- request payload size limits
- key/value size limits
- key-count limits for batch commands
- transaction queue length limits
- idle connection timeouts

## Server Runtime

- Tokio multi-thread runtime
- concurrent client sessions
- engine work is funneled through a dedicated engine worker
- protocol startup negotiation is required before command execution
- optional background snapshotter
- optional background expiration sweeper
- plaintext TCP by default
- TLS accept path when `--ssl` is enabled
- auth and compression enabled by default with explicit disable flags

## Structured INFO

`INFO` returns deterministic key/value entries with section prefixes rather than one unstructured blob. Current sections:
- `server.*`
- `transport.*`
- `storage.*`
- `persistence.*`
- `security.*`
- `runtime.*`
- `metrics.*`

Examples include `server.version`, `transport.protocol_version`, `storage.key_count`, `persistence.wal_size_bytes`, `security.tls_enabled`, `runtime.idle_timeout_seconds`, and `metrics.requests_total`.

Runtime/security examples also include quota and operational settings such as `runtime.max_key_bytes`, `runtime.max_value_bytes`, `runtime.max_keys_per_batch`, `runtime.max_transaction_queue_len`, `runtime.requests_per_second`, `runtime.request_burst`, `runtime.backup_dir`, `security.auth_enabled`, `security.rbac_enabled`, `security.mtls_enabled`, and `transport.compression_mode`.

## Client Runtime

- interactive REPL
- local-only commands:
  - `help`
  - `exit`
- output modes:
  - `plain`
  - `table`
  - `json`

The interactive client should print command results cleanly. Per-command transport logs are intentionally suppressed in normal output.
The local `help` command is formatted as a readable command reference with usage strings and examples rather than a single-line command list.

## Packaging, Docker, and Data Directory

- default container/server data directory: `/var/lib/vaylix`
- intended Docker volume mount:
  - `-v vaylix-data:/var/lib/vaylix`

This path is the durable storage root for:
- snapshots
- WAL
- manifest
- storage keyring
- encrypted auth/RBAC metadata
- logical backup files under the backup directory

## CI and Release

Pull request CI runs:
- `cargo fmt --check`
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `cargo test --workspace`
- `cargo audit`

Release workflow goal:
- publish multi-OS client binaries
- publish multi-OS server binaries
- publish a multi-arch server image to GHCR with both `latest` and the release version tag, for example `0.2.0`

## Current Gaps

- full distributed ACID semantics are not implemented
- no replication or sharding yet
- backup/restore is command-level logical JSON only; there is no separate streaming backup utility yet
- no TLS certificate automation or rotation workflow yet
- TLS is opt-in rather than mandatory

## Guidance for Agents

- keep transport concerns out of the engine
- keep docs honest about current capability vs roadmap
- do not reintroduce a user-facing raw `data_key` CLI argument
- prefer UUID-based request tracking consistently
- add tests for any protocol, persistence, auth, TLS, or workflow change
