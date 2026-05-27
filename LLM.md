# Vaylix Project Context

This file is the authoritative non-sensitive project context for humans and AI agents working in this repository. Any change to protocol behavior, CLI semantics, persistence format, authentication, TLS, workflows, or operational defaults must update this file in the same change.

## Project Summary

Vaylix is a Rust database workspace centered on a transport-first architecture:

`client -> transport -> TCP/TLS -> transport -> server -> engine`

The current implementation is a single-node, string-to-string key/value database with:
- a custom framed binary protocol
- a shared transport crate used by both client and server
- a Tokio multi-client server
- authenticated client connections
- optional TLS client/server transport
- encrypted-at-rest WAL and snapshots
- append-only audit logging
- default-on outbound frame-level zstd compression
- deterministic command parsing and explicit error codes

The long-term target is broader:
- scale from a single node to replicated and sharded deployments
- keep the transport layer evolvable enough for replication traffic and cluster coordination
- harden transactional behavior toward stronger ACID guarantees than the current session-queued model
- add auditability and compression negotiation without breaking engine layering

## Workspace Layout

- `crates/command`
  - lexer, parser, command metadata, parser errors
- `crates/transport`
  - frame layout, opcodes, request/response types, codec, sync/async framed I/O
- `crates/engine`
  - in-memory state, expirations, WAL, snapshots, manifest, recovery, storage encryption, key rotation
- `crates/server`
  - Tokio listener, authentication, TLS accept, session handling, quotas, rate limiting, engine worker runtime
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
- Frame header includes:
  - magic
  - version
  - flags
  - payload length
  - frame checksum
- Requests contain:
  - `request_id: UUID`
  - opcode
  - payload
- Responses contain:
  - `request_id: UUID`
  - status
  - payload
- Remote errors are structured:
  - stable error code
  - friendly error name
  - message

### Request IDs

Request IDs are UUIDs, not random integers. This removes the old local counter/random collision concerns and keeps the protocol ready for future pipelining and multiplexing.

## TLS

TLS is supported but disabled by default.

Client behavior:
- the client uses plaintext TCP by default
- `--ssl` opens a TLS connection
- connection URL query `ssl=true` also enables TLS
- system root store by default
- optional custom CA via `--tls-ca-cert`

Server inputs:
- `--ssl`
- `--tls-cert`
- `--tls-key`

When `--ssl` is enabled on the server, both `--tls-cert` and `--tls-key` are required. Plain TCP remains useful for local development and private test networks, but production deployments should enable TLS.

## Authentication

Authentication is enabled by default.

Development defaults:
- username: `vaylix`
- password: `vaylix`

These defaults exist for local development only. Production deployments should always override them.
Use `--disable-auth` only for local/trusted testing. When auth is disabled on the server, commands execute without an `AUTH` handshake.

Client connection string format:
- `vaylix://user:password@host:port`

Supported query parameters:
- `ssl=true`
- `output=plain|table|json`
- `ca_cert=/path/to/ca.pem`
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
- timestamp
- connection id
- peer address
- authenticated username if present
- request id
- opcode name
- response status
- error code when applicable
- latency in milliseconds

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
- readers decompress automatically based on the frame flag
- frame checksums validate the on-wire compressed payload
- `--disable-compression` disables outbound compression on that process

Still missing:
- compression negotiation
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
- optional background snapshotter
- optional background expiration sweeper
- plaintext TCP by default
- TLS accept path when `--ssl` is enabled
- auth and compression enabled by default with explicit disable flags

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

## Packaging, Docker, and Data Directory

- default container/server data directory: `/var/lib/vaylix`
- intended Docker volume mount:
  - `-v vaylix-data:/var/lib/vaylix`

This path is the durable storage root for:
- snapshots
- WAL
- manifest
- storage keyring

## CI and Release

Pull request CI runs:
- `cargo fmt --check`
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `cargo test --workspace`
- `cargo audit`

Release workflow goal:
- publish multi-OS client binaries
- publish multi-OS server binaries
- publish a multi-arch server image to GHCR

## Current Gaps

- full distributed ACID semantics are not implemented
- no replication or sharding yet
- no ACL or multi-user authorization model
- no online backup/restore tool in the current tree
- no TLS certificate automation or rotation workflow yet
- TLS is opt-in rather than mandatory

## Guidance for Agents

- keep transport concerns out of the engine
- keep docs honest about current capability vs roadmap
- do not reintroduce a user-facing raw `data_key` CLI argument
- prefer UUID-based request tracking consistently
- add tests for any protocol, persistence, auth, TLS, or workflow change
