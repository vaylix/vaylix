# Changelog

All notable changes to Vaylix will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project follows semantic versioning after `v0.1.0`.

## [Unreleased]

### Added

- Breaking transport protocol v2 using `VTP2` magic and protocol version `2`.
- Required startup capability negotiation before command frames.
- Negotiated capabilities for zstd compression, request deadlines, server metrics, pipelining, and trace context.
- Optional request metadata for deadline milliseconds, trace id, and sequence number.
- Negotiated frame size limits and decompressed frame size validation.
- Server integration coverage for v2 handshake, old protocol rejection, compression negotiation, deadline rejection, and pipelined request correlation.
- Logical `BACKUP` and `RESTORE` commands with consistent online JSON dumps and WAL-backed atomic restore.
- Structured `INFO` output with server, transport, storage, persistence, security, runtime, and metrics sections.
- Pretty client `HELP` output with command usage instead of a single-line command list.
- Added server-side RBAC without a separate binary: users, roles, permissions, admin commands, and per-command authorization.
- Added encrypted persisted auth/RBAC metadata under the data directory.
- Added client/server/transport support for `create user`, `drop user`, `create role`, `drop role`, `grant role`, `revoke role`, `grant permission`, `revoke permission`, `show users`, `show roles`, and `whoami`.
- Added tests for RBAC persistence, permission enforcement, and TCP-level read-only user behavior.

### Changed

- Updated all workspace crate versions to `0.2.0`.
- Docker release publishing now explicitly targets `linux/amd64,linux/arm64` and keeps stable release tags available as both `latest` and the semver version, for example `0.2.0`.

### Compatibility

- `0.2.0` clients and servers are not wire-compatible with `0.1.0` clients and servers.

## [0.1.0] - 2026-05-27

### Added

- Single-node string key/value database engine.
- Shared framed binary transport protocol with UUID request IDs, checksums, structured statuses, and explicit error codes.
- Tokio multi-client server with a dedicated engine worker.
- Interactive client REPL with `plain`, `table`, and `json` output modes.
- Default-on username/password authentication with local-development defaults.
- Optional TLS and mTLS support.
- Default-on zstd frame compression with explicit opt-out.
- WAL plus encrypted snapshot persistence with server-managed storage keyring.
- Append-only audit logging.
- Runtime guardrails for request size, key/value size, batch size, rate limits, transaction queue length, and idle connections.
- Session transaction commands with single-node atomic `MULTI` / `EXEC` / `DISCARD` semantics.
- Docker persistence path at `/var/lib/vaylix`.

### Known Limitations

- Single-node only; replication and sharding are not implemented.
- No distributed ACID, MVCC, or cluster commit coordination.
- No ACL/RBAC beyond one configured credential pair.
- TLS is supported but disabled by default.
- No online backup/restore tooling yet.
