# Changelog

All notable changes to Vaylix will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project follows semantic versioning after `v0.1.0`.

## [Unreleased]

- No unreleased changes yet.

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
