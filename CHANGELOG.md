# Changelog

All notable changes to Vaylix will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project follows semantic versioning after `v0.1.0`.

## [Unreleased]

## [0.6.0] - 2026-06-02

### Added

- Added stateful WAL writer support that keeps the active segment open, appends batches, rotates safely across batch boundaries, and pays the configured flush/sync boundary once per batch.
- Added server-side write batching for eligible single-command mutations, including bounded draining and group commit for `sync` durability mode.
- Added managed benchmark controls for WAL durability mode and write acknowledgement mode so single-node and quorum matrices can be run from `vaylix-bench`.
- Added bounded benchmark error samples so failed load profiles report representative command-level failure reasons.

### Changed

- Reduced standalone write-path lock scope by skipping HA apply-lock acquisition when the node is explicitly running in standalone mode.
- Made per-request server logging opt-in through `--log-requests` / `VAYLIX_LOG_REQUESTS` instead of logging every request on the hot path by default.
- Raised the default transport compression threshold so small request/response frames are not compressed unnecessarily.
- Reduced storage encryption hot-path cost by deriving stable per-key salts from the storage key UUID and secret while keeping backwards-compatible decryption for existing random-salt envelopes.
- Reduced rollback allocation for normal WAL application by tracking touched keys instead of cloning the full state for every entry.

### Fixed

- Fixed batched command execution so generated WAL entries advance engine metadata and cannot reuse sequence numbers across batches.
- Fixed in-memory WAL cache replacement to reject conflicting duplicate sequence entries instead of allowing ambiguous replication state.
- Fixed benchmark backup/restore and read profiles so valid operational outcomes are reported clearly and failed profiles include bounded diagnostics.

## [0.5.3] - 2026-06-02

### Added

- Added an image-internal Rust `vaylix-init` binary that prepares container data directories, repairs ownership for Linux bind mounts, drops to the configured runtime UID/GID, and execs the server without requiring a shell entrypoint.

### Changed

- Switched the runtime container image from Debian slim plus shell/gosu bootstrap to Debian 13 distroless `gcr.io/distroless/cc-debian13`.
- Kept out-of-the-box Docker bind-mount behavior by running only the init bootstrap as root and the database server process as UID/GID `65532`.
- Changed the server/storage default data directory to `/var/lib/vaylix` in all runtimes instead of an OS-specific user data directory.
- Documented the distroless runtime model and benchmark workflow in the top-level project docs.
- Documented local Valkey comparison results for in-memory and AOF fsync-always benchmark modes.

### Fixed

- Fixed the benchmark load generator so valid `GET` misses count as completed read operations instead of failed benchmark operations.

## [0.5.2] - 2026-06-01

### Changed

- Switched the runtime container image to a Debian 12 slim base with a root bootstrap entrypoint that repairs `/var/lib/vaylix` ownership and then drops to the fixed unprivileged runtime UID/GID.
- Added a Docker `HEALTHCHECK` that uses the `vaylix healthcheck` subcommand against the local framed protocol instead of requiring a second client binary or shell tooling.

### Fixed

- Fixed container readiness probes against auth-enabled servers by allowing `vaylix healthcheck --kind readiness` to authenticate with `VAYLIX_HEALTHCHECK_USER` / `VAYLIX_HEALTHCHECK_PASSWORD`, explicit CLI credentials, or the configured `VAYLIX_USER` / `VAYLIX_PASSWORD`.
- Fixed Docker startup probe races by adding a healthcheck start period before failed probes count against the container.
- Cleaned healthcheck error rendering so `SRV-039` reports the concrete failure once.
- Fixed Linux bind-mounted data directories failing at startup with `ENG-002` / `Permission denied` by repairing ownership before launching the server process.


## [0.5.1] - 2026-06-01

### Changed

- Split server startup configuration, offline admin command handling, maintenance-mode state, and auth-lockout accounting into focused modules with direct unit coverage.
- Split server runtime internals into responsibility modules for engine worker ownership, session state, validation, authorization, command execution, transaction handling, audit events, auth helpers, and replication quorum/timing/persistence helpers.
- Split transport request/response/codec internals into directory modules while preserving all public re-exports and frame/request/response formats.
- Split client internals into TLS setup, response rendering, help text, and CLI/URL configuration modules while preserving `vaylix-client` behavior.
- Kept the `vaylix` binary behavior unchanged while reducing `main.rs` to argument parsing, admin dispatch, launch configuration, and server start orchestration.

### Fixed

- Fixed persisted Docker-volume auth stores so changing `VAYLIX_USER` / `VAYLIX_PASSWORD` retires the previous env-managed bootstrap admin instead of allowing old credentials to continue authenticating.
- Added auth-store metadata migration so existing v0.5.0 auth stores are upgraded without losing custom users or RBAC roles, including legacy single-admin stores created from non-default startup credentials.

## [0.5.0] - 2026-05-31

### Added

- Raft-style cluster runtime with follower/candidate/leader roles, current term, voted-for state, leader hints, election timeouts, heartbeats, pre-vote, and automatic leader failover.
- Cluster-internal transport opcodes for vote requests, heartbeats, append entries, and snapshot installation over the existing framed protocol.
- Quorum-backed write acknowledgement semantics: `replica` / `majority` now commits after a voting majority, `all` waits for every voter, and `local` remains explicitly weaker.
- Three-node HA integration coverage for leader election, failover, old-leader rejoin, follower catch-up, and split-brain write rejection.
- Cluster administration and diagnostics surfaces: `show cluster`, `cluster join`, `cluster remove`, quorum fields in replication status, and role-aware `health`.
- Snapshot-install fallback for followers that are behind retained WAL history.

### Changed

- Default server write acknowledgement mode is now quorum-backed `replica` / `majority` instead of local-only acknowledgement.
- Leader write fanout now uses bounded foreground append attempts and commit-index based completion so successful writes are tied to replicated durable position.
- Replication append fanout now batches WAL lookups from an in-memory WAL cache instead of replaying WAL from disk per follower.
- Election and term handling now preserve monotonic commit index across step-down while clearing stale follower progress.
- Followers and candidates reject mutating commands; followers may still serve explicitly stale local reads.

### Fixed

- Fixed persisted Docker-volume auth stores continuing to accept the default bootstrap password after `VAYLIX_PASSWORD` was changed.
- Fixed HA write-path self-deadlocks caused by nested replication apply-lock acquisition during synchronous write commit.
- Fixed follower polling so legacy configured followers continue discovering leader progress after initial registration.
- Fixed stale replication unit tests that expected non-Raft vote switching and commit-index regression.

## [0.4.0] - 2026-05-30

### Added

- Manual leader/follower replication with WAL-based catch-up, snapshot bootstrap, follower acknowledgements, and explicit `local` / `replica` / `all` write-ack modes.
- Replication administration and diagnostics commands: `health`, `show replication`, `promote follower`, `pause replication`, and `resume replication`.
- Replication-aware `INFO` sections covering role, node identity, upstream/leader state, retention floor, and health.
- Follower-side background replication polling over the existing transport with authenticated internal replication requests.
- Replication integration coverage that verifies leader/follower sync and replica-ack write gating.

### Changed

- Mutating commands and transactions are now rejected on follower nodes.
- Transaction commits and standalone writes now publish local durable sequence progress into replication state and can block on configured follower acknowledgements.
- WAL retention now has a floor-aware pruning primitive so future pruning decisions can preserve follower-required history.

## [0.3.0] - 2026-05-30

### Changed

- `EXEC` now returns a structured typed transport payload instead of a lossy string list.
- The Rust client now renders `EXEC` output from typed transport results rather than reparsing flattened strings.

### Breaking

- `0.3.0` changes the wire format of successful `EXEC` responses.
- `0.2.x` clients that assume `EXEC` returns `Response::strings(...)` are not wire-compatible with `0.3.0` servers for transaction result decoding.

## [0.2.0] - 2026-05-29

### Added

- Segmented WAL storage with manifest format `3`, active/sealed segment naming, and retention controls.
- Offline storage subcommands: `storage migrate`, `storage verify`, `pitr inspect`, and offline `pitr restore`.
- Maintenance mode with `maintenance on`, `maintenance off`, and `maintenance status`.
- Auth password policy enforcement for user creation and password rotation.
- Auth failure window and temporary lockout controls.
- TLS operational metadata, startup expiry validation, and Unix `SIGHUP` reload support.
- Runtime and `INFO` coverage for WAL segments, recovery duration, snapshot duration, lockouts, maintenance mode, and TLS reload state.
- Transaction lifetime enforcement and rejection of sequence-tagged requests during active transactions.
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
- Added key-pattern RBAC grants with `grant permission <permission> on <pattern> to <role>` and matching revoke syntax.
- Added destructive/admin permissions for `clear`, `user_admin`, and `role_admin`.
- Added password rotation through `alter user <username> password <password>`.
- Added sandboxed server-side logical backup and restore files with `backup to <path>`, `restore from <path>`, `restore check <json>`, and `restore check from <path>`.
- Added `--backup-dir` / `VAYLIX_BACKUP_DIR`, defaulting to `<data-dir>/backups`.
- Expanded structured `INFO` with runtime guard, auth, TLS/mTLS, compression, and backup directory settings.
- Added tests for key-pattern authorization, destructive permission denial, password rotation, sandboxed backup paths, restore dry-run validation, and TCP-level RBAC/backup flows.
- Added SHA-256 hash chaining for audit JSON lines so event modification, deletion, and reordering are detected on server startup.
- Added `show grants`, `show grants for user <username>`, and `show grants for role <role>` for deterministic RBAC introspection.
- Added semantic audit event types and sanitized detail maps for authentication and RBAC/auth mutations.
- Added slow-command audit events with `--slow-command-threshold-ms` / `VAYLIX_SLOW_COMMAND_THRESHOLD_MS`.
- Added backup sidecar manifests for `backup to <path>` plus `backup verify <json>` and `backup verify from <path>`.
- Added `metrics prom` for Prometheus text exposition over the database protocol.
- Added storage compatibility tests for unsupported manifest, encrypted envelope, and logical backup versions.
- Added release SBOM generation and keyless Sigstore/cosign signing/attestation workflow steps.

### Changed

- Snapshots now seal the active WAL segment, rotate to a new active segment, and prune retained segments instead of truncating the entire WAL history.
- `METRICS` now uses OpenTelemetry-aligned dotted metric names under `vaylix.*`, and `METRICS PROM` exports Prometheus-safe underscore-translated names derived from that contract.
- Shortened the README into an OSS-style usage entry point with detailed architecture kept in `LLM.md`.

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

- No automatic failover, elections, or quorum commits.
- No MVCC, snapshot reads, or distributed transactions.
- TLS is supported but disabled by default.
