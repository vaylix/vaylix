# Vaylix Stability Policy

Vaylix treats stability as an implementation contract, not a marketing claim.

## Scope

The stability policy covers:

- the VTP protocol negotiation boundary
- command syntax and deterministic error codes
- WAL, snapshot, manifest, keyring, and logical backup formats
- INFO and METRICS field naming
- Docker runtime defaults and data-directory behavior
- documented HA and consistency semantics

## Pre-1.0 Rules

- Patch releases may fix correctness, security, durability, and operational defects.
- Minor releases may add compatible fields, metrics, or diagnostics.
- Storage or protocol incompatibilities must be explicit in release notes.
- Silent storage upgrades are not allowed.
- Implicit migration is not allowed.

## 1.0 Lockdown Target

Before `1.0.0`, Vaylix must have:

- a frozen storage format version
- explicit protocol compatibility rules
- documented consistency semantics
- reproducible release builds
- deterministic recovery behavior for corruption and interrupted writes
- stability documents updated with every compatibility-impacting change
- mutation, model, concurrency, security, supply-chain, semver, soak, recovery, and coverage signals tracked in CI or staged hardening reports

## Error Codes

Error codes are operator-facing API. A code may be added in a minor release, but an existing code must not be reused for a different class of failure. The canonical code list is [ERROR_CODES.md](ERROR_CODES.md).

## INFO and METRICS

Existing field names should remain stable within a major line. New fields may be added. Removing or changing the meaning of an existing field requires a documented compatibility note.

## Pre-1.0 Hardening Evidence

The code repository keeps machine-runnable hardening gates alongside the default PR gates:

- `crates/engine/tests/model_semantics.rs` is the seeded engine/reference-model oracle.
- `.cargo/mutants.toml` and `hardening/mutation-baseline.md` track mutation testing for durability, consensus/auth-adjacent, and transport surfaces.
- `crates/server/tests/loom_invariants.rs` contains gated loom models for commit waiters, acknowledgement ordering, read-index advancement, and shared-batch responses.
- `crates/server/tests/clock_policy.rs` guards replication and election production paths against wall-clock timing.
- `crates/engine/src/engine/state.rs` has deterministic TTL clock-step coverage proving expired keys do not resurrect and live keys are not expired early when the observed wall clock moves backward.
- `crates/command/src/parser.rs` has explicit rejection coverage for non-goal command surfaces: distributed transactions, sharding, MVCC, explicit linearizable reads, read-index commands, and online PITR archive restore.
- `crates/server/src/audit.rs` has concurrent append coverage that reopens and verifies the resulting audit hash chain.
- `crates/server/tests/network_chaos.rs` provides a gated real-process TCP proxy smoke for latency and forced disconnect behavior.
- `crates/server/tests/tcp_integration.rs` includes a gated HA RPC fault matrix for latency, jitter, bandwidth caps, packet loss, half-open connections, slow-reader/slow-writer behavior, majority-loss partitions, heal, and slow-follower catch-up.
- `crates/engine/tests/soak_endurance.rs`, the gated cluster soak in `crates/server/tests/tcp_integration.rs`, and `crates/engine/tests/recovery_characterization.rs` provide short CI gates plus scheduled long-soak and recovery/RTO characterization coverage.
- `deny.toml`, `crates/server/tests/error_code_catalog.rs`, `cargo-semver-checks` for `command`/`engine`/`transport`, and coverage reporting enforce supply-chain and public-contract visibility.

These are evidence gates, not new user-facing features. They do not change Vaylix 0.10.x consistency semantics.
