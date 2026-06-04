# Vaylix 0.8.0 Release Notes

Vaylix `0.8.0` is a stabilization and protocol-hardening release focused on binary-safe values, deterministic CAS, transport hot-path efficiency, and stronger fail-closed recovery coverage.

## Highlights

- Values are now opaque bytes end-to-end while keys remain UTF-8 strings.
- Stored values now carry a persisted `u64` version.
- `SET <key> <value> IF VERSION <version>` provides version-based compare-and-set.
- WAL, snapshots, logical backups, and replication preserve exact byte payloads and value versions.
- Logical backups now use v2 with base64 values; v1 text backups remain restorable as version `1` byte values.
- VTP decoding avoids avoidable UUID/body copies and now has dedicated parse/encode/pipeline microbenchmarks.
- Large async zstd compression/decompression is offloaded from network tasks.
- Corrupted WAL/snapshot/manifest/compressed-frame paths fail closed in tests.
- Read consistency is explicit: fast-path reads are standalone/leader-only after auth/RBAC/validation gates; follower reads use the existing stale/local engine behavior.

## Upgrade Notes

- All Rust crates are versioned `0.8.0`.
- No wire protocol version bump is introduced.
- No RBAC or TLS negotiation semantics changed.
- New durable data records byte values and versions. Existing UTF-8 string values migrate through compatibility deserialization.
- New logical backups are v2. Existing v1 text logical backups can still be restored.

## Scope Boundaries

- No sharding changes beyond the existing sharded in-memory store.
- No MVCC.
- No distributed transactions.
- No Lua or rich data structures.
- No linearizable follower reads.

