## Summary

This PR is the Vaylix `0.8.0` stabilization and protocol-hardening pass. It makes values binary-safe end-to-end, adds deterministic version-based CAS, tightens transport hot paths, and adds fail-closed recovery/read-consistency tests without expanding the command surface beyond CAS.

## Changes

- Refactored stored values from UTF-8 strings to opaque byte payloads across engine state, WAL, snapshots, logical backup/restore, transport responses, read index, and replication.
- Added `version: u64` to stored values and persisted it through WAL, snapshots, backups, and replication.
- Added `SET <key> <value> IF VERSION <version>` compare-and-set semantics.
- Preserved legacy v1 logical backup restore by migrating text values to byte values with version `1`; new backups use v2 with base64 values.
- Reduced transport decode overhead by parsing UUIDs from fixed 16-byte slices and avoiding a second body payload copy for socket-read frames.
- Offloaded large async zstd compression/decompression from network tasks and added corrupted compressed-frame detection.
- Added transport Criterion coverage for parse latency, encode latency, and pipelined request throughput.
- Added fail-closed tests for corrupted snapshots, manifest checksum mismatch, partial WAL, and WAL checksum mismatch.
- Added explicit read-consistency tests for auth-gated fast-path reads and follower fallback behavior.

## Compatibility

- No command family expansion except `SET ... IF VERSION`.
- No wire protocol version bump.
- No RBAC semantic change.
- No TLS negotiation change.
- New snapshots/WAL/logical backups persist byte values and versions.
- Legacy text logical backups remain restorable through a guarded v1 path.

## Validation

- `cargo fmt --all --check`
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `cargo test --workspace`
- `cargo test --package server --test tcp_integration`
- `cargo bench -p engine --bench engine_bench --no-run`
- `cargo bench -p transport --bench transport_bench --no-run`
- `git diff --check`
