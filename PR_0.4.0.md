# PR: Vaylix 0.4.0 Replication and Health Foundation

## Summary

This release adds the first serious primary/replica operational model to Vaylix.

`0.4.0` introduces:

- manual leader/follower replication over the existing transport
- explicit write-ack modes tied to durable sequence progress
- follower snapshot bootstrap plus WAL catch-up
- follower write rejection and stricter transaction behavior under replication
- replication health and diagnostics through `INFO`, `health`, and `show replication`

This is not automatic high availability. There are still no elections, quorum commits, or split-brain prevention. The scope is deliberate: make replication real before claiming HA.

## What Changed

### Replication

- Added replication role configuration:
  - `standalone`
  - `leader`
  - `follower`
- Added replication identity and topology flags/env:
  - node ID
  - replication group ID
  - leader advertise address
  - follower upstream target
  - upstream auth settings
- Added WAL-based follower replication using:
  - full snapshot bootstrap when needed
  - incremental WAL fetch and apply afterward
- Added write-ack modes:
  - `local`
  - `replica`
  - `all`
- Added manual follower promotion with maintenance-mode gating
- Added replication pause/resume controls

### Transaction and Write Semantics

- Followers now reject mutating commands
- Followers reject transactional execution paths that would write locally
- Leader commits publish local durable sequence progress into replication state
- Write acknowledgement modes can block leader success responses until follower acknowledgement requirements are satisfied

### Recovery and WAL

- Added engine replication snapshot export/import primitives
- Added WAL entry export since sequence for follower catch-up
- Added floor-aware sealed WAL pruning primitive for follower-safe retention decisions

### Observability and Health

- Added `health`
- Added `show replication`
- Expanded `INFO` with:
  - replication role
  - node/group identity
  - write-ack mode
  - leader/upstream metadata
  - retention floor
  - health status
- Added replication operator audit events for:
  - promotion
  - pause
  - resume

## Public Surface Changes

### New Commands

- `health`
- `show replication`
- `promote follower`
- `pause replication`
- `resume replication`

### New Runtime Configuration

- `VAYLIX_REPLICATION_ROLE`
- `VAYLIX_NODE_ID`
- `VAYLIX_REPLICATION_GROUP_ID`
- `VAYLIX_REPLICATION_ADVERTISE_ADDR`
- `VAYLIX_REPLICATION_UPSTREAM`
- `VAYLIX_REPLICATION_USER`
- `VAYLIX_REPLICATION_PASSWORD`
- `VAYLIX_WRITE_ACK_MODE`
- `VAYLIX_REPLICATION_ACK_TIMEOUT_MS`
- `VAYLIX_REPLICATION_POLL_INTERVAL_MS`
- `VAYLIX_REPLICATION_FETCH_BATCH_SIZE`
- `VAYLIX_REPLICATION_STALE_AFTER_SECONDS`

### New Error Cases

- replication ack timeout
- replication ack unavailable
- follower write rejection
- promotion denied

## Validation

- `cargo fmt`
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `cargo test --workspace --all-targets`

Integration coverage now includes leader/follower replication with replica-ack write gating.

## Scope Boundary

`0.4.0` is a replication/manual-failover release, not a true HA release.

Still not implemented:

- automatic failover
- leader election
- quorum commit coordination
- fencing / split-brain prevention
- replication-aware backup rejoin workflows
