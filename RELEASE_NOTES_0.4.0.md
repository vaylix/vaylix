# Vaylix 0.4.0

`0.4.0` adds manual primary/replica replication, explicit write-ack modes, and operator-facing health/replication diagnostics.

## Highlights

- Leader/follower replication over the existing transport
- Snapshot bootstrap plus WAL catch-up for followers
- Write acknowledgement modes:
  - `local`
  - `replica`
  - `all`
- New operational commands:
  - `health`
  - `show replication`
  - `promote follower`
  - `pause replication`
  - `resume replication`
- Followers reject local writes and transactional write paths
- `INFO` now includes replication role, health, and retention metadata

## Why This Release Exists

`0.3.0` fixed typed `EXEC` results but Vaylix was still operationally single-node.

`0.4.0` establishes the first real replication model:

- a leader can wait for follower acknowledgements before reporting success
- a follower can bootstrap from snapshot state and continue from retained WAL
- operators can inspect and control replication state without external tooling

## What Is New

### Replication

- configurable node role:
  - standalone
  - leader
  - follower
- follower upstream polling and authenticated internal replication requests
- manual follower promotion gated by maintenance mode

### Health and Diagnostics

- `health` returns machine-readable readiness-style output
- `show replication` exposes node role, lag, follower state, and retention floor
- `INFO` now includes replication status and health fields

### Transaction and Durability Behavior

- mutating commands on followers are rejected
- transaction commits and standalone writes can block on configured follower acknowledgement mode
- WAL retention support now includes a floor-aware pruning primitive for follower-safe history retention decisions

## Compatibility

- Protocol v2 remains the transport baseline
- `0.4.0` keeps the structured typed `EXEC` response introduced in `0.3.0`
- `0.2.x` clients remain incompatible with `0.3.0+` transaction result decoding

## Important Limitation

This is not high availability in the strict sense.

`0.4.0` does **not** include:

- automatic failover
- elections
- quorum consensus
- split-brain prevention

It is replication plus manual failover, not a consensus-backed HA cluster.

## Validation

- `cargo fmt`
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `cargo test --workspace --all-targets`
