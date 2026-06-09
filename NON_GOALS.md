# Non-Goals

This file lists capabilities that Vaylix does not implement today. These are not hidden features or implied guarantees.

## Not Implemented

- Sharding
- MVCC
- Distributed ACID transactions
- Linearizable follower reads
- Online WAL-archive PITR
- A Redis-compatible data-structure surface
- Pub/sub

## Current Alternatives

- Use three voting nodes with quorum acknowledgement for HA write durability.
- Route strict read-after-write traffic to the leader.
- Use versioned compare-and-set for single-key conditional updates.
- Use offline PITR-oriented storage commands for recovery workflows that can operate on a stopped or copied data directory.

## Test Coverage

The command parser has explicit rejection coverage for non-goal command surfaces such as distributed transactions, sharding, MVCC, explicit linearizable reads, read-index commands, and online PITR archive restore. These tests prevent accidental CLI/API drift from being mistaken for delivered semantics.
