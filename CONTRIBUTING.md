# Contributing to Vaylix

Vaylix is a systems project. Contributions are expected to be explicit, test-backed, and honest about operational tradeoffs.

## Before Opening a PR

- read [README.md](README.md)
- read [LLM.md](LLM.md)
- search for existing issues or open discussions
- open a design discussion first for protocol, persistence-format, auth, TLS, or architectural changes

## Core Rules

- keep the engine independent from sockets, framing, and transport byte parsing
- keep wire compatibility changes intentional
- keep error boundaries explicit and code-bearing
- keep persistence format changes versioned and tested
- update `LLM.md` whenever architecture or operational behavior changes
- update `README.md` whenever user-facing setup or runtime behavior changes

## Local Development

Build:

```bash
cargo build --workspace
```

Format:

```bash
cargo fmt --check
```

Lint:

```bash
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

Test:

```bash
cargo test --workspace
```

## What Reviewers Expect

- small, coherent changes
- matching tests for new behavior
- documentation updates when behavior changes
- compatibility notes for protocol or persistence changes
- direct handling of failure cases, not just happy-path code

## High-Risk Change Areas

These require extra care:
- transport protocol changes
- TLS and authentication changes
- request/response compatibility changes
- WAL, snapshot, manifest, or storage-encryption changes
- transaction semantics
- workflow and release automation changes

## Testing Expectations

At minimum, relevant changes should include:
- unit tests for the changed logic
- integration tests when networking, auth, or TLS behavior changes
- corruption or recovery tests when persistence changes

If test coverage is intentionally incomplete, state that clearly in the PR description.

## Documentation Discipline

Do not let docs drift.

If you change:
- commands
- CLI flags
- connection string semantics
- protocol behavior
- persistence behavior
- security behavior
- release or CI workflows

then update the relevant top-level docs in the same PR.

## Security Expectations

- never commit secrets, private keys, tokens, or environment files
- use non-sensitive sample credentials only
- treat auth, TLS, and persistence-encryption code as high-review areas

## Current Project Reality

Contributors should not overstate current capability. The codebase is still:
- single-node
- string-value only
- without replication or sharding
- without distributed ACID guarantees

Work that improves those areas is welcome, but it should be described as implementation work toward the roadmap, not as already-delivered capability.
