# Contributing to Vaylix

Thanks for your interest in contributing.

Vaylix is a long-term systems programming project focused on understanding database internals from first principles: storage engines, WAL recovery, transport protocols, persistence, concurrency, networking, and distributed systems architecture.

The project intentionally prioritizes architectural clarity over feature velocity.

---

## Before Contributing

Please:

- Read the README first
- Check existing issues and pull requests
- Open a discussion before making large architectural changes
- Keep changes focused and incremental

Large refactors without discussion will likely not be merged.

---

## Development Setup

Clone the repository:

```bash
git clone https://github.com/vaylix/vaylix.git
cd vaylix
```

Build the workspace:

```bash
cargo build
```

Run the server:

```bash
cargo run -p server -- --bind 127.0.0.1 --port 9173
```

Run the client:

```bash
cargo run -p client
```

Run tests:

```bash
cargo test
```

Format the codebase:

```bash
cargo fmt
```

Run Clippy:

```bash
cargo clippy --all-targets --all-features
```

---

## Workspace Structure

```text
crates/
├── engine/
├── transport/
├── server/
└── client/
```

### engine

Contains:

- storage engine
- WAL
- snapshots
- execution
- state management

The engine must remain transport-agnostic.

---

### transport

Contains:

- framing
- binary protocol
- request/response codecs
- transport errors
- protocol abstractions

Both the client and server depend on this crate.

---

### server

Contains:

- TCP listener
- client session handling
- request routing
- engine orchestration

---

### client

Contains:

- REPL
- terminal UX
- remote transport client

---

## Architectural Principles

### Keep Layers Separate

The engine should never deal with:

- raw TCP sockets
- framing
- byte parsing
- protocol transport concerns

Networking belongs in transport/server.

---

### Avoid Premature Complexity

Prefer:

- explicit code
- simple abstractions
- understandable control flow
- incremental evolution

Avoid introducing:

- unnecessary macros
- deep trait hierarchies
- framework-heavy abstractions
- speculative distributed systems logic

---

### Preserve Protocol Stability

Transport changes affect:

- client compatibility
- future replication
- protocol versioning
- wire format guarantees

Be careful when modifying transport semantics.

---

### Keep Error Boundaries Clean

Avoid mixing:

- transport errors
- WAL errors
- parser errors
- engine errors

Each layer should own its own failure semantics.

---

## Pull Request Guidelines

Please keep pull requests:

- focused
- well-scoped
- formatted
- documented when necessary

Include:

- rationale for architectural changes
- performance implications if relevant
- protocol compatibility notes if transport changes are involved

---

## Commit Style

Recommended examples:

```text
engine: add WAL checksum validation
transport: implement framed request decoding
server: add client idle timeout handling
client: improve rustyline completion behavior
```

Small, descriptive commits are preferred over large monolithic ones.

---

## Reporting Bugs

When opening an issue, include:

- operating system
- Rust version
- reproduction steps
- logs/output if relevant
- protocol payload examples if transport-related

---

## Roadmap Areas

Areas currently being explored:

- Vaylix Transport Protocol (VTP)
- binary framing
- WAL durability
- snapshot recovery
- concurrent client coordination
- async networking with Tokio
- protocol versioning
- replication streams
- observability and tracing

---

## Philosophy

Vaylix exists primarily as a systems learning project.

The goal is not just to build a database, but to understand:

- why storage engines are layered
- why WAL exists
- how transport protocols evolve
- how databases recover from crashes
- why distributed systems become difficult
- how concurrency changes storage semantics

Architectural clarity matters more than rapid feature growth.
