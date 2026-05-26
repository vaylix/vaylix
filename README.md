# Vaylix

Vaylix is an experimental key-value database written in Rust.

The project began as a systems programming exercise focused on understanding database internals from first principles: storage engines, write-ahead logging, snapshots, protocol design, concurrency, and distributed systems architecture.

Vaylix is currently evolving from a REPL-driven in-memory database into a persistent storage engine with WAL-based recovery and a future TCP client/server architecture.

> Vaylix is intentionally built in layers. The goal is not just to build a database, but to understand why databases are designed the way they are.

---

## Features

### Query Engine

- Interactive REPL built with Rustyline
- Custom lexer and parser
- Quoted string parsing
- Command validation and completion
- Syntax highlighting and inline hints
- Persistent shell history

### Storage Engine

- In-memory key-value engine
- Snapshot-based persistence
- Write-ahead logging (WAL)
- Crash recovery through WAL replay
- Binary serialization using Postcard
- Multi-key delete support

### Architecture

- Modular engine/storage separation
- Snapshot + WAL recovery model
- Structured command pipeline
- Extensible protocol layer for future TCP support
- Filesystem abstraction for OS-specific paths

---

## Example

```text
vaylix> set name "John Doe"
OK

vaylix> get name
John Doe

vaylix> exists name
true

vaylix> count
1

vaylix> snapshot
OK
```

---

## Supported Commands

```text
set <key> <value>
get <key>
delete <key> [key...]
exists <key>
list
count
clear
snapshot
help
exit
```

---

## Storage Architecture

Vaylix currently uses a hybrid persistence model:

- Writes are appended to a write-ahead log (WAL)
- Snapshots periodically checkpoint the full engine state
- On startup, snapshots are restored first and WAL entries are replayed afterward

This provides:

- Durable writes
- Crash recovery
- Fast startup recovery through snapshot checkpointing
- Separation between durability and in-memory state transitions

---

## Roadmap

Planned work includes:

- TCP server/client protocol
- Concurrent client handling
- Async networking with Tokio
- Automatic snapshot checkpointing
- WAL compaction and checksums
- Replication experiments
- Namespace support
- Transaction support
- Binary wire protocol
- Storage engine optimizations
- Distributed systems experiments

---

## Limitations

Vaylix is experimental software and is not production-ready.

Current limitations include:

- Single-process architecture
- No authentication or access control
- No replication or clustering
- No transaction isolation guarantees
- No concurrent write coordination
- Limited corruption recovery
- No benchmarking or performance tuning yet

---

## Building

```bash
cargo build --release
```

---

## Running

```bash
cargo run
```

---

## Philosophy

Vaylix is designed as a long-term systems programming project.

The focus is on understanding:

- How databases recover from crashes
- Why WAL exists
- How storage engines separate durability from state transitions
- How protocols and networking layers evolve
- How concurrency changes storage semantics
- Why distributed systems become difficult at scale

The architecture is expected to evolve aggressively over time.

---

## License

MIT
