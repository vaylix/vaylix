# Vaylix

Vaylix is a transport-oriented database engine written in Rust.

The project focuses on understanding database internals from first principles: storage engines, write-ahead logging, binary protocols, persistence, concurrency, networking, and distributed systems architecture.

Vaylix is built as a layered system with a dedicated storage engine, WAL-based recovery, a modular client/server architecture, and an evolving binary transport layer.

> The goal is not just to build a database, but to understand why databases are designed the way they are.

---

## Features

### Query Engine

- Interactive remote client built with Rustyline
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
- Snapshot + WAL recovery model
- Configurable WAL durability modes

### Architecture

- Dedicated engine, transport, server, and client crates
- Layered transport abstraction shared between client and server
- Snapshot + WAL persistence architecture
- Binary framed protocol foundation
- Modular request/response transport design
- Filesystem abstraction for OS-specific paths
- Docker and multi-architecture release support

---

## Example

```text
$ vaylix

        ■ ■ ■

    ████████████
      ████████
        ████
          ██
           █

        Vaylix
 transport-oriented storage

vaylix> set name "John Doe"
OK

vaylix> get name
John Doe

vaylix> count
1
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

- Stable Vaylix Transport Protocol (VTP)
- Concurrent client handling
- Async networking with Tokio
- Automatic snapshot checkpointing
- WAL compaction and checksums
- Replication experiments
- Namespace support
- Transaction support
- Protocol versioning and compression
- Storage engine optimizations
- Distributed systems experiments
- TLS support
- Replication transport streams
- Observability and tracing

---

## Limitations

Vaylix is experimental software and is not production-ready.

Current limitations include:

- No authentication or access control
- No replication or clustering
- No transaction isolation guarantees
- No concurrent write coordination
- Limited corruption recovery
- No benchmarking or performance tuning yet
- Limited multi-client coordination

---

## Building

```bash
cargo build --release
```

Build specific workspace packages:

```bash
cargo build --release -p server
cargo build --release -p client
```

---

## Running

Start the server:

```bash
cargo run -p server -- --bind 127.0.0.1 --port 9173
```

Start the client:

```bash
cargo run -p client
```

---

## Docker

Run the latest container:

```bash
docker run \
  -p 9173:9173 \
  ghcr.io/vaylix/vaylix:latest
```

Configure runtime settings:

```bash
docker run \
  -e VAYLIX_PORT=9173 \
  -e VAYLIX_MAX_CONNECTIONS=512 \
  ghcr.io/vaylix/vaylix:latest
```

---

## Philosophy

Vaylix is designed as a long-term systems programming project.

The focus is on understanding:

- How databases recover from crashes
- Why WAL exists
- How storage engines separate durability from state transitions
- How transport protocols evolve over time
- How concurrency changes storage semantics
- Why distributed systems become difficult at scale
- Why transport abstraction matters in distributed systems

The architecture is expected to evolve aggressively over time.

---

## License

MIT
