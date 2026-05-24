# Veyra

Veyra is a lightweight key-value database written in Rust.

The project began as a systems programming exercise focused on understanding database internals, parser design, storage engines, and distributed systems architecture from first principles.

Veyra is currently an in-memory database with a custom REPL, parser, lexer, command completion, syntax highlighting, and persistent shell history support.

The project is under active development and the architecture is expected to evolve significantly over time.

## Current Features

- In-memory key-value store
- Interactive REPL built with Rustyline
- Custom lexer and parser
- Command completion and validation
- Syntax highlighting and inline hints
- Persistent shell history
- Multi-key delete support
- Quoted string parsing
- Modular architecture for future storage/networking layers

## Example

```text
veyra> set name "John Doe"
OK

veyra> get name
John Doe

veyra> exists name
true

veyra> delete name
OK
```

## Supported Commands

```text
set <key> <value>
get <key>
delete <key> [key...]
exists <key>
list
clear
count
help
exit
```

## Project Structure

```text
src/
├── command.rs      # Command definitions and metadata
├── lexer.rs        # Tokenizer
├── parser.rs       # Command parser
├── store.rs        # In-memory storage engine
├── paths.rs        # OS-specific data/config paths
└── repl/
    ├── helper.rs   # Completion/highlighting/validation
    ├── repl.rs     # REPL runtime
    └── mod.rs
```

## Roadmap

Planned work includes:

- Persistent storage engine
- Write-ahead logging (WAL)
- Snapshotting and compaction
- TCP server/client architecture
- Concurrent client handling
- Replication experiments
- Storage engine optimizations
- Namespace support
- Transaction support

The long-term goal is to gradually evolve Veyra from a simple in-memory database into a distributed systems playground for learning storage and database internals.

## Limitations

Veyra is currently experimental software.

Current limitations include:

- No persistence layer yet
- No networking layer
- No replication
- No concurrency support
- No transaction guarantees
- No crash recovery
- No authentication or access control
- Not production-ready

## Building

```bash
cargo build
```

## Running

```bash
cargo run
```

## Development Status

Veyra is in active development.

Breaking changes, architectural refactors, parser rewrites, and storage format changes are expected while the project evolves.

## License

MIT
