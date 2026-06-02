# Benchmarking

Vaylix includes three complementary benchmarking paths:

- `criterion` microbenchmarks for `engine` and `transport`
- `vaylix-bench`, an async end-to-end load generator for the framed protocol
- `cargo flamegraph` guidance for CPU attribution

## Requirements

- Linux for `perf` / `cargo flamegraph`
- a running Vaylix server for end-to-end load tests
- a running three-node quorum cluster when benchmarking replicated writes

Install flamegraph once:

```bash
cargo install flamegraph
```

## Criterion

Engine microbenchmarks:

```bash
cargo bench -p engine --bench engine_bench
```

Transport microbenchmarks:

```bash
cargo bench -p transport --bench transport_bench
```

Convenience aliases:

```bash
cargo bench-engine
cargo bench-transport
```

The current Criterion suite covers:

- engine ideal-path benchmarks for all storage-facing commands that execute directly against the engine:
  `get`, `set`, `setnx`, conditional `set`, `getdel`, `getex`, `mget`, `mset`, `delete`,
  `delete_many`, `exists`, `incr`, `decr`, `expire`, `ttl`, `persist`, `rename`,
  `renamenx`, `scan`, `dbsize`, `count`, `list`, `info`, `clear`, `snapshot`,
  `logical_backup`, `validate_logical_backup`, and `restore_logical_backup`
- engine batch-shape benchmarks for `mset` plus `scan` at increasing entry counts
- transport request encode/decode round-trips for every wire opcode using representative
  payloads for data, admin, backup, HA, and replication traffic
- transport response encode/decode round-trips for representative payload shapes with and
  without zstd compression

Not every command belongs in the engine layer. `AUTH`, RBAC/admin, maintenance, cluster, and
replication control paths are benchmarked at the transport/load-generator layers because the
engine does not execute them.

## Async Load Generator

The workspace includes `crates/bench`, published as the local binary `vaylix-bench`.

Custom run:

```bash
cargo run -p bench -- run \
  --addr 127.0.0.1:9173 \
  --connections 32 \
  --duration-seconds 30 \
  --seed-keys 2048 \
  --keyspace 10000 \
  --value-size 256 \
  --workload mixed
```

Authenticated run:

```bash
VAYLIX_BENCH_USERNAME=vaylix \
VAYLIX_BENCH_PASSWORD=vaylix \
cargo run -p bench -- run --addr 127.0.0.1:9173
```

Convenience alias:

```bash
cargo bench-load -- run --addr 127.0.0.1:9173
```

TLS run:

```bash
cargo run -p bench -- run \
  --addr 127.0.0.1:9173 \
  --tls \
  --tls-ca-cert ./bench-certs/ca.crt \
  --username vaylix \
  --password vaylix
```

mTLS run:

```bash
cargo run -p bench -- run \
  --addr 127.0.0.1:9173 \
  --tls \
  --tls-ca-cert ./bench-certs/ca.crt \
  --tls-client-cert ./bench-certs/client.crt \
  --tls-client-key ./bench-certs/client.key \
  --username vaylix \
  --password vaylix
```

The load generator prints a JSON report with:

- completed and failed operations
- operations per second
- p50 / p95 / p99 latency in microseconds
- benchmark parameters used for the run

## Command Profiles

The load generator also includes command-specific end-to-end profiles. These measure whole
user-visible flows rather than single request latency.

Transaction flow:

```bash
cargo run -p bench -- transaction-flow \
  --addr 127.0.0.1:9173 \
  --connections 16 \
  --duration-seconds 30
```

Each measured operation executes `MULTI`, four queued `SET` commands, and `EXEC`.

Backup/restore path:

```bash
cargo run -p bench -- backup-restore \
  --addr 127.0.0.1:9173 \
  --connections 1 \
  --seed-keys 128 \
  --duration-seconds 30
```

Each measured operation writes one stable key, runs `BACKUP`, validates the dump with
`RESTORE CHECK`, and then runs `RESTORE`.

Auth/RBAC admin churn:

```bash
cargo run -p bench -- auth-rbac-churn \
  --addr 127.0.0.1:9173 \
  --connections 4 \
  --duration-seconds 30
```

Each measured operation creates a user and role, grants permission and role membership,
reads grants, revokes both grants, then drops the role and user.

Quorum replication write cost:

```bash
cargo run -p bench -- quorum-write-cost \
  --addr 127.0.0.1:9173 \
  --connections 32 \
  --duration-seconds 30
```

Run this against the current leader of a quorum cluster. It measures acknowledged `SET`
latency under the server's configured write acknowledgement mode.

Managed variants are available for local smoke and repeatable baselines:

```bash
cargo run -p bench -- managed-transaction-flow --server-bin target/release/vaylix
cargo run -p bench -- managed-backup-restore --server-bin target/release/vaylix
cargo run -p bench -- managed-auth-rbac-churn --server-bin target/release/vaylix
cargo run -p bench -- managed-quorum-write-cost --server-bin target/release/vaylix
```

## Baseline Suite

Single-node baseline:

```bash
VAYLIX_BENCH_USERNAME=vaylix \
VAYLIX_BENCH_PASSWORD=vaylix \
cargo run -p bench -- baseline-single-node --addr 127.0.0.1:9173 --duration-seconds 30
```

Current preset:

- `64` connections
- `8192` seeded keys
- `25000` keyspace
- `512` byte values
- mixed read/write workload

Quorum baseline:

```bash
VAYLIX_BENCH_USERNAME=vaylix \
VAYLIX_BENCH_PASSWORD=vaylix \
cargo run -p bench -- baseline-quorum --addr 127.0.0.1:9173 --duration-seconds 30
```

Run this against the elected leader of a three-node cluster using quorum-backed writes.

Current preset:

- `32` connections
- `4096` seeded keys
- `10000` keyspace
- `256` byte values
- write-heavy `SET` workload intended to expose quorum ack cost

## Managed Launcher

Generate example benchmark certificates:

```bash
cargo run -p bench -- example-certs --out-dir ./bench-certs
```

Managed single-node baseline:

```bash
cargo run -p bench -- managed-single-node \
  --server-bin target/debug/vaylix \
  --duration-seconds 30
```

Managed single-node with TLS:

```bash
cargo run -p bench -- managed-single-node \
  --server-bin target/debug/vaylix \
  --duration-seconds 30 \
  --tls
```

Managed single-node with mTLS:

```bash
cargo run -p bench -- managed-single-node \
  --server-bin target/debug/vaylix \
  --duration-seconds 30 \
  --tls \
  --mtls
```

Managed quorum baseline:

```bash
cargo run -p bench -- managed-quorum \
  --server-bin target/debug/vaylix \
  --duration-seconds 30
```

Managed quorum launcher behavior:

- allocates free localhost ports
- creates isolated temp data directories
- spawns one leader and two follower nodes
- waits briefly for startup/election
- benchmarks the elected-leader entrypoint
- tears child processes down after the run

For quick smoke runs, override the heavy baseline defaults:

```bash
cargo run -p bench -- managed-single-node \
  --server-bin target/debug/vaylix \
  --duration-seconds 2 \
  --connections 4 \
  --seed-keys 32 \
  --keyspace 128 \
  --value-size 64 \
  --tls
```

## Flamegraph

Profile a running benchmark target instead of guessing where CPU time goes.

Engine bench:

```bash
cargo flamegraph -p engine --bench engine_bench -- --bench
```

Transport bench:

```bash
cargo flamegraph -p transport --bench transport_bench -- --bench
```

Profile the server binary itself under a realistic deployment:

```bash
cargo flamegraph -p server --bin vaylix -- \
  --bind 127.0.0.1 \
  --port 9173 \
  --data-dir .tmp-vaylix-flamegraph \
  --user vaylix \
  --password vaylix
```

Then drive load from another shell with `vaylix-bench`.

## Practical Notes

- Do not compare single-node and quorum numbers directly without recording `--write-ack-mode`, replication topology, auth state, and compression state.
- Read-heavy workloads treat `GET` misses as completed database operations. A miss is a valid key/value read result, not a benchmark transport or server failure.
- Failed operations during load usually indicate rate limiting, auth errors, or a benchmark target that is already saturated. Treat that as a signal, not noise.
- Criterion isolates micro-level regressions; `vaylix-bench` captures contention, protocol, and replication effects; flamegraphs explain where the CPU went.
- The managed launcher is for reproducible local baselines, not production orchestration. Use external orchestration when benchmarking real deployment topology, filesystems, or network characteristics.

## Local Valkey Comparison

The `0.5.3` benchmark pass used Docker Hub image `valkey/valkey:8-alpine` for a local reference point. This is not an apples-to-apples product benchmark:

- Vaylix numbers use the Vaylix framed protocol, authentication, default compression negotiation, serialized engine worker, and WAL `flush`.
- Valkey numbers use `valkey-benchmark` inside the Valkey container over RESP.
- Valkey in-memory mode has no durable write guarantee. The AOF run with `appendfsync always` is closer to a durability-stressed write path, but still has different storage, protocol, and command semantics.

Vaylix local single-node command run:

```bash
target/release/vaylix \
  --bind 127.0.0.1 \
  --port 19175 \
  --data-dir .tmp-vaylix-compare \
  --user vaylix \
  --password vaylix \
  --requests-per-second 100000 \
  --request-burst 100000

target/release/vaylix-bench run \
  --addr 127.0.0.1:19175 \
  --username vaylix \
  --password vaylix \
  --duration-seconds 5 \
  --connections 4 \
  --keyspace 256 \
  --value-size 64 \
  --workload set
```

Valkey in-memory run:

```bash
docker run -d --name valkey-compare valkey/valkey:8-alpine --save "" --appendonly no
docker exec valkey-compare valkey-benchmark -h 127.0.0.1 -p 6379 -t set,get -n 10000 -c 4 -d 64 -r 256 --csv
```

Valkey AOF fsync-always run:

```bash
docker run -d --name valkey-compare valkey/valkey:8-alpine --save "" --appendonly yes --appendfsync always
docker exec valkey-compare valkey-benchmark -h 127.0.0.1 -p 6379 -t set,get -n 10000 -c 4 -d 64 -r 256 --csv
```

Observed local results:

| Target | Mode | Operation | Throughput | p50 | p95 | p99 |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| Vaylix | WAL flush | SET | 70.2 ops/s | 56.735 ms | 60.991 ms | 65.247 ms |
| Vaylix | WAL flush | GET | 5,645.8 ops/s | 0.693 ms | 0.795 ms | 0.894 ms |
| Vaylix | WAL flush | mixed | 232.2 ops/s | 14.375 ms | 56.799 ms | 58.431 ms |
| Valkey | in-memory | SET | 89,285.71 ops/s | 0.039 ms | 0.055 ms | 0.063 ms |
| Valkey | in-memory | GET | 84,033.61 ops/s | 0.039 ms | 0.055 ms | 0.055 ms |
| Valkey | AOF fsync always | SET | 8,285.00 ops/s | 0.471 ms | 0.567 ms | 0.727 ms |
| Valkey | AOF fsync always | GET | 156,250.00 ops/s | 0.023 ms | 0.031 ms | 0.047 ms |

The main signal is not that these systems are equivalent; they are not. The useful signal is that Vaylix write throughput is currently dominated by its serialized durable write path and should be treated as the first major performance target before marketing benchmark claims.
