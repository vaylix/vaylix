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
- up to eight distinct error samples when operations fail

For `0.7.0` and later, read-heavy profiles should be run against leader or standalone nodes when
measuring the committed read fast path and sharded in-memory store. Followers intentionally keep
stale/local read behavior and are not the baseline for leader read latency.

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
latency under the server's configured write acknowledgement mode. On `0.7.0` and later, this
profile exercises the HA write coordinator path that batches concurrent leader writes into one
local WAL batch and one replicated frontier.

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
  --duration-seconds 30 \
  --wal-sync flush \
  --write-ack-mode local
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
  --duration-seconds 30 \
  --wal-sync flush \
  --write-ack-mode majority
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

Durability matrix runs:

```bash
for mode in buffered flush sync; do
  cargo run -p bench -- managed-single-node \
    --server-bin target/release/vaylix \
    --duration-seconds 10 \
    --connections 16 \
    --seed-keys 256 \
    --keyspace 4096 \
    --value-size 256 \
    --workload set \
    --wal-sync "$mode" \
    --write-ack-mode local
done
```

Quorum acknowledgement matrix:

```bash
for ack in majority all; do
  cargo run -p bench -- managed-quorum-write-cost \
    --server-bin target/release/vaylix \
    --duration-seconds 10 \
    --connections 16 \
    --wal-sync flush \
    --write-ack-mode "$ack"
done
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

Use Docker Hub image `valkey/valkey:8-alpine` for a local reference point when you need a rough external write/read target. This is not an apples-to-apples product benchmark:

- Vaylix numbers use the Vaylix framed protocol, authentication, negotiated compression, and the configured WAL/write-ack mode.
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

Do not commit local comparison numbers unless a release explicitly asks for them. Hardware, filesystem, container runtime, auth state, durability mode, and replication topology materially change the result.
