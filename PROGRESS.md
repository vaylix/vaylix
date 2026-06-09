# Vaylix Pre-1.0 Hardening Progress

This is the append-only working audit trail for the unattended hardening pass.

## 2026-06-08T10:26:26Z - Initial Repo Audit

- Workstream: setup / scope validation
- Facts:
  - Workspace crate versions are already `0.9.0`; this pass keeps them there.
  - `LLM.md` and `STABILITY.md` exist in the code repo.
  - Current CI has fmt, clippy, workspace tests, bounded fuzz smokes, audit, and release binary smoke builds.
  - `cargo add proptest --dev -p engine` added `proptest v1.11.0` for model-based engine semantic tests.
- Format/contract impact: none.
- Failure/root cause/fix: no test failure yet.
- Re-run result: pending.

## 2026-06-08T10:31:44Z - XW3 Engine Model Oracle

- Workstream: XW3 model-based engine semantic testing
- Added `crates/engine/tests/model_semantics.rs`.
- Coverage:
  - seeded proptest sequence generation
  - `VAYLIX_TEST_SEED` override and seed printing
  - `VAYLIX_MODEL_CASES` case-count override
  - reproducible failure text with step index and generated sequence
  - set/setnx/CAS-style `IF VERSION`, get/getdel/getex, mget/mset, del/exists, incr/decr, expire/ttl/persist, rename/renamenx, scan/dbsize/count/list/clear
  - command-batch and transaction rollback on expected integer errors
- Format/contract impact: none.
- Failure/root cause/fix:
  - `cargo fmt --check` initially failed on the new test file.
  - Root cause: formatting not yet applied.
  - Fix: ran `cargo fmt`.
- Re-run result:
  - `cargo test -p engine --test model_semantics -- --nocapture` passed with `VAYLIX_TEST_SEED=11400714819323198485`.

## 2026-06-08T10:43:27Z - XW1 Focused Mutation Smoke

- Workstream: XW1 mutation testing
- Installed and ran `cargo-mutants v27.1.0`.
- Initial focused command-evaluation run:
  - 31 mutants tested
  - 25 caught
  - 4 unviable
  - 2 missed
- Missed mutants:
  - `crates/engine/src/engine/core.rs:902:16: delete ! in Engine::execute_command_batch`
  - `crates/engine/src/engine/core.rs:921:12: delete ! in Engine::execute_command_batch`
- Root cause:
  - The model oracle compared live command results and keyspace state, but not WAL sequence advancement or recovery after reopening.
  - A mutant could mutate live memory without appending durable WAL entries and still satisfy the old oracle.
- Fix:
  - Added `command_batch_sequences_and_wal_recovery_match_mutations_only`.
  - The test asserts read-only commands keep `last_applied_sequence` unchanged, the mutating command advances it, and the written key survives a reopen.
- Re-run result:
  - `cargo test -p engine --test model_semantics -- --nocapture` passed.
  - Focused mutation run passed: 31 mutants tested, 27 caught, 4 unviable, 0 missed.

## 2026-06-08T10:50:00Z - XW1/XW9 Tooling Config

- Workstream: XW1 mutation testing and XW9 API/error-code stability
- Added `.cargo/mutants.toml` for scheduled critical-surface mutation runs.
- Added `hardening/mutation-baseline.md` with the focused mutation baseline and critical-surface inventory.
- Added `crates/server/tests/error_code_catalog.rs` to fail when source error codes are missing from `ERROR_CODES.md` or duplicated there.
- Updated `ERROR_CODES.md` with local CLI error codes.
- Added scheduled `.github/workflows/hardening.yml` jobs for sharded mutation testing, coverage summary, cargo-deny, and `transport` semver checks.
- Format/contract impact: documents existing CLI-local codes; no runtime behavior change.
- Failure/root cause/fix: pending validation.
- Re-run result: pending.

## 2026-06-08T11:05:00Z - XW9 Supply Chain and SemVer Validation

- Workstream: XW9 supply-chain and API-stability automation
- Added `deny.toml`.
- Added `license = "MIT"` to workspace crate manifests.
- Added explicit `version = "0.9.0"` to internal path dependencies so license/bans analysis treats workspace crates as first-class packages.
- Failure/root cause/fix:
  - Initial `cargo-deny` failed on missing crate license metadata and wildcard/unversioned internal path dependencies.
  - Root cause: manifests were acceptable to Cargo but underspecified for supply-chain policy.
  - Fix: added license metadata and explicit internal dependency versions without changing crate versions.
- Re-run result:
  - `cargo deny check advisories bans licenses sources` passed. Duplicate dependency versions remain warnings, not denials.
  - `cargo semver-checks -p transport --baseline-rev origin/main` passed: 196 checks passed, 57 skipped, no semver update required.

## 2026-06-08T11:15:00Z - XW10 Coverage Wiring

- Workstream: XW10 coverage visibility
- Added `coverage-critical` to `.github/workflows/hardening.yml`.
- Failure/root cause/fix:
  - Local `cargo-llvm-cov` could not run because the machine uses Homebrew Rust (`rustc 1.96.0`) without `rustup`, `llvm-tools-preview`, `llvm-cov`, or `llvm-profdata`.
  - Root cause: local toolchain lacks the coverage instrumentation tools; this is an environment blocker, not a code failure.
  - Fix: the CI workflow installs `llvm-tools-preview` and `cargo-llvm-cov` before generating `target/llvm-cov/summary.json`.
- Re-run result:
  - Local coverage remains not run on this machine.
  - CI coverage job is wired and runnable on GitHub-hosted Rust toolchains.

## 2026-06-08T11:35:00Z - XW2/XW4/XW6/XW7/XW8 Harnesses

- Workstreams: XW2 concurrency model checking, XW4 short soak, XW6 recovery characterization, XW7 clock policy, XW8 auth parser fuzzing
- Added `crates/server/tests/loom_invariants.rs` gated by `--features loom-tests`.
- Added `crates/server/tests/clock_policy.rs`.
- Added `crates/engine/tests/soak_endurance.rs` gated by `--features soak-tests`.
- Added `crates/engine/tests/recovery_characterization.rs` gated by `--features capacity-tests`.
- Added `fuzz/fuzz_targets/auth_handshake.rs` and wired it into fuzz smoke CI.
- Added hardening workflow jobs for loom, clock policy, short soak, recovery characterization, Miri smoke, and ASan/TSan smoke.
- Failure/root cause/fix:
  - First loom run was terminated after `shared_batch_responses_do_not_precede_frontier_acknowledgement` exceeded 60 seconds.
  - Root cause: two independent spin-waiting clients generated an unnecessarily large schedule space.
  - Fix: rewrote that model with a condition variable while preserving the invariant: no response is visible before durable and quorum frontiers cover that response sequence.
- Re-run result:
  - `cargo test --locked -p server --test clock_policy` passed.
  - `cargo test --locked -p server --test loom_invariants --features loom-tests -- --nocapture` passed: 4 tests, 35.26s.
  - `cargo test --locked -p engine --test soak_endurance --features soak-tests -- --nocapture` passed: seed `11400714819323198484`, 9,619 ops, 5 WAL segments, 29,589 WAL bytes, zero FD growth.
  - `cargo test --locked -p engine --test recovery_characterization --features capacity-tests -- --nocapture` passed: seed `15111065706836454658`, 2,000-entry WAL recovery 21 ms, snapshot recovery 34 ms.
  - `cargo check --manifest-path fuzz/Cargo.toml` passed.

## 2026-06-08T11:45:00Z - Current Residual Gaps

- Workstream: scope honesty
- Real-process toxiproxy chaos and multi-hour 3-node soak are not fully committed in this pass.
- TTL expiration still uses persisted wall-clock timestamps; at this point the clock-policy test guards replication/election monotonic timing but does not prove TTL behavior under wall-clock jumps.
- Local Miri/sanitizer jobs are wired in CI but not run locally because this machine has no `rustup` nightly toolchain.
- Format/contract impact: no runtime guarantee changed.

## 2026-06-08T12:15:00Z - Default Gates and Mutation Re-run

- Workstreams: validation loop
- Failure/root cause/fix:
  - `cargo clippy --locked --workspace --all-targets --all-features -- -D warnings` failed on `crates/engine/tests/soak_endurance.rs` for manual `% 257 == 0` multiple checking.
  - Root cause: new test used a pattern Clippy rejects on the current Rust toolchain.
  - Fix: replaced it with `operations.is_multiple_of(257)`.
- Re-run result:
  - `cargo fmt --check` passed.
  - `cargo clippy --locked --workspace --all-targets --all-features -- -D warnings` passed.
  - `cargo test --locked --workspace` passed.
  - `cargo audit --file Cargo.lock` initially could not run because `cargo-audit` was not installed locally; installed `cargo-audit v0.22.2`.
  - `cargo audit --file Cargo.lock` passed.
  - `cargo deny check advisories bans licenses sources` passed with duplicate-version warnings.
  - `cargo semver-checks -p transport --baseline-rev origin/main` passed: 196 checks passed, 57 skipped.
- Mutation failure/root cause/fix:
  - Focused mutation command without `--no-config` timed out one mutant: 31 tested, 26 caught, 4 unviable, 1 timeout.
  - Root cause: the committed scheduled mutation config enabled `all_features`, causing feature-gated loom/soak/capacity tests to be built into each mutation temp copy.
  - Fix: focused local smoke now uses `--no-config`, and `.cargo/mutants.toml` sets `all_features = false`; feature-gated heavy tests remain separate hardening jobs.
- Mutation re-run result:
  - `cargo mutants --no-config -p engine --file 'crates/engine/src/engine/core.rs' --re 'next_value_version|parse_integer_value|execute_transaction|execute_command_batch|evaluate_transaction_command' --baseline run --minimum-test-timeout 30 --build-timeout 180 --timeout 900 -- --test model_semantics` passed: 31 mutants tested, 27 caught, 4 unviable, 0 missed.
- Final gated rerun result:
  - `cargo test --locked -p engine --test soak_endurance --features soak-tests -- --nocapture` passed after the Clippy fix: seed `11400714819323198484`, 9,347 ops, 5 WAL segments, 27,197 WAL bytes, zero FD growth.
  - `cargo test --locked -p engine --test recovery_characterization --features capacity-tests -- --nocapture` passed after the Clippy fix: seed `15111065706836454658`, 2,000-entry WAL recovery 23 ms, snapshot recovery 33 ms.

## 2026-06-08T12:40:00Z - XW7 TTL Clock-Step Coverage

- Workstream: XW7 clock and timekeeping edge cases
- Added `ttl_handles_wall_clock_steps_without_resurrection_or_early_expiry` in `crates/engine/src/engine/state.rs`.
- Coverage:
  - A backward wall-clock step before the expiration deadline keeps the key live and does not expire it early.
  - A backward wall-clock step after the key was expired and purged does not resurrect it.
- Scope note:
  - This covers deterministic state-level TTL semantics using explicit `now_ms`.
  - Full process-level wall-clock injection remains open.
- Re-run result:
  - `cargo fmt --check` passed.
  - `cargo test --locked -p engine ttl_handles_wall_clock_steps_without_resurrection_or_early_expiry -- --nocapture` passed.

## 2026-06-08T13:05:00Z - XW5 Real-Process Network Chaos Smoke

- Workstream: XW5 real-network chaos
- Added `crates/server/tests/network_chaos.rs`, gated by `--features chaos-tests`.
- Added `chaos-tests` feature to the server crate.
- Added `network-chaos-smoke` to `.github/workflows/hardening.yml`.
- Coverage:
  - Starts the real `vaylix` server binary as a child process with an isolated data directory.
  - Routes client traffic through an actual local TCP proxy.
  - Injects per-direction proxy latency for a successful write/read round trip.
  - Forces a proxy disconnect during handshake and asserts it surfaces as a bounded connection failure.
  - Reconnects through the latency proxy and verifies the already-acknowledged value remains readable.
- Failure/root cause/fix:
  - Initial test treated the intentional disconnect as a panic because the shared connection helper expected every handshake to succeed.
  - Root cause: expected-failure path used an infallible helper.
  - Fix: split the helper into fallible and infallible variants.
- Scope note:
  - This is real process/proxy coverage, but not the full toxiproxy-style HA fault matrix.
- Re-run result:
  - `cargo fmt` passed.
  - `cargo test --locked -p server --test network_chaos --features chaos-tests -- --nocapture` passed with `VAYLIX_TEST_SEED=11936045730314344057`.

## 2026-06-08T13:30:00Z - XW4 Short 3-Node Cluster Soak

- Workstream: XW4 soak/endurance and resource-leak detection
- Added `cluster-soak-tests` feature to the server crate.
- Added `short_three_node_cluster_soak_bounds_wal_and_replication_lag` to `crates/server/tests/tcp_integration.rs`.
- Added `cluster-soak-short` to `.github/workflows/hardening.yml`.
- Coverage:
  - Starts a real 3-node in-process TCP cluster using the existing HA integration setup.
  - Waits for an elected writable leader.
  - Runs 30 quorum `SET` operations and periodic `INCR` operations through the leader.
  - Waits for the final write to become visible on all three nodes.
  - Checks each node's WAL size and segment count against a short-soak envelope.
- Scope note:
  - This is a short CI soak and does not replace a multi-hour endurance run.
- Re-run result:
  - `cargo fmt` passed.
  - `cargo test --locked -p server --test tcp_integration short_three_node_cluster_soak_bounds_wal_and_replication_lag --features cluster-soak-tests -- --nocapture` passed.

## 2026-06-08T17:30:17Z - XW8/XW9 Non-Goal and Contract Hardening

- Workstreams: XW8 security hardening, XW9 API-stability automation, non-goal proof tests
- Added `audit::tests::concurrent_records_preserve_a_verifiable_chain`.
- Coverage:
  - Multiple threads append audit records through one `AuditLogger`.
  - The resulting file is checked for contiguous sequence numbers and hash-chain linkage.
  - The logger reopens the resulting file successfully, proving the concurrent path leaves a verifiable chain.
- Added `parser::tests::rejects_non_goal_command_surfaces`.
- Coverage:
  - Distributed transaction, sharding, MVCC, explicit linearizable-read, read-index, and online PITR command surfaces remain rejected by the text parser.
- Widened scheduled Rust semver checks from `transport` only to `command`, `engine`, and `transport`.
- Repo-bound limitation:
  - A TypeScript SDK API snapshot/diff check cannot be implemented in this repository because there is no `package.json`, `tsconfig.json`, or TypeScript SDK package present.
- Failure/root cause/fix:
  - First parser non-goal test failed because `set isolation serializable` is valid as ordinary key/value data: key `isolation`, value `serializable`.
  - Root cause: the test confused data accepted by the generic `SET <key> <value>` command with a dedicated isolation-level command surface.
  - Fix: removed that ambiguous example and kept only command forms that must be rejected.
- Re-run result:
  - `cargo test --locked -p server concurrent_records_preserve_a_verifiable_chain -- --nocapture` passed.
  - `cargo test --locked -p command rejects_non_goal_command_surfaces -- --nocapture` passed.
  - `cargo semver-checks -p command --baseline-rev origin/main` passed: 196 checks passed, 57 skipped.
  - `cargo semver-checks -p engine --baseline-rev origin/main` passed: 196 checks passed, 57 skipped.
  - `cargo semver-checks -p client --baseline-rev origin/main` could not run because `client` has no library target.

## 2026-06-08T17:34:04Z - Post-Hardening Gate Re-run

- Workstreams: validation loop
- Re-run result:
  - `cargo fmt --check` passed.
  - `cargo clippy --locked --workspace --all-targets --all-features -- -D warnings` passed.
  - `cargo test --locked --workspace` passed.
  - `cargo audit --file Cargo.lock` passed.
  - `cargo deny check advisories bans licenses sources` passed with duplicate-version warnings.
  - `cargo semver-checks -p command --baseline-rev origin/main` passed: 196 checks passed, 57 skipped.
  - `cargo semver-checks -p engine --baseline-rev origin/main` passed: 196 checks passed, 57 skipped.
  - `cargo semver-checks -p transport --baseline-rev origin/main` passed: 196 checks passed, 57 skipped.
  - `cargo check --manifest-path fuzz/Cargo.toml` passed.
  - `cargo test --locked -p server --test loom_invariants --features loom-tests -- --nocapture` passed.
  - `cargo test --locked -p server --test network_chaos --features chaos-tests -- --nocapture` passed with `VAYLIX_TEST_SEED=11936045730314344057`.
  - `cargo test --locked -p server --test tcp_integration short_three_node_cluster_soak_bounds_wal_and_replication_lag --features cluster-soak-tests -- --nocapture` passed.
  - `cargo test --locked -p engine --test soak_endurance --features soak-tests -- --nocapture` passed with seed `11400714819323198484`, 9,460 ops, 5 WAL segments, 27,466 WAL bytes, zero FD growth.
  - `cargo test --locked -p engine --test recovery_characterization --features capacity-tests -- --nocapture` passed with seed `15111065706836454658`, 2,000-entry WAL recovery 23 ms, snapshot recovery 31 ms.
- Local toolchain limitations still present:
  - Miri and sanitizer smoke jobs are wired in CI but not run locally because this machine does not have `rustup`/nightly (`cargo +nightly` is unavailable).
  - Coverage is wired in CI but not run locally because this machine does not have `llvm-tools-preview`, `llvm-cov`, or `llvm-profdata`.

## 2026-06-08T17:35:53Z - Focused Mutation Re-run

- Workstream: XW1 mutation testing
- Re-run result:
  - `cargo mutants --no-config -p engine --file 'crates/engine/src/engine/core.rs' --re 'next_value_version|parse_integer_value|execute_transaction|execute_command_batch|evaluate_transaction_command' --baseline run --minimum-test-timeout 30 --build-timeout 180 --timeout 900 -- --test model_semantics` passed.
  - 31 mutants tested in 78 seconds: 27 caught, 4 unviable, 0 missed.

## 2026-06-08T17:37:22Z - Compatibility Doc Link Repair

- Workstream: documentation consistency
- Added `COMPATIBILITY_1_0.md`, `DEPLOYMENT.md`, and `NON_GOALS.md`.
- Root cause:
  - `README.md` and `CHANGELOG.md` referenced these contract documents, but the files were not present in this checkout.
- Fix:
  - Added concise docs matching the current 0.9.x behavior without adding new guarantees.
- Re-run result:
  - Markdown link target existence check for `STABILITY.md`, `COMPATIBILITY_1_0.md`, `ERROR_CODES.md`, `NON_GOALS.md`, and `DEPLOYMENT.md` passed.
  - `git diff --check` passed.
  - `cargo fmt --check` passed.

## 2026-06-08T18:03:09Z - XW4/XW6 Gap Closure Start

- Workstreams: XW4 soak/endurance, XW5 HA network chaos, XW6 RTO characterization
- Added a full HA RPC fault matrix to `crates/server/tests/tcp_integration.rs` behind `chaos-tests`.
- Added server-side capacity RTO tests for leader failover distribution and late-follower snapshot install/catch-up behind `capacity-tests`.
- Extended `crates/engine/tests/recovery_characterization.rs` to run a WAL/snapshot recovery matrix and backup/snapshot contention latency measurement.
- Parameterized the 3-node cluster soak with `VAYLIX_CLUSTER_SOAK_SECONDS` and `VAYLIX_CLUSTER_SOAK_OPS` while preserving the short CI default.
- Root cause/fix:
  - The HA matrix exposed that half-open proxy relays could keep a replication pool connection stuck after the test healed the proxy.
  - Fixed the proxy relay to observe fault reset and close the half-open relay so the client pool reconnects.
- Root cause/fix:
  - Late followers at sequence 0 could receive retained WAL entries starting after sequence 1 instead of a snapshot when the leader had already snapshotted and pruned older entries.
  - Fixed leader fanout to send a replication snapshot whenever retained entries are no longer contiguous from the follower's expected sequence.
- Targeted re-run result before long soak:
  - `cargo test --locked -p server --test tcp_integration ha_rpc_fault_matrix_preserves_quorum_and_bounded_errors --features chaos-tests -- --nocapture` passed.
  - `cargo test --locked -p server --test tcp_integration leader_failover_rto_distribution_stays_within_short_baseline --features capacity-tests -- --nocapture` passed with election samples `[2305, 2316, 2321]` ms.
  - `cargo test --locked -p server --test tcp_integration late_follower_snapshot_install_and_catchup_rto_stays_within_short_baseline --features capacity-tests -- --nocapture` passed with 160 preload entries, 345 ms catch-up, and a 44,875-byte persisted follower snapshot.
  - `cargo test --locked -p engine --test recovery_characterization --features capacity-tests -- --nocapture` passed with WAL recovery matrix entries 512/2,000/5,000 at 5/20/59 ms and snapshot recovery at 7/27/79 ms. Backup/snapshot contention p99 was 46,218 us.
  - `cargo test --locked -p server --test tcp_integration short_three_node_cluster_soak_bounds_wal_and_replication_lag --features cluster-soak-tests -- --nocapture` passed with 30 ops in 1,050 ms.
- Long soak start:
  - Launching 2-hour single-node engine soak with `VAYLIX_SOAK_SECONDS=7200`.
  - Launching 2-hour 3-node cluster soak with `VAYLIX_CLUSTER_SOAK_SECONDS=7200`.
  - Logs: `hardening/long-engine-soak-20260608.log` and `hardening/long-cluster-soak-20260608.log`.

## 2026-06-09T07:09:23Z - Long Cluster Soak Verifier Fix

- Workstream: XW4 soak/endurance
- Re-run result:
  - `VAYLIX_SOAK_SECONDS=7200 cargo test --locked -p engine --test soak_endurance --features soak-tests -- --nocapture` passed after 7,200.15 seconds: seed `11400714819323198484`, 11,075,673 ops, 442 backup entries, 5 WAL segments, 26,332 WAL bytes, zero FD growth, 69 ms last snapshot duration.
  - `VAYLIX_CLUSTER_SOAK_SECONDS=7200 cargo test --locked -p server --test tcp_integration short_three_node_cluster_soak_bounds_wal_and_replication_lag --features cluster-soak-tests -- --nocapture` failed after 7,212.67 seconds at the final all-node visibility check.
- Root cause:
  - The cluster soak harness reused request IDs after a long enough run (`SET` at index 1000 collided with earlier `GET` IDs, and later operations collided with the final read IDs).
  - The final convergence check also selected the last churn key, which could be part of the TTL workload instead of a stable post-workload sentinel.
- Fix:
  - Added a dedicated long-run request-ID allocator for the cluster soak.
  - Added a non-expiring sentinel write after the workload and made the final all-node convergence check read that key.
  - Added final per-node WAL envelope logging for the long-run capacity notes.
- Re-run result:
  - The 10-second duration cluster soak passed with 386 ops and all three nodes at 6 WAL segments / 93,128 WAL bytes.
  - The 2-hour cluster soak failed again after 7,233.35 seconds: the sentinel write reached the leader and one follower, but the second follower was still `NotFound` after the 30-second convergence window.
- Root cause:
  - Foreground leader fanout intentionally aborted remaining follower RPCs as soon as quorum commit was reached in majority/`replica` mode.
  - The background catch-up path reused that same quorum-short-circuit behavior. Under sustained writes, the fast follower repeatedly satisfied quorum and the slower follower could be starved indefinitely.
- Fix:
  - Split append fanout completion semantics: foreground write-ack fanout may still stop after quorum, while background catch-up fanout attempts all voters.
  - Added a real-process chaos regression that keeps one follower slow across repeated quorum writes and then requires all nodes to converge after the fault clears.
- Re-run result:
  - `cargo test --locked -p server --test tcp_integration ha_rpc_fault_matrix_preserves_quorum_and_bounded_errors --features chaos-tests -- --nocapture` passed.
  - Short cluster soak passed.
  - 10-second timed cluster soak passed with 401 ops and all three nodes at 6 WAL segments / 96,670 WAL bytes.
  - The 2-hour cluster soak rerun failed after 7,233.48 seconds: the sentinel write reached the leader and one follower, but the other follower was still `NotFound`.
- Root cause:
  - The background consensus loop checked `heartbeat_due()` twice.
  - The liveness heartbeat records leader activity, making the second `heartbeat_due()` check false and preventing the append/catch-up heartbeat from running on most ticks.
- Fix:
  - Snapshot the heartbeat-due decision once per loop tick and use it for both liveness heartbeat and append/catch-up heartbeat scheduling.
- Re-run result:
  - The 2-hour cluster soak rerun failed after 7,234.83 seconds: the sentinel write reached the leader and one follower, but the other follower was still `NotFound`.
- Root cause:
  - Aborted foreground fanout tasks could leave already-enqueued append requests in the per-peer replication worker.
  - A slower follower could spend the convergence window draining stale append/heartbeat work queued before the latest catch-up frontier, so the final sentinel append did not reach it promptly even after the workload stopped.
- Fix:
  - Added per-peer replication request coalescing in `ReplicationClientPool`: latest append work supersedes stale append/heartbeat requests, and append requests take precedence over heartbeats.
  - Added final cluster-soak failure diagnostics that include each node's `SHOW REPLICATION` view.
- Re-run result:
  - `VAYLIX_CLUSTER_SOAK_SECONDS=10 VAYLIX_CLUSTER_SOAK_OPS=30 cargo test --locked -p server --test tcp_integration short_three_node_cluster_soak_bounds_wal_and_replication_lag --features cluster-soak-tests -- --nocapture` passed with 318 ops and all three nodes at 5 WAL segments / 76,536 WAL bytes.
  - `cargo test --locked -p server --test tcp_integration ha_rpc_fault_matrix_preserves_quorum_and_bounded_errors --features chaos-tests -- --nocapture` passed with seed `7640891576956012809`.
  - A 2-minute pressure cluster soak first passed convergence but exposed that the short WAL envelope was being reused for duration mode without node-local periodic snapshots.
- Root cause:
  - The cluster soak harness used a fixed short-run WAL cap for endurance mode.
  - Client-issued `SAVE` commands did not reliably exercise follower-local retention before inspection; the server's periodic snapshotter was disabled in the test runtimes.
- Fix:
  - Enabled the server periodic snapshotter on all three cluster-soak nodes when `VAYLIX_CLUSTER_SOAK_SECONDS` is set.
  - Kept the short CI envelope strict, and added a bounded endurance envelope of 32 WAL segments / 1 MiB.
- Re-run result:
  - `VAYLIX_CLUSTER_SOAK_SECONDS=120 VAYLIX_CLUSTER_SOAK_OPS=60 cargo test --locked -p server --test tcp_integration short_three_node_cluster_soak_bounds_wal_and_replication_lag --features cluster-soak-tests -- --nocapture` passed with 2,979 ops; nodes reported 9 WAL segments and about 96 KB each.
  - The next 2-hour cluster soak rerun is pending.

## 2026-06-09T16:15:57Z - Duration-Mode Cluster Soak Bound Fix

- Workstream: XW4 soak/endurance
- Re-run result:
  - `VAYLIX_CLUSTER_SOAK_SECONDS=7200 VAYLIX_CLUSTER_SOAK_OPS=30 cargo test --locked -p server --test tcp_integration short_three_node_cluster_soak_bounds_wal_and_replication_lag --features cluster-soak-tests -- --nocapture` was stopped after the process exceeded the requested duration by more than 30 minutes.
- Root cause:
  - The cluster soak loop treated `VAYLIX_CLUSTER_SOAK_OPS` as a minimum even when `VAYLIX_CLUSTER_SOAK_SECONDS` was set.
  - Under the long HA stress profile, individual quorum writes could slow enough that the harness kept running past the configured duration instead of producing bounded endurance evidence.
- Fix:
  - Changed duration-mode cluster soak semantics to stop when the requested duration elapses.
  - Kept fixed-op behavior for the short default CI run where `VAYLIX_CLUSTER_SOAK_SECONDS` is not set.
- Re-run result:
  - `VAYLIX_CLUSTER_SOAK_SECONDS=10 VAYLIX_CLUSTER_SOAK_OPS=30 cargo test --locked -p server --test tcp_integration short_three_node_cluster_soak_bounds_wal_and_replication_lag --features cluster-soak-tests -- --nocapture` passed with 336 ops in 10,071 ms; all nodes reported 6 WAL segments / 81,008 WAL bytes.
  - `VAYLIX_CLUSTER_SOAK_SECONDS=7200 VAYLIX_CLUSTER_SOAK_OPS=30 cargo test --locked -p server --test tcp_integration short_three_node_cluster_soak_bounds_wal_and_replication_lag --features cluster-soak-tests -- --nocapture` was interrupted before completion; the log had no final `cluster_soak` metrics or `test result`, so it is not counted as evidence.
  - Pending: clean detached 2-hour cluster soak rerun.
