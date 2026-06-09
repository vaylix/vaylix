#![cfg(feature = "capacity-tests")]

use engine::{Engine, EngineOptions, Paths, StorageEngine, WalSyncPolicy, inspect_wal};
use std::fs;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time::{Duration, Instant};

static TEST_COUNTER: AtomicU64 = AtomicU64::new(1);

#[test]
fn cold_start_recovery_stays_within_short_baseline() {
    let seed = test_seed();
    eprintln!("VAYLIX_TEST_SEED={seed}");

    for entries in recovery_entry_counts() {
        measure_cold_start_recovery(seed, entries);
    }
}

#[test]
fn backup_and_snapshot_tail_latency_stays_within_short_baseline() {
    let seed = test_seed() ^ 0x71a1_1a7e_5eed;
    let entries = env_u64("VAYLIX_BACKUP_STALL_ENTRIES").unwrap_or(2_000);
    let operations = env_u64("VAYLIX_BACKUP_STALL_OPS").unwrap_or(256);
    eprintln!("VAYLIX_TEST_SEED={seed}");
    eprintln!("VAYLIX_BACKUP_STALL_ENTRIES={entries}");
    eprintln!("VAYLIX_BACKUP_STALL_OPS={operations}");

    let root = temp_dir(seed);
    let paths = Paths::from_data_dir(&root).expect("paths should initialize");
    let mut engine = Engine::from_paths_with_options(
        paths.clone(),
        EngineOptions {
            wal_sync: WalSyncPolicy::Flush,
            wal_segment_size_bytes: 32 * 1024,
            wal_retain_segments: 4,
            ..EngineOptions::default()
        },
    )
    .expect("engine should open");
    for index in 0..entries {
        engine
            .set(
                format!("stall:key:{index:05}"),
                format!("value:{seed}:{index}").into_bytes(),
            )
            .expect("write should succeed");
    }

    let engine = Arc::new(Mutex::new(engine));
    let barrier = Arc::new(Barrier::new(2));
    let maintenance_engine = Arc::clone(&engine);
    let maintenance_barrier = Arc::clone(&barrier);
    let maintenance = thread::spawn(move || {
        maintenance_barrier.wait();
        let mut durations = Vec::new();
        for _ in 0..3 {
            let started = Instant::now();
            let mut engine = maintenance_engine
                .lock()
                .expect("engine mutex should not be poisoned");
            engine.snapshot().expect("snapshot should succeed");
            let backup = engine.logical_backup().expect("backup should succeed");
            engine
                .validate_logical_backup(&backup)
                .expect("backup validation should succeed");
            durations.push(started.elapsed().as_millis());
            drop(engine);
            thread::sleep(Duration::from_millis(2));
        }
        durations
    });

    let mut op_latencies_us = Vec::new();
    barrier.wait();
    for index in 0..operations {
        let started = Instant::now();
        {
            let mut engine = engine.lock().expect("engine mutex should not be poisoned");
            if index % 3 == 0 {
                engine
                    .set(
                        format!("stall:live:{index:05}"),
                        format!("value:{seed}:{index}").into_bytes(),
                    )
                    .expect("live write should succeed");
            } else {
                let _ = engine
                    .get(&format!("stall:key:{:05}", index % entries))
                    .expect("live read should succeed");
            }
        }
        op_latencies_us.push(started.elapsed().as_micros());
        if index % 32 == 0 {
            thread::sleep(Duration::from_millis(1));
        }
    }

    let maintenance_durations = maintenance.join().expect("maintenance thread should join");
    op_latencies_us.sort_unstable();
    let p50 = percentile(&op_latencies_us, 50);
    let p99 = percentile(&op_latencies_us, 99);
    let max = op_latencies_us.last().copied().unwrap_or(0);
    eprintln!(
        "backup_snapshot_contention entries={entries} ops={operations} op_p50_us={p50} op_p99_us={p99} op_max_us={max} maintenance_ms={:?}",
        maintenance_durations
    );
    assert!(
        p99 <= 2_000_000,
        "backup/snapshot contention p99 exceeded short baseline: {p99}us"
    );

    drop(engine);
    fs::remove_dir_all(&root).ok();
}

fn measure_cold_start_recovery(seed: u64, entries: u64) {
    let root = temp_dir(seed);
    let paths = Paths::from_data_dir(&root).expect("paths should initialize");

    {
        let mut engine = Engine::from_paths_with_options(
            paths.clone(),
            EngineOptions {
                wal_sync: WalSyncPolicy::Flush,
                wal_segment_size_bytes: 64 * 1024,
                wal_retain_segments: 8,
                ..EngineOptions::default()
            },
        )
        .expect("engine should open");
        for index in 0..entries {
            engine
                .set(
                    format!("recovery:key:{index:05}"),
                    format!("value:{seed}:{index}").into_bytes(),
                )
                .expect("write should succeed");
        }
    }

    let wal_report = inspect_wal(&paths.wal_dir).expect("wal inspection should succeed");
    let recovery_started = Instant::now();
    let mut recovered = Engine::from_paths_with_options(paths.clone(), EngineOptions::default())
        .expect("engine should recover from WAL");
    let recovery_ms = recovery_started.elapsed().as_millis();
    assert_eq!(
        recovered.db_size().expect("dbsize should succeed") as u64,
        entries
    );
    eprintln!(
        "wal_recovery entries={entries} wal_bytes={} wal_segments={} recovery_ms={recovery_ms}",
        wal_report.total_size_bytes, wal_report.segment_count
    );
    assert!(
        recovery_ms <= 5_000,
        "short WAL recovery baseline exceeded: {recovery_ms}ms"
    );

    recovered.snapshot().expect("snapshot should succeed");
    drop(recovered);

    let snapshot_bytes = fs::metadata(&paths.snapshot_path)
        .expect("snapshot metadata should exist")
        .len();
    let snapshot_started = Instant::now();
    let mut snapshot_recovered =
        Engine::from_paths_with_options(paths.clone(), EngineOptions::default())
            .expect("engine should recover from snapshot");
    let snapshot_recovery_ms = snapshot_started.elapsed().as_millis();
    assert_eq!(
        snapshot_recovered
            .db_size()
            .expect("snapshot dbsize should succeed") as u64,
        entries
    );
    eprintln!(
        "snapshot_recovery entries={entries} snapshot_bytes={snapshot_bytes} recovery_ms={snapshot_recovery_ms}"
    );
    assert!(
        snapshot_recovery_ms <= 5_000,
        "short snapshot recovery baseline exceeded: {snapshot_recovery_ms}ms"
    );

    drop(snapshot_recovered);
    fs::remove_dir_all(&root).ok();
}

fn recovery_entry_counts() -> Vec<u64> {
    if let Some(value) = std::env::var("VAYLIX_RECOVERY_MATRIX").ok() {
        let counts = value
            .split(',')
            .filter_map(|part| part.trim().parse::<u64>().ok())
            .filter(|count| *count > 0)
            .collect::<Vec<_>>();
        if !counts.is_empty() {
            return counts;
        }
    }
    if let Some(entries) = env_u64("VAYLIX_RECOVERY_ENTRIES") {
        return vec![entries];
    }
    vec![512, 2_000, 5_000]
}

fn test_seed() -> u64 {
    std::env::var("VAYLIX_TEST_SEED")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or_else(|| 0xd1b5_4a32_d192_ed03 ^ TEST_COUNTER.fetch_add(1, Ordering::Relaxed))
}

fn env_u64(name: &str) -> Option<u64> {
    std::env::var(name).ok()?.parse::<u64>().ok()
}

fn temp_dir(seed: u64) -> std::path::PathBuf {
    let suffix = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("vaylix-recovery-{seed}-{suffix}"))
}

fn percentile(values: &[u128], percentile: usize) -> u128 {
    if values.is_empty() {
        return 0;
    }
    let index = ((values.len() - 1) * percentile).div_ceil(100);
    values[index]
}
