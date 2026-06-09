#![cfg(feature = "soak-tests")]

use engine::{
    Engine, EngineOptions, Expiration, Paths, SetCondition, SetOptions, StorageEngine,
    WalSyncPolicy, inspect_wal,
};
use std::fs;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

static TEST_COUNTER: AtomicU64 = AtomicU64::new(1);

#[test]
fn short_single_node_soak_stays_within_resource_envelope() {
    let seed = test_seed();
    let duration = Duration::from_secs(env_u64("VAYLIX_SOAK_SECONDS").unwrap_or(2));
    eprintln!("VAYLIX_TEST_SEED={seed}");
    eprintln!("VAYLIX_SOAK_SECONDS={}", duration.as_secs());

    let root = temp_dir(seed);
    let paths = Paths::from_data_dir(&root).expect("paths should initialize");
    let mut engine = Engine::from_paths_with_options(
        paths.clone(),
        EngineOptions {
            wal_sync: WalSyncPolicy::Flush,
            wal_segment_size_bytes: 8 * 1024,
            wal_retain_segments: 4,
            ..EngineOptions::default()
        },
    )
    .expect("engine should open");

    let rss_start = resident_set_bytes();
    let fd_start = open_fd_count();
    let started = Instant::now();
    let deadline = started + duration;
    let mut rng = SplitMix64::new(seed);
    let mut operations = 0u64;

    while Instant::now() < deadline || operations < 2_000 {
        let key = format!("key:{:03}", rng.next() % 256);
        let value = format!("value:{}:{}", operations, rng.next()).into_bytes();

        match rng.next() % 12 {
            0 => {
                engine.set(key, value).expect("set should succeed");
            }
            1 => {
                let _ = engine.set_with_options(
                    key,
                    value,
                    SetOptions {
                        condition: Some(SetCondition::Nx),
                        expiration: None,
                        keep_ttl: false,
                        if_version: None,
                    },
                );
            }
            2 => {
                let entries = (0..4)
                    .map(|offset| (format!("batch:{offset}:{}", rng.next() % 64), value.clone()))
                    .collect::<Vec<_>>();
                engine.mset(&entries).expect("mset should succeed");
            }
            3 => {
                let _ = engine.incr(&format!("counter:{}", rng.next() % 16));
            }
            4 => {
                let _ = engine.decr(&format!("counter:{}", rng.next() % 16));
            }
            5 => {
                let _ = engine.expire(&key, 1);
            }
            6 => {
                let _ = engine.get_ex(&key, Some(Expiration::Seconds(1)), false);
            }
            7 => {
                let _ = engine.persist(&key);
            }
            8 => {
                let _ = engine.delete(&key);
            }
            9 => {
                let _ = engine.get(&key).expect("get should not fail");
            }
            10 => {
                let _ = engine.ttl(&key).expect("ttl should not fail");
            }
            _ => {
                let _ = engine.sweep_expired().expect("sweep should not fail");
            }
        }

        if operations.is_multiple_of(257) {
            engine.snapshot().expect("snapshot should succeed");
            let backup = engine.logical_backup().expect("backup should succeed");
            let _ = engine
                .validate_logical_backup(&backup)
                .expect("backup validation should succeed");
        }

        operations += 1;
    }

    engine.snapshot().expect("final snapshot should succeed");
    let backup = engine
        .logical_backup()
        .expect("final backup should succeed");
    let backup_entries = engine
        .validate_logical_backup(&backup)
        .expect("final backup validation should succeed");
    let wal_report = inspect_wal(&paths.wal_dir).expect("wal inspection should succeed");
    let info = engine.info().expect("info should succeed");

    let elapsed = started.elapsed();
    let rss_end = resident_set_bytes();
    let fd_end = open_fd_count();

    eprintln!(
        "soak ops={operations} elapsed_ms={} backup_entries={backup_entries} wal_segments={} wal_bytes={}",
        elapsed.as_millis(),
        wal_report.segment_count,
        wal_report.total_size_bytes
    );
    for key in [
        "key_count",
        "wal_size_bytes",
        "wal_segment_count",
        "last_snapshot_duration_ms",
    ] {
        eprintln!("info.{key}={}", info_value(&info, key).unwrap_or("missing"));
    }

    if let (Some(start), Some(end)) = (rss_start, rss_end) {
        let growth = end.saturating_sub(start);
        eprintln!("rss_growth_bytes={growth}");
        assert!(
            growth <= 128 * 1024 * 1024,
            "RSS growth exceeded short-soak envelope: {growth} bytes"
        );
    }
    if let (Some(start), Some(end)) = (fd_start, fd_end) {
        let growth = end.saturating_sub(start);
        eprintln!("fd_growth={growth}");
        assert!(
            growth <= 8,
            "open file descriptor growth exceeded short-soak envelope: {growth}"
        );
    }

    assert!(
        wal_report.total_size_bytes <= 256 * 1024,
        "WAL grew past short-soak retention envelope: {} bytes",
        wal_report.total_size_bytes
    );
    assert!(
        wal_report.segment_count <= 6,
        "unexpected WAL segment count after snapshot pruning: {}",
        wal_report.segment_count
    );
    assert!(paths.snapshot_path.exists(), "snapshot file should exist");
    assert!(paths.manifest_path.exists(), "manifest file should exist");

    drop(engine);
    fs::remove_dir_all(&root).ok();
}

fn test_seed() -> u64 {
    std::env::var("VAYLIX_TEST_SEED")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or_else(|| 0x9e37_79b9_7f4a_7c15 ^ TEST_COUNTER.fetch_add(1, Ordering::Relaxed))
}

fn env_u64(name: &str) -> Option<u64> {
    std::env::var(name).ok()?.parse::<u64>().ok()
}

fn temp_dir(seed: u64) -> std::path::PathBuf {
    let suffix = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("vaylix-soak-{seed}-{suffix}"))
}

fn info_value<'a>(info: &'a [(String, String)], key: &str) -> Option<&'a str> {
    info.iter()
        .find_map(|(candidate, value)| (candidate == key).then_some(value.as_str()))
}

fn open_fd_count() -> Option<u64> {
    #[cfg(unix)]
    {
        fs::read_dir("/proc/self/fd")
            .or_else(|_| fs::read_dir("/dev/fd"))
            .ok()
            .map(|entries| entries.filter_map(Result::ok).count() as u64)
    }
    #[cfg(not(unix))]
    {
        None
    }
}

fn resident_set_bytes() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        let statm = fs::read_to_string("/proc/self/statm").ok()?;
        let resident_pages = statm.split_whitespace().nth(1)?.parse::<u64>().ok()?;
        Some(resident_pages.saturating_mul(4096))
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut value = self.state;
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        value ^ (value >> 31)
    }
}
