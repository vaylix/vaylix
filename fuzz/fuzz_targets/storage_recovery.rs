#![no_main]

use std::collections::hash_map::DefaultHasher;
use std::fs;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;

use engine::{
    Engine, EngineOptions, Paths, WalReplayTarget, inspect_wal, load_keyring, replay_until,
};
use libfuzzer_sys::fuzz_target;

const FILE_SLOTS: usize = 6;

fuzz_target!(|data: &[u8]| {
    let root = temp_root(data);
    let paths = match Paths::from_data_dir(&root) {
        Ok(paths) => paths,
        Err(_) => return,
    };

    let chunks = split_input(data, FILE_SLOTS);
    let _ = fs::write(&paths.snapshot_path, chunks[0]);
    let _ = fs::write(&paths.manifest_path, chunks[1]);
    let _ = fs::write(&paths.keyring_path, chunks[2]);
    let _ = fs::create_dir_all(&paths.wal_dir);
    let _ = fs::write(paths.wal_dir.join("active-1.wal"), chunks[3]);
    let _ = fs::write(paths.wal_dir.join("1-2.wal"), chunks[4]);
    let _ = fs::write(&paths.wal_path, chunks[5]);

    let _ = load_keyring(&paths.keyring_path);
    let _ = inspect_wal(&paths.wal_dir);
    let _ = replay_until(&paths.wal_dir, None, WalReplayTarget::Sequence(u64::MAX));
    let _ = replay_until(&paths.wal_dir, None, WalReplayTarget::TimestampMs(u64::MAX));
    let _ = Engine::from_paths_with_options(paths, EngineOptions::default());

    let _ = fs::remove_dir_all(root);
});

fn temp_root(data: &[u8]) -> PathBuf {
    let mut hasher = DefaultHasher::new();
    data.hash(&mut hasher);
    std::env::temp_dir().join(format!(
        "vaylix-fuzz-storage-{}-{}",
        std::process::id(),
        hasher.finish()
    ))
}

fn split_input(data: &[u8], parts: usize) -> Vec<&[u8]> {
    if parts == 0 {
        return Vec::new();
    }

    let mut chunks = Vec::with_capacity(parts);
    let mut cursor = 0usize;
    for index in 0..parts {
        let remaining_parts = parts - index;
        let remaining_len = data.len().saturating_sub(cursor);
        let chunk_len = remaining_len / remaining_parts;
        let end = cursor.saturating_add(chunk_len);
        chunks.push(&data[cursor..end]);
        cursor = end;
    }
    chunks
}
