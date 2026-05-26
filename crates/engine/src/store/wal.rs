use crate::{EngineError, Result, WalSyncPolicy};
use crc32fast::hash;
use postcard::{Error, from_bytes, to_allocvec};
use serde::{Deserialize, Serialize};
use std::{
    fs::OpenOptions,
    io::{ErrorKind, Read, Write},
    path::Path,
};

const MAX_WAL_ENTRY_SIZE: u32 = 4 * 1024 * 1024;

/// A single operation captured by the write-ahead log.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum WalOperation {
    /// Insert or replace a key/value pair.
    Set { key: String, value: String },
    /// Delete a key and any associated expiration.
    Delete { key: String },
    /// Attach an absolute expiration timestamp to a key.
    Expire { key: String, expires_at_ms: u64 },
    /// Remove any expiration from a key.
    Persist { key: String },
    /// Clear the entire database.
    Clear,
    /// Validate and update a numeric string value atomically.
    CheckInteger { key: String, delta: i64 },
}

/// A durable atomic batch of storage mutations.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WalEntry {
    /// Monotonic sequence number assigned by the engine.
    pub sequence: u64,
    /// Entry creation time in unix milliseconds.
    pub created_at_ms: u64,
    /// Mutations applied atomically as one logical unit.
    pub operations: Vec<WalOperation>,
}

impl WalEntry {
    /// Builds a new WAL entry.
    pub fn new(sequence: u64, created_at_ms: u64, operations: Vec<WalOperation>) -> Self {
        Self {
            sequence,
            created_at_ms,
            operations,
        }
    }
}

/// Appends a WAL entry durably to disk.
pub fn append(entry: &WalEntry, path: &Path, sync_policy: WalSyncPolicy) -> Result<()> {
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .append(true)
        .open(path)?;

    let bytes = to_allocvec(entry).map_err(EngineError::WalSerialize)?;
    let length = u32::try_from(bytes.len())
        .map_err(|_| EngineError::Io(std::io::Error::other("wal entry too large")))?;

    let checksum = hash(&bytes);
    file.write_all(&length.to_le_bytes())?;
    file.write_all(&checksum.to_le_bytes())?;
    file.write_all(&bytes)?;
    match sync_policy {
        WalSyncPolicy::Buffered => {}
        WalSyncPolicy::Flush => {
            file.flush()?;
        }
        WalSyncPolicy::SyncData => {
            file.sync_data()?;
        }
    }

    Ok(())
}

/// Replays the WAL from disk, tolerating a truncated tail entry.
pub fn replay(path: &Path) -> Result<Vec<WalEntry>> {
    let mut file = match OpenOptions::new().read(true).open(path) {
        Ok(file) => file,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => return Err(err.into()),
    };

    let mut entries = Vec::new();

    loop {
        let mut length_buf = [0u8; 4];

        match file.read_exact(&mut length_buf) {
            Ok(()) => {}
            Err(err) if err.kind() == ErrorKind::UnexpectedEof => break,
            Err(err) => return Err(err.into()),
        }

        let length = u32::from_le_bytes(length_buf);
        if length == 0 || length > MAX_WAL_ENTRY_SIZE {
            break;
        }

        let mut checksum_buf = [0u8; 4];
        match file.read_exact(&mut checksum_buf) {
            Ok(()) => {}
            Err(err) if err.kind() == ErrorKind::UnexpectedEof => break,
            Err(err) => return Err(err.into()),
        }
        let expected_checksum = u32::from_le_bytes(checksum_buf);

        let mut entry_buf = vec![0u8; length as usize];
        match file.read_exact(&mut entry_buf) {
            Ok(()) => {}
            Err(err) if err.kind() == ErrorKind::UnexpectedEof => break,
            Err(err) => return Err(err.into()),
        }

        if hash(&entry_buf) != expected_checksum {
            return Err(EngineError::ChecksumMismatch {
                resource: "wal entry",
            });
        }

        let entry: WalEntry = match from_bytes(&entry_buf) {
            Ok(value) => value,
            Err(Error::DeserializeUnexpectedEnd) => break,
            Err(err) => return Err(EngineError::WalDeserialize(err)),
        };

        entries.push(entry);
    }

    Ok(entries)
}

/// Truncates the WAL after a durable snapshot has been written.
pub fn truncate(path: &Path) -> Result<()> {
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)?;

    file.set_len(0)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use crc32fast::hash;
    use std::fs::{self, OpenOptions};
    use std::io::Write;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use postcard::to_allocvec;

    use super::{WalEntry, WalOperation, append, replay, truncate};
    use crate::WalSyncPolicy;

    fn temp_path(name: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("veyra-{name}-{unique}.wal"))
    }

    fn sample_entry(sequence: u64) -> WalEntry {
        WalEntry::new(
            sequence,
            111,
            vec![WalOperation::Set {
                key: "name".to_string(),
                value: "alice".to_string(),
            }],
        )
    }

    #[test]
    fn appends_and_replays_entries() {
        let path = temp_path("wal-round-trip");

        append(&sample_entry(1), &path, WalSyncPolicy::Flush).unwrap();
        append(
            &WalEntry::new(
                2,
                112,
                vec![
                    WalOperation::Delete {
                        key: "old".to_string(),
                    },
                    WalOperation::Persist {
                        key: "persisted".to_string(),
                    },
                ],
            ),
            &path,
            WalSyncPolicy::Flush,
        )
        .unwrap();

        let entries = replay(&path).unwrap();

        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].sequence, 1);
        assert_eq!(entries[1].sequence, 2);

        fs::remove_file(path).ok();
    }

    #[test]
    fn replay_ignores_truncated_tail() {
        let path = temp_path("wal-truncated");

        append(&sample_entry(1), &path, WalSyncPolicy::Flush).unwrap();

        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(&10_u32.to_le_bytes()).unwrap();
        file.write_all(&123_u32.to_le_bytes()).unwrap();
        file.write_all(b"short").unwrap();
        file.flush().unwrap();

        let entries = replay(&path).unwrap();
        assert_eq!(entries.len(), 1);

        fs::remove_file(path).ok();
    }

    #[test]
    fn replay_stops_on_invalid_length_or_corrupt_entry() {
        let invalid_length_path = temp_path("wal-invalid-length");
        let mut invalid_length_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&invalid_length_path)
            .unwrap();
        invalid_length_file.write_all(&0_u32.to_le_bytes()).unwrap();
        invalid_length_file.flush().unwrap();

        assert!(replay(&invalid_length_path).unwrap().is_empty());
        fs::remove_file(&invalid_length_path).ok();

        let corrupt_path = temp_path("wal-corrupt");
        let mut corrupt_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&corrupt_path)
            .unwrap();
        let valid = to_allocvec(&sample_entry(1)).unwrap();
        corrupt_file
            .write_all(&(valid.len() as u32).to_le_bytes())
            .unwrap();
        corrupt_file.write_all(&hash(&valid).to_le_bytes()).unwrap();
        corrupt_file.write_all(&valid).unwrap();
        let mut corrupt = to_allocvec(&sample_entry(2)).unwrap();
        *corrupt.last_mut().unwrap() = 0xff;
        corrupt_file
            .write_all(&(corrupt.len() as u32).to_le_bytes())
            .unwrap();
        corrupt_file.write_all(&hash(&valid).to_le_bytes()).unwrap();
        corrupt_file.write_all(&corrupt).unwrap();
        corrupt_file.flush().unwrap();

        assert!(replay(&corrupt_path).is_err());
        fs::remove_file(&corrupt_path).ok();
    }

    #[test]
    fn truncates_wal_file() {
        let path = temp_path("wal-truncate");

        append(&sample_entry(1), &path, WalSyncPolicy::Flush).unwrap();
        truncate(&path).unwrap();

        let metadata = fs::metadata(&path).unwrap();
        assert_eq!(metadata.len(), 0);

        fs::remove_file(path).ok();
    }

    #[test]
    fn replay_missing_file_returns_empty_log() {
        let path = temp_path("wal-missing");
        assert!(replay(&path).unwrap().is_empty());
    }
}
