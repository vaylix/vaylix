use crate::{EngineError, Result};
use postcard::{Error, from_bytes, to_allocvec};
use serde::{Deserialize, Serialize};
use std::{
    fs::OpenOptions,
    io::{ErrorKind, Read, Write},
    path::PathBuf,
};

const MAX_WAL_ENTRY_SIZE: u32 = 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WalEntry {
    Set { key: String, value: String },

    Delete { key: String },

    Clear,
}

pub fn append(entry: &WalEntry, path: &PathBuf) -> Result<()> {
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .append(true)
        .open(path)?;

    let bytes = to_allocvec(entry).map_err(EngineError::WalSerialize)?;

    let length = bytes.len() as u32;

    file.write_all(&length.to_le_bytes())?;

    file.write_all(&bytes)?;

    file.flush()?;

    Ok(())
}

pub fn replay(path: &PathBuf) -> Result<Vec<WalEntry>> {
    let mut file = match OpenOptions::new().read(true).open(path) {
        Ok(file) => file,

        Err(err) => {
            if err.kind() == ErrorKind::NotFound {
                return Ok(Vec::new());
            }

            return Err(err.into());
        }
    };

    let mut entries = Vec::new();

    loop {
        let mut length_buf = [0u8; 4];

        match file.read_exact(&mut length_buf) {
            Ok(_) => {}

            Err(err) => {
                if err.kind() == std::io::ErrorKind::UnexpectedEof {
                    break;
                }

                return Err(err.into());
            }
        }

        let length = u32::from_le_bytes(length_buf);

        if length == 0 || length > MAX_WAL_ENTRY_SIZE {
            break;
        }

        let mut entry_buf = vec![0u8; length as usize];

        match file.read_exact(&mut entry_buf) {
            Ok(_) => {}

            Err(err) => {
                if err.kind() == ErrorKind::UnexpectedEof {
                    break;
                }

                return Err(err.into());
            }
        }

        let entry: WalEntry = match from_bytes(&entry_buf) {
            Ok(value) => value,

            Err(Error::DeserializeUnexpectedEnd) => {
                break;
            }

            Err(err) => {
                return Err(EngineError::WalDeserialize(err));
            }
        };

        entries.push(entry);
    }

    Ok(entries)
}

pub fn truncate(path: &PathBuf) -> Result<()> {
    let file = OpenOptions::new().create(true).write(true).open(path)?;

    file.set_len(0)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs::{self, OpenOptions};
    use std::io::Write;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use postcard::to_allocvec;

    use super::{WalEntry, append, replay, truncate};

    fn temp_path(name: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("veyra-{name}-{unique}.wal"))
    }

    #[test]
    fn appends_and_replays_entries() {
        let path = temp_path("wal-round-trip");

        append(
            &WalEntry::Set {
                key: "name".to_string(),
                value: "alice".to_string(),
            },
            &path,
        )
        .unwrap();
        append(
            &WalEntry::Delete {
                key: "old".to_string(),
            },
            &path,
        )
        .unwrap();
        append(&WalEntry::Clear, &path).unwrap();

        let entries = replay(&path).unwrap();

        assert_eq!(entries.len(), 3);
        assert!(matches!(
            &entries[0],
            WalEntry::Set { key, value } if key == "name" && value == "alice"
        ));
        assert!(matches!(&entries[1], WalEntry::Delete { key } if key == "old"));
        assert!(matches!(&entries[2], WalEntry::Clear));

        fs::remove_file(path).ok();
    }

    #[test]
    fn replay_ignores_truncated_tail() {
        let path = temp_path("wal-truncated");

        append(
            &WalEntry::Set {
                key: "name".to_string(),
                value: "alice".to_string(),
            },
            &path,
        )
        .unwrap();

        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(&10_u32.to_le_bytes()).unwrap();
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
        let valid = to_allocvec(&WalEntry::Clear).unwrap();
        corrupt_file
            .write_all(&(valid.len() as u32).to_le_bytes())
            .unwrap();
        corrupt_file.write_all(&valid).unwrap();
        corrupt_file.write_all(&1_u32.to_le_bytes()).unwrap();
        corrupt_file.write_all(&[99]).unwrap();
        corrupt_file.flush().unwrap();

        assert!(replay(&corrupt_path).is_err());
        fs::remove_file(&corrupt_path).ok();
    }

    #[test]
    fn truncates_wal_file() {
        let path = temp_path("wal-truncate");

        append(
            &WalEntry::Set {
                key: "name".to_string(),
                value: "alice".to_string(),
            },
            &path,
        )
        .unwrap();
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
