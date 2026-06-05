use crate::Result;
use crate::config::StorageKeyring;
use crate::store::crypto::{decrypt, encrypt};
use std::{fs, io::ErrorKind, path::Path};

use super::durable;

/// Saves snapshot bytes atomically using a temporary file and rename.
pub fn save(
    data: &[u8],
    path: &Path,
    temp_path: &Path,
    keyring: Option<&StorageKeyring>,
) -> Result<()> {
    let durable_bytes = match keyring {
        Some(keyring) => encrypt(keyring.active(), "snapshot", data)?,
        None => data.to_vec(),
    };
    durable::atomic_replace(path, temp_path, &durable_bytes)
}

/// Loads raw snapshot bytes when a snapshot exists.
pub fn load(path: &Path, keyring: Option<&StorageKeyring>) -> Result<Option<Vec<u8>>> {
    match fs::read(path) {
        Ok(bytes) => {
            let decoded = match keyring {
                Some(keyring) => decrypt(keyring, "snapshot", &bytes)?,
                None => bytes,
            };
            Ok(Some(decoded))
        }
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err.into()),
    }
}

#[cfg(test)]
mod tests {
    use crate::StorageKeyring;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::{load, save};

    fn temp_path(name: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("vaylix-{name}-{unique}.bin"))
    }

    #[test]
    fn saves_and_loads_snapshot_bytes() {
        let path = temp_path("snapshot");
        let temp_path = temp_path("snapshot-tmp");
        let payload = b"snapshot-bytes";

        let keyring = StorageKeyring {
            active: crate::StorageKey {
                id: uuid::Uuid::now_v7(),
                secret: "snapshot-key".to_string(),
                created_at_ms: 1,
            },
            previous: Vec::new(),
        };
        save(payload, &path, &temp_path, Some(&keyring)).unwrap();
        let loaded = load(&path, Some(&keyring)).unwrap();

        assert_eq!(loaded.as_deref(), Some(payload.as_slice()));

        fs::remove_file(path).ok();
        fs::remove_file(temp_path).ok();
    }

    #[test]
    fn returns_none_for_missing_snapshot() {
        let path = temp_path("missing-snapshot");
        let keyring = StorageKeyring {
            active: crate::StorageKey {
                id: uuid::Uuid::now_v7(),
                secret: "snapshot-key".to_string(),
                created_at_ms: 1,
            },
            previous: Vec::new(),
        };
        assert_eq!(load(&path, Some(&keyring)).unwrap(), None);
    }

    #[test]
    fn rejects_wrong_snapshot_key() {
        let path = temp_path("snapshot-encrypted");
        let temp_path = temp_path("snapshot-encrypted-tmp");
        let right = StorageKeyring {
            active: crate::StorageKey {
                id: uuid::Uuid::now_v7(),
                secret: "right-key".to_string(),
                created_at_ms: 1,
            },
            previous: Vec::new(),
        };
        let wrong = StorageKeyring {
            active: crate::StorageKey {
                id: uuid::Uuid::now_v7(),
                secret: "wrong-key".to_string(),
                created_at_ms: 1,
            },
            previous: Vec::new(),
        };
        save(b"snapshot-bytes", &path, &temp_path, Some(&right)).unwrap();

        assert!(load(&path, Some(&wrong)).is_err());

        fs::remove_file(path).ok();
        fs::remove_file(temp_path).ok();
    }
}
