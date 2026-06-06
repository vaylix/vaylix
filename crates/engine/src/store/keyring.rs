use crate::config::{StorageKey, StorageKeyring};
use crate::{EngineError, Result};
use rand::random;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::ErrorKind;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

use super::{binary, durable};

const DEFAULT_SECRET_BYTES: usize = 32;
const DEFAULT_ROTATION_MS: u64 = 30 * 24 * 60 * 60 * 1000;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct StoredKey {
    id: Uuid,
    secret: String,
    created_at_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct StoredKeyring {
    active: StoredKey,
    previous: Vec<StoredKey>,
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn random_secret() -> String {
    let bytes = random::<[u8; DEFAULT_SECRET_BYTES]>();
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

fn to_runtime(keyring: StoredKeyring) -> StorageKeyring {
    StorageKeyring {
        active: StorageKey {
            id: keyring.active.id,
            secret: keyring.active.secret,
            created_at_ms: keyring.active.created_at_ms,
        },
        previous: keyring
            .previous
            .into_iter()
            .map(|key| StorageKey {
                id: key.id,
                secret: key.secret,
                created_at_ms: key.created_at_ms,
            })
            .collect(),
    }
}

fn from_runtime(keyring: &StorageKeyring) -> StoredKeyring {
    StoredKeyring {
        active: StoredKey {
            id: keyring.active.id,
            secret: keyring.active.secret.clone(),
            created_at_ms: keyring.active.created_at_ms,
        },
        previous: keyring
            .previous
            .iter()
            .map(|key| StoredKey {
                id: key.id,
                secret: key.secret.clone(),
                created_at_ms: key.created_at_ms,
            })
            .collect(),
    }
}

pub fn save(keyring: &StorageKeyring, path: &Path, temp_path: &Path) -> Result<()> {
    let bytes = binary::encode(&from_runtime(keyring))
        .map_err(|err| EngineError::ManifestSerialize(err.to_string()))?;
    durable::atomic_replace(path, temp_path, &bytes)
}

pub fn load(path: &Path) -> Result<Option<StorageKeyring>> {
    match fs::read(path) {
        Ok(bytes) => {
            let stored: StoredKeyring = binary::decode(&bytes)
                .map_err(|err| EngineError::ManifestDeserialize(err.to_string()))?;
            Ok(Some(to_runtime(stored)))
        }
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err.into()),
    }
}

pub fn load_or_create(path: &Path, temp_path: &Path) -> Result<StorageKeyring> {
    match load(path)? {
        Some(keyring) => Ok(keyring),
        None => {
            let keyring = StorageKeyring {
                active: StorageKey {
                    id: Uuid::now_v7(),
                    secret: random_secret(),
                    created_at_ms: now_millis(),
                },
                previous: Vec::new(),
            };
            save(&keyring, path, temp_path)?;
            Ok(keyring)
        }
    }
}

pub fn rotate_if_due(path: &Path, temp_path: &Path, keyring: &mut StorageKeyring) -> Result<bool> {
    let now = now_millis();
    if now.saturating_sub(keyring.active.created_at_ms) < DEFAULT_ROTATION_MS {
        return Ok(false);
    }
    let previous_active = keyring.active.clone();
    keyring.previous.push(previous_active);
    keyring.active = StorageKey {
        id: Uuid::now_v7(),
        secret: random_secret(),
        created_at_ms: now,
    };
    save(keyring, path, temp_path)?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::{DEFAULT_SECRET_BYTES, load_or_create, random_secret, rotate_if_due};
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_path(name: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("vaylix-keyring-{name}-{unique}.bin"))
    }

    #[test]
    fn creates_and_reloads_keyring() {
        let path = temp_path("keyring");
        let temp = temp_path("keyring-tmp");
        let created = load_or_create(&path, &temp).unwrap();
        let reloaded = load_or_create(&path, &temp).unwrap();
        assert_eq!(created.active.id, reloaded.active.id);
        fs::remove_file(path).ok();
        fs::remove_file(temp).ok();
    }

    #[test]
    fn rotates_keyring_when_due() {
        let path = temp_path("rotate");
        let temp = temp_path("rotate-tmp");
        let mut keyring = load_or_create(&path, &temp).unwrap();
        keyring.active.created_at_ms = 0;
        assert!(rotate_if_due(&path, &temp, &mut keyring).unwrap());
        assert_eq!(keyring.previous.len(), 1);
        fs::remove_file(path).ok();
        fs::remove_file(temp).ok();
    }

    #[test]
    fn generated_secret_is_lowercase_hex() {
        let secret = random_secret();
        assert_eq!(secret.len(), DEFAULT_SECRET_BYTES * 2);
        assert!(secret.bytes().all(|byte| byte.is_ascii_hexdigit()));
        assert!(!secret.bytes().any(|byte| byte.is_ascii_uppercase()));
    }
}
