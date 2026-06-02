use crate::config::{StorageKey, StorageKeyring};
use crate::{EngineError, Result};
use argon2::{Algorithm, Argon2, Params, Version};
use chacha20poly1305::aead::{Aead, KeyInit, Payload};
use chacha20poly1305::{ChaCha20Poly1305, Key, Nonce};
use crc32fast::hash;
use rand::random;
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use uuid::Uuid;

const ENVELOPE_MAGIC: &[u8; 4] = b"VXE1";
const ENVELOPE_VERSION: u8 = 1;
const ENVELOPE_ALGORITHM_CHACHA20_POLY1305: u8 = 1;
const SALT_LEN: usize = 16;
const NONCE_LEN: usize = 12;
const KEY_LEN: usize = 32;
const KEY_ID_LEN: usize = 16;

type DerivedKeyCacheKey = (Uuid, String);
type DerivedKeyCache = HashMap<DerivedKeyCacheKey, [u8; KEY_LEN]>;

static DERIVED_KEY_CACHE: OnceLock<Mutex<DerivedKeyCache>> = OnceLock::new();

fn derive_key(secret: &str, salt: &[u8; SALT_LEN]) -> Result<[u8; KEY_LEN]> {
    let mut key = [0_u8; KEY_LEN];
    let params =
        Params::new(19_456, 2, 1, Some(KEY_LEN)).map_err(|_| EngineError::CryptoFailure {
            resource: "key derivation",
        })?;
    let argon2 = Argon2::new(Algorithm::Argon2id, Version::V0x13, params);
    argon2
        .hash_password_into(secret.as_bytes(), salt, &mut key)
        .map_err(|_| EngineError::CryptoFailure {
            resource: "key derivation",
        })?;
    Ok(key)
}

fn stable_salt(key: &StorageKey) -> [u8; SALT_LEN] {
    *key.id.as_bytes()
}

fn derive_stable_key(key: &StorageKey, salt: &[u8; SALT_LEN]) -> Result<[u8; KEY_LEN]> {
    if *salt != stable_salt(key) {
        return derive_key(&key.secret, salt);
    }

    let cache = DERIVED_KEY_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    if let Some(derived_key) = cache
        .lock()
        .map_err(|_| EngineError::CryptoFailure {
            resource: "key cache",
        })?
        .get(&(key.id, key.secret.clone()))
        .copied()
    {
        return Ok(derived_key);
    }

    let derived_key = derive_key(&key.secret, salt)?;
    cache
        .lock()
        .map_err(|_| EngineError::CryptoFailure {
            resource: "key cache",
        })?
        .insert((key.id, key.secret.clone()), derived_key);
    Ok(derived_key)
}

/// Encrypts plaintext into a versioned, self-describing storage envelope.
pub fn encrypt(key: &StorageKey, resource: &'static str, plaintext: &[u8]) -> Result<Vec<u8>> {
    let salt = stable_salt(key);
    let nonce_bytes = random::<[u8; NONCE_LEN]>();
    let derived_key = derive_stable_key(key, &salt)?;
    let cipher = ChaCha20Poly1305::new(Key::from_slice(&derived_key));
    let ciphertext = cipher
        .encrypt(
            Nonce::from_slice(&nonce_bytes),
            Payload {
                msg: plaintext,
                aad: resource.as_bytes(),
            },
        )
        .map_err(|_| EngineError::CryptoFailure { resource })?;
    let checksum = hash(&ciphertext);

    let mut envelope = Vec::with_capacity(
        ENVELOPE_MAGIC.len() + 2 + KEY_ID_LEN + SALT_LEN + NONCE_LEN + 8 + ciphertext.len(),
    );
    envelope.extend_from_slice(ENVELOPE_MAGIC);
    envelope.push(ENVELOPE_VERSION);
    envelope.push(ENVELOPE_ALGORITHM_CHACHA20_POLY1305);
    envelope.extend_from_slice(key.id.as_bytes());
    envelope.extend_from_slice(&salt);
    envelope.extend_from_slice(&nonce_bytes);
    envelope.extend_from_slice(&(ciphertext.len() as u32).to_le_bytes());
    envelope.extend_from_slice(&checksum.to_le_bytes());
    envelope.extend_from_slice(&ciphertext);

    Ok(envelope)
}

/// Decrypts a storage envelope back into plaintext.
pub fn decrypt(
    keyring: &StorageKeyring,
    resource: &'static str,
    envelope: &[u8],
) -> Result<Vec<u8>> {
    let minimum_len = ENVELOPE_MAGIC.len() + 2 + KEY_ID_LEN + SALT_LEN + NONCE_LEN + 8;
    if envelope.len() < minimum_len {
        return Err(EngineError::UnsupportedStorageFormat { resource });
    }

    if &envelope[..4] != ENVELOPE_MAGIC {
        return Err(EngineError::UnsupportedStorageFormat { resource });
    }

    let version = envelope[4];
    let algorithm = envelope[5];
    if version != ENVELOPE_VERSION || algorithm != ENVELOPE_ALGORITHM_CHACHA20_POLY1305 {
        return Err(EngineError::UnsupportedStorageFormat { resource });
    }

    let key_id_start = 6;
    let salt_start = key_id_start + KEY_ID_LEN;
    let nonce_start = salt_start + SALT_LEN;
    let length_start = nonce_start + NONCE_LEN;
    let checksum_start = length_start + 4;
    let payload_start = checksum_start + 4;

    let key_id = Uuid::from_slice(&envelope[key_id_start..salt_start])
        .map_err(|_| EngineError::UnsupportedStorageFormat { resource })?;
    let salt: [u8; SALT_LEN] = envelope[salt_start..nonce_start]
        .try_into()
        .map_err(|_| EngineError::UnsupportedStorageFormat { resource })?;
    let nonce_bytes: [u8; NONCE_LEN] = envelope[nonce_start..length_start]
        .try_into()
        .map_err(|_| EngineError::UnsupportedStorageFormat { resource })?;
    let ciphertext_len = u32::from_le_bytes(
        envelope[length_start..checksum_start]
            .try_into()
            .map_err(|_| EngineError::UnsupportedStorageFormat { resource })?,
    ) as usize;
    let checksum = u32::from_le_bytes(
        envelope[checksum_start..payload_start]
            .try_into()
            .map_err(|_| EngineError::UnsupportedStorageFormat { resource })?,
    );

    if envelope.len() != payload_start + ciphertext_len {
        return Err(EngineError::UnsupportedStorageFormat { resource });
    }

    let ciphertext = &envelope[payload_start..];
    if hash(ciphertext) != checksum {
        return Err(EngineError::ChecksumMismatch { resource });
    }

    let key = keyring
        .get(key_id)
        .ok_or(EngineError::UnsupportedStorageFormat { resource })?;
    let derived_key = derive_stable_key(key, &salt)?;
    let cipher = ChaCha20Poly1305::new(Key::from_slice(&derived_key));
    cipher
        .decrypt(
            Nonce::from_slice(&nonce_bytes),
            Payload {
                msg: ciphertext,
                aad: resource.as_bytes(),
            },
        )
        .map_err(|_| EngineError::CryptoFailure { resource })
}

#[cfg(test)]
mod tests {
    use super::{decrypt, encrypt};
    use crate::config::{StorageKey, StorageKeyring};
    use uuid::Uuid;

    #[test]
    fn round_trips_encrypted_payload() {
        let key = StorageKey {
            id: Uuid::now_v7(),
            secret: "secret-key".to_string(),
            created_at_ms: 1,
        };
        let envelope = encrypt(&key, "snapshot", b"hello").unwrap();
        let plaintext = decrypt(
            &StorageKeyring {
                active: key,
                previous: Vec::new(),
            },
            "snapshot",
            &envelope,
        )
        .unwrap();
        assert_eq!(plaintext, b"hello");
    }

    #[test]
    fn rejects_wrong_key() {
        let key = StorageKey {
            id: Uuid::now_v7(),
            secret: "secret-key".to_string(),
            created_at_ms: 1,
        };
        let envelope = encrypt(&key, "snapshot", b"hello").unwrap();
        let wrong = StorageKeyring {
            active: StorageKey {
                id: Uuid::now_v7(),
                secret: "wrong-key".to_string(),
                created_at_ms: 1,
            },
            previous: Vec::new(),
        };
        assert!(decrypt(&wrong, "snapshot", &envelope).is_err());
    }

    #[test]
    fn rejects_unsupported_envelope_version_and_algorithm() {
        let key = StorageKey {
            id: Uuid::now_v7(),
            secret: "secret-key".to_string(),
            created_at_ms: 1,
        };
        let keyring = StorageKeyring {
            active: key.clone(),
            previous: Vec::new(),
        };

        let mut wrong_version = encrypt(&key, "snapshot", b"hello").unwrap();
        wrong_version[4] = 99;
        assert!(decrypt(&keyring, "snapshot", &wrong_version).is_err());

        let mut wrong_algorithm = encrypt(&key, "snapshot", b"hello").unwrap();
        wrong_algorithm[5] = 99;
        assert!(decrypt(&keyring, "snapshot", &wrong_algorithm).is_err());
    }
}
