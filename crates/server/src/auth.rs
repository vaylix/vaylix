use argon2::Argon2;
use argon2::password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use rand::random;

use crate::error::{Result, ServerError};

pub const DEFAULT_USERNAME: &str = "vaylix";
pub const DEFAULT_PASSWORD: &str = "vaylix";

/// Immutable authentication settings for the server.
#[derive(Clone)]
pub struct AuthConfig {
    username: String,
    password_hash: String,
}

impl AuthConfig {
    /// Builds an auth config from a plaintext password using Argon2 hashing.
    pub fn new(username: String, password: String) -> Result<Self> {
        let salt = SaltString::encode_b64(&random::<[u8; 16]>())
            .map_err(|_| ServerError::AuthenticationConfiguration)?;
        let password_hash = Argon2::default()
            .hash_password(password.as_bytes(), &salt)
            .map_err(|_| ServerError::AuthenticationConfiguration)?
            .to_string();

        Ok(Self {
            username,
            password_hash,
        })
    }

    /// Verifies a client credential pair.
    pub fn verify(&self, username: &str, password: &str) -> bool {
        if username != self.username {
            return false;
        }

        let Ok(parsed_hash) = PasswordHash::new(&self.password_hash) else {
            return false;
        };

        Argon2::default()
            .verify_password(password.as_bytes(), &parsed_hash)
            .is_ok()
    }

    /// Returns the configured username.
    pub fn username(&self) -> &str {
        &self.username
    }
}
