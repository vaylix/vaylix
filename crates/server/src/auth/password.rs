use argon2::Argon2;
use argon2::password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use rand::random;

use super::UserRecord;
use crate::error::{Result, ServerError};

const MIN_PASSWORD_LEN: usize = 12;

pub(super) fn password_matches(user: &UserRecord, password: &str) -> Result<bool> {
    let parsed_hash = PasswordHash::new(&user.password_hash)
        .map_err(|_| ServerError::AuthenticationConfiguration)?;
    Ok(Argon2::default()
        .verify_password(password.as_bytes(), &parsed_hash)
        .is_ok())
}

pub(super) fn hash_password(password: &str) -> Result<String> {
    let salt = SaltString::encode_b64(&random::<[u8; 16]>())
        .map_err(|_| ServerError::AuthenticationConfiguration)?;
    Ok(Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map_err(|_| ServerError::AuthenticationConfiguration)?
        .to_string())
}

pub(super) fn validate_password_policy(password: &str) -> Result<()> {
    if password.len() < MIN_PASSWORD_LEN {
        return Err(ServerError::PasswordPolicyViolation);
    }
    let has_letter = password.chars().any(|char| char.is_ascii_alphabetic());
    let has_digit = password.chars().any(|char| char.is_ascii_digit());
    if !has_letter || !has_digit {
        return Err(ServerError::PasswordPolicyViolation);
    }
    Ok(())
}
