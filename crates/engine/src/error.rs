use thiserror::Error;

pub type Result<T> = std::result::Result<T, EngineError>;

#[derive(Debug, Error)]
pub enum EngineError {
    #[error("could not determine project directories")]
    ProjectDirsUnavailable,
    #[error("filesystem I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("snapshot serialization failed: {0}")]
    SnapshotSerialize(String),
    #[error("snapshot deserialization failed: {0}")]
    SnapshotDeserialize(String),
    #[error("manifest serialization failed: {0}")]
    ManifestSerialize(String),
    #[error("manifest deserialization failed: {0}")]
    ManifestDeserialize(String),
    #[error("checksum validation failed for {resource}")]
    ChecksumMismatch { resource: &'static str },
    #[error("encrypted storage operation failed for {resource}")]
    CryptoFailure { resource: &'static str },
    #[error("unsupported storage format for {resource}")]
    UnsupportedStorageFormat { resource: &'static str },
    #[error("storage migration is required for {resource}")]
    StorageMigrationRequired { resource: &'static str },
    #[error("invalid storage operation: {0}")]
    InvalidStorageOperation(String),
    #[error("restore point is unavailable: {0}")]
    RestorePointUnavailable(String),
    #[error("write-ahead log serialization failed: {0}")]
    WalSerialize(String),
    #[error("write-ahead log deserialization failed: {0}")]
    WalDeserialize(String),
    #[error("value for key '{key}' is not a valid integer: {value}")]
    InvalidIntegerValue { key: String, value: String },
    #[error("numeric overflow for key '{key}'")]
    NumericOverflow { key: String },
    #[error("{0}")]
    UnsupportedCommand(String),
}

impl EngineError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::ProjectDirsUnavailable => "ENG-001",
            Self::Io(_) => "ENG-002",
            Self::SnapshotSerialize(_) => "ENG-003",
            Self::SnapshotDeserialize(_) => "ENG-004",
            Self::ManifestSerialize(_) => "ENG-005",
            Self::ManifestDeserialize(_) => "ENG-006",
            Self::ChecksumMismatch { .. } => "ENG-007",
            Self::CryptoFailure { .. } => "ENG-008",
            Self::UnsupportedStorageFormat { .. } => "ENG-009",
            Self::StorageMigrationRequired { .. } => "ENG-010",
            Self::InvalidStorageOperation(_) => "ENG-011",
            Self::RestorePointUnavailable(_) => "ENG-012",
            Self::WalSerialize(_) => "ENG-013",
            Self::WalDeserialize(_) => "ENG-014",
            Self::InvalidIntegerValue { .. } => "ENG-015",
            Self::NumericOverflow { .. } => "ENG-016",
            Self::UnsupportedCommand(_) => "ENG-017",
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Self::ProjectDirsUnavailable => "Project Directories Unavailable",
            Self::Io(_) => "Filesystem I/O Failure",
            Self::SnapshotSerialize(_) => "Snapshot Serialization Failure",
            Self::SnapshotDeserialize(_) => "Snapshot Deserialization Failure",
            Self::ManifestSerialize(_) => "Manifest Serialization Failure",
            Self::ManifestDeserialize(_) => "Manifest Deserialization Failure",
            Self::ChecksumMismatch { .. } => "Checksum Validation Failure",
            Self::CryptoFailure { .. } => "Encrypted Storage Failure",
            Self::UnsupportedStorageFormat { .. } => "Unsupported Storage Format",
            Self::StorageMigrationRequired { .. } => "Storage Migration Required",
            Self::InvalidStorageOperation(_) => "Invalid Storage Operation",
            Self::RestorePointUnavailable(_) => "Restore Point Unavailable",
            Self::WalSerialize(_) => "WAL Serialization Failure",
            Self::WalDeserialize(_) => "WAL Deserialization Failure",
            Self::InvalidIntegerValue { .. } => "Invalid Integer Value",
            Self::NumericOverflow { .. } => "Numeric Overflow",
            Self::UnsupportedCommand(_) => "Unsupported Command",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::EngineError;

    #[test]
    fn exposes_stable_codes_and_names() {
        let err = EngineError::ProjectDirsUnavailable;

        assert_eq!(err.code(), "ENG-001");
        assert_eq!(err.name(), "Project Directories Unavailable");
    }
}
