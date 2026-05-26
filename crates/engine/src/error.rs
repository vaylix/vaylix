use thiserror::Error;

pub type Result<T> = std::result::Result<T, EngineError>;

#[derive(Debug, Error)]
pub enum EngineError {
    #[error("could not determine project directories")]
    ProjectDirsUnavailable,
    #[error("filesystem I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("snapshot serialization failed: {0}")]
    SnapshotSerialize(#[source] postcard::Error),
    #[error("snapshot deserialization failed: {0}")]
    SnapshotDeserialize(#[source] postcard::Error),
    #[error("manifest serialization failed: {0}")]
    ManifestSerialize(#[source] postcard::Error),
    #[error("manifest deserialization failed: {0}")]
    ManifestDeserialize(#[source] postcard::Error),
    #[error("checksum validation failed for {resource}")]
    ChecksumMismatch { resource: &'static str },
    #[error("write-ahead log serialization failed: {0}")]
    WalSerialize(#[source] postcard::Error),
    #[error("write-ahead log deserialization failed: {0}")]
    WalDeserialize(#[source] postcard::Error),
    #[error("value for key '{key}' is not a valid integer: {value}")]
    InvalidIntegerValue { key: String, value: String },
    #[error("numeric overflow for key '{key}'")]
    NumericOverflow { key: String },
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
            Self::WalSerialize(_) => "ENG-008",
            Self::WalDeserialize(_) => "ENG-009",
            Self::InvalidIntegerValue { .. } => "ENG-010",
            Self::NumericOverflow { .. } => "ENG-011",
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
            Self::WalSerialize(_) => "WAL Serialization Failure",
            Self::WalDeserialize(_) => "WAL Deserialization Failure",
            Self::InvalidIntegerValue { .. } => "Invalid Integer Value",
            Self::NumericOverflow { .. } => "Numeric Overflow",
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
