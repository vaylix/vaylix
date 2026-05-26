use thiserror::Error;

pub type Result<T> = std::result::Result<T, TransportError>;

#[derive(Debug, Error)]
pub enum TransportError {
    #[error("invalid frame")]
    InvalidFrame,
    #[error("unknown opcode: 0x{0:02x}")]
    UnknownOpcode(u8),
    #[error("unknown status: 0x{0:02x}")]
    UnknownStatus(u8),
    #[error("unsupported command for transport: {0}")]
    UnsupportedCommand(&'static str),
    #[error("protocol version mismatch: expected {expected}, got {actual}")]
    VersionMismatch { expected: u8, actual: u8 },
    #[error("frame too large: {length} bytes exceeds maximum {max} bytes")]
    FrameTooLarge { length: usize, max: usize },
    #[error("unexpected end of input")]
    UnexpectedEof,
    #[error("corrupted payload")]
    CorruptedPayload,
    #[error("frame checksum mismatch")]
    ChecksumMismatch,
    #[error("unsupported frame flags: 0x{0:02x}")]
    UnsupportedFlags(u8),
    #[error("frame compression failure")]
    CompressionFailure,
    #[error("invalid utf-8 in payload")]
    InvalidUtf8(#[from] std::string::FromUtf8Error),
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),
}

impl TransportError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::InvalidFrame => "TRN-001",
            Self::UnknownOpcode(_) => "TRN-002",
            Self::UnknownStatus(_) => "TRN-003",
            Self::UnsupportedCommand(_) => "TRN-004",
            Self::VersionMismatch { .. } => "TRN-005",
            Self::FrameTooLarge { .. } => "TRN-006",
            Self::UnexpectedEof => "TRN-007",
            Self::CorruptedPayload => "TRN-008",
            Self::ChecksumMismatch => "TRN-009",
            Self::UnsupportedFlags(_) => "TRN-010",
            Self::CompressionFailure => "TRN-011",
            Self::InvalidUtf8(_) => "TRN-012",
            Self::Io(_) => "TRN-013",
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Self::InvalidFrame => "Invalid Frame",
            Self::UnknownOpcode(_) => "Unknown Opcode",
            Self::UnknownStatus(_) => "Unknown Status",
            Self::UnsupportedCommand(_) => "Unsupported Command",
            Self::VersionMismatch { .. } => "Version Mismatch",
            Self::FrameTooLarge { .. } => "Frame Too Large",
            Self::UnexpectedEof => "Unexpected End Of Frame",
            Self::CorruptedPayload => "Corrupted Payload",
            Self::ChecksumMismatch => "Checksum Mismatch",
            Self::UnsupportedFlags(_) => "Unsupported Frame Flags",
            Self::CompressionFailure => "Compression Failure",
            Self::InvalidUtf8(_) => "Invalid UTF-8 Payload",
            Self::Io(_) => "Transport I/O Failure",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::TransportError;

    #[test]
    fn exposes_stable_codes_and_names() {
        let err = TransportError::CorruptedPayload;

        assert_eq!(err.code(), "TRN-008");
        assert_eq!(err.name(), "Corrupted Payload");
    }
}
