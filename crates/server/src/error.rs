use thiserror::Error;

pub type Result<T> = std::result::Result<T, ServerError>;

/// Process and runtime errors for the server binary.
#[derive(Debug, Error)]
pub enum ServerError {
    #[error("failed to bind TCP listener: {0}")]
    Bind(#[source] std::io::Error),
    #[error("failed to accept client connection: {0}")]
    Accept(#[source] std::io::Error),
    #[error("connection slot pool is closed")]
    ConnectionPoolClosed,
    #[error("filesystem I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("engine worker is unavailable")]
    EngineWorkerClosed,
    #[error("engine failure: {0}")]
    Engine(#[from] engine::EngineError),
    #[error("transport failure: {0}")]
    Transport(#[from] transport::TransportError),
    #[error("engine lock is poisoned")]
    EngineLockPoisoned,
    #[error("client authentication is required")]
    AuthenticationRequired,
    #[error("client authentication failed")]
    AuthenticationFailed,
    #[error("server authentication configuration is invalid")]
    AuthenticationConfiguration,
    #[error("TLS configuration is invalid")]
    TlsConfiguration,
    #[error("TLS handshake failed: {0}")]
    TlsHandshake(#[source] std::io::Error),
    #[error("request rate limit exceeded")]
    RateLimitExceeded,
    #[error("request exceeds configured quotas")]
    QuotaExceeded,
    #[error("transaction already active for this session")]
    TransactionAlreadyActive,
    #[error("no active transaction for this session")]
    NoActiveTransaction,
    #[error("unsupported remote command")]
    UnsupportedRemoteCommand,
    #[error("permission denied")]
    PermissionDenied,
    #[error("unknown permission: {0}")]
    InvalidPermission(String),
    #[error("user already exists: {0}")]
    UserAlreadyExists(String),
    #[error("user not found: {0}")]
    UserNotFound(String),
    #[error("role already exists: {0}")]
    RoleAlreadyExists(String),
    #[error("role not found: {0}")]
    RoleNotFound(String),
    #[error("role is protected: {0}")]
    ProtectedRole(String),
    #[error("cannot remove the last admin user")]
    LastAdminUser,
    #[error("auth store serialization failed: {0}")]
    AuthStoreEncode(String),
    #[error("auth store deserialization failed: {0}")]
    AuthStoreDecode(String),
}

impl ServerError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::Bind(_) => "SRV-001",
            Self::Accept(_) => "SRV-002",
            Self::ConnectionPoolClosed => "SRV-003",
            Self::Io(_) => "SRV-004",
            Self::EngineWorkerClosed => "SRV-005",
            Self::Engine(err) => err.code(),
            Self::Transport(err) => err.code(),
            Self::EngineLockPoisoned => "SRV-006",
            Self::AuthenticationRequired => "SRV-007",
            Self::AuthenticationFailed => "SRV-008",
            Self::AuthenticationConfiguration => "SRV-009",
            Self::TlsConfiguration => "SRV-010",
            Self::TlsHandshake(_) => "SRV-011",
            Self::RateLimitExceeded => "SRV-012",
            Self::QuotaExceeded => "SRV-013",
            Self::TransactionAlreadyActive => "SRV-014",
            Self::NoActiveTransaction => "SRV-015",
            Self::UnsupportedRemoteCommand => "SRV-016",
            Self::PermissionDenied => "SRV-017",
            Self::InvalidPermission(_) => "SRV-018",
            Self::UserAlreadyExists(_) => "SRV-019",
            Self::UserNotFound(_) => "SRV-020",
            Self::RoleAlreadyExists(_) => "SRV-021",
            Self::RoleNotFound(_) => "SRV-022",
            Self::ProtectedRole(_) => "SRV-023",
            Self::LastAdminUser => "SRV-024",
            Self::AuthStoreEncode(_) => "SRV-025",
            Self::AuthStoreDecode(_) => "SRV-026",
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Self::Bind(_) => "Listener Bind Failure",
            Self::Accept(_) => "Connection Accept Failure",
            Self::ConnectionPoolClosed => "Connection Slot Pool Closed",
            Self::Io(_) => "Filesystem I/O Failure",
            Self::EngineWorkerClosed => "Engine Worker Closed",
            Self::Engine(err) => err.name(),
            Self::Transport(err) => err.name(),
            Self::EngineLockPoisoned => "Engine Lock Poisoned",
            Self::AuthenticationRequired => "Authentication Required",
            Self::AuthenticationFailed => "Authentication Failed",
            Self::AuthenticationConfiguration => "Authentication Configuration Invalid",
            Self::TlsConfiguration => "TLS Configuration Invalid",
            Self::TlsHandshake(_) => "TLS Handshake Failure",
            Self::RateLimitExceeded => "Rate Limit Exceeded",
            Self::QuotaExceeded => "Quota Exceeded",
            Self::TransactionAlreadyActive => "Transaction Already Active",
            Self::NoActiveTransaction => "No Active Transaction",
            Self::UnsupportedRemoteCommand => "Unsupported Remote Command",
            Self::PermissionDenied => "Permission Denied",
            Self::InvalidPermission(_) => "Invalid Permission",
            Self::UserAlreadyExists(_) => "User Already Exists",
            Self::UserNotFound(_) => "User Not Found",
            Self::RoleAlreadyExists(_) => "Role Already Exists",
            Self::RoleNotFound(_) => "Role Not Found",
            Self::ProtectedRole(_) => "Protected Role",
            Self::LastAdminUser => "Last Admin User",
            Self::AuthStoreEncode(_) => "Auth Store Serialization Failure",
            Self::AuthStoreDecode(_) => "Auth Store Deserialization Failure",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ServerError;

    #[test]
    fn exposes_stable_codes_and_names() {
        let err = ServerError::AuthenticationRequired;

        assert_eq!(err.code(), "SRV-007");
        assert_eq!(err.name(), "Authentication Required");
    }
}
