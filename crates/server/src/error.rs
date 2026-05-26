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
    #[error("transaction already active for this session")]
    TransactionAlreadyActive,
    #[error("no active transaction for this session")]
    NoActiveTransaction,
    #[error("unsupported remote command")]
    UnsupportedRemoteCommand,
}

impl ServerError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::Bind(_) => "SRV-001",
            Self::Accept(_) => "SRV-002",
            Self::ConnectionPoolClosed => "SRV-003",
            Self::Engine(err) => err.code(),
            Self::Transport(err) => err.code(),
            Self::EngineLockPoisoned => "SRV-004",
            Self::AuthenticationRequired => "SRV-005",
            Self::AuthenticationFailed => "SRV-006",
            Self::AuthenticationConfiguration => "SRV-007",
            Self::TransactionAlreadyActive => "SRV-008",
            Self::NoActiveTransaction => "SRV-009",
            Self::UnsupportedRemoteCommand => "SRV-010",
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Self::Bind(_) => "Listener Bind Failure",
            Self::Accept(_) => "Connection Accept Failure",
            Self::ConnectionPoolClosed => "Connection Slot Pool Closed",
            Self::Engine(err) => err.name(),
            Self::Transport(err) => err.name(),
            Self::EngineLockPoisoned => "Engine Lock Poisoned",
            Self::AuthenticationRequired => "Authentication Required",
            Self::AuthenticationFailed => "Authentication Failed",
            Self::AuthenticationConfiguration => "Authentication Configuration Invalid",
            Self::TransactionAlreadyActive => "Transaction Already Active",
            Self::NoActiveTransaction => "No Active Transaction",
            Self::UnsupportedRemoteCommand => "Unsupported Remote Command",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ServerError;

    #[test]
    fn exposes_stable_codes_and_names() {
        let err = ServerError::AuthenticationRequired;

        assert_eq!(err.code(), "SRV-005");
        assert_eq!(err.name(), "Authentication Required");
    }
}
