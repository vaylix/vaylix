use thiserror::Error;

pub type Result<T> = std::result::Result<T, ServerError>;

#[derive(Debug, Error)]
pub enum ServerError {
    #[error("failed to bind TCP listener: {0}")]
    Bind(#[source] std::io::Error),
    #[error("failed to accept client connection: {0}")]
    Accept(#[source] std::io::Error),
    #[error("engine failure: {0}")]
    Engine(#[from] engine::EngineError),
    #[error("transport failure: {0}")]
    Transport(#[from] transport::TransportError),
    #[error("unsupported remote command")]
    UnsupportedRemoteCommand,
}

impl ServerError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::Bind(_) => "SRV-001",
            Self::Accept(_) => "SRV-002",
            Self::Engine(err) => err.code(),
            Self::Transport(err) => err.code(),
            Self::UnsupportedRemoteCommand => "SRV-005",
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Self::Bind(_) => "Listener Bind Failure",
            Self::Accept(_) => "Connection Accept Failure",
            Self::Engine(err) => err.name(),
            Self::Transport(err) => err.name(),
            Self::UnsupportedRemoteCommand => "Unsupported Remote Command",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ServerError;

    #[test]
    fn exposes_stable_codes_and_names() {
        let err = ServerError::UnsupportedRemoteCommand;

        assert_eq!(err.code(), "SRV-005");
        assert_eq!(err.name(), "Unsupported Remote Command");
    }
}
