use thiserror::Error;

pub type Result<T> = std::result::Result<T, ClientError>;

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("command parsing failed: {0}")]
    Command(#[from] command::CommandError),
    #[error("transport failure: {0}")]
    Transport(#[from] transport::TransportError),
    #[error("filesystem I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("line editor failure: {0}")]
    Readline(#[from] rustyline::error::ReadlineError),
    #[error("could not determine project directories")]
    ProjectDirsUnavailable,
    #[error("mismatched response id: expected {expected}, got {actual}")]
    ResponseIdMismatch { expected: u32, actual: u32 },
    #[error("local command should not receive a response")]
    LocalCommandResponse,
}

impl ClientError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::Command(err) => err.code(),
            Self::Transport(err) => err.code(),
            Self::Io(_) => "CLI-003",
            Self::Readline(_) => "CLI-004",
            Self::ProjectDirsUnavailable => "CLI-005",
            Self::ResponseIdMismatch { .. } => "CLI-006",
            Self::LocalCommandResponse => "CLI-007",
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Self::Command(err) => err.name(),
            Self::Transport(err) => err.name(),
            Self::Io(_) => "Filesystem I/O Failure",
            Self::Readline(_) => "Readline Failure",
            Self::ProjectDirsUnavailable => "Project Directories Unavailable",
            Self::ResponseIdMismatch { .. } => "Response Id Mismatch",
            Self::LocalCommandResponse => "Local Command Response",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ClientError;

    #[test]
    fn exposes_stable_codes_and_names() {
        let err = ClientError::ProjectDirsUnavailable;

        assert_eq!(err.code(), "CLI-005");
        assert_eq!(err.name(), "Project Directories Unavailable");
    }
}
