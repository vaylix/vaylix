use thiserror::Error;

pub type Result<T> = std::result::Result<T, CommandError>;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum CommandError {
    #[error("empty command")]
    EmptyCommand,
    #[error("unknown command: {command}")]
    UnknownCommand { command: String },
    #[error("usage: {usage}")]
    InvalidArity { usage: String },
    #[error("expected opening quote")]
    ExpectedOpeningQuote,
    #[error("unterminated quoted string starting at byte {start}")]
    UnterminatedQuotedString { start: usize },
}

impl CommandError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::EmptyCommand => "CMD-001",
            Self::UnknownCommand { .. } => "CMD-002",
            Self::InvalidArity { .. } => "CMD-003",
            Self::ExpectedOpeningQuote => "CMD-004",
            Self::UnterminatedQuotedString { .. } => "CMD-005",
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Self::EmptyCommand => "Empty Command",
            Self::UnknownCommand { .. } => "Unknown Command",
            Self::InvalidArity { .. } => "Invalid Command Arity",
            Self::ExpectedOpeningQuote => "Expected Opening Quote",
            Self::UnterminatedQuotedString { .. } => "Unterminated Quoted String",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::CommandError;

    #[test]
    fn exposes_stable_codes_and_names() {
        let err = CommandError::UnknownCommand {
            command: "wat".to_string(),
        };

        assert_eq!(err.code(), "CMD-002");
        assert_eq!(err.name(), "Unknown Command");
    }
}
