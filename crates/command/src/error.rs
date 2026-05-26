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
    #[error("invalid integer for {field}: {value}")]
    InvalidInteger { field: &'static str, value: String },
    #[error("invalid option for {command}: {option}")]
    InvalidOption {
        command: &'static str,
        option: String,
    },
    #[error("conflicting options for {command}: {detail}")]
    ConflictingOptions {
        command: &'static str,
        detail: &'static str,
    },
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
            Self::InvalidInteger { .. } => "CMD-004",
            Self::InvalidOption { .. } => "CMD-005",
            Self::ConflictingOptions { .. } => "CMD-006",
            Self::ExpectedOpeningQuote => "CMD-007",
            Self::UnterminatedQuotedString { .. } => "CMD-008",
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Self::EmptyCommand => "Empty Command",
            Self::UnknownCommand { .. } => "Unknown Command",
            Self::InvalidArity { .. } => "Invalid Command Arity",
            Self::InvalidInteger { .. } => "Invalid Integer Argument",
            Self::InvalidOption { .. } => "Invalid Command Option",
            Self::ConflictingOptions { .. } => "Conflicting Command Options",
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
