use crate::command::Command;
use crate::lexer::{Token, tokenize};
use crate::{CommandError, Result};

pub struct Parser;

impl Parser {
    pub fn parse(input: &str) -> Result<Command> {
        let tokens = tokenize(input)?;

        if tokens.is_empty() {
            return Err(CommandError::EmptyCommand);
        }

        let command = Self::token_text(&tokens[0]).to_ascii_lowercase();

        match command.as_str() {
            "get" => Self::parse_get(&tokens),
            "set" => Self::parse_set(&tokens),
            "delete" => Self::parse_delete(&tokens),
            "exists" => Self::parse_exists(&tokens),
            "list" => Self::parse_no_args(&tokens, Command::List, "list"),
            "clear" => Self::parse_no_args(&tokens, Command::Clear, "clear"),
            "count" => Self::parse_no_args(&tokens, Command::Count, "count"),
            "help" => Self::parse_no_args(&tokens, Command::Help, "help"),
            "exit" | "quit" => Self::parse_no_args(&tokens, Command::Exit, "exit"),
            "snapshot" => Self::parse_no_args(&tokens, Command::Snapshot, "snapshot"),
            unknown => Err(CommandError::UnknownCommand {
                command: unknown.to_string(),
            }),
        }
    }

    fn parse_get(tokens: &[Token]) -> Result<Command> {
        Self::expect_len(tokens, 2, "get <key>")?;

        Ok(Command::Get {
            key: Self::token_text(&tokens[1]).to_string(),
        })
    }

    fn parse_set(tokens: &[Token]) -> Result<Command> {
        Self::expect_len(tokens, 3, "set <key> <value>")?;

        Ok(Command::Set {
            key: Self::token_text(&tokens[1]).to_string(),
            value: Self::token_text(&tokens[2]).to_string(),
        })
    }

    fn parse_delete(tokens: &[Token]) -> Result<Command> {
        if tokens.len() < 2 {
            return Err(CommandError::InvalidArity {
                usage: "delete <key> [key...]".to_string(),
            });
        }

        Ok(Command::Delete {
            keys: tokens[1..]
                .iter()
                .map(|token| Self::token_text(token).to_string())
                .collect(),
        })
    }

    fn parse_exists(tokens: &[Token]) -> Result<Command> {
        Self::expect_len(tokens, 2, "exists <key>")?;

        Ok(Command::Exists {
            key: Self::token_text(&tokens[1]).to_string(),
        })
    }

    fn parse_no_args(tokens: &[Token], command: Command, usage: &str) -> Result<Command> {
        if tokens.len() != 1 {
            return Err(CommandError::InvalidArity {
                usage: usage.to_string(),
            });
        }

        Ok(command)
    }

    fn expect_len(tokens: &[Token], expected: usize, usage: &str) -> Result<()> {
        if tokens.len() != expected {
            return Err(CommandError::InvalidArity {
                usage: usage.to_string(),
            });
        }

        Ok(())
    }

    fn token_text(token: &Token) -> &str {
        match token {
            Token::Word(value) | Token::QuotedString(value) => value,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Parser;
    use crate::Command;

    #[test]
    fn parses_all_supported_commands() {
        assert_eq!(
            Parser::parse("get name").unwrap(),
            Command::Get {
                key: "name".to_string()
            }
        );
        assert_eq!(
            Parser::parse("set name value").unwrap(),
            Command::Set {
                key: "name".to_string(),
                value: "value".to_string()
            }
        );
        assert_eq!(
            Parser::parse("delete one two").unwrap(),
            Command::Delete {
                keys: vec!["one".to_string(), "two".to_string()]
            }
        );
        assert_eq!(
            Parser::parse("exists name").unwrap(),
            Command::Exists {
                key: "name".to_string()
            }
        );
        assert_eq!(Parser::parse("list").unwrap(), Command::List);
        assert_eq!(Parser::parse("clear").unwrap(), Command::Clear);
        assert_eq!(Parser::parse("count").unwrap(), Command::Count);
        assert_eq!(Parser::parse("help").unwrap(), Command::Help);
        assert_eq!(Parser::parse("exit").unwrap(), Command::Exit);
        assert_eq!(Parser::parse("snapshot").unwrap(), Command::Snapshot);
    }

    #[test]
    fn parses_quoted_values() {
        assert_eq!(
            Parser::parse(r#"set name "John Doe""#).unwrap(),
            Command::Set {
                key: "name".to_string(),
                value: "John Doe".to_string()
            }
        );
    }

    #[test]
    fn parses_quit_alias() {
        assert_eq!(Parser::parse("quit").unwrap(), Command::Exit);
    }

    #[test]
    fn rejects_invalid_arity() {
        assert!(Parser::parse("get").is_err());
        assert!(Parser::parse("set name").is_err());
        assert!(Parser::parse("exists").is_err());
        assert!(Parser::parse("list extra").is_err());
    }

    #[test]
    fn rejects_unknown_commands_and_empty_input() {
        assert!(Parser::parse("").is_err());
        assert!(Parser::parse("   ").is_err());
        assert!(Parser::parse("unknown thing").is_err());
    }

    #[test]
    fn preserves_case_in_arguments() {
        assert_eq!(
            Parser::parse(r#"set UserName "Alice Smith""#).unwrap(),
            Command::Set {
                key: "UserName".to_string(),
                value: "Alice Smith".to_string()
            }
        );
    }
}
