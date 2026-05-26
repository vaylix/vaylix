use anyhow::{Result, bail};

use crate::command::Command;
use crate::lexer::{Token, tokenize};

pub struct Parser;

impl Parser {
    pub fn parse(input: &str) -> Result<Command> {
        let tokens = tokenize(input)?;

        if tokens.is_empty() {
            bail!("empty command");
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
            unknown => bail!("unknown command: {}", unknown),
        }
    }

    fn parse_get(tokens: &[Token]) -> Result<Command> {
        Self::expect_len(tokens, 2, "usage: get <key>")?;

        Ok(Command::Get {
            key: Self::token_text(&tokens[1]).to_string(),
        })
    }

    fn parse_set(tokens: &[Token]) -> Result<Command> {
        Self::expect_len(tokens, 3, "usage: set <key> <value>")?;

        Ok(Command::Set {
            key: Self::token_text(&tokens[1]).to_string(),
            value: Self::token_text(&tokens[2]).to_string(),
        })
    }

    fn parse_delete(tokens: &[Token]) -> Result<Command> {
        if tokens.len() < 2 {
            bail!("usage: delete <key> [key...]");
        }

        Ok(Command::Delete {
            keys: tokens[1..]
                .iter()
                .map(|token| Self::token_text(token).to_string())
                .collect(),
        })
    }

    fn parse_exists(tokens: &[Token]) -> Result<Command> {
        Self::expect_len(tokens, 2, "usage: exists <key>")?;

        Ok(Command::Exists {
            key: Self::token_text(&tokens[1]).to_string(),
        })
    }

    fn parse_no_args(tokens: &[Token], command: Command, usage: &str) -> Result<Command> {
        if tokens.len() != 1 {
            bail!("usage: {usage}");
        }

        Ok(command)
    }

    fn expect_len(tokens: &[Token], expected: usize, usage: &str) -> Result<()> {
        if tokens.len() != expected {
            bail!(usage.to_string());
        }

        Ok(())
    }

    fn token_text(token: &Token) -> &str {
        match token {
            Token::Word(value) | Token::QuotedString(value) => value,
        }
    }
}
