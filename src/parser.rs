use anyhow::{Result, bail};

use crate::command::Command;
use crate::lexer::{Token, tokenize};

pub fn parse(input: &str) -> Result<Command> {
    let tokens = tokenize(input)?;

    if tokens.is_empty() {
        bail!("empty command");
    }

    let command = token_text(&tokens[0]).to_ascii_lowercase();

    match command.as_str() {
        "get" => parse_get(&tokens),
        "set" => parse_set(&tokens),
        "delete" => parse_delete(&tokens),
        "exists" => parse_exists(&tokens),
        "list" => parse_no_args(&tokens, Command::List, "list"),
        "clear" => parse_no_args(&tokens, Command::Clear, "clear"),
        "count" => parse_no_args(&tokens, Command::Count, "count"),
        "help" => parse_no_args(&tokens, Command::Help, "help"),
        "exit" | "quit" => parse_no_args(&tokens, Command::Exit, "exit"),
        "snapshot" => parse_no_args(&tokens, Command::Snapshot, "snapshot"),
        unknown => bail!("unknown command: {}", unknown),
    }
}

fn parse_get(tokens: &[Token]) -> Result<Command> {
    expect_len(tokens, 2, "usage: get <key>")?;

    Ok(Command::Get {
        key: token_text(&tokens[1]).to_string(),
    })
}

fn parse_set(tokens: &[Token]) -> Result<Command> {
    expect_len(tokens, 3, "usage: set <key> <value>")?;

    Ok(Command::Set {
        key: token_text(&tokens[1]).to_string(),
        value: token_text(&tokens[2]).to_string(),
    })
}

fn parse_delete(tokens: &[Token]) -> Result<Command> {
    if tokens.len() < 2 {
        bail!("usage: delete <key> [key...]");
    }

    Ok(Command::Delete {
        keys: tokens[1..]
            .iter()
            .map(|token| token_text(token).to_string())
            .collect(),
    })
}

fn parse_exists(tokens: &[Token]) -> Result<Command> {
    expect_len(tokens, 2, "usage: exists <key>")?;

    Ok(Command::Exists {
        key: token_text(&tokens[1]).to_string(),
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
