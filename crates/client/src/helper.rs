use engine::{COMMANDS, command_info};
use rustyline::completion::{Completer, Pair};
use rustyline::error::ReadlineError;
use rustyline::highlight::Highlighter;
use rustyline::hint::Hinter;
use rustyline::validate::{ValidationContext, ValidationResult, Validator};
use rustyline::{Context, Helper};
use std::borrow::Cow;

#[derive(Helper)]
pub struct ClientHelper {
    commands: Vec<String>,
}

impl ClientHelper {
    pub fn new() -> Self {
        Self {
            commands: COMMANDS
                .iter()
                .map(|command| command.name.to_string())
                .collect(),
        }
    }
}

impl Completer for ClientHelper {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &Context<'_>,
    ) -> Result<(usize, Vec<Pair>), ReadlineError> {
        let before_cursor = &line[..pos];
        let words: Vec<&str> = before_cursor.split_whitespace().collect();

        // Complete only the first word for now: commands.
        if words.len() <= 1 && !before_cursor.ends_with(' ') {
            let prefix = words.first().copied().unwrap_or("");

            let matches = self
                .commands
                .iter()
                .filter(|cmd| cmd.starts_with(prefix))
                .map(|cmd| Pair {
                    display: cmd.clone(),
                    replacement: cmd.clone(),
                })
                .collect();

            return Ok((0, matches));
        }

        Ok((pos, Vec::new()))
    }
}

impl Hinter for ClientHelper {
    type Hint = String;

    fn hint(&self, line: &str, _pos: usize, _ctx: &Context<'_>) -> Option<String> {
        let trimmed = line.trim();

        if trimmed.is_empty() || trimmed.contains(char::is_whitespace) {
            return None;
        }

        let command = command_info(trimmed)?;
        let hint = command.usage.strip_prefix(command.name).unwrap_or("");

        if hint.is_empty() {
            None
        } else {
            Some(hint.to_string())
        }
    }
}

impl Highlighter for ClientHelper {
    fn highlight<'l>(&self, line: &'l str, _pos: usize) -> Cow<'l, str> {
        let mut parts = line.splitn(2, ' ');
        let command = parts.next().unwrap_or("");
        let rest = parts.next();

        if command_info(command).is_some() {
            match rest {
                Some(rest) => Cow::Owned(format!("\x1b[1;36m{command}\x1b[0m {rest}")),
                None => Cow::Owned(format!("\x1b[1;36m{command}\x1b[0m")),
            }
        } else {
            Cow::Borrowed(line)
        }
    }

    fn highlight_hint<'h>(&self, hint: &'h str) -> Cow<'h, str> {
        Cow::Owned(format!("\x1b[2m{hint}\x1b[0m"))
    }
}

impl Validator for ClientHelper {
    fn validate(&self, ctx: &mut ValidationContext) -> rustyline::Result<ValidationResult> {
        let input = ctx.input().trim();

        if input.is_empty() {
            return Ok(ValidationResult::Valid(None));
        }

        let command = input.split_whitespace().next().unwrap_or("");

        if command_info(command).is_some() {
            Ok(ValidationResult::Valid(None))
        } else {
            Ok(ValidationResult::Invalid(Some(format!(
                "unknown command: {command}"
            ))))
        }
    }
}
