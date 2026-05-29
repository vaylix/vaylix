use command::COMMANDS;
use rustyline::completion::{Completer, Pair};
use rustyline::error::ReadlineError;
use rustyline::highlight::Highlighter;
use rustyline::hint::Hinter;
use rustyline::validate::{ValidationContext, ValidationResult, Validator};
use rustyline::{Context, Helper};
use std::borrow::Cow;
use std::collections::BTreeSet;

#[derive(Helper)]
pub struct ClientHelper {
    commands: Vec<&'static str>,
}

impl ClientHelper {
    pub fn new() -> Self {
        Self {
            commands: COMMANDS.iter().map(|command| command.name).collect(),
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
        let (start, candidates) = complete_command_keywords(before_cursor, &self.commands);
        let pairs = candidates
            .into_iter()
            .map(|candidate| Pair {
                display: candidate.clone(),
                replacement: candidate,
            })
            .collect();
        Ok((start, pairs))
    }
}

impl Hinter for ClientHelper {
    type Hint = String;

    fn hint(&self, line: &str, _pos: usize, _ctx: &Context<'_>) -> Option<String> {
        hint_for_input(line)
    }
}

impl Highlighter for ClientHelper {
    fn highlight<'l>(&self, line: &'l str, _pos: usize) -> Cow<'l, str> {
        if let Some(prefix) = highlightable_prefix(line) {
            let suffix = &line[prefix.len()..];
            Cow::Owned(format!("\x1b[1;36m{prefix}\x1b[0m{suffix}"))
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

        if known_root_token(command) {
            Ok(ValidationResult::Valid(None))
        } else {
            Ok(ValidationResult::Invalid(Some(format!(
                "unknown command: {command}"
            ))))
        }
    }
}

fn known_root_token(token: &str) -> bool {
    COMMANDS.iter().any(|command| {
        command
            .name
            .split_whitespace()
            .next()
            .is_some_and(|root| root.eq_ignore_ascii_case(token))
    })
}

fn complete_command_keywords(before_cursor: &str, commands: &[&str]) -> (usize, Vec<String>) {
    let ends_with_space = before_cursor
        .chars()
        .last()
        .is_some_and(char::is_whitespace);
    let words: Vec<&str> = before_cursor.split_whitespace().collect();
    let (consumed, partial) = if ends_with_space {
        (words.as_slice(), "")
    } else if let Some((last, rest)) = words.split_last() {
        (rest, *last)
    } else {
        (&[][..], "")
    };

    let mut candidates = BTreeSet::new();
    for command in commands {
        let tokens: Vec<&str> = command.split_whitespace().collect();
        if consumed.len() >= tokens.len() {
            continue;
        }
        if !consumed
            .iter()
            .zip(tokens.iter())
            .all(|(left, right)| right.eq_ignore_ascii_case(left))
        {
            continue;
        }
        let candidate = tokens[consumed.len()];
        if starts_with_ignore_ascii_case(candidate, partial) {
            candidates.insert(candidate.to_string());
        }
    }

    (
        before_cursor.len().saturating_sub(partial.len()),
        candidates.into_iter().collect(),
    )
}

fn hint_for_input(line: &str) -> Option<String> {
    let trimmed = line.trim_end();
    if trimmed.is_empty() {
        return None;
    }

    let command = COMMANDS
        .iter()
        .filter(|command| command.name.eq_ignore_ascii_case(trimmed))
        .max_by_key(|command| command.name.len())?;
    let hint = command.usage.strip_prefix(command.name).unwrap_or("");

    if hint.is_empty() {
        None
    } else {
        Some(hint.to_string())
    }
}

fn highlightable_prefix(line: &str) -> Option<&str> {
    COMMANDS
        .iter()
        .filter(|command| starts_with_full_phrase_ignore_ascii_case(line, command.name))
        .map(|command| &line[..command.name.len()])
        .max_by_key(|prefix| prefix.len())
}

fn starts_with_full_phrase_ignore_ascii_case(input: &str, phrase: &str) -> bool {
    let Some(prefix) = input.get(..phrase.len()) else {
        return false;
    };
    prefix.eq_ignore_ascii_case(phrase)
        && input
            .chars()
            .nth(phrase.len())
            .is_none_or(char::is_whitespace)
}

fn starts_with_ignore_ascii_case(value: &str, prefix: &str) -> bool {
    if prefix.is_empty() {
        return true;
    }
    let Some(start) = value.get(..prefix.len()) else {
        return false;
    };
    start.eq_ignore_ascii_case(prefix)
}

#[cfg(test)]
mod tests {
    use super::{
        complete_command_keywords, highlightable_prefix, hint_for_input, known_root_token,
    };

    #[test]
    fn completes_multiword_command_keywords() {
        let command_names: Vec<&str> = crate::helper::COMMANDS
            .iter()
            .map(|command| command.name)
            .collect();
        let (_, top_level) = complete_command_keywords("", &command_names);
        assert!(top_level.contains(&"create".to_string()));
        assert!(top_level.contains(&"grant".to_string()));

        let (_, create_choices) = complete_command_keywords("create ", &command_names);
        assert_eq!(create_choices, vec!["role".to_string(), "user".to_string()]);

        let (_, show_choices) = complete_command_keywords("show grants ", &command_names);
        assert_eq!(show_choices, vec!["for".to_string()]);

        let (_, maintenance_choices) = complete_command_keywords("maintenance ", &command_names);
        assert_eq!(
            maintenance_choices,
            vec!["off".to_string(), "on".to_string(), "status".to_string()]
        );
    }

    #[test]
    fn hints_use_real_command_phrases() {
        assert_eq!(
            hint_for_input("create user").as_deref(),
            Some(" <username> password <password>")
        );
        assert_eq!(
            hint_for_input("backup verify from").as_deref(),
            Some(" <path>")
        );
        assert_eq!(hint_for_input("show grants"), None);
    }

    #[test]
    fn highlights_full_multiword_prefixes() {
        assert_eq!(
            highlightable_prefix("create user alice"),
            Some("create user")
        );
        assert_eq!(
            highlightable_prefix("show grants for user alice"),
            Some("show grants for user")
        );
        assert_eq!(
            highlightable_prefix("maintenance status"),
            Some("maintenance status")
        );
    }

    #[test]
    fn validator_knows_real_root_tokens() {
        assert!(known_root_token("create"));
        assert!(known_root_token("grant"));
        assert!(known_root_token("show"));
        assert!(known_root_token("quit"));
        assert!(!known_root_token("unknown"));
    }
}
