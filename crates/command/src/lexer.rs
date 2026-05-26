use crate::{CommandError, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Token {
    Word(String),
    QuotedString(String),
}

pub fn tokenize(input: &str) -> Result<Vec<Token>> {
    let mut lexer = Lexer::new(input);
    lexer.tokenize()
}

struct Lexer<'a> {
    chars: std::iter::Peekable<std::str::CharIndices<'a>>,
}

impl<'a> Lexer<'a> {
    fn new(input: &'a str) -> Self {
        Self {
            chars: input.char_indices().peekable(),
        }
    }

    fn tokenize(&mut self) -> Result<Vec<Token>> {
        let mut tokens = Vec::new();

        while let Some((_, ch)) = self.chars.peek().copied() {
            match ch {
                ' ' | '\t' | '\n' | '\r' => {
                    self.chars.next();
                }
                '"' => tokens.push(Token::QuotedString(self.read_quoted_string()?)),
                _ => tokens.push(Token::Word(self.read_word())),
            }
        }

        Ok(tokens)
    }

    fn read_word(&mut self) -> String {
        let mut word = String::new();

        while let Some((_, ch)) = self.chars.peek().copied() {
            match ch {
                ' ' | '\t' | '\n' | '\r' | '"' => break,
                _ => {
                    word.push(ch);
                    self.chars.next();
                }
            }
        }

        word
    }

    fn read_quoted_string(&mut self) -> Result<String> {
        let Some((start, '"')) = self.chars.next() else {
            return Err(CommandError::ExpectedOpeningQuote);
        };

        let mut value = String::new();
        let mut escaped = false;

        while let Some((_, ch)) = self.chars.next() {
            if escaped {
                match ch {
                    '"' => value.push('"'),
                    '\\' => value.push('\\'),
                    'n' => value.push('\n'),
                    't' => value.push('\t'),
                    other => value.push(other),
                }

                escaped = false;
                continue;
            }

            match ch {
                '\\' => escaped = true,
                '"' => return Ok(value),
                other => value.push(other),
            }
        }

        Err(CommandError::UnterminatedQuotedString { start })
    }
}

#[cfg(test)]
mod tests {
    use super::{Token, tokenize};

    #[test]
    fn tokenizes_words_and_quoted_strings() {
        assert_eq!(
            tokenize(r#"set name "John Doe""#).unwrap(),
            vec![
                Token::Word("set".to_string()),
                Token::Word("name".to_string()),
                Token::QuotedString("John Doe".to_string()),
            ]
        );
    }

    #[test]
    fn tokenizes_escaped_characters_inside_quotes() {
        assert_eq!(
            tokenize(r#"set message "line 1\nline 2\t\"ok\"\\done""#).unwrap(),
            vec![
                Token::Word("set".to_string()),
                Token::Word("message".to_string()),
                Token::QuotedString("line 1\nline 2\t\"ok\"\\done".to_string()),
            ]
        );
    }

    #[test]
    fn skips_mixed_whitespace() {
        assert_eq!(
            tokenize("  get\tname \n").unwrap(),
            vec![
                Token::Word("get".to_string()),
                Token::Word("name".to_string()),
            ]
        );
    }

    #[test]
    fn rejects_unterminated_quotes() {
        assert!(tokenize(r#"set name "oops"#).is_err());
    }
}
