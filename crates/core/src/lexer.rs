use anyhow::{Result, bail};

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
                '"' => {
                    tokens.push(Token::QuotedString(self.read_quoted_string()?));
                }
                _ => {
                    tokens.push(Token::Word(self.read_word()));
                }
            }
        }

        Ok(tokens)
    }

    fn read_word(&mut self) -> String {
        let mut word = String::new();

        while let Some((_, ch)) = self.chars.peek().copied() {
            match ch {
                ' ' | '\t' | '\n' | '\r' => break,
                '"' => break,
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
            bail!("expected opening quote");
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

        bail!("unterminated quoted string starting at byte {start}")
    }
}
