use anyhow::Result;
use rustyline::Editor;
use rustyline::config::{Builder, CompletionType, EditMode};
use rustyline::error::ReadlineError;
use rustyline::history::DefaultHistory;
use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;

use crate::helper::VeyraHelper;
use veyra_core::{Parser, VeyraPaths};

pub struct Client {
    editor: Editor<VeyraHelper, rustyline::history::DefaultHistory>,
    stream: TcpStream,
    paths: VeyraPaths,
}

const PROMPT: &str = "veyra> ";

impl Client {
    pub fn new() -> Result<Self> {
        let config = Builder::new()
            .completion_type(CompletionType::List)
            .edit_mode(EditMode::Emacs)
            .auto_add_history(true)
            .build();

        let helper = VeyraHelper::new();
        let paths = VeyraPaths::new()?;
        let stream = TcpStream::connect("127.0.0.1:6379")?;

        let mut editor = Editor::<VeyraHelper, DefaultHistory>::with_config(config)?;

        editor.set_helper(Some(helper));
        editor.load_history(&paths.history_path).ok();

        Ok(Self {
            editor,
            stream,
            paths,
        })
    }

    pub fn run(&mut self) -> Result<()> {
        loop {
            let readline = self.editor.readline(PROMPT);

            match readline {
                Ok(line) => {
                    let line = line.trim();

                    if line.is_empty() {
                        continue;
                    }

                    if let Err(err) = Parser::parse(line) {
                        println!("{err}");
                        continue;
                    }

                    if self.execute(line)? {
                        self.editor.save_history(&self.paths.history_path)?;
                        break;
                    }
                }
                Err(ReadlineError::Interrupted) => {
                    println!("Exiting...");
                    self.editor.save_history(&self.paths.history_path)?;
                    break;
                }

                Err(ReadlineError::Eof) => {
                    println!("Exiting...");
                    self.editor.save_history(&self.paths.history_path)?;
                    break;
                }

                Err(err) => {
                    println!("{err}");
                }
            }
        }

        Ok(())
    }

    fn execute(&mut self, line: &str) -> Result<bool> {
        self.stream.write_all(format!("{line}\n").as_bytes())?;

        self.stream.flush()?;

        let mut reader = BufReader::new(self.stream.try_clone()?);

        let mut response = String::new();

        reader.read_line(&mut response)?;

        let response = response.trim();

        println!("{response}");

        Ok(line.eq_ignore_ascii_case("exit"))
    }
}
