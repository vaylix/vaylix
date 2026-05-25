use anyhow::Result;
use rustyline::Editor;
use rustyline::config::{Builder, CompletionType, EditMode};
use rustyline::error::ReadlineError;
use rustyline::history::DefaultHistory;

use crate::command::Command;
use crate::engine::{Engine, StorageEngine};
use crate::parser;
use crate::paths::VeyraPaths;
use crate::repl::helper::VeyraHelper;

pub struct Repl {
    editor: Editor<VeyraHelper, rustyline::history::DefaultHistory>,
    engine: Engine,
    paths: VeyraPaths,
}

const PROMPT: &str = "veyra> ";

impl Repl {
    pub fn new() -> Result<Self> {
        let config = Builder::new()
            .completion_type(CompletionType::List)
            .edit_mode(EditMode::Emacs)
            .auto_add_history(true)
            .build();

        let helper = VeyraHelper::new();
        let paths = VeyraPaths::new()?;
        let engine = Engine::new()?;

        let mut editor = Editor::<VeyraHelper, DefaultHistory>::with_config(config)?;

        editor.set_helper(Some(helper));
        editor.load_history(&paths.history_path).ok();

        Ok(Self {
            editor,
            engine,
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

                    let command = match parser::parse(line) {
                        Ok(command) => command,
                        Err(err) => {
                            println!("{err}");
                            continue;
                        }
                    };

                    if self.execute(command) {
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

    fn execute(&mut self, command: Command) -> bool {
        match command {
            Command::Get { key } => match self.engine.get(&key) {
                Ok(Some(value)) => println!("{}", value),

                Ok(None) => println!("Could not find value"),

                Err(err) => println!("{err}"),
            },

            Command::Set { key, value } => match self.engine.set(key, value) {
                Ok(_) => println!("OK"),

                Err(err) => println!("{err}"),
            },

            Command::Delete { keys } => {
                if keys.len() > 1 {
                    match self.engine.delete_many(&keys) {
                        Ok(_) => println!("OK"),

                        Err(err) => println!("{err}"),
                    }
                } else {
                    match self.engine.delete(&keys[0]) {
                        Ok(_) => println!("OK"),

                        Err(err) => println!("{err}"),
                    }
                }
            }

            Command::Exists { key } => match self.engine.exists(&key) {
                Ok(value) => println!("{}", value),

                Err(err) => println!("{err}"),
            },

            Command::List => match self.engine.list() {
                Ok(values) => {
                    for (key, value) in values {
                        println!("{key} = {value}");
                    }
                }

                Err(err) => println!("{err}"),
            },

            Command::Clear => match self.engine.clear() {
                Ok(_) => println!("OK"),

                Err(err) => println!("{err}"),
            },

            Command::Count => match self.engine.count() {
                Ok(value) => println!("{}", value),

                Err(err) => println!("{err}"),
            },

            Command::Help => {
                println!("Commands:");

                println!("  set <key> <value>");

                println!("  get <key>");

                println!("  delete <key> [key...]");

                println!("  exists <key>");

                println!("  list");

                println!("  clear");

                println!("  count");

                println!("  exit");
            }

            Command::Exit => {
                println!("Exiting...");

                return true;
            }

            Command::Snapshot => match self.engine.snapshot() {
                Ok(_) => println!("OK"),

                Err(err) => println!("{err}"),
            },
        }

        false
    }
}
