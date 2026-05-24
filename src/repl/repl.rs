use anyhow::Result;
use rustyline::Editor;
use rustyline::config::{Builder, CompletionType, EditMode};
use rustyline::error::ReadlineError;
use rustyline::history::DefaultHistory;

use crate::command::Command;
use crate::parser;
use crate::paths::VeyraPaths;
use crate::repl::helper::VeyraHelper;
use crate::store::Store;

pub struct Repl {
    editor: Editor<VeyraHelper, rustyline::history::DefaultHistory>,
    store: Store,
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

        let mut editor = Editor::<VeyraHelper, DefaultHistory>::with_config(config)?;

        editor.set_helper(Some(helper));
        editor.load_history(&paths.data_dir.join("history")).ok();

        Ok(Self {
            editor,
            store: Store::new()?,
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
                        self.editor
                            .save_history(&self.paths.data_dir.join("history"))?;
                        break;
                    }
                }
                Err(ReadlineError::Interrupted) => {
                    println!("Exiting...");
                    self.editor
                        .save_history(&self.paths.data_dir.join("history"))?;
                    break;
                }

                Err(ReadlineError::Eof) => {
                    println!("Exiting...");
                    self.editor
                        .save_history(&self.paths.data_dir.join("history"))?;
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
            Command::Get { key } => match self.store.get(&key) {
                Ok(value) => println!("{value}"),

                Err(err) => println!("{err}"),
            },

            Command::Set { key, value } => match self.store.set(key, value) {
                Ok(_) => println!("OK"),

                Err(err) => println!("{err}"),
            },

            Command::Delete { keys } => {
                if keys.len() > 1 {
                    match self.store.delete_many(&keys) {
                        Ok(_) => println!("OK"),

                        Err(err) => println!("{err}"),
                    }
                } else {
                    match self.store.delete(&keys[0]) {
                        Ok(_) => println!("OK"),

                        Err(err) => println!("{err}"),
                    }
                }
            }

            Command::Exists { key } => match self.store.exists(&key) {
                Ok(value) => println!("{}", value),

                Err(err) => println!("{err}"),
            },

            Command::List => match self.store.list() {
                Ok(values) => {
                    for (key, value) in values {
                        println!("{key} = {value}");
                    }
                }

                Err(err) => println!("{err}"),
            },

            Command::Clear => match self.store.clear() {
                Ok(_) => println!("OK"),

                Err(err) => println!("{err}"),
            },

            Command::Count => match self.store.count() {
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
        }

        false
    }
}
