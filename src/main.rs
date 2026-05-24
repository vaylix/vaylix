use anyhow::Result;
use repl::Repl;

mod command;
mod error;
mod lexer;
mod parser;
mod paths;
mod repl;
mod store;

fn main() -> Result<()> {
    let mut repl = Repl::new()?;
    repl.run()
}
